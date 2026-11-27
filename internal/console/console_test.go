package console

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/policy"
)

func TestConsoleTokenAuthAndConfigRejection(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	initialYAML := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  rules:
    reply_gate: "mentioned"
`
	if err := os.WriteFile(cfgPath, []byte(initialYAML), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgMgr, err := config.NewManager(cfgPath, policy.ValidateConfig)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	const testTok = "unit-test-console-tok"
	srv, err := NewServer(Dependencies{
		Listen: "127.0.0.1:0",
		Token:  testTok,
		Config: cfgMgr,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Unauthenticated API request must return 401.
	req := httptest.NewRequest(http.MethodPost, "/api/config/save", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", rec.Code)
	}

	// Authenticated dashboard request with Bearer token must return 200.
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer "+testTok)
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK || !strings.Contains(rec2.Body.String(), "fatfish") {
		t.Fatalf("expected 200 dashboard with valid token, got code=%d", rec2.Code)
	}

	// Submitting invalid expr rule via /api/config/save must return 400 and preserve live config.
	badYAML := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  rules:
    reply_gate: "nonexistent_var == true"
`
	form := url.Values{"yaml": {badYAML}}
	req3 := httptest.NewRequest(http.MethodPost, "/api/config/save", strings.NewReader(form.Encode()))
	req3.Header.Set("Authorization", "Bearer "+testTok)
	req3.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid expr rule, got %d (%s)", rec3.Code, rec3.Body.String())
	}

	// Dry-run validate endpoint must accept valid YAML without mutating live config.
	req4 := httptest.NewRequest(http.MethodPost, "/api/config/validate", strings.NewReader(url.Values{"yaml": {initialYAML}}.Encode()))
	req4.Header.Set("Authorization", "Bearer "+testTok)
	req4.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec4 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("expected 200 on valid dry-run, got %d (%s)", rec4.Code, rec4.Body.String())
	}
}

func TestLogBroadcasterWithAttrsConcurrentSafe(t *testing.T) {
	lb := NewLogBroadcaster(io.Discard, slog.LevelInfo, 50)
	baseLogger := slog.New(lb)
	childLogger := baseLogger.With("component", "test")

	ch, unsub := lb.Subscribe()
	defer unsub()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				childLogger.Info("hello", "g", id, "n", j)
			}
		}(i)
	}
	wg.Wait()
	if len(lb.Recent()) == 0 {
		t.Fatal("expected buffered logs from derived logger")
	}
	select {
	case <-ch:
	default:
		t.Fatal("expected subscriber to receive log from derived logger")
	}
}
