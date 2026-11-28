package console

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConsolePprofRequiresAuth(t *testing.T) {
	const testToken = "test-console-token"
	srv, err := NewServer(Dependencies{
		Listen: "127.0.0.1:0",
		Token:  testToken,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Unauthenticated request must be rejected (redirect or 401).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("expected pprof without auth to be rejected, got 200")
	}

	// Authenticated request succeeds.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req2.Header.Set("Authorization", "Bearer "+testToken)
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected pprof with Bearer token to return 200, got %d", rec2.Code)
	}
}
