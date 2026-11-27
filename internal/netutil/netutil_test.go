package netutil

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestIsBlockedAddr(t *testing.T) {
	blocked := []string{
		"127.0.0.1",
		"10.0.0.1",
		"172.16.5.4",
		"169.254.169.254",
		"100.64.0.1",
		"::1",
		"fc00::1",
		"fe80::1",
		"::ffff:127.0.0.1",
	}
	for _, s := range blocked {
		addr := netip.MustParseAddr(s)
		if !IsBlockedAddr(addr) {
			t.Errorf("expected %s to be blocked", s)
		}
	}
	if rfc1918 := netip.AddrFrom4([4]byte{192, 168, 0, 1}); !IsBlockedAddr(rfc1918) {
		t.Errorf("expected %s to be blocked", rfc1918)
	}

	allowed := []string{
		"1.1.1.1",
		"8.8.8.8",
		"93.184.216.34",
		"2606:2800:220:1:248:1893:25c8:1946",
	}
	for _, s := range allowed {
		addr := netip.MustParseAddr(s)
		if IsBlockedAddr(addr) {
			t.Errorf("expected %s to be allowed", s)
		}
	}
}

func TestSSRFBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("secret-internal-data"))
	}))
	defer srv.Close()

	// Default safe client must block 127.0.0.1 even via post-DNS Dialer.Control.
	client := NewSafeHTTPClient(SafeClientOptions{Timeout: 3 * time.Second})
	_, _, _, err := FetchLimited(context.Background(), client, srv.URL, 1024, nil)
	if err == nil {
		t.Fatalf("expected SSRF error when requesting %s, got nil", srv.URL)
	}
	if !errors.Is(err, ErrBlockedIP) {
		t.Fatalf("expected ErrBlockedIP, got %v", err)
	}

	// Explicitly allowlisting 127.0.0.1 permits configured local endpoints.
	allowClient := NewSafeHTTPClient(SafeClientOptions{
		Timeout:    3 * time.Second,
		AllowHosts: []string{"127.0.0.1"},
	})
	body, _, code, err := FetchLimited(context.Background(), allowClient, srv.URL, 1024, nil)
	if err != nil {
		t.Fatalf("expected allowlisted loopback to succeed, got %v", err)
	}
	if code != http.StatusOK || string(body) != "secret-internal-data" {
		t.Fatalf("unexpected response: code=%d body=%q", code, string(body))
	}

	// Non-http schemes must be rejected.
	if err := ValidateURL("file:///etc/passwd"); !errors.Is(err, ErrInvalidScheme) {
		t.Fatalf("expected ErrInvalidScheme for file://, got %v", err)
	}
}
