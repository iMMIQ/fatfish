package netutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

var (
	// ErrBlockedIP is returned when a dial target resolves to a private/loopback/link-local IP.
	ErrBlockedIP = errors.New("netutil: destination IP is blocked by SSRF policy")
	// ErrInvalidScheme is returned when a URL does not use http or https.
	ErrInvalidScheme = errors.New("netutil: only http and https schemes are allowed")
	// ErrResponseTooLarge is returned when a body exceeds the configured byte limit.
	ErrResponseTooLarge = errors.New("netutil: response body exceeds maximum size")
)

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

// IsBlockedAddr returns true if the IP falls within a private, loopback, link-local, or reserved range.
func IsBlockedAddr(addr netip.Addr) bool {
	if !addr.IsValid() {
		return true
	}
	unmapped := addr.Unmap()
	for _, p := range blockedPrefixes {
		if p.Contains(unmapped) {
			return true
		}
	}
	return false
}

// SafeClientOptions configures an SSRF-protected HTTP client.
type SafeClientOptions struct {
	Timeout    time.Duration
	AllowHosts []string // Explicitly permitted IPs or CIDRs (e.g. local inference host)
	UserAgent  string
}

type hostAllowlist struct {
	addrs    map[netip.Addr]struct{}
	prefixes []netip.Prefix
	hosts    map[string]struct{}
}

func parseAllowlist(entries []string) *hostAllowlist {
	al := &hostAllowlist{
		addrs: make(map[netip.Addr]struct{}),
		hosts: make(map[string]struct{}),
	}
	for _, raw := range entries {
		s := strings.TrimSpace(strings.ToLower(raw))
		if s == "" {
			continue
		}
		if p, err := netip.ParsePrefix(s); err == nil {
			al.prefixes = append(al.prefixes, p)
			continue
		}
		if a, err := netip.ParseAddr(s); err == nil {
			al.addrs[a.Unmap()] = struct{}{}
			continue
		}
		al.hosts[s] = struct{}{}
	}
	return al
}

func (a *hostAllowlist) allowsAddr(addr netip.Addr) bool {
	if a == nil {
		return false
	}
	u := addr.Unmap()
	if _, ok := a.addrs[u]; ok {
		return true
	}
	for _, p := range a.prefixes {
		if p.Contains(u) {
			return true
		}
	}
	return false
}

// SafeDialer returns a net.Dialer whose Control hook validates the resolved IP address
// immediately before connect(), neutralizing DNS rebinding and IP-encoding bypasses.
func SafeDialer(timeout time.Duration, allowHosts []string) *net.Dialer {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	al := parseAllowlist(allowHosts)
	return &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("%w: invalid address %q", ErrBlockedIP, address)
			}
			addr, err := netip.ParseAddr(host)
			if err != nil {
				return fmt.Errorf("%w: unparseable IP %q", ErrBlockedIP, host)
			}
			if IsBlockedAddr(addr) && !al.allowsAddr(addr) {
				return fmt.Errorf("%w: %s", ErrBlockedIP, addr.String())
			}
			return nil
		},
	}
}

// NewSafeHTTPClient builds an http.Client with post-DNS SSRF blocking and redirect validation.
func NewSafeHTTPClient(opts SafeClientOptions) *http.Client {
	if opts.Timeout <= 0 {
		opts.Timeout = 20 * time.Second
	}
	dialer := SafeDialer(minDuration(opts.Timeout, 10*time.Second), opts.AllowHosts)
	tr := &http.Transport{
		Proxy:                 nil, // Never proxy untrusted SSRF-checked requests through arbitrary local proxies
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   opts.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 6 {
				return errors.New("netutil: too many redirects")
			}
			return ValidateURL(req.URL.String())
		},
	}
}

// ValidateURL performs pre-flight scheme validation.
func ValidateURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("netutil: invalid url: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("%w: %q", ErrInvalidScheme, u.Scheme)
	}
	if u.Hostname() == "" {
		return errors.New("netutil: missing hostname")
	}
	return nil
}

// FetchLimited fetches a URL using client, enforcing scheme validation and maxBytes body cap.
func FetchLimited(ctx context.Context, client *http.Client, rawURL string, maxBytes int64, headers map[string]string) ([]byte, string, int, error) {
	if err := ValidateURL(rawURL); err != nil {
		return nil, "", 0, err
	}
	if maxBytes <= 0 {
		maxBytes = 10 << 20 // 10 MiB
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer resp.Body.Close()

	limited := io.LimitReader(resp.Body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, resp.Header.Get("Content-Type"), resp.StatusCode, err
	}
	if int64(len(data)) > maxBytes {
		return nil, resp.Header.Get("Content-Type"), resp.StatusCode, fmt.Errorf("%w: >%d bytes", ErrResponseTooLarge, maxBytes)
	}
	return data, resp.Header.Get("Content-Type"), resp.StatusCode, nil
}

// WrapExternalData wraps untrusted external content with framing boundaries to mitigate prompt injection.
func WrapExternalData(source, content string) string {
	src := strings.ReplaceAll(source, `"`, "'")
	return fmt.Sprintf("<external_data source=%q>\n%s\n</external_data>", src, strings.TrimSpace(content))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
