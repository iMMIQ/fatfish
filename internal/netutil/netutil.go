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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// RateLimiter is a pure-Go token-bucket rate limiter for outbound HTTP requests.
type RateLimiter struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	tokens   float64
	lastTick time.Time
}

// NewRateLimiter creates a token-bucket limiter allowing rps requests/sec with the given burst.
func NewRateLimiter(rps float64, burst int) *RateLimiter {
	if rps <= 0 {
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return &RateLimiter{
		rate:     rps,
		burst:    float64(burst),
		tokens:   float64(burst),
		lastTick: time.Now(),
	}
}

// Allow consumes one token immediately if available.
func (l *RateLimiter) Allow() bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(time.Now())
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// Wait blocks until a token is available or ctx is cancelled.
func (l *RateLimiter) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	for {
		l.mu.Lock()
		now := time.Now()
		l.refillLocked(now)
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		need := 1 - l.tokens
		waitDur := time.Duration(need / l.rate * float64(time.Second))
		if waitDur < time.Millisecond {
			waitDur = time.Millisecond
		}
		l.mu.Unlock()

		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *RateLimiter) refillLocked(now time.Time) {
	if elapsed := now.Sub(l.lastTick).Seconds(); elapsed > 0 {
		l.tokens += elapsed * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.lastTick = now
	}
}

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
	Timeout         time.Duration
	AllowHosts      []string // Explicitly permitted IPs, CIDRs, or hostnames
	UserAgent       string
	RateLimitPerSec float64 // Optional token-bucket requests/sec cap
	RateBurst       int     // Optional burst size (default 1 when RateLimitPerSec > 0)
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

func (a *hostAllowlist) allowsHost(host string) bool {
	if a == nil {
		return false
	}
	_, ok := a.hosts[strings.ToLower(strings.TrimSpace(host))]
	return ok
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

var (
	sharedTrMu sync.RWMutex
	sharedTrs  = make(map[string]*http.Transport)
)

func allowHostsKey(allowHosts []string) string {
	if len(allowHosts) == 0 {
		return ""
	}
	norm := make([]string, 0, len(allowHosts))
	for _, h := range allowHosts {
		if s := strings.ToLower(strings.TrimSpace(h)); s != "" {
			norm = append(norm, s)
		}
	}
	sort.Strings(norm)
	return strings.Join(norm, ",")
}

func newSafeTransport(dialTimeout time.Duration, allowHosts []string) *http.Transport {
	al := parseAllowlist(allowHosts)
	safeDialer := SafeDialer(dialTimeout, allowHosts)
	plainDialer := &net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: nil, // Never proxy untrusted SSRF-checked requests through arbitrary local proxies
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err == nil && al.allowsHost(host) {
				return plainDialer.DialContext(ctx, network, address)
			}
			return safeDialer.DialContext(ctx, network, address)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// SharedSafeTransport returns a shared SSRF-safe *http.Transport for allowHosts (10s dial timeout),
// pooling TCP/TLS connections across tools (parser, web_search, read_source, quote, weather, travel, room).
func SharedSafeTransport(allowHosts []string) *http.Transport {
	key := allowHostsKey(allowHosts)
	sharedTrMu.RLock()
	tr, ok := sharedTrs[key]
	sharedTrMu.RUnlock()
	if ok {
		return tr
	}
	sharedTrMu.Lock()
	defer sharedTrMu.Unlock()
	if tr, ok := sharedTrs[key]; ok {
		return tr
	}
	// Bound cached transport variants to prevent unbounded growth across repeated allowlist edits.
	if len(sharedTrs) >= 8 {
		for k, old := range sharedTrs {
			old.CloseIdleConnections()
			delete(sharedTrs, k)
		}
	}
	tr = newSafeTransport(10*time.Second, allowHosts)
	sharedTrs[key] = tr
	return tr
}

// NewSafeHTTPClient builds an http.Client with post-DNS SSRF blocking, hostname/IP allowlisting,
// shared TCP/TLS connection pooling, optional default User-Agent, and redirect validation.
func NewSafeHTTPClient(opts SafeClientOptions) *http.Client {
	if opts.Timeout <= 0 {
		opts.Timeout = 20 * time.Second
	}
	dialTimeout := minDuration(opts.Timeout, 10*time.Second)
	var tr *http.Transport
	if dialTimeout == 10*time.Second {
		tr = SharedSafeTransport(opts.AllowHosts)
	} else {
		tr = newSafeTransport(dialTimeout, opts.AllowHosts)
	}
	var rt http.RoundTripper = tr
	if strings.TrimSpace(opts.UserAgent) != "" {
		rt = &uaRoundTripper{base: tr, userAgent: strings.TrimSpace(opts.UserAgent)}
	}
	if opts.RateLimitPerSec > 0 {
		rt = &rateLimitRoundTripper{base: rt, limiter: NewRateLimiter(opts.RateLimitPerSec, opts.RateBurst)}
	}
	return &http.Client{
		Transport: rt,
		Timeout:   opts.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 6 {
				return errors.New("netutil: too many redirects")
			}
			return ValidateURL(req.URL.String())
		},
	}
}

type uaRoundTripper struct {
	base      http.RoundTripper
	userAgent string
}

func (u *uaRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") == "" {
		clone := req.Clone(req.Context())
		clone.Header.Set("User-Agent", u.userAgent)
		return u.base.RoundTrip(clone)
	}
	return u.base.RoundTrip(req)
}

type rateLimitRoundTripper struct {
	base    http.RoundTripper
	limiter *RateLimiter
}

func (r *rateLimitRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := r.limiter.Wait(req.Context()); err != nil {
		return nil, err
	}
	return r.base.RoundTrip(req)
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
	if _, hasUA := client.Transport.(*uaRoundTripper); !hasUA {
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	}
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
