package parser

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/netutil"
)

var (
	// ErrUnsupportedContent indicates the URL points to non-textual binary/media content skipped in fast mode.
	ErrUnsupportedContent = errors.New("parser: unsupported content type")
	// ErrBlockedBySite indicates the target site returned an anti-bot, captcha, or login challenge wall.
	ErrBlockedBySite = errors.New("parser: blocked by site challenge or captcha")
	// ErrEmptyContent indicates no useful title or textual content could be extracted.
	ErrEmptyContent = errors.New("parser: empty extracted content")
)

// Options configures the shared SSRF-safe link parser service.
type Options struct {
	AllowHosts      []string
	Timeout         time.Duration
	CacheTTL        time.Duration
	MaxCacheEntries int
	TwitterAPIBase  string
	GitHubToken     string
	BilibiliCookie  string
	IgnoreDomains   []string
}

// ParseOptions controls per-request parsing depth.
type ParseOptions struct {
	// Deep enables heavier extraction (e.g. arXiv PDF pages, full README, deeper HTML cap) for read_source.
	Deep bool
}

// ExtractedLink is a candidate URL discovered in an inbound message.
type ExtractedLink struct {
	URL      string
	FromCard bool
}

// Result is the structured plain-text representation of a parsed web link.
type Result struct {
	Platform     string    `json:"platform"`
	OriginalURL  string    `json:"original_url"`
	CanonicalURL string    `json:"canonical_url"`
	Title        string    `json:"title,omitempty"`
	Author       string    `json:"author,omitempty"`
	PublishedAt  string    `json:"published_at,omitempty"`
	Stats        string    `json:"stats,omitempty"`
	Summary      string    `json:"summary,omitempty"`
	Extra        string    `json:"extra,omitempty"`
	RawText      string    `json:"raw_text,omitempty"`
	FetchedAt    time.Time `json:"fetched_at"`
}

// CompactForContext formats a bounded plain-text summary suitable for L0 persistence, FTS5, and LLM turn context.
func (r *Result) CompactForContext(maxRunes int) string {
	if r == nil {
		return ""
	}
	if maxRunes <= 0 {
		maxRunes = 450
	}
	var head []string
	if r.Title != "" {
		head = append(head, r.Title)
	}
	if r.Author != "" {
		head = append(head, "作者: "+r.Author)
	}
	if r.Stats != "" {
		head = append(head, r.Stats)
	}
	if r.PublishedAt != "" {
		head = append(head, r.PublishedAt)
	}
	var b strings.Builder
	if r.Platform != "" && r.Platform != "generic" {
		fmt.Fprintf(&b, "[%s] ", r.Platform)
	}
	b.WriteString(strings.Join(head, " | "))
	if s := strings.TrimSpace(r.Summary); s != "" {
		if b.Len() > 0 {
			b.WriteString("\n摘要: ")
		}
		b.WriteString(s)
	}
	return clipRunesCompact(strings.TrimSpace(b.String()), maxRunes)
}

// FormatForGroupText renders a clean, concise, risk-safe plain-text link summary for proactive group output.
// It intentionally avoids echoing raw URLs to minimize QQ outbound link risk control.
func (r *Result) FormatForGroupText() string {
	if r == nil {
		return ""
	}
	tag := platformLabel(r.Platform)
	var lines []string

	if r.Platform == "twitter" {
		head := tag
		if r.Author != "" {
			head += " " + r.Author
		} else if r.Title != "" {
			head += " " + r.Title
		}
		lines = append(lines, head)
		if r.Stats != "" {
			lines = append(lines, r.Stats)
		}
		if s := strings.TrimSpace(r.Summary); s != "" {
			lines = append(lines, clipRunesCompact(s, 220))
		}
		return strings.TrimSpace(strings.Join(lines, "\n"))
	}

	title := strings.TrimSpace(r.Title)
	if title != "" {
		lines = append(lines, tag+" "+clipRunesCompact(title, 90))
	} else {
		lines = append(lines, tag)
	}

	var meta []string
	if r.Author != "" {
		if r.Platform == "bilibili" {
			meta = append(meta, "UP: "+r.Author)
		} else if r.Platform == "weixin" {
			meta = append(meta, "公众号: "+r.Author)
		} else {
			meta = append(meta, r.Author)
		}
	}
	if r.Stats != "" {
		meta = append(meta, r.Stats)
	}
	if r.PublishedAt != "" && r.Platform != "github" {
		meta = append(meta, r.PublishedAt)
	}
	if len(meta) > 0 {
		lines = append(lines, strings.Join(meta, " | "))
	}

	if s := strings.TrimSpace(r.Summary); s != "" {
		lines = append(lines, clipRunesCompact(s, 200))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func platformLabel(p string) string {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "bilibili":
		return "【B站】"
	case "github":
		return "【GitHub】"
	case "arxiv":
		return "【arXiv】"
	case "huggingface":
		return "【HuggingFace】"
	case "twitter":
		return "【X】"
	case "weixin":
		return "【公众号】"
	default:
		return "【网页】"
	}
}

// FormatForReadSource returns the detailed text view used by the read_source agent tool.
func (r *Result) FormatForReadSource() string {
	if r == nil {
		return ""
	}
	if strings.TrimSpace(r.RawText) != "" {
		return strings.TrimSpace(r.RawText)
	}
	var b strings.Builder
	if r.Platform != "" {
		fmt.Fprintf(&b, "Platform: %s\n", r.Platform)
	}
	if r.CanonicalURL != "" {
		fmt.Fprintf(&b, "URL: %s\n", r.CanonicalURL)
	}
	if r.Title != "" {
		fmt.Fprintf(&b, "Title: %s\n", r.Title)
	}
	if r.Author != "" {
		fmt.Fprintf(&b, "Author: %s\n", r.Author)
	}
	if r.PublishedAt != "" {
		fmt.Fprintf(&b, "Published: %s\n", r.PublishedAt)
	}
	if r.Stats != "" {
		fmt.Fprintf(&b, "Stats: %s\n", r.Stats)
	}
	if r.Summary != "" {
		fmt.Fprintf(&b, "Summary:\n%s\n", r.Summary)
	}
	if r.Extra != "" {
		fmt.Fprintf(&b, "\n%s\n", r.Extra)
	}
	return strings.TrimSpace(b.String())
}

type cacheEntry struct {
	res       *Result
	deep      bool
	expiresAt time.Time
}

// Service performs SSRF-safe URL extraction, short-link expansion, site-specific extraction, and TTL caching.
type Service struct {
	mu          sync.RWMutex
	opts        Options
	fastClient  *http.Client
	deepClient  *http.Client
	ignoreHosts map[string]struct{}

	// Optional API base overrides (used by tests; production uses standard endpoints).
	bilibiliAPIBase     string
	githubAPIBase       string
	arxivAPIBase        string
	arxivPDFBase        string
	hfMirrorBase        string
	hfOriginBase        string
	twitterAPIBase      string
	twitterFallbackBase string

	cacheMu sync.RWMutex
	cache   map[string]cacheEntry
}

// NewService creates a new Link Parser service.
func NewService(opts Options) *Service {
	s := &Service{
		bilibiliAPIBase:     "https://api.bilibili.com",
		githubAPIBase:       "https://api.github.com",
		arxivAPIBase:        "https://export.arxiv.org",
		arxivPDFBase:        "https://arxiv.org",
		hfMirrorBase:        "https://hf-mirror.com",
		hfOriginBase:        "https://huggingface.co",
		twitterAPIBase:      "https://api.fxtwitter.com",
		twitterFallbackBase: "https://api.vxtwitter.com",
		cache:               make(map[string]cacheEntry),
	}
	s.UpdateOptions(opts)
	return s
}

// UpdateOptions hot-reloads HTTP client allowlists, tokens, timeouts, and ignored domains.
func (s *Service) UpdateOptions(opts Options) {
	if opts.Timeout <= 0 {
		opts.Timeout = 12 * time.Second
	}
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = 20 * time.Minute
	}
	if opts.MaxCacheEntries <= 0 {
		opts.MaxCacheEntries = 256
	}
	deepTimeout := opts.Timeout * 2
	if deepTimeout < 25*time.Second {
		deepTimeout = 25 * time.Second
	}

	fastCli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
		Timeout:         opts.Timeout,
		AllowHosts:      opts.AllowHosts,
		RateLimitPerSec: 6,
		RateBurst:       12,
	})
	deepCli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
		Timeout:    deepTimeout,
		AllowHosts: opts.AllowHosts,
	})
	ign := make(map[string]struct{}, len(opts.IgnoreDomains))
	for _, d := range opts.IgnoreDomains {
		d = strings.ToLower(strings.TrimSpace(d))
		if d != "" {
			ign[d] = struct{}{}
		}
	}

	s.mu.Lock()
	s.opts = opts
	s.fastClient = fastCli
	s.deepClient = deepCli
	s.ignoreHosts = ign
	if tb := strings.TrimRight(strings.TrimSpace(opts.TwitterAPIBase), "/"); tb != "" {
		s.twitterAPIBase = tb
	} else {
		s.twitterAPIBase = "https://api.fxtwitter.com"
	}
	s.mu.Unlock()
}

func (s *Service) snapshotConfig() (Options, *http.Client, *http.Client, map[string]struct{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.opts, s.fastClient, s.deepClient, s.ignoreHosts
}

// ExtractMessageLinks extracts candidate parsable URLs from msg using the service's current ignore list.
func (s *Service) ExtractMessageLinks(msg *model.Message, maxURLs int) []ExtractedLink {
	opts, _, _, _ := s.snapshotConfig()
	return ExtractURLs(msg, maxURLs, opts.IgnoreDomains, opts.AllowHosts)
}

// ParseMessageLinks parses up to maxURLs candidate links from msg concurrently while preserving order.
func (s *Service) ParseMessageLinks(ctx context.Context, msg *model.Message, maxURLs int) []*Result {
	links := s.ExtractMessageLinks(msg, maxURLs)
	if len(links) == 0 {
		return nil
	}
	results := make([]*Result, len(links))
	var wg sync.WaitGroup
	for i, lk := range links {
		idx, rawURL := i, lk.URL
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Parse(ctx, rawURL, ParseOptions{Deep: false})
			if err == nil && res != nil {
				results[idx] = res
			}
		}()
	}
	wg.Wait()

	out := make([]*Result, 0, len(results))
	seenCanon := make(map[string]struct{}, len(results))
	for _, r := range results {
		if r == nil {
			continue
		}
		key := r.CanonicalURL
		if key == "" {
			key = r.OriginalURL
		}
		if _, dup := seenCanon[key]; dup {
			continue
		}
		seenCanon[key] = struct{}{}
		out = append(out, r)
	}
	return out
}

// Parse resolves, extracts, and caches structured information for rawURL.
func (s *Service) Parse(ctx context.Context, rawURL string, popts ParseOptions) (*Result, error) {
	rawURL = strings.TrimSpace(rawURL)
	if err := netutil.ValidateURL(rawURL); err != nil {
		return nil, err
	}
	opts, fastCli, deepCli, ign := s.snapshotConfig()
	cli := fastCli
	if popts.Deep {
		cli = deepCli
	}

	normOrig := CanonicalizeURL(rawURL)
	if cached, ok := s.cacheGet(normOrig, popts.Deep); ok {
		return cached, nil
	}

	targetURL := rawURL
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := strings.ToLower(u.Hostname())
	if !popts.Deep && isIgnoredHost(host, ign) {
		return nil, ErrUnsupportedContent
	}

	// Expand known shorteners safely before matching site extractors.
	if isShortenerHost(host) {
		if expanded, eerr := s.expandShortURL(ctx, cli, rawURL); eerr == nil && expanded != "" {
			targetURL = expanded
			if eu, perr := url.Parse(expanded); perr == nil {
				u = eu
				host = strings.ToLower(u.Hostname())
			}
		}
	}

	canon := CanonicalizeURL(targetURL)
	if canon != normOrig {
		if cached, ok := s.cacheGet(canon, popts.Deep); ok {
			s.cachePut(normOrig, cached, popts.Deep, opts.CacheTTL, opts.MaxCacheEntries)
			return cached, nil
		}
	}

	var res *Result
	switch {
	case matchBilibili(host, u.Path):
		res, err = s.extractBilibili(ctx, cli, u, targetURL, popts.Deep, opts.BilibiliCookie)
	case matchGitHub(host, u.Path):
		res, err = s.extractGitHub(ctx, cli, u, popts.Deep, opts.GitHubToken)
	case matchArxiv(host, u.Path):
		res, err = s.extractArxiv(ctx, cli, targetURL, popts.Deep)
	case matchHuggingFace(host, u.Path):
		res, err = s.extractHuggingFace(ctx, cli, u, popts.Deep)
	case matchTwitter(host, u.Path):
		res, err = s.extractTwitter(ctx, cli, u)
	case matchWeixin(host):
		res, err = s.extractWeixin(ctx, cli, targetURL, popts.Deep)
	default:
		res, err = s.extractGenericHTML(ctx, cli, targetURL, popts.Deep)
	}
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, ErrEmptyContent
	}
	res.OriginalURL = rawURL
	if res.CanonicalURL == "" {
		res.CanonicalURL = canon
	}
	res.FetchedAt = time.Now()

	s.cachePut(normOrig, res, popts.Deep, opts.CacheTTL, opts.MaxCacheEntries)
	if res.CanonicalURL != "" && res.CanonicalURL != normOrig {
		s.cachePut(res.CanonicalURL, res, popts.Deep, opts.CacheTTL, opts.MaxCacheEntries)
	}
	return res, nil
}

func (s *Service) cacheGet(key string, needDeep bool) (*Result, bool) {
	now := time.Now()
	s.cacheMu.RLock()
	ent, ok := s.cache[key]
	s.cacheMu.RUnlock()
	if !ok || now.After(ent.expiresAt) || ent.res == nil {
		return nil, false
	}
	if needDeep && !ent.deep {
		return nil, false
	}
	cp := *ent.res
	return &cp, true
}

func (s *Service) cachePut(key string, res *Result, deep bool, ttl time.Duration, maxEntries int) {
	if key == "" || res == nil || ttl <= 0 {
		return
	}
	cp := *res
	now := time.Now()
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if prev, ok := s.cache[key]; ok && prev.deep && !deep && now.Before(prev.expiresAt) {
		return // Never downgrade a cached deep result with a shallow one.
	}
	if len(s.cache) >= maxEntries {
		// Evict expired entries first; if still full, drop oldest entry.
		var oldestKey string
		var oldestTime time.Time
		for k, v := range s.cache {
			if now.After(v.expiresAt) {
				delete(s.cache, k)
				continue
			}
			if oldestKey == "" || v.expiresAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.expiresAt
			}
		}
		if len(s.cache) >= maxEntries && oldestKey != "" {
			delete(s.cache, oldestKey)
		}
	}
	s.cache[key] = cacheEntry{
		res:       &cp,
		deep:      deep,
		expiresAt: now.Add(ttl),
	}
}

func (s *Service) expandShortURL(ctx context.Context, cli *http.Client, rawURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", defaultBrowserUA)
	resp, err := cli.Do(req)
	if err != nil {
		return "", err
	}
	finalURL := resp.Request.URL.String()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if err := netutil.ValidateURL(finalURL); err != nil {
		return "", err
	}
	return finalURL, nil
}

// --- URL extraction, filtering & canonicalization ---

var (
	rawURLRe  = regexp.MustCompile(`https?://[^\s<>"'\x00-\x1f\p{Han}\p{Hiragana}\p{Katakana}\p{Hangul}]+`)
	bareBVRe  = regexp.MustCompile(`(?:^|[^0-9A-Za-z/])(BV1[0-9A-Za-z]{9})(?:$|[^0-9A-Za-z])`)
	bvidRe    = regexp.MustCompile(`BV[0-9A-Za-z]{10}`)
	avidRe    = regexp.MustCompile(`(?i)/(?:video/)?av(\d+)`)
	arxivIDRe = regexp.MustCompile(`(\d{4}\.\d{4,5})(v\d+)?`)
)

var builtInIgnoreHosts = map[string]struct{}{
	"gchat.qpic.cn":            {},
	"multimedia.nt.qq.com.cn":  {},
	"c2cpicdw.qpic.cn":         {},
	"p.qpic.cn":                {},
	"qlogo.cn":                 {},
	"q1.qlogo.cn":              {},
	"q2.qlogo.cn":              {},
	"q4.qlogo.cn":              {},
	"thirdqq.qlogo.cn":         {},
	"i0.hdslb.com":             {},
	"i1.hdslb.com":             {},
	"i2.hdslb.com":             {},
	"mmbiz.qpic.cn":            {},
	"wx.qlogo.cn":              {},
	"pbs.twimg.com":            {},
	"avatars.githubusercontent.com": {},
}

var staticFileExts = map[string]struct{}{
	".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".webp": {}, ".bmp": {}, ".ico": {}, ".svg": {}, ".avif": {},
	".mp4": {}, ".mov": {}, ".webm": {}, ".mkv": {}, ".avi": {}, ".flv": {}, ".m3u8": {},
	".mp3": {}, ".wav": {}, ".ogg": {}, ".flac": {}, ".amr": {}, ".silk": {}, ".m4a": {},
	".zip": {}, ".rar": {}, ".7z": {}, ".tar": {}, ".gz": {}, ".xz": {}, ".bz2": {},
	".apk": {}, ".exe": {}, ".msi": {}, ".dmg": {}, ".deb": {}, ".rpm": {}, ".iso": {},
	".css": {}, ".js": {}, ".woff": {}, ".woff2": {}, ".ttf": {},
}

var shortenerHosts = map[string]struct{}{
	"b23.tv":       {},
	"bili2233.cn":  {},
	"t.co":         {},
	"t.cn":         {},
	"bit.ly":       {},
	"tinyurl.com":  {},
	"u.nu":         {},
	"is.gd":        {},
	"youtu.be":     {},
	"dwz.cn":       {},
	"163cn.tv":     {},
	"xhslink.com":  {},
	"xhslink.cn":   {},
	"v.douyin.com": {},
}

var trackingQueryParams = map[string]struct{}{
	"spm_id_from":    {},
	"vd_source":      {},
	"buvid":          {},
	"is_story_h5":    {},
	"mid":            {},
	"p_id":           {},
	"plat_id":        {},
	"share_from":     {},
	"share_medium":   {},
	"share_plat":     {},
	"share_session_id": {},
	"share_source":   {},
	"share_tag":      {},
	"spmid":          {},
	"timestamp":      {},
	"unique_k":       {},
	"up_id":          {},
	"bbid":           {},
	"ts":             {},
	"fbclid":         {},
	"gclid":          {},
	"si":             {},
	"s":              {},
	"t":              {},
	"feature":        {},
	"from":           {},
	"isappinstalled": {},
	"chksm":          {},
	"mpshare":        {},
	"scene":          {},
	"srcid":          {},
	"sharer_shareinfo": {},
	"sharer_shareinfo_first": {},
	"poc_token":      {},
}

func isShortenerHost(host string) bool {
	_, ok := shortenerHosts[strings.ToLower(strings.TrimSpace(host))]
	return ok
}

func isIgnoredHost(host string, extra map[string]struct{}) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return true
	}
	if _, ok := builtInIgnoreHosts[h]; ok {
		return true
	}
	for dom := range extra {
		if h == dom || strings.HasSuffix(h, "."+dom) {
			return true
		}
	}
	return false
}

// ExtractURLs extracts bounded, deduplicated, parsable URLs from a normalized Message (cards + text + bare BV IDs).
func ExtractURLs(msg *model.Message, maxURLs int, ignoreDomains []string, allowHosts []string) []ExtractedLink {
	if msg == nil {
		return nil
	}
	if maxURLs <= 0 {
		maxURLs = 2
	}
	extraIgn := make(map[string]struct{}, len(ignoreDomains))
	for _, d := range ignoreDomains {
		d = strings.ToLower(strings.TrimSpace(d))
		if d != "" {
			extraIgn[d] = struct{}{}
		}
	}
	allowSet := make(map[string]struct{}, len(allowHosts))
	for _, h := range allowHosts {
		allowSet[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
	}

	var out []ExtractedLink
	seen := make(map[string]struct{})
	addCandidate := func(raw string, fromCard bool) {
		if len(out) >= maxURLs {
			return
		}
		cleaned := trimURLTrailingPunct(strings.TrimSpace(raw))
		if cleaned == "" {
			return
		}
		if !isCandidateParsableURL(cleaned, extraIgn, allowSet) {
			return
		}
		canon := CanonicalizeURL(cleaned)
		if _, dup := seen[canon]; dup {
			return
		}
		seen[canon] = struct{}{}
		out = append(out, ExtractedLink{URL: cleaned, FromCard: fromCard})
	}

	// 1. Prefer explicit QQ share card URLs first.
	for _, p := range msg.Parts {
		if p.Kind == model.PartCard && strings.TrimSpace(p.URL) != "" {
			addCandidate(p.URL, true)
		}
	}
	// 2. Scan plain text parts for http(s) URLs and bare Bilibili BV IDs.
	for _, p := range msg.Parts {
		if p.Kind != model.PartText || strings.TrimSpace(p.Text) == "" {
			continue
		}
		for _, m := range rawURLRe.FindAllString(p.Text, -1) {
			addCandidate(m, false)
		}
		if len(out) < maxURLs {
			for _, sub := range bareBVRe.FindAllStringSubmatch(p.Text, -1) {
				if len(sub) >= 2 {
					addCandidate("https://www.bilibili.com/video/"+sub[1], false)
				}
			}
		}
	}
	return out
}

func trimURLTrailingPunct(u string) string {
	for len(u) > 0 {
		last := u[len(u)-1]
		switch last {
		case '.', ',', ';', ':', '!', '?', '"', '\'', '>', ']', '}':
			u = u[:len(u)-1]
		case ')':
			// Keep ')' if there is a matching '(' inside the URL (e.g. Wikipedia links).
			if strings.Count(u, "(") >= strings.Count(u, ")") {
				return u
			}
			u = u[:len(u)-1]
		default:
			return u
		}
	}
	return u
}

func isCandidateParsableURL(raw string, extraIgn, allowSet map[string]struct{}) bool {
	if err := netutil.ValidateURL(raw); err != nil {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return false
	}
	if _, explicitlyAllowed := allowSet[host]; !explicitlyAllowed {
		if host == "localhost" || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
			return false
		}
		if addr, err := netip.ParseAddr(host); err == nil && netutil.IsBlockedAddr(addr) {
			return false
		}
	}
	if isIgnoredHost(host, extraIgn) {
		return false
	}
	// Skip direct static media/archive files.
	ext := strings.ToLower(path.Ext(u.Path))
	if _, bad := staticFileExts[ext]; bad {
		return false
	}
	// Skip bare homepages without path or query (e.g. "https://github.com" or "https://www.baidu.com/").
	cleanPath := strings.Trim(u.Path, "/")
	if cleanPath == "" && strings.TrimSpace(u.RawQuery) == "" {
		return false
	}
	return true
}

// CanonicalizeURL normalizes scheme/host and removes common tracking query parameters for caching & dedup.
func CanonicalizeURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""

	// Normalize Bilibili video URLs to canonical BV link.
	host := u.Hostname()
	if strings.Contains(host, "bilibili.com") {
		if bvid := bvidRe.FindString(u.Path); bvid != "" {
			return "https://www.bilibili.com/video/" + bvid
		}
	}
	// Normalize arXiv URLs to canonical /abs/<id>.
	if strings.Contains(host, "arxiv.org") {
		if m := arxivIDRe.FindStringSubmatch(u.Path); len(m) >= 2 {
			return "https://arxiv.org/abs/" + m[1]
		}
	}
	// Strip tracking query parameters while keeping meaningful ones (e.g. WeChat __biz/mid/idx/sn, YouTube v, etc.).
	if u.RawQuery != "" {
		q := u.Query()
		for k := range q {
			kl := strings.ToLower(k)
			if strings.HasPrefix(kl, "utm_") {
				q.Del(k)
				continue
			}
			// Keep WeChat's mid parameter; strip Bilibili's share mid.
			if kl == "mid" && strings.Contains(host, "weixin.qq.com") {
				continue
			}
			if _, drop := trackingQueryParams[kl]; drop {
				q.Del(k)
			}
		}
		u.RawQuery = q.Encode()
	}
	if u.Path != "/" {
		u.Path = strings.TrimRight(u.Path, "/")
	}
	return u.String()
}

func clipRunesCompact(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return strings.TrimSpace(string(r[:max])) + "..."
}
