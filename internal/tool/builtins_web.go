package tool

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/xuri/excelize/v2"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/netutil"
	"github.com/iMMIQ/fatfish/internal/parser"
	"github.com/iMMIQ/fatfish/internal/vec"
)

// --- 3. web_search ---

type WebSearchArgs struct {
	Query  string `json:"query" jsonschema:"required,description=Search query string"`
	Engine string `json:"engine,omitempty" jsonschema:"enum=bing,enum=baidu,enum=github,enum=hf_mirror,enum=arxiv,description=Search source (default bing)"`
	Limit  int    `json:"limit,omitempty" jsonschema:"description=Max results to return (default 5, max 10)"`
}

type WebSearchTool struct {
	mu     sync.RWMutex
	client *http.Client
}

func NewWebSearchTool(allowHosts []string) *WebSearchTool {
	return &WebSearchTool{
		client: netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
			Timeout:    15 * time.Second,
			AllowHosts: allowHosts,
		}),
	}
}

func (t *WebSearchTool) UpdateAllowHosts(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 15 * time.Second, AllowHosts: allowHosts})
	t.mu.Lock()
	t.client = cli
	t.mu.Unlock()
}

func (t *WebSearchTool) httpClient() *http.Client {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	return c
}

func (t *WebSearchTool) Name() string   { return "web_search" }
func (t *WebSearchTool) ReadOnly() bool { return true }
func (t *WebSearchTool) Description() string {
	return "Specialized search for github/hf_mirror/arxiv, or emergency fallback (bing/baidu) ONLY when mcp_web_search_prime_web_search_prime fails or errors. Do NOT call bing/baidu alongside web_search_prime for normal web, news, hotel, or travel queries."
}

func (t *WebSearchTool) Execute(ctx context.Context, _ ToolContext, args WebSearchArgs) (ToolResult, error) {
	q := strings.TrimSpace(args.Query)
	if q == "" {
		return ToolResult{}, errors.New("web_search: query is required")
	}
	limit := args.Limit
	if limit <= 0 || limit > 10 {
		limit = 5
	}
	eng := strings.ToLower(strings.TrimSpace(args.Engine))
	if eng == "" {
		eng = "bing"
	}

	var out string
	var err error
	switch eng {
	case "github":
		out, err = t.searchGitHub(ctx, q, limit)
	case "hf_mirror":
		out, err = t.searchHFMirror(ctx, q, limit)
	case "arxiv":
		out, err = t.searchArxiv(ctx, q, limit)
	case "baidu":
		out, err = t.searchBaidu(ctx, q, limit)
	default:
		out, err = t.searchBing(ctx, q, limit)
	}
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{TextForLLM: netutil.WrapExternalData("web_search:"+eng, out)}, nil
}

func (t *WebSearchTool) searchBing(ctx context.Context, q string, limit int) (string, error) {
	u := "https://www.bing.com/search?q=" + url.QueryEscape(q)
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), u, 2<<20, map[string]string{"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
	if err != nil {
		return "", err
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	var lines []string
	doc.Find("li.b_algo").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		a := s.Find("h2 a")
		title := strings.TrimSpace(a.Text())
		href, _ := a.Attr("href")
		snip := strings.TrimSpace(s.Find("p").First().Text())
		if title != "" && href != "" {
			lines = append(lines, fmt.Sprintf("- %s\n  URL: %s\n  %s", title, href, snip))
		}
		return len(lines) < limit
	})
	if len(lines) == 0 {
		return "(no results)", nil
	}
	return strings.Join(lines, "\n"), nil
}

func (t *WebSearchTool) searchBaidu(ctx context.Context, q string, limit int) (string, error) {
	u := "https://www.baidu.com/s?wd=" + url.QueryEscape(q)
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), u, 2<<20, nil)
	if err != nil {
		return "", err
	}
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	var lines []string
	doc.Find("div.result, div.c-container").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		a := s.Find("h3 a").First()
		title := strings.TrimSpace(a.Text())
		href, _ := a.Attr("href")
		snip := strings.TrimSpace(s.Text())
		if len([]rune(snip)) > 160 {
			snip = string([]rune(snip)[:160]) + "..."
		}
		if title != "" {
			lines = append(lines, fmt.Sprintf("- %s (%s)\n  %s", title, href, snip))
		}
		return len(lines) < limit
	})
	if len(lines) == 0 {
		return "(no results)", nil
	}
	return strings.Join(lines, "\n"), nil
}

func (t *WebSearchTool) searchGitHub(ctx context.Context, q string, limit int) (string, error) {
	u := fmt.Sprintf("https://api.github.com/search/repositories?q=%s&per_page=%d", url.QueryEscape(q), limit)
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), u, 2<<20, map[string]string{"Accept": "application/vnd.github+json"})
	if err != nil {
		return "", err
	}
	var parsed struct {
		Items []struct {
			FullName    string `json:"full_name"`
			HTMLURL     string `json:"html_url"`
			Description string `json:"description"`
			Stars       int    `json:"stargazers_count"`
			Language    string `json:"language"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	var lines []string
	for _, it := range parsed.Items {
		lines = append(lines, fmt.Sprintf("- %s [%s ★%d] %s\n  %s", it.FullName, it.Language, it.Stars, it.HTMLURL, it.Description))
	}
	if len(lines) == 0 {
		return "(no repositories found)", nil
	}
	return strings.Join(lines, "\n"), nil
}

func (t *WebSearchTool) searchHFMirror(ctx context.Context, q string, limit int) (string, error) {
	u := fmt.Sprintf("https://hf-mirror.com/api/models?search=%s&limit=%d", url.QueryEscape(q), limit)
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), u, 2<<20, nil)
	if err != nil {
		return "", err
	}
	var items []struct {
		ID        string `json:"id"`
		Downloads int    `json:"downloads"`
		Likes     int    `json:"likes"`
		Pipeline  string `json:"pipeline_tag"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return "", err
	}
	var lines []string
	for _, it := range items {
		lines = append(lines, fmt.Sprintf("- %s [%s, dl=%d, likes=%d] https://hf-mirror.com/%s", it.ID, it.Pipeline, it.Downloads, it.Likes, it.ID))
	}
	if len(lines) == 0 {
		return "(no models found)", nil
	}
	return strings.Join(lines, "\n"), nil
}

func (t *WebSearchTool) searchArxiv(ctx context.Context, q string, limit int) (string, error) {
	u := fmt.Sprintf("https://export.arxiv.org/api/query?search_query=all:%s&start=0&max_results=%d", url.QueryEscape(q), limit)
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), u, 2<<20, nil)
	if err != nil {
		return "", err
	}
	var feed struct {
		Entries []struct {
			ID      string `xml:"id"`
			Title   string `xml:"title"`
			Summary string `xml:"summary"`
		} `xml:"entry"`
	}
	if err := xml.Unmarshal(body, &feed); err != nil {
		return "", err
	}
	var lines []string
	for _, e := range feed.Entries {
		title := strings.Join(strings.Fields(e.Title), " ")
		sum := strings.Join(strings.Fields(e.Summary), " ")
		if len([]rune(sum)) > 220 {
			sum = string([]rune(sum)[:220]) + "..."
		}
		lines = append(lines, fmt.Sprintf("- %s\n  %s\n  %s", title, strings.TrimSpace(e.ID), sum))
	}
	if len(lines) == 0 {
		return "(no papers found)", nil
	}
	return strings.Join(lines, "\n"), nil
}

// --- 4. read_source ---

type ReadSourceArgs struct {
	URL       string `json:"url" jsonschema:"required,description=URL to read (supports bilibili, b23.tv, github, arxiv, hf-mirror/huggingface, x/twitter, weixin articles, generic web pages, and .pdf/.xlsx/.docx/.txt files)"`
	Query     string `json:"query,omitempty" jsonschema:"description=Optional question or keywords to semantically extract the most relevant sections from long financial reports, PDFs, or spreadsheets"`
	Pages     string `json:"pages,omitempty" jsonschema:"description=Optional PDF page range e.g. '1-5' or '12-18' for targeted reading"`
	Translate bool   `json:"translate,omitempty" jsonschema:"description=Optionally translate long foreign text to Simplified Chinese via internal translator"`
}

type ReadSourceTool struct {
	mu           sync.RWMutex
	client       *http.Client
	parser       *parser.Service
	sharedParser bool
	hub          *infer.Hub
}

func NewReadSourceTool(hub *infer.Hub, allowHosts []string) *ReadSourceTool {
	return &ReadSourceTool{
		client: netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
			Timeout:    25 * time.Second,
			AllowHosts: allowHosts,
		}),
		parser: parser.NewService(parser.Options{
			AllowHosts: allowHosts,
			Timeout:    15 * time.Second,
		}),
		hub: hub,
	}
}

// SetParser injects a shared parser.Service so read_source shares cache and site config with proactive link parsing.
func (t *ReadSourceTool) SetParser(p *parser.Service) {
	if p == nil {
		return
	}
	t.mu.Lock()
	t.parser = p
	t.sharedParser = true
	t.mu.Unlock()
}

func (t *ReadSourceTool) UpdateAllowHosts(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 25 * time.Second, AllowHosts: allowHosts})
	t.mu.Lock()
	t.client = cli
	ownParser := t.parser
	isShared := t.sharedParser
	t.mu.Unlock()
	if ownParser != nil && !isShared {
		ownParser.UpdateOptions(parser.Options{
			AllowHosts: allowHosts,
			Timeout:    15 * time.Second,
		})
	}
}

func (t *ReadSourceTool) httpClient() *http.Client {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	return c
}

func (t *ReadSourceTool) parserSvc() *parser.Service {
	t.mu.RLock()
	p := t.parser
	t.mu.RUnlock()
	return p
}

func (t *ReadSourceTool) Name() string   { return "read_source" }
func (t *ReadSourceTool) ReadOnly() bool { return true }
func (t *ReadSourceTool) Description() string {
	return "Deep-read Bilibili videos/dynamics, GitHub repos/issues/PRs, arXiv papers, HuggingFace models, X/Twitter posts, WeChat articles, web pages, and PDF/XLSX/DOCX/TXT documents."
}

func (t *ReadSourceTool) Execute(ctx context.Context, _ ToolContext, args ReadSourceArgs) (ToolResult, error) {
	rawURL := strings.TrimSpace(args.URL)
	if err := netutil.ValidateURL(rawURL); err != nil {
		return ToolResult{}, err
	}
	u, _ := url.Parse(rawURL)
	host := strings.ToLower(u.Hostname())
	lowPath := strings.ToLower(u.Path)
	firstPage, lastPage := parsePageRange(args.Pages)

	// Binary office/PDF documents (or explicit PDF page ranges) use dedicated document extractors.
	isExplicitDoc := firstPage > 0 || lastPage > 0 ||
		strings.HasSuffix(lowPath, ".xlsx") ||
		strings.HasSuffix(lowPath, ".docx") ||
		(strings.HasSuffix(lowPath, ".pdf") && !strings.Contains(host, "arxiv.org"))

	var content string
	var err error
	if isExplicitDoc {
		content, err = t.readGenericOrDoc(ctx, rawURL, firstPage, lastPage)
	} else if psvc := t.parserSvc(); psvc != nil {
		res, perr := psvc.Parse(ctx, rawURL, parser.ParseOptions{Deep: true})
		if perr == nil && res != nil {
			content = res.FormatForReadSource()
		} else if errors.Is(perr, parser.ErrUnsupportedContent) {
			// URL without doc suffix whose Content-Type turned out to be PDF/XLSX/DOCX.
			content, err = t.readGenericOrDoc(ctx, rawURL, firstPage, lastPage)
		} else {
			err = perr
		}
	} else {
		content, err = t.readGenericOrDoc(ctx, rawURL, firstPage, lastPage)
	}
	if err != nil {
		return ToolResult{}, err
	}

	if q := strings.TrimSpace(args.Query); q != "" && len([]rune(content)) > 5000 {
		content = t.selectRelevantChunks(ctx, content, q, 6, 7500)
	}

	if args.Translate && t.hub != nil && len([]rune(content)) > 40 {
		if tr, err := t.hub.Translate(ctx, clipRunes(content, 4000), "Simplified Chinese"); err == nil && tr != "" {
			content = tr
		}
	}
	return ToolResult{TextForLLM: netutil.WrapExternalData(rawURL, clipRunes(content, 8000))}, nil
}

func (t *ReadSourceTool) readGenericOrDoc(ctx context.Context, rawURL string, firstPage, lastPage int) (string, error) {
	data, ct, code, err := netutil.FetchLimited(ctx, t.httpClient(), rawURL, 10<<20, nil)
	if err != nil {
		return "", err
	}
	if code >= 400 {
		return "", fmt.Errorf("HTTP %d", code)
	}
	lowPath := strings.ToLower(rawURL)
	if parsed, perr := url.Parse(rawURL); perr == nil && parsed.Path != "" {
		lowPath = strings.ToLower(parsed.Path)
	}
	lowCT := strings.ToLower(ct)

	switch {
	case strings.Contains(lowCT, "application/pdf") || strings.HasSuffix(lowPath, ".pdf"):
		return extractPDFPages(ctx, data, firstPage, lastPage)
	case strings.Contains(lowCT, "spreadsheetml") || strings.HasSuffix(lowPath, ".xlsx"):
		return extractXLSXText(data)
	case strings.Contains(lowCT, "wordprocessingml") || strings.HasSuffix(lowPath, ".docx"):
		return extractDOCXText(data)
	case strings.Contains(lowCT, "text/html"):
		doc, err := goquery.NewDocumentFromReader(bytes.NewReader(data))
		if err != nil {
			return string(data), nil
		}
		doc.Find("script, style, noscript, svg, nav, footer").Remove()
		title := strings.TrimSpace(doc.Find("title").First().Text())
		body := strings.Join(strings.Fields(doc.Find("body").Text()), " ")
		if title != "" {
			return "Title: " + title + "\n" + body, nil
		}
		return body, nil
	default:
		return string(data), nil
	}
}

func parsePageRange(spec string) (int, int) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, 0
	}
	if parts := strings.SplitN(spec, "-", 2); len(parts) == 2 {
		a, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
		b, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
		if a > 0 && b >= a {
			return a, b
		}
		if a > 0 {
			return a, a + 5
		}
	}
	if p, err := strconv.Atoi(spec); err == nil && p > 0 {
		return p, p
	}
	return 0, 0
}

// selectRelevantChunks splits long documents (e.g., 100-page financial filings) into overlapping chunks
// and ranks them against query using local rerank -> embed cosine -> lexical overlap fallback.
func (t *ReadSourceTool) selectRelevantChunks(ctx context.Context, content, query string, topK, maxRunes int) string {
	return selectRelevantChunksWithHub(ctx, t.hub, content, query, topK, maxRunes)
}

func selectRelevantChunksWithHub(ctx context.Context, hub *infer.Hub, content, query string, topK, maxRunes int) string {
	chunks := splitOverlappingChunks(content, 1000, 150, 64)
	if len(chunks) <= topK {
		return clipRunes(content, maxRunes)
	}
	type scored struct {
		idx   int
		score float64
	}
	scores := make([]scored, len(chunks))
	for i := range chunks {
		scores[i] = scored{idx: i, score: lexicalOverlapScore(chunks[i], query)}
	}

	ranked := false
	if hub != nil {
		if rres, err := hub.Rerank(ctx, query, chunks); err == nil && len(rres) > 0 {
			for _, r := range rres {
				if r.Index >= 0 && r.Index < len(scores) {
					scores[r.Index].score = r.Score*0.85 + scores[r.Index].score*0.15
				}
			}
			ranked = true
		} else if embs, err := hub.Embed(ctx, append([]string{query}, chunks...)); err == nil && len(embs) == len(chunks)+1 {
			qv := embs[0]
			vec.NormalizeInPlace(qv)
			for i := range chunks {
				cv := embs[i+1]
				vec.NormalizeInPlace(cv)
				sim := float64(dotFloat32(qv, cv))
				scores[i].score = sim*0.80 + scores[i].score*0.20
			}
			ranked = true
		}
	}
	_ = ranked

	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })
	if len(scores) > topK {
		scores = scores[:topK]
	}
	// Restore original document reading order for selected top chunks.
	sort.Slice(scores, func(i, j int) bool { return scores[i].idx < scores[j].idx })

	var b strings.Builder
	fmt.Fprintf(&b, "[Document Semantic Excerpt for query=%q (%d/%d chunks selected)]\n", query, len(scores), len(chunks))
	// Always include a compact opening header if chunk 0 wasn't already selected.
	if scores[0].idx != 0 {
		b.WriteString("--- [Opening Header] ---\n")
		b.WriteString(clipRunes(chunks[0], 500) + "\n\n")
	}
	for _, sc := range scores {
		fmt.Fprintf(&b, "--- [Section %d/%d] ---\n%s\n\n", sc.idx+1, len(chunks), chunks[sc.idx])
	}
	return clipRunes(strings.TrimSpace(b.String()), maxRunes)
}

func splitOverlappingChunks(s string, chunkRunes, overlap, maxChunks int) []string {
	r := []rune(strings.TrimSpace(s))
	if len(r) == 0 {
		return nil
	}
	step := chunkRunes - overlap
	if step < 100 {
		step = 100
	}
	var out []string
	for start := 0; start < len(r) && len(out) < maxChunks; start += step {
		end := start + chunkRunes
		if end > len(r) {
			end = len(r)
		}
		out = append(out, strings.TrimSpace(string(r[start:end])))
		if end == len(r) {
			break
		}
	}
	return out
}

func lexicalOverlapScore(chunk, query string) float64 {
	cLow := strings.ToLower(chunk)
	qLow := strings.ToLower(strings.TrimSpace(query))
	if qLow == "" {
		return 0
	}
	// Mix whitespace words + 2-char CJK bigrams for mixed CN/EN financial queries.
	terms := make(map[string]struct{})
	for _, tok := range strings.Fields(qLow) {
		if len([]rune(tok)) >= 2 {
			terms[tok] = struct{}{}
		}
		rs := []rune(tok)
		for i := 0; i+1 < len(rs); i++ {
			if rs[i] > 127 && rs[i+1] > 127 {
				terms[string(rs[i:i+2])] = struct{}{}
			}
		}
	}
	if len(terms) == 0 {
		if strings.Contains(cLow, qLow) {
			return 1.0
		}
		return 0
	}
	hits := 0
	for term := range terms {
		if strings.Contains(cLow, term) {
			hits++
		}
	}
	return float64(hits) / float64(len(terms))
}

func dotFloat32(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var sum float32
	for i := 0; i < n; i++ {
		sum += a[i] * b[i]
	}
	return sum
}

func extractPDFText(ctx context.Context, pdfBytes []byte) (string, error) {
	return extractPDFPages(ctx, pdfBytes, 0, 0)
}

func extractPDFPages(ctx context.Context, pdfBytes []byte, firstPage, lastPage int) (string, error) {
	return parser.ExtractPDFPages(ctx, pdfBytes, firstPage, lastPage)
}

const (
	maxDocUnzipBytes  = 32 << 20  // 32 MiB decompressed cap (zip-bomb guard)
	maxDocXMLBytes    = 16 << 20  // 16 MiB XML cap
	maxDocOutputBytes = 256 << 10 // 256 KiB extracted text cap
)

func extractXLSXText(xlsxBytes []byte) (string, error) {
	f, err := excelize.OpenReader(bytes.NewReader(xlsxBytes), excelize.Options{
		UnzipSizeLimit:    maxDocUnzipBytes,
		UnzipXMLSizeLimit: maxDocXMLBytes,
	})
	if err != nil {
		return "", err
	}
	defer f.Close()
	var b strings.Builder
	for _, sheet := range f.GetSheetList() {
		fmt.Fprintf(&b, "## Sheet: %s\n", sheet)
		rows, err := f.GetRows(sheet)
		if err != nil {
			continue
		}
		for i, row := range rows {
			if i >= 120 || b.Len() >= maxDocOutputBytes {
				b.WriteString("...(truncated rows)\n")
				break
			}
			b.WriteString(strings.Join(row, "\t") + "\n")
		}
	}
	return b.String(), nil
}

func extractDOCXText(docxBytes []byte) (string, error) {
	zr, err := zip.NewReader(bytes.NewReader(docxBytes), int64(len(docxBytes)))
	if err != nil {
		return "", err
	}
	for _, f := range zr.File {
		if f.Name != "word/document.xml" {
			continue
		}
		if f.UncompressedSize64 > maxDocXMLBytes {
			return "", fmt.Errorf("docx: word/document.xml exceeds %d MiB limit", maxDocXMLBytes>>20)
		}
		rc, err := f.Open()
		if err != nil {
			return "", err
		}
		defer rc.Close()
		dec := xml.NewDecoder(io.LimitReader(rc, maxDocXMLBytes))
		var out strings.Builder
		for {
			tok, err := dec.Token()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return out.String(), nil
			}
			switch x := tok.(type) {
			case xml.CharData:
				out.Write(x)
			case xml.EndElement:
				if x.Name.Local == "p" {
					out.WriteByte('\n')
				}
			}
			if out.Len() >= maxDocOutputBytes {
				out.WriteString("\n...(truncated)")
				break
			}
		}
		return strings.TrimSpace(out.String()), nil
	}
	return "", errors.New("docx: word/document.xml not found")
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func clipRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "\n...(truncated)"
}
