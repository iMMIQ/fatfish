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
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/xuri/excelize/v2"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/netutil"
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
	return "Fallback web search (bing/baidu) and specialized github/hf_mirror/arxiv search. Prefer MCP web_search_prime for general web queries when available."
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
	URL       string `json:"url" jsonschema:"required,description=URL to read (supports bilibili, b23.tv, arxiv, hf-mirror/huggingface, generic web pages, and .pdf/.xlsx/.docx/.txt files)"`
	Query     string `json:"query,omitempty" jsonschema:"description=Optional question or keywords to semantically extract the most relevant sections from long financial reports, PDFs, or spreadsheets"`
	Pages     string `json:"pages,omitempty" jsonschema:"description=Optional PDF page range e.g. '1-5' or '12-18' for targeted reading"`
	Translate bool   `json:"translate,omitempty" jsonschema:"description=Optionally translate long foreign text to Simplified Chinese via internal translator"`
}

type ReadSourceTool struct {
	mu     sync.RWMutex
	client *http.Client
	hub    *infer.Hub
}

func NewReadSourceTool(hub *infer.Hub, allowHosts []string) *ReadSourceTool {
	return &ReadSourceTool{
		client: netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
			Timeout:    25 * time.Second,
			AllowHosts: allowHosts,
		}),
		hub: hub,
	}
}

func (t *ReadSourceTool) UpdateAllowHosts(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 25 * time.Second, AllowHosts: allowHosts})
	t.mu.Lock()
	t.client = cli
	t.mu.Unlock()
}

func (t *ReadSourceTool) httpClient() *http.Client {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	return c
}

func (t *ReadSourceTool) Name() string   { return "read_source" }
func (t *ReadSourceTool) ReadOnly() bool { return true }
func (t *ReadSourceTool) Description() string {
	return "Deep-read Bilibili videos, arXiv papers, HuggingFace models, web pages, and PDF/XLSX/DOCX/TXT documents."
}

var (
	bvidRe    = regexp.MustCompile(`BV[0-9A-Za-z]{10}`)
	arxivIDRe = regexp.MustCompile(`(\d{4}\.\d{4,5})(v\d+)?`)
)

func (t *ReadSourceTool) Execute(ctx context.Context, _ ToolContext, args ReadSourceArgs) (ToolResult, error) {
	rawURL := strings.TrimSpace(args.URL)
	if err := netutil.ValidateURL(rawURL); err != nil {
		return ToolResult{}, err
	}
	u, _ := url.Parse(rawURL)
	host := strings.ToLower(u.Hostname())

	var content string
	var err error
	switch {
	case strings.Contains(host, "b23.tv") || strings.Contains(host, "bilibili.com"):
		content, err = t.readBilibili(ctx, rawURL)
	case strings.Contains(host, "arxiv.org"):
		content, err = t.readArxiv(ctx, rawURL)
	case strings.Contains(host, "huggingface.co") || strings.Contains(host, "hf-mirror.com"):
		content, err = t.readHuggingFace(ctx, u)
	default:
		firstPage, lastPage := parsePageRange(args.Pages)
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

func (t *ReadSourceTool) readBilibili(ctx context.Context, rawURL string) (string, error) {
	bvid := bvidRe.FindString(rawURL)
	if bvid == "" {
		// Resolve b23.tv short link via SSRF-safe client.
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		resp, err := t.httpClient().Do(req)
		if err != nil {
			return "", err
		}
		finalURL := resp.Request.URL.String()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		bvid = bvidRe.FindString(finalURL)
	}
	if bvid == "" {
		return t.readGenericOrDoc(ctx, rawURL, 0, 0)
	}

	apiURL := "https://api.bilibili.com/x/web-interface/view?bvid=" + bvid
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), apiURL, 2<<20, map[string]string{"Referer": "https://www.bilibili.com/"})
	if err != nil {
		return "", err
	}
	var view struct {
		Code int `json:"code"`
		Data struct {
			Aid      int64  `json:"aid"`
			Cid      int64  `json:"cid"`
			Bvid     string `json:"bvid"`
			Title    string `json:"title"`
			Desc     string `json:"desc"`
			Duration int    `json:"duration"`
			Owner    struct {
				Name string `json:"name"`
			} `json:"owner"`
			Stat struct {
				View  int `json:"view"`
				Like  int `json:"like"`
				Coin  int `json:"coin"`
				Reply int `json:"reply"`
			} `json:"stat"`
			Pages []struct {
				Cid  int64  `json:"cid"`
				Page int    `json:"page"`
				Part string `json:"part"`
			} `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Bilibili %s\nTitle: %s\nUP: %s | Duration: %ds | Views: %d | Likes: %d | Coins: %d\nDescription: %s\n",
		view.Data.Bvid, view.Data.Title, view.Data.Owner.Name, view.Data.Duration,
		view.Data.Stat.View, view.Data.Stat.Like, view.Data.Stat.Coin, view.Data.Desc)
	if len(view.Data.Pages) > 1 {
		b.WriteString("Pages:\n")
		for i, p := range view.Data.Pages {
			if i >= 10 {
				break
			}
			fmt.Fprintf(&b, "  P%d: %s\n", p.Page, p.Part)
		}
	}
	// Best-effort subtitle fetch via player endpoint (SPEC §10.2).
	cid := view.Data.Cid
	if cid == 0 && len(view.Data.Pages) > 0 {
		cid = view.Data.Pages[0].Cid
	}
	if cid > 0 {
		pURL := fmt.Sprintf("https://api.bilibili.com/x/player/v2?bvid=%s&cid=%d", bvid, cid)
		if pBody, _, _, perr := netutil.FetchLimited(ctx, t.httpClient(), pURL, 1<<20, map[string]string{"Referer": "https://www.bilibili.com/"}); perr == nil {
			var pl struct {
				Data struct {
					Subtitle struct {
						Subtitles []struct {
							SubtitleURL string `json:"subtitle_url"`
						} `json:"subtitles"`
					} `json:"subtitle"`
				} `json:"data"`
			}
			if json.Unmarshal(pBody, &pl) == nil && len(pl.Data.Subtitle.Subtitles) > 0 {
				subURL := strings.TrimSpace(pl.Data.Subtitle.Subtitles[0].SubtitleURL)
				if strings.HasPrefix(subURL, "//") {
					subURL = "https:" + subURL
				}
				if strings.HasPrefix(subURL, "http") {
					if sBody, _, _, serr := netutil.FetchLimited(ctx, t.httpClient(), subURL, 2<<20, nil); serr == nil {
						var sj struct {
							Body []struct {
								Content string `json:"content"`
							} `json:"body"`
						}
						if json.Unmarshal(sBody, &sj) == nil && len(sj.Body) > 0 {
							var lines []string
							for _, seg := range sj.Body {
								if s := strings.TrimSpace(seg.Content); s != "" {
									lines = append(lines, s)
								}
							}
							if len(lines) > 0 {
								b.WriteString("Subtitles:\n  " + clipRunes(strings.Join(lines, " "), 2500) + "\n")
							}
						}
					}
				}
			}
		}
	}
	// Fetch top comments if aid available.
	if view.Data.Aid > 0 {
		cURL := fmt.Sprintf("https://api.bilibili.com/x/v2/reply?type=1&oid=%d&sort=2&ps=5", view.Data.Aid)
		if cBody, _, _, cerr := netutil.FetchLimited(ctx, t.httpClient(), cURL, 1<<20, nil); cerr == nil {
			var cr struct {
				Data struct {
					Replies []struct {
						Member  struct{ Uname string `json:"uname"` } `json:"member"`
						Content struct{ Message string `json:"message"` } `json:"content"`
						Like    int `json:"like"`
					} `json:"replies"`
				} `json:"data"`
			}
			if json.Unmarshal(cBody, &cr) == nil && len(cr.Data.Replies) > 0 {
				b.WriteString("Top Comments:\n")
				for _, r := range cr.Data.Replies {
					fmt.Fprintf(&b, "  - %s (♥%d): %s\n", r.Member.Uname, r.Like, r.Content.Message)
				}
			}
		}
	}
	return b.String(), nil
}

func (t *ReadSourceTool) readArxiv(ctx context.Context, rawURL string) (string, error) {
	m := arxivIDRe.FindStringSubmatch(rawURL)
	if len(m) < 2 {
		return t.readGenericOrDoc(ctx, rawURL, 0, 0)
	}
	id := m[1]
	atomURL := "https://export.arxiv.org/api/query?id_list=" + id
	body, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), atomURL, 2<<20, nil)
	if err != nil {
		return "", err
	}
	var feed struct {
		Entries []struct {
			Title     string `xml:"title"`
			Summary   string `xml:"summary"`
			Published string `xml:"published"`
			Authors   []struct {
				Name string `xml:"name"`
			} `xml:"author"`
		} `xml:"entry"`
	}
	if err := xml.Unmarshal(body, &feed); err != nil || len(feed.Entries) == 0 {
		return t.readGenericOrDoc(ctx, rawURL, 0, 0)
	}
	e := feed.Entries[0]
	authors := make([]string, 0, len(e.Authors))
	for _, a := range e.Authors {
		authors = append(authors, a.Name)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "arXiv:%s (%s)\nTitle: %s\nAuthors: %s\nAbstract: %s",
		id, e.Published, strings.Join(strings.Fields(e.Title), " "), strings.Join(authors, ", "), strings.Join(strings.Fields(e.Summary), " "))

	// Extract first pages of the PDF via pdftotext when available (SPEC §10.2 Atom + PDF pages).
	if _, lerr := exec.LookPath("pdftotext"); lerr == nil {
		pdfURL := "https://arxiv.org/pdf/" + id + ".pdf"
		if pdfBytes, _, code, perr := netutil.FetchLimited(ctx, t.httpClient(), pdfURL, 10<<20, nil); perr == nil && code == 200 && len(pdfBytes) > 0 {
			if pdfTxt, eerr := extractPDFPages(ctx, pdfBytes, 1, 4); eerr == nil && pdfTxt != "" {
				out.WriteString("\n\nPDF Excerpt (pages 1-4):\n")
				out.WriteString(clipRunes(pdfTxt, 5000))
			}
		}
	}
	return out.String(), nil
}

func (t *ReadSourceTool) readHuggingFace(ctx context.Context, u *url.URL) (string, error) {
	repo := strings.Trim(u.Path, "/")
	parts := strings.Split(repo, "/")
	if len(parts) < 2 {
		return t.readGenericOrDoc(ctx, u.String(), 0, 0)
	}
	repoID := parts[0] + "/" + parts[1]
	metaBody, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), "https://hf-mirror.com/api/models/"+repoID, 2<<20, nil)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("HuggingFace Model: " + repoID + "\n")
	var meta map[string]any
	if json.Unmarshal(metaBody, &meta) == nil {
		fmt.Fprintf(&b, "Downloads: %v | Likes: %v | Pipeline: %v\n", meta["downloads"], meta["likes"], meta["pipeline_tag"])
	}
	if readme, _, code, rerr := netutil.FetchLimited(ctx, t.httpClient(), "https://hf-mirror.com/"+repoID+"/raw/main/README.md", 2<<20, nil); rerr == nil && code == 200 {
		b.WriteString("\nREADME:\n")
		b.WriteString(clipRunes(string(readme), 5000))
	}
	return b.String(), nil
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
	if t.hub != nil {
		if rres, err := t.hub.Rerank(ctx, query, chunks); err == nil && len(rres) > 0 {
			for _, r := range rres {
				if r.Index >= 0 && r.Index < len(scores) {
					scores[r.Index].score = r.Score*0.85 + scores[r.Index].score*0.15
				}
			}
			ranked = true
		} else if embs, err := t.hub.Embed(ctx, append([]string{query}, chunks...)); err == nil && len(embs) == len(chunks)+1 {
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
	tmp, err := os.CreateTemp("", "fatfish-*.pdf")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(pdfBytes); err != nil {
		tmp.Close()
		return "", err
	}
	tmp.Close()

	args := []string{"-layout"}
	if firstPage > 0 {
		args = append(args, "-f", strconv.Itoa(firstPage))
	}
	if lastPage > 0 {
		args = append(args, "-l", strconv.Itoa(lastPage))
	}
	args = append(args, tmp.Name(), "-")
	cmd := exec.CommandContext(ctx, "pdftotext", args...)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("pdftotext failed: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
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
