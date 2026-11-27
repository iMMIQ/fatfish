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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/xuri/excelize/v2"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/netutil"
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
		content, err = t.readGenericOrDoc(ctx, rawURL)
	}
	if err != nil {
		return ToolResult{}, err
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
		return t.readGenericOrDoc(ctx, rawURL)
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
		return t.readGenericOrDoc(ctx, rawURL)
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
		return t.readGenericOrDoc(ctx, rawURL)
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
		return t.readGenericOrDoc(ctx, u.String())
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

func (t *ReadSourceTool) readGenericOrDoc(ctx context.Context, rawURL string) (string, error) {
	data, ct, code, err := netutil.FetchLimited(ctx, t.httpClient(), rawURL, 10<<20, nil)
	if err != nil {
		return "", err
	}
	if code >= 400 {
		return "", fmt.Errorf("HTTP %d", code)
	}
	lowURL := strings.ToLower(rawURL)
	lowCT := strings.ToLower(ct)

	switch {
	case strings.Contains(lowCT, "application/pdf") || strings.HasSuffix(lowURL, ".pdf"):
		return extractPDFText(ctx, data)
	case strings.Contains(lowCT, "spreadsheetml") || strings.HasSuffix(lowURL, ".xlsx"):
		return extractXLSXText(data)
	case strings.Contains(lowCT, "wordprocessingml") || strings.HasSuffix(lowURL, ".docx"):
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

func extractXLSXText(xlsxBytes []byte) (string, error) {
	f, err := excelize.OpenReader(bytes.NewReader(xlsxBytes))
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
			if i >= 120 {
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
		rc, err := f.Open()
		if err != nil {
			return "", err
		}
		defer rc.Close()
		dec := xml.NewDecoder(rc)
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
		}
		return strings.TrimSpace(out.String()), nil
	}
	return "", errors.New("docx: word/document.xml not found")
}

// --- 6. quote ---

type QuoteArgs struct {
	Symbol string `json:"symbol" jsonschema:"required,description=Stock or index ticker e.g. usNVDA, usSMH, hk00700, hkHSTECH, hkHSI, sh000001, sh600519, sz399006"`
	Days   int    `json:"days,omitempty" jsonschema:"description=Optional number of daily K-line bars to fetch (default 15, max 60)"`
}

type QuoteTool struct {
	mu     sync.RWMutex
	client *http.Client
}

func NewQuoteTool(allowHosts []string) *QuoteTool {
	return &QuoteTool{
		client: netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
			Timeout:    10 * time.Second,
			AllowHosts: allowHosts,
		}),
	}
}

func (t *QuoteTool) UpdateAllowHosts(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 10 * time.Second, AllowHosts: allowHosts})
	t.mu.Lock()
	t.client = cli
	t.mu.Unlock()
}

func (t *QuoteTool) httpClient() *http.Client {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	return c
}

func (t *QuoteTool) Name() string   { return "quote" }
func (t *QuoteTool) ReadOnly() bool { return true }
func (t *QuoteTool) Description() string {
	return "Fetch real-time market quote and recent daily K-line data for US (usNVDA), HK (hk00700), and A-share (sh600519/sz000001) tickers."
}

func (t *QuoteTool) Execute(ctx context.Context, _ ToolContext, args QuoteArgs) (ToolResult, error) {
	sym := normalizeTicker(args.Symbol)
	if sym == "" {
		return ToolResult{}, errors.New("quote: symbol is required")
	}
	days := args.Days
	if days <= 0 {
		days = 15
	}
	if days > 60 {
		days = 60
	}

	// 1. Realtime quote from Tencent qt.gtimg.cn (GBK encoded).
	qtURL := "https://qt.gtimg.cn/q=" + url.QueryEscape(sym)
	raw, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), qtURL, 512<<10, nil)
	if err != nil {
		return ToolResult{}, err
	}
	utf8Text, err := decodeGBK(raw)
	if err != nil {
		utf8Text = string(raw)
	}

	var b strings.Builder
	summary, quoteCode := parseTencentQuote(utf8Text)
	if summary != "" {
		b.WriteString(summary + "\n")
	} else {
		return ToolResult{}, fmt.Errorf("quote: no market data returned for %q", sym)
	}

	// 2. Daily K-line bars (up to 60 days).
	// For US tickers, Tencent fqkline requires the exchange suffix (e.g. usSMH.OQ) returned in quoteCode.
	klSym := sym
	if strings.HasPrefix(sym, "us") && strings.Contains(quoteCode, ".") {
		klSym = "us" + quoteCode
	}
	klURL := fmt.Sprintf("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,day,,,%d,qfq", url.QueryEscape(klSym), days)
	if klRaw, _, _, kerr := netutil.FetchLimited(ctx, t.httpClient(), klURL, 2<<20, nil); kerr == nil {
		if klLines := parseTencentKline(klRaw, klSym); len(klLines) > 0 {
			b.WriteString("\nRecent Daily K-line (date, open, close, high, low, vol):\n")
			b.WriteString(strings.Join(klLines, "\n"))
		}
	}

	return ToolResult{TextForLLM: netutil.WrapExternalData("quote:"+sym, b.String())}, nil
}

func normalizeTicker(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	low := strings.ToLower(s)
	// Common index aliases (Chinese & bare English).
	switch low {
	case "hstech", "恒生科技", "恒生科技指数", "恒科":
		return "hkHSTECH"
	case "hsi", "恒生指数", "恒指":
		return "hkHSI"
	case "hscei", "国企指数", "恒生国企":
		return "hkHSCEI"
	case "上证", "上证指数", "大盘", "sse":
		return "sh000001"
	case "深证", "深证成指":
		return "sz399001"
	case "创业板", "创业板指":
		return "sz399006"
	case "科创50", "科创板":
		return "sh000688"
	case "沪深300", "csi300":
		return "sh000300"
	}
	if strings.HasPrefix(low, "us") {
		return "us" + strings.ToUpper(s[2:])
	}
	if strings.HasPrefix(low, "hk") {
		rest := s[2:]
		if isAllDigits(rest) {
			return "hk" + rest
		}
		// HK indices like hkHSTECH / hkHSI / hkHSCEI are case-sensitive on qt.gtimg.cn.
		return "hk" + strings.ToUpper(rest)
	}
	if strings.HasPrefix(low, "sh") || strings.HasPrefix(low, "sz") {
		return low
	}
	// Bare 6-digit A-share or bare US ticker heuristic.
	if len(s) == 6 && isAllDigits(s) {
		if strings.HasPrefix(s, "6") {
			return "sh" + s
		}
		return "sz" + s
	}
	if len(s) == 5 && isAllDigits(s) {
		return "hk" + s
	}
	return "us" + strings.ToUpper(s)
}

func decodeGBK(b []byte) (string, error) {
	reader := transform.NewReader(bytes.NewReader(b), simplifiedchinese.GBK.NewDecoder())
	out, err := io.ReadAll(reader)
	return string(out), err
}

func parseTencentQuote(raw string) (summary string, code string) {
	start := strings.IndexByte(raw, '"')
	end := strings.LastIndexByte(raw, '"')
	if start < 0 || end <= start {
		return "", ""
	}
	fields := strings.Split(raw[start+1:end], "~")
	if len(fields) < 35 {
		return "", ""
	}
	name, code, price, prevClose, open, pct, high, low := fields[1], fields[2], fields[3], fields[4], fields[5], fields[32], fields[33], fields[34]
	return fmt.Sprintf("%s (%s): Price=%s | Change=%s%% | Open=%s | PrevClose=%s | High=%s | Low=%s",
		name, code, price, pct, open, prevClose, high, low), code
}

func parseTencentKline(raw []byte, sym string) []string {
	var parsed struct {
		Data map[string]map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}
	stock := parsed.Data[sym]
	if stock == nil {
		for _, v := range parsed.Data {
			stock = v
			break
		}
	}
	if stock == nil {
		return nil
	}
	var bars [][]any
	for _, k := range []string{"qfqday", "day"} {
		if r, ok := stock[k]; ok && json.Unmarshal(r, &bars) == nil && len(bars) > 0 {
			break
		}
	}
	var lines []string
	for _, bar := range bars {
		if len(bar) < 6 {
			continue
		}
		lines = append(lines, fmt.Sprintf("%v | O:%v C:%v H:%v L:%v V:%v", bar[0], bar[1], bar[2], bar[3], bar[4], bar[5]))
	}
	return lines
}

func isAllDigits(s string) bool {
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

// Keep strconv referenced if needed in helper extensions.
var _ = strconv.Itoa
