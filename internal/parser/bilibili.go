package parser

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/sync/errgroup"

	"github.com/iMMIQ/fatfish/internal/netutil"
)

// Bilibili WBI mixin key permutation table (public web player spec; no account login required).
var biliMixinKeyEncTab = [64]int{
	46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
	27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
	37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
	22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
}

var (
	biliOpusRe    = regexp.MustCompile(`/(?:opus/)?(\d{15,})`)
	biliArticleRe = regexp.MustCompile(`(?i)/read/cv(\d+)`)
	biliBangumiRe = regexp.MustCompile(`(?i)/bangumi/play/(ep|ss)(\d+)`)
	biliLiveRe    = regexp.MustCompile(`^/(?:blanc/|h5/)?(\d+)(?:/|$)`)
	biliTSLineRe  = regexp.MustCompile(`\[(\d{1,2}):(\d{2})(?::(\d{2}))?\]`)
	biliHMSTimeRe = regexp.MustCompile(`(?i)^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s?)?$`)
)

// biliSession holds anonymous (no-login) WBI signing keys and guest buvid cookies cached in RAM.
type biliSession struct {
	mu           sync.RWMutex
	mixinKey     string
	wbiExpires   time.Time
	wbiFailUntil time.Time

	buvid3         string
	buvid4         string
	buvidExpires   time.Time
	buvidFailUntil time.Time

	warnMu   sync.Mutex
	lastWarn map[string]time.Time
}

func newBiliSession() *biliSession {
	return &biliSession{
		lastWarn: make(map[string]time.Time),
	}
}

func (b *biliSession) warnThrottled(s *Service, key, msg string, args ...any) {
	if s == nil || b == nil {
		return
	}
	now := time.Now()
	b.warnMu.Lock()
	if prev, ok := b.lastWarn[key]; ok && now.Sub(prev) < 5*time.Minute {
		b.warnMu.Unlock()
		return
	}
	b.lastWarn[key] = now
	b.warnMu.Unlock()
	s.logger().Warn(msg, args...)
}

// matchBilibili matches Bilibili video, dynamic/opus, article, bangumi, live, and shortener URLs.
func matchBilibili(host, urlPath string) bool {
	if strings.Contains(host, "b23.tv") || strings.Contains(host, "bili2233.cn") {
		return true
	}
	if host == "live.bilibili.com" {
		return biliLiveRe.MatchString(urlPath)
	}
	if host == "t.bilibili.com" || strings.HasSuffix(host, ".bilibili.com") || host == "bilibili.com" {
		lp := strings.ToLower(urlPath)
		return strings.Contains(lp, "/video/") ||
			strings.Contains(lp, "/opus/") ||
			strings.Contains(lp, "/read/cv") ||
			strings.Contains(lp, "/bangumi/play/") ||
			strings.Contains(lp, "/festival/") ||
			host == "t.bilibili.com" ||
			bvidRe.MatchString(urlPath) ||
			avidRe.MatchString(urlPath)
	}
	return false
}

// ensureBilibiliSession populates anonymous WBI mixinKey and guest buvid3/buvid4 (no account login).
func (s *Service) ensureBilibiliSession(ctx context.Context, cli *http.Client, baseHeaders map[string]string) (mixinKey string, buvidCookie string) {
	bs := s.bili
	if bs == nil {
		return "", ""
	}
	now := time.Now()
	bs.mu.RLock()
	needWBI := (bs.mixinKey == "" || now.After(bs.wbiExpires)) && now.After(bs.wbiFailUntil)
	needBuvid := (bs.buvid3 == "" || now.After(bs.buvidExpires)) && now.After(bs.buvidFailUntil)
	mk := bs.mixinKey
	b3, b4 := bs.buvid3, bs.buvid4
	bs.mu.RUnlock()

	if !needWBI && !needBuvid {
		return mk, formatBuvidCookie(b3, b4)
	}

	// Singleflight refresh with a tight bounded timeout so cold start never stalls link parsing.
	_, _, _ = s.biliSF.Do("session_refresh", func() (any, error) {
		rctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
		defer cancel()

		var wg sync.WaitGroup
		if needWBI {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key, err := s.fetchBilibiliWBIKey(rctx, cli, baseHeaders)
				bs.mu.Lock()
				if err == nil && key != "" {
					bs.mixinKey = key
					bs.wbiExpires = time.Now().Add(12 * time.Hour)
					bs.wbiFailUntil = time.Time{}
				} else {
					bs.wbiFailUntil = time.Now().Add(2 * time.Minute)
				}
				bs.mu.Unlock()
			}()
		}
		if needBuvid {
			wg.Add(1)
			go func() {
				defer wg.Done()
				nb3, nb4, err := s.fetchBilibiliBuvid(rctx, cli, baseHeaders)
				bs.mu.Lock()
				if err == nil && nb3 != "" {
					bs.buvid3 = nb3
					bs.buvid4 = nb4
					bs.buvidExpires = time.Now().Add(24 * time.Hour)
					bs.buvidFailUntil = time.Time{}
				} else {
					bs.buvidFailUntil = time.Now().Add(5 * time.Minute)
				}
				bs.mu.Unlock()
			}()
		}
		wg.Wait()
		return nil, nil
	})

	bs.mu.RLock()
	mk = bs.mixinKey
	b3, b4 = bs.buvid3, bs.buvid4
	bs.mu.RUnlock()
	return mk, formatBuvidCookie(b3, b4)
}

func (s *Service) fetchBilibiliWBIKey(ctx context.Context, cli *http.Client, headers map[string]string) (string, error) {
	body, _, code, err := netutil.FetchLimited(ctx, cli, s.bilibiliAPIBase+"/x/web-interface/nav", 512<<10, headers)
	if err != nil || code != 200 {
		return "", fmt.Errorf("wbi nav http %d: %v", code, err)
	}
	var resp struct {
		Data struct {
			WbiImg struct {
				ImgURL string `json:"img_url"`
				SubURL string `json:"sub_url"`
			} `json:"wbi_img"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", err
	}
	imgKey := extractWBIKeyStem(resp.Data.WbiImg.ImgURL)
	subKey := extractWBIKeyStem(resp.Data.WbiImg.SubURL)
	if imgKey == "" || subKey == "" {
		return "", ErrEmptyContent
	}
	return deriveWBIMixinKey(imgKey, subKey), nil
}

func (s *Service) fetchBilibiliBuvid(ctx context.Context, cli *http.Client, headers map[string]string) (string, string, error) {
	body, _, code, err := netutil.FetchLimited(ctx, cli, s.bilibiliAPIBase+"/x/frontend/finger/spi", 256<<10, headers)
	if err != nil || code != 200 {
		return "", "", fmt.Errorf("spi http %d: %v", code, err)
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			B3 string `json:"b_3"`
			B4 string `json:"b_4"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 || resp.Data.B3 == "" {
		return "", "", ErrEmptyContent
	}
	return strings.TrimSpace(resp.Data.B3), strings.TrimSpace(resp.Data.B4), nil
}

func extractWBIKeyStem(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	if u, err := url.Parse(rawURL); err == nil && u.Path != "" {
		rawURL = u.Path
	}
	base := path.Base(rawURL)
	if dot := strings.LastIndexByte(base, '.'); dot > 0 {
		base = base[:dot]
	}
	return strings.TrimSpace(base)
}

func deriveWBIMixinKey(imgKey, subKey string) string {
	orig := imgKey + subKey
	if len(orig) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(32)
	for i := 0; i < 32; i++ {
		idx := biliMixinKeyEncTab[i]
		if idx < len(orig) {
			b.WriteByte(orig[idx])
		}
	}
	return b.String()
}

// signWBIParams computes Bilibili WBI query parameters (wts + w_rid) sorted by ASCII key.
func signWBIParams(params url.Values, mixinKey string, now time.Time) string {
	if mixinKey == "" {
		return params.Encode()
	}
	keys := make([]string, 0, len(params)+1)
	vals := make(map[string]string, len(params)+1)
	for k, vs := range params {
		if len(vs) == 0 {
			continue
		}
		vals[k] = sanitizeWBIValue(vs[0])
		keys = append(keys, k)
	}
	vals["wts"] = strconv.FormatInt(now.Unix(), 10)
	if _, exists := params["wts"]; !exists {
		keys = append(keys, "wts")
	}
	sort.Strings(keys)

	var q strings.Builder
	for i, k := range keys {
		if i > 0 {
			q.WriteByte('&')
		}
		q.WriteString(url.QueryEscape(k))
		q.WriteByte('=')
		q.WriteString(url.QueryEscape(vals[k]))
	}
	// Go's url.QueryEscape encodes spaces as '+', while Bilibili WBI expects '%20'.
	encoded := strings.ReplaceAll(q.String(), "+", "%20")
	sum := md5.Sum([]byte(encoded + mixinKey))
	return encoded + "&w_rid=" + hex.EncodeToString(sum[:])
}

func sanitizeWBIValue(v string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '!', '\'', '(', ')', '*':
			return -1
		default:
			return r
		}
	}, v)
}

func formatBuvidCookie(b3, b4 string) string {
	b3 = strings.TrimSpace(b3)
	b4 = strings.TrimSpace(b4)
	if b3 == "" {
		return ""
	}
	if b4 != "" {
		return "buvid3=" + b3 + "; buvid4=" + b4
	}
	return "buvid3=" + b3
}

func mergeBilibiliCookie(userCookie, guestBuvid string) string {
	uc := strings.TrimSpace(userCookie)
	gb := strings.TrimSpace(guestBuvid)
	if uc == "" {
		return gb
	}
	if gb == "" || strings.Contains(strings.ToLower(uc), "buvid3=") {
		return uc
	}
	return strings.TrimRight(uc, "; ") + "; " + gb
}

func normalizeBiliAssetURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		return "https:" + raw
	}
	if strings.HasPrefix(raw, "http://") {
		if u, err := url.Parse(raw); err == nil {
			h := strings.ToLower(u.Hostname())
			if strings.HasSuffix(h, ".hdslb.com") || strings.HasSuffix(h, ".bilibili.com") {
				return "https://" + strings.TrimPrefix(raw, "http://")
			}
		}
	}
	return raw
}

func parseBilibiliPageParam(u *url.URL) int {
	if u == nil {
		return 1
	}
	pStr := strings.TrimSpace(u.Query().Get("p"))
	if pStr == "" {
		return 1
	}
	p, err := strconv.Atoi(pStr)
	if err != nil || p < 1 {
		return 1
	}
	return p
}

// parseStartTimeSeconds parses ?t=125 or ?t=2m5s or ?start_progress=125000 into whole seconds.
func parseStartTimeSeconds(rawURL string) int {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return 0
	}
	q := u.Query()
	if t := strings.TrimSpace(q.Get("t")); t != "" {
		if f, err := strconv.ParseFloat(t, 64); err == nil && f > 0 {
			return int(f)
		}
		if m := biliHMSTimeRe.FindStringSubmatch(t); len(m) == 4 && (m[1] != "" || m[2] != "" || m[3] != "") {
			h, _ := strconv.Atoi(m[1])
			mn, _ := strconv.Atoi(m[2])
			sc, _ := strconv.Atoi(m[3])
			return h*3600 + mn*60 + sc
		}
	}
	if sp := strings.TrimSpace(q.Get("start_progress")); sp != "" {
		if ms, err := strconv.Atoi(sp); err == nil && ms >= 1000 {
			return ms / 1000
		}
	}
	return 0
}

// --- Top-Level Bilibili Dispatcher ---

func (s *Service) extractBilibili(ctx context.Context, cli *http.Client, u *url.URL, targetURL string, deep bool, cookie string) (*Result, error) {
	headers := map[string]string{
		"Referer": "https://www.bilibili.com/",
		"Origin":  "https://www.bilibili.com",
	}
	if strings.TrimSpace(cookie) != "" {
		headers["Cookie"] = strings.TrimSpace(cookie)
	}

	host := strings.ToLower(u.Hostname())

	// 1. Live room: live.bilibili.com/<room_id>
	if host == "live.bilibili.com" {
		if m := biliLiveRe.FindStringSubmatch(u.Path); len(m) >= 2 {
			if res, err := s.extractBilibiliLive(ctx, cli, m[1], headers); err == nil {
				return res, nil
			}
		}
	}

	// 2. Bangumi / PGC: /bangumi/play/ep<id> or /bangumi/play/ss<id>
	if m := biliBangumiRe.FindStringSubmatch(u.Path); len(m) >= 3 {
		if res, err := s.extractBilibiliBangumi(ctx, cli, strings.ToLower(m[1]), m[2], headers); err == nil {
			return res, nil
		}
	}

	// 3. Video: BV or av (including /festival/...?bvid=BV...)
	bvid := bvidRe.FindString(targetURL)
	var aid string
	if bvid == "" {
		if m := avidRe.FindStringSubmatch(u.Path); len(m) >= 2 {
			aid = m[1]
		}
	}
	if bvid != "" || aid != "" {
		pageNum := parseBilibiliPageParam(u)
		return s.extractBilibiliVideo(ctx, cli, bvid, aid, pageNum, deep, headers, cookie)
	}

	// 4. Dynamic / Opus
	if host == "t.bilibili.com" || strings.Contains(strings.ToLower(u.Path), "/opus/") {
		if m := biliOpusRe.FindStringSubmatch(u.Path); len(m) >= 2 {
			if res, err := s.extractBilibiliOpus(ctx, cli, m[1], headers, cookie); err == nil {
				return res, nil
			}
		}
	}

	// 5. Article (cv...)
	if m := biliArticleRe.FindStringSubmatch(u.Path); len(m) >= 2 {
		if res, err := s.extractBilibiliArticle(ctx, cli, m[1], deep, headers); err == nil {
			return res, nil
		}
	}

	return s.extractGenericHTML(ctx, cli, targetURL, deep)
}

// --- 1. Bilibili Video Extractor (Concurrent View + WBI AI Summary + Subtitles + Pinned/Top Replies) ---

type biliVideoViewData struct {
	Aid      int64  `json:"aid"`
	Cid      int64  `json:"cid"`
	Bvid     string `json:"bvid"`
	Title    string `json:"title"`
	Desc     string `json:"desc"`
	Dynamic  string `json:"dynamic"`
	Pic      string `json:"pic"`
	Duration int    `json:"duration"`
	PubDate  int64  `json:"pubdate"`
	TName    string `json:"tname"`
	Owner    struct {
		Mid  int64  `json:"mid"`
		Name string `json:"name"`
	} `json:"owner"`
	Staff []struct {
		Title string `json:"title"`
		Name  string `json:"name"`
	} `json:"staff"`
	Stat struct {
		View     int `json:"view"`
		Danmaku  int `json:"danmaku"`
		Like     int `json:"like"`
		Coin     int `json:"coin"`
		Favorite int `json:"favorite"`
		Share    int `json:"share"`
		Reply    int `json:"reply"`
	} `json:"stat"`
	ArgueInfo struct {
		ArgueMsg string `json:"argue_msg"`
	} `json:"argue_info"`
	UgcSeason struct {
		Title string `json:"title"`
	} `json:"ugc_season"`
	Pages []struct {
		Cid      int64  `json:"cid"`
		Page     int    `json:"page"`
		Part     string `json:"part"`
		Duration int    `json:"duration"`
	} `json:"pages"`
}

func (s *Service) extractBilibiliVideo(
	ctx context.Context,
	cli *http.Client,
	bvid, aid string,
	pageNum int,
	deep bool,
	baseHeaders map[string]string,
	userCookie string,
) (*Result, error) {
	// Fetch video metadata concurrently with anonymous WBI/buvid session warmup so cold start adds 0ms latency.
	apiURL := s.bilibiliAPIBase + "/x/web-interface/view?"
	if bvid != "" {
		apiURL += "bvid=" + url.QueryEscape(bvid)
	} else {
		apiURL += "aid=" + url.QueryEscape(aid)
	}

	var (
		viewBody []byte
		viewCode int
		viewErr  error
		mixinKey string
		guestCK  string
	)
	var initWG sync.WaitGroup
	initWG.Add(2)
	go func() {
		defer initWG.Done()
		viewBody, _, viewCode, viewErr = netutil.FetchLimited(ctx, cli, apiURL, 2<<20, baseHeaders)
	}()
	go func() {
		defer initWG.Done()
		mixinKey, guestCK = s.ensureBilibiliSession(ctx, cli, baseHeaders)
	}()
	initWG.Wait()

	if viewErr != nil {
		return nil, viewErr
	}
	if viewCode >= 400 {
		return nil, fmt.Errorf("bilibili view HTTP %d", viewCode)
	}

	var view struct {
		Code    int               `json:"code"`
		Message string            `json:"message"`
		Data    biliVideoViewData `json:"data"`
	}
	if err := json.Unmarshal(viewBody, &view); err != nil {
		return nil, err
	}
	if view.Code != 0 || strings.TrimSpace(view.Data.Title) == "" {
		return nil, fmt.Errorf("bilibili view error code=%d msg=%s", view.Code, view.Message)
	}

	d := view.Data
	headers := cloneMap(baseHeaders)
	if mergedCK := mergeBilibiliCookie(userCookie, guestCK); mergedCK != "" {
		headers["Cookie"] = mergedCK
	}

	// Resolve target page (supports ?p=N) for accurate cid, duration, and title.
	cid := d.Cid
	effectiveDuration := d.Duration
	selectedPart := ""
	if pageNum > 1 && pageNum <= len(d.Pages) {
		p := d.Pages[pageNum-1]
		if p.Cid > 0 {
			cid = p.Cid
		}
		if p.Duration > 0 {
			effectiveDuration = p.Duration
		}
		selectedPart = strings.TrimSpace(p.Part)
	} else if cid == 0 && len(d.Pages) > 0 {
		cid = d.Pages[0].Cid
	}

	canon := "https://www.bilibili.com/video/" + d.Bvid
	if d.Bvid == "" && d.Aid > 0 {
		canon = fmt.Sprintf("https://www.bilibili.com/video/av%d", d.Aid)
	}
	if pageNum > 1 {
		canon += fmt.Sprintf("?p=%d", pageNum)
	}

	// Author string (include co-creators when joint upload).
	author := strings.TrimSpace(d.Owner.Name)
	if len(d.Staff) > 1 {
		var co []string
		for i, st := range d.Staff {
			if i >= 4 {
				break
			}
			nm := strings.TrimSpace(st.Name)
			if nm != "" {
				co = append(co, nm)
			}
		}
		if len(co) > 1 {
			author = strings.Join(co, " / ")
			if len(d.Staff) > 4 {
				author += fmt.Sprintf(" 等%d人", len(d.Staff))
			}
		}
	}

	// Rich stats line (category, duration, views, danmaku, likes, coins, favorites).
	statSegs := make([]string, 0, 7)
	if d.TName != "" {
		statSegs = append(statSegs, d.TName)
	}
	statSegs = append(statSegs, "时长 "+formatDuration(effectiveDuration))
	statSegs = append(statSegs, formatCountCN(d.Stat.View)+"播放")
	if d.Stat.Danmaku > 0 {
		statSegs = append(statSegs, formatCountCN(d.Stat.Danmaku)+"弹幕")
	}
	statSegs = append(statSegs, formatCountCN(d.Stat.Like)+"赞")
	statSegs = append(statSegs, formatCountCN(d.Stat.Coin)+"币")
	if d.Stat.Favorite > 0 {
		statSegs = append(statSegs, formatCountCN(d.Stat.Favorite)+"藏")
	}
	stats := strings.Join(statSegs, " · ")

	pubStr := ""
	if d.PubDate > 0 {
		pubStr = time.Unix(d.PubDate, 0).Format("2006-01-02")
	}

	// Concurrent enrichment: AI Conclusion + Player Subtitles/ViewPoints + Pinned/Top Comments.
	subTimeout := 3200 * time.Millisecond
	if deep {
		subTimeout = 8 * time.Second
	}
	subCtx, subCancel := context.WithTimeout(ctx, subTimeout)
	defer subCancel()

	var (
		aiSummary     string
		aiOutline     string
		viewPoints    []string
		subtitlePlain string
		subtitleTS    string
		subtitleLang  string
		pinnedComment string
		topComments   []string
		topRawLines   []string
	)

	var eg errgroup.Group
	// 1. Official Bilibili AI video summary (requires WBI signing; works anonymously on most popular videos).
	if cid > 0 && d.Bvid != "" && mixinKey != "" {
		eg.Go(func() error {
			aiSummary, aiOutline = s.fetchBilibiliAIConclusion(subCtx, cli, d.Bvid, cid, d.Owner.Mid, mixinKey, headers)
			return nil
		})
	}
	// 2. Player subtitles (language-prioritized + sparse timestamps) & chapter view_points.
	if cid > 0 && d.Bvid != "" {
		eg.Go(func() error {
			viewPoints, subtitlePlain, subtitleTS, subtitleLang = s.fetchBilibiliSubtitlesAndChapters(subCtx, cli, d.Bvid, cid, mixinKey, headers)
			return nil
		})
	}
	// 3. Pinned UP comment + filtered top replies.
	if d.Aid > 0 {
		eg.Go(func() error {
			limit := 3
			if deep {
				limit = 5
			}
			pinnedComment, topComments, topRawLines = s.fetchBilibiliReplies(subCtx, cli, d.Aid, limit, mixinKey, headers)
			return nil
		})
	}
	_ = eg.Wait()

	title := strings.TrimSpace(d.Title)
	if selectedPart != "" && !strings.Contains(title, selectedPart) {
		title = fmt.Sprintf("%s [P%d: %s]", title, pageNum, selectedPart)
	}
	argueMsg := collapseSpaces(d.ArgueInfo.ArgueMsg)
	desc := collapseSpaces(d.Desc)
	if desc == "-" {
		desc = ""
	}
	dynText := collapseSpaces(d.Dynamic)
	if dynText == desc || dynText == "-" {
		dynText = ""
	}

	// Build detailed RawText for read_source (preserving full timestamped subtitles so query rerank works!).
	var raw strings.Builder
	fmt.Fprintf(&raw, "Bilibili %s\nTitle: %s\nUP: %s | %s", d.Bvid, title, author, stats)
	if pubStr != "" {
		fmt.Fprintf(&raw, " | Published: %s", pubStr)
	}
	raw.WriteByte('\n')
	if d.UgcSeason.Title != "" {
		fmt.Fprintf(&raw, "Collection: %s\n", strings.TrimSpace(d.UgcSeason.Title))
	}
	if argueMsg != "" {
		fmt.Fprintf(&raw, "Official Notice: %s\n", argueMsg)
	}
	if desc != "" {
		fmt.Fprintf(&raw, "Description: %s\n", desc)
	}
	if dynText != "" {
		fmt.Fprintf(&raw, "Author Note: %s\n", dynText)
	}
	if len(d.Pages) > 1 {
		raw.WriteString("Pages:\n")
		for i, p := range d.Pages {
			if i >= 15 {
				fmt.Fprintf(&raw, "  ...(total %d pages)\n", len(d.Pages))
				break
			}
			marker := ""
			if p.Page == pageNum {
				marker = " [CURRENT]"
			}
			fmt.Fprintf(&raw, "  P%d (%s): %s%s\n", p.Page, formatDuration(p.Duration), p.Part, marker)
		}
	}
	if len(viewPoints) > 0 {
		raw.WriteString("Chapters:\n")
		for _, vp := range viewPoints {
			raw.WriteString("  - " + vp + "\n")
		}
	}
	if aiSummary != "" || aiOutline != "" {
		raw.WriteString("\nAI Summary:\n")
		if aiSummary != "" {
			raw.WriteString("  " + aiSummary + "\n")
		}
		if aiOutline != "" {
			raw.WriteString(aiOutline)
		}
	}
	if pinnedComment != "" {
		fmt.Fprintf(&raw, "\nUP Pinned Comment:\n  %s\n", pinnedComment)
	}
	if len(topRawLines) > 0 {
		raw.WriteString("\nTop Comments:\n")
		for _, line := range topRawLines {
			raw.WriteString("  - " + line + "\n")
		}
	}
	if subtitleTS != "" {
		langNote := ""
		if subtitleLang != "" {
			langNote = " (" + subtitleLang + ")"
		}
		// Keep full timestamped transcript up to 24000 runes so read_source's semantic rerank
		// (>5000 runes) or head/mid/tail sampling has access to the entire video, not just the intro.
		fmt.Fprintf(&raw, "\nSubtitles%s:\n%s\n", langNote, clipRunesCompact(subtitleTS, 24000))
	}

	// Build concise Extra & Summary for fast context / backward compatibility.
	var extra strings.Builder
	if len(d.Pages) > 1 {
		var pNames []string
		for i, p := range d.Pages {
			if i >= 5 {
				break
			}
			pNames = append(pNames, fmt.Sprintf("P%d %s", p.Page, p.Part))
		}
		extra.WriteString("分P: " + strings.Join(pNames, " / ") + "\n")
	}
	if len(viewPoints) > 0 {
		vpShow := viewPoints
		if len(vpShow) > 5 {
			vpShow = vpShow[:5]
		}
		extra.WriteString("章节: " + strings.Join(vpShow, " / ") + "\n")
	}
	if aiSummary != "" {
		extra.WriteString("AI总结: " + clipRunesCompact(aiSummary, 220) + "\n")
	} else if subtitlePlain != "" {
		extra.WriteString("字幕摘录: " + sampleTextHeadMidTail(subtitlePlain, 280) + "\n")
	}
	if pinnedComment != "" {
		extra.WriteString("UP置顶: " + clipRunesCompact(pinnedComment, 140) + "\n")
	}
	for i, tc := range topComments {
		if i >= 2 {
			break
		}
		extra.WriteString(tc + "\n")
	}

	summary := ""
	switch {
	case aiSummary != "":
		summary = clipRunesCompact(aiSummary, 240)
	case desc != "":
		summary = clipRunesCompact(desc, 220)
	case dynText != "":
		summary = clipRunesCompact(dynText, 220)
	}
	if extraTxt := strings.TrimSpace(extra.String()); extraTxt != "" {
		if summary != "" {
			if aiSummary == "" {
				summary += " | " + collapseSpaces(clipRunesCompact(extraTxt, 240))
			}
		} else {
			summary = collapseSpaces(clipRunesCompact(extraTxt, 280))
		}
	}

	topJoined := strings.Join(topComments, "\n")
	hasTranscript := aiSummary != "" || subtitleTS != ""

	return &Result{
		Platform:      "bilibili",
		CanonicalURL:  canon,
		Title:         title,
		Author:        author,
		PublishedAt:   pubStr,
		Stats:         stats,
		Summary:       summary,
		Extra:         strings.TrimSpace(extra.String()),
		RawText:       strings.TrimSpace(raw.String()),
		CoverURL:      normalizeBiliAssetURL(d.Pic),
		AISummary:     aiSummary,
		PinnedComment: pinnedComment,
		TopComments:   topJoined,
		ArgueMsg:      argueMsg,
		DurationSec:   effectiveDuration,
		VideoBvid:     d.Bvid,
		VideoCid:      cid,
		HasTranscript: hasTranscript,
	}, nil
}

func (s *Service) fetchBilibiliAIConclusion(
	ctx context.Context,
	cli *http.Client,
	bvid string,
	cid, upMid int64,
	mixinKey string,
	headers map[string]string,
) (summary string, outline string) {
	params := url.Values{}
	params.Set("bvid", bvid)
	params.Set("cid", strconv.FormatInt(cid, 10))
	if upMid > 0 {
		params.Set("up_mid", strconv.FormatInt(upMid, 10))
	}
	signed := signWBIParams(params, mixinKey, time.Now())
	u := s.bilibiliAPIBase + "/x/web-interface/view/conclusion/get?" + signed
	body, _, code, err := netutil.FetchLimited(ctx, cli, u, 512<<10, headers)
	if err != nil || code != 200 {
		return "", ""
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			Code        int `json:"code"` // 0 = ok, 1 = unsupported/no speech, -1 = not generated
			ModelResult struct {
				Summary string `json:"summary"`
				Outline []struct {
					Title       string `json:"title"`
					Timestamp   int    `json:"timestamp"`
					PartOutline []struct {
						Timestamp int    `json:"timestamp"`
						Content   string `json:"content"`
					} `json:"part_outline"`
				} `json:"outline"`
			} `json:"model_result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", ""
	}
	if resp.Code != 0 {
		if resp.Code == -352 || resp.Code == -403 || resp.Code == -412 {
			s.bili.warnThrottled(s, "ai_conclusion_risk", "bilibili ai conclusion risk-limited", "code", resp.Code, "bvid", bvid)
		}
		return "", ""
	}
	if resp.Data.Code != 0 {
		return "", ""
	}
	summary = collapseSpaces(resp.Data.ModelResult.Summary)
	if len(resp.Data.ModelResult.Outline) > 0 {
		var ob strings.Builder
		for i, sec := range resp.Data.ModelResult.Outline {
			if i >= 12 {
				break
			}
			secTitle := collapseSpaces(sec.Title)
			if secTitle != "" {
				fmt.Fprintf(&ob, "  [%s] %s\n", formatDuration(sec.Timestamp), secTitle)
			}
			for j, pt := range sec.PartOutline {
				if j >= 6 {
					break
				}
				if c := collapseSpaces(pt.Content); c != "" {
					fmt.Fprintf(&ob, "    - [%s] %s\n", formatDuration(pt.Timestamp), c)
				}
			}
		}
		outline = ob.String()
	}
	return summary, outline
}

type biliPlayerResp struct {
	Code int `json:"code"`
	Data struct {
		Subtitle struct {
			Subtitles []struct {
				Lan         string `json:"lan"`
				LanDoc      string `json:"lan_doc"`
				SubtitleURL string `json:"subtitle_url"`
			} `json:"subtitles"`
		} `json:"subtitle"`
		ViewPoints []struct {
			From    int    `json:"from"`
			To      int    `json:"to"`
			Content string `json:"content"`
		} `json:"view_points"`
	} `json:"data"`
}

func (s *Service) fetchBilibiliSubtitlesAndChapters(
	ctx context.Context,
	cli *http.Client,
	bvid string,
	cid int64,
	mixinKey string,
	headers map[string]string,
) (viewPoints []string, plainText string, tsText string, lang string) {
	fetchPlayer := func(useWBI bool) (*biliPlayerResp, bool) {
		var pURL string
		if useWBI && mixinKey != "" {
			params := url.Values{}
			params.Set("bvid", bvid)
			params.Set("cid", strconv.FormatInt(cid, 10))
			pURL = s.bilibiliAPIBase + "/x/player/wbi/v2?" + signWBIParams(params, mixinKey, time.Now())
		} else {
			pURL = fmt.Sprintf("%s/x/player/v2?bvid=%s&cid=%d", s.bilibiliAPIBase, url.QueryEscape(bvid), cid)
		}
		pBody, _, pcode, perr := netutil.FetchLimited(ctx, cli, pURL, 1<<20, headers)
		if perr != nil || pcode != 200 {
			return nil, false
		}
		var pl biliPlayerResp
		if json.Unmarshal(pBody, &pl) != nil {
			return nil, false
		}
		if pl.Code != 0 {
			if pl.Code == -352 || pl.Code == -403 || pl.Code == -412 {
				s.bili.warnThrottled(s, "player_risk", "bilibili player api risk-limited", "code", pl.Code, "bvid", bvid)
			}
			return nil, false
		}
		return &pl, true
	}

	var pl *biliPlayerResp
	if mixinKey != "" {
		pl, _ = fetchPlayer(true)
	}
	if pl == nil {
		pl, _ = fetchPlayer(false)
	}
	if pl == nil {
		return nil, "", "", ""
	}

	for i, vp := range pl.Data.ViewPoints {
		if i >= 12 {
			break
		}
		if c := collapseSpaces(vp.Content); c != "" {
			viewPoints = append(viewPoints, fmt.Sprintf("[%s] %s", formatDuration(vp.From), c))
		}
	}

	if len(pl.Data.Subtitle.Subtitles) == 0 {
		return viewPoints, "", "", ""
	}

	// Pick highest-priority subtitle track (Simplified Chinese > AI Chinese > Traditional > English > first).
	bestIdx := -1
	bestScore := -1
	for i, st := range pl.Data.Subtitle.Subtitles {
		if strings.TrimSpace(st.SubtitleURL) == "" {
			continue
		}
		sc := subtitleLangPriority(st.Lan, st.LanDoc)
		if sc > bestScore {
			bestScore = sc
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		return viewPoints, "", "", ""
	}

	chosen := pl.Data.Subtitle.Subtitles[bestIdx]
	subURL := normalizeBiliAssetURL(chosen.SubtitleURL)
	if !strings.HasPrefix(subURL, "http") {
		return viewPoints, "", "", ""
	}
	sBody, _, scode, serr := netutil.FetchLimited(ctx, cli, subURL, 3<<20, nil)
	if serr != nil || scode != 200 {
		return viewPoints, "", "", ""
	}
	var sj struct {
		Body []struct {
			From    float64 `json:"from"`
			To      float64 `json:"to"`
			Content string  `json:"content"`
		} `json:"body"`
	}
	if json.Unmarshal(sBody, &sj) != nil || len(sj.Body) == 0 {
		return viewPoints, "", "", ""
	}

	var plainParts []string
	var tsBuilder strings.Builder
	lastMarkerSec := -999
	for _, seg := range sj.Body {
		txt := collapseSpaces(seg.Content)
		if txt == "" {
			continue
		}
		plainParts = append(plainParts, txt)
		curSec := int(seg.From)
		if curSec < 0 {
			curSec = 0
		}
		if lastMarkerSec < 0 || curSec-lastMarkerSec >= 45 {
			if tsBuilder.Len() > 0 {
				tsBuilder.WriteByte('\n')
			}
			fmt.Fprintf(&tsBuilder, "[%s] %s", formatDuration(curSec), txt)
			lastMarkerSec = curSec
		} else {
			tsBuilder.WriteByte(' ')
			tsBuilder.WriteString(txt)
		}
	}
	langLabel := strings.TrimSpace(chosen.LanDoc)
	if langLabel == "" {
		langLabel = strings.TrimSpace(chosen.Lan)
	}
	return viewPoints, strings.Join(plainParts, " "), strings.TrimSpace(tsBuilder.String()), langLabel
}

func subtitleLangPriority(lan, lanDoc string) int {
	l := strings.ToLower(strings.TrimSpace(lan))
	d := strings.ToLower(strings.TrimSpace(lanDoc))
	switch {
	case l == "zh-cn" || l == "zh-hans":
		return 100
	case l == "ai-zh":
		return 90
	case l == "zh-hk" || l == "zh-tw" || l == "zh-hant":
		return 80
	case strings.HasPrefix(l, "zh") || strings.Contains(d, "中文"):
		return 75
	case l == "en-us" || l == "en":
		return 50
	case l == "ai-en" || strings.HasPrefix(l, "en"):
		return 40
	default:
		return 10
	}
}

type biliReplyItem struct {
	Member  struct{ Uname string `json:"uname"` } `json:"member"`
	Content struct{ Message string `json:"message"` } `json:"content"`
	Like    int `json:"like"`
}

type biliReplyResp struct {
	Code int `json:"code"`
	Data struct {
		Top struct {
			Upper *biliReplyItem `json:"upper"`
		} `json:"top"`
		Upper struct {
			Top *biliReplyItem `json:"top"`
		} `json:"upper"`
		Replies []biliReplyItem `json:"replies"`
	} `json:"data"`
}

func (s *Service) fetchBilibiliReplies(
	ctx context.Context,
	cli *http.Client,
	aid int64,
	limit int,
	mixinKey string,
	headers map[string]string,
) (pinned string, compact []string, rawLines []string) {
	fetchRep := func(useWBI bool) (*biliReplyResp, bool) {
		var cURL string
		if useWBI && mixinKey != "" {
			params := url.Values{}
			params.Set("type", "1")
			params.Set("oid", strconv.FormatInt(aid, 10))
			params.Set("mode", "3") // hot comments
			params.Set("pagination_str", `{"offset":""}`)
			params.Set("plat", "1")
			params.Set("web_location", "1315875")
			cURL = s.bilibiliAPIBase + "/x/v2/reply/wbi/main?" + signWBIParams(params, mixinKey, time.Now())
		} else {
			cURL = fmt.Sprintf("%s/x/v2/reply?type=1&oid=%d&sort=2&ps=%d", s.bilibiliAPIBase, aid, limit+3)
		}
		cBody, _, ccode, cerr := netutil.FetchLimited(ctx, cli, cURL, 1<<20, headers)
		if cerr != nil || ccode != 200 {
			return nil, false
		}
		var cr biliReplyResp
		if json.Unmarshal(cBody, &cr) != nil {
			return nil, false
		}
		if cr.Code != 0 {
			if cr.Code == -352 || cr.Code == -403 || cr.Code == -412 {
				s.bili.warnThrottled(s, "reply_risk", "bilibili reply api risk-limited", "code", cr.Code, "aid", aid)
			}
			return nil, false
		}
		return &cr, true
	}

	var cr *biliReplyResp
	if mixinKey != "" {
		cr, _ = fetchRep(true)
	}
	if cr == nil {
		cr, _ = fetchRep(false)
	}
	if cr == nil {
		return "", nil, nil
	}

	topItem := cr.Data.Top.Upper
	if topItem == nil {
		topItem = cr.Data.Upper.Top
	}
	if topItem != nil {
		if msg := collapseSpaces(topItem.Content.Message); msg != "" {
			pinned = msg
		}
	}

	for _, r := range cr.Data.Replies {
		msgTxt := collapseSpaces(r.Content.Message)
		if msgTxt == "" || isLowValueBiliComment(msgTxt) {
			continue
		}
		if pinned != "" && msgTxt == pinned {
			continue
		}
		rawLines = append(rawLines, fmt.Sprintf("%s (♥%d): %s", r.Member.Uname, r.Like, msgTxt))
		if len(compact) < limit {
			compact = append(compact, fmt.Sprintf("热评(%s): %s", r.Member.Uname, clipRunesCompact(msgTxt, 95)))
		}
		if len(rawLines) >= limit {
			break
		}
	}
	return pinned, compact, rawLines
}

func isLowValueBiliComment(s string) bool {
	trimmed := strings.TrimSpace(s)
	runes := []rune(trimmed)
	if len(runes) == 0 {
		return true
	}
	// Strip [doge]-style bracket emotes to measure actual verbal content.
	noEmote := strings.TrimSpace(biliEmoteBracketRe.ReplaceAllString(trimmed, ""))
	if len([]rune(noEmote)) <= 1 {
		return true
	}
	if len([]rune(noEmote)) <= 6 {
		low := strings.ToLower(noEmote)
		for _, noise := range []string{"第一", "沙发", "前排", "热乎", "热乎的", "打卡", "来了", "围观", "好耶", "666", "草", "哈哈", "哈哈哈"} {
			if low == noise {
				return true
			}
		}
	}
	return false
}

var biliEmoteBracketRe = regexp.MustCompile(`\[[^\[\]]{1,12}\]`)

// applyBilibiliTimestampFocus annotates a returned Result copy when the caller requested a ?t= jump point.
func applyBilibiliTimestampFocus(res *Result, startSec int) {
	if res == nil || res.Platform != "bilibili" || startSec <= 0 {
		return
	}
	tsTag := formatDuration(startSec)
	excerpt := extractTranscriptWindow(res.RawText, startSec, 20, 90)
	if excerpt != "" {
		note := fmt.Sprintf("[空降定位 %s] %s", tsTag, clipRunesCompact(excerpt, 240))
		if res.Summary != "" {
			res.Summary = note + " | " + res.Summary
		} else {
			res.Summary = note
		}
		res.RawText = fmt.Sprintf("Requested Timestamp Focus (%s):\n  %s\n\n%s", tsTag, clipRunesCompact(excerpt, 900), res.RawText)
	} else {
		note := fmt.Sprintf("[空降定位 %s]", tsTag)
		if res.Summary != "" && !strings.Contains(res.Summary, note) {
			res.Summary = note + " " + res.Summary
		}
	}
}

// extractTranscriptWindow finds timestamped subtitle lines around targetSec within [targetSec-before, targetSec+after].
func extractTranscriptWindow(rawText string, targetSec, beforeSec, afterSec int) string {
	idx := strings.Index(rawText, "\nSubtitles")
	if idx < 0 {
		return ""
	}
	subBlock := rawText[idx:]
	lines := strings.Split(subBlock, "\n")
	lo := targetSec - beforeSec
	if lo < 0 {
		lo = 0
	}
	hi := targetSec + afterSec

	var matched []string
	var closestLine string
	closestDiff := 1 << 30
	for _, line := range lines {
		m := biliTSLineRe.FindStringSubmatch(line)
		if len(m) < 3 {
			continue
		}
		sec := parseTimestampMatch(m)
		diff := sec - targetSec
		if diff < 0 {
			diff = -diff
		}
		if diff < closestDiff {
			closestDiff = diff
			closestLine = strings.TrimSpace(line)
		}
		if sec >= lo && sec <= hi {
			matched = append(matched, strings.TrimSpace(line))
		}
	}
	if len(matched) > 0 {
		return strings.Join(matched, " ")
	}
	if closestDiff <= 120 && closestLine != "" {
		return closestLine
	}
	return ""
}

func parseTimestampMatch(m []string) int {
	a, _ := strconv.Atoi(m[1])
	b, _ := strconv.Atoi(m[2])
	if len(m) >= 4 && m[3] != "" {
		c, _ := strconv.Atoi(m[3])
		return a*3600 + b*60 + c
	}
	return a*60 + b
}

// sampleTextHeadMidTail samples beginning, middle, and ending segments so long transcripts don't lose conclusions.
func sampleTextHeadMidTail(s string, maxRunes int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= maxRunes || maxRunes < 90 {
		return clipRunesCompact(s, maxRunes)
	}
	headLen := maxRunes * 30 / 100
	midLen := maxRunes * 35 / 100
	tailLen := maxRunes - headLen - midLen - 12
	if tailLen < 20 {
		return clipRunesCompact(s, maxRunes)
	}
	midStart := (len(r) - midLen) / 2
	tailStart := len(r) - tailLen
	return strings.TrimSpace(string(r[:headLen])) + " ... " +
		strings.TrimSpace(string(r[midStart:midStart+midLen])) + " ... " +
		strings.TrimSpace(string(r[tailStart:]))
}

// --- 2. Bilibili Dynamic / Opus Extractor (Original + Repost + Attached Video) ---

type biliDynamicItem struct {
	Modules struct {
		ModuleAuthor struct {
			Name string `json:"name"`
		} `json:"module_author"`
		ModuleDynamic struct {
			Desc struct {
				Text string `json:"text"`
			} `json:"desc"`
			Major struct {
				Type string `json:"type"`
				Opus struct {
					Title   string `json:"title"`
					Summary struct {
						Text string `json:"text"`
					} `json:"summary"`
					Pics []struct {
						URL string `json:"url"`
					} `json:"pics"`
				} `json:"opus"`
				Archive struct {
					Bvid         string `json:"bvid"`
					Title        string `json:"title"`
					Desc         string `json:"desc"`
					Cover        string `json:"cover"`
					DurationText string `json:"duration_text"`
				} `json:"archive"`
				Draw struct {
					Items []struct {
						Src string `json:"src"`
					} `json:"items"`
				} `json:"draw"`
			} `json:"major"`
		} `json:"module_dynamic"`
		ModuleStat struct {
			Like    struct{ Count int `json:"count"` } `json:"like"`
			Comment struct{ Count int `json:"count"` } `json:"comment"`
			Forward struct{ Count int `json:"count"` } `json:"forward"`
		} `json:"module_stat"`
	} `json:"modules"`
	Orig *biliDynamicItem `json:"orig"`
}

func (s *Service) extractBilibiliOpus(ctx context.Context, cli *http.Client, id string, baseHeaders map[string]string, userCookie string) (*Result, error) {
	_, guestCK := s.ensureBilibiliSession(ctx, cli, baseHeaders)
	headers := cloneMap(baseHeaders)
	if mergedCK := mergeBilibiliCookie(userCookie, guestCK); mergedCK != "" {
		headers["Cookie"] = mergedCK
	}

	apiURL := fmt.Sprintf("%s/x/polymer/web-dynamic/v1/detail?id=%s&features=itemOpusStyle", s.bilibiliAPIBase, url.QueryEscape(id))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 2<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("bilibili opus fetch failed: %v (HTTP %d)", err, code)
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			Item biliDynamicItem `json:"item"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 {
		if resp.Code == -352 || resp.Code == -403 {
			s.bili.warnThrottled(s, "opus_risk", "bilibili dynamic api risk-limited", "code", resp.Code, "id", id)
		}
		return nil, ErrEmptyContent
	}

	item := resp.Data.Item
	mods := item.Modules
	title := strings.TrimSpace(mods.ModuleDynamic.Major.Opus.Title)
	text := strings.TrimSpace(mods.ModuleDynamic.Major.Opus.Summary.Text)
	if text == "" {
		text = strings.TrimSpace(mods.ModuleDynamic.Desc.Text)
	}

	var cover string
	var extraParts []string
	if len(mods.ModuleDynamic.Major.Opus.Pics) > 0 {
		cover = normalizeBiliAssetURL(mods.ModuleDynamic.Major.Opus.Pics[0].URL)
		extraParts = append(extraParts, fmt.Sprintf("含%d张图", len(mods.ModuleDynamic.Major.Opus.Pics)))
	} else if len(mods.ModuleDynamic.Major.Draw.Items) > 0 {
		cover = normalizeBiliAssetURL(mods.ModuleDynamic.Major.Draw.Items[0].Src)
		extraParts = append(extraParts, fmt.Sprintf("含%d张图", len(mods.ModuleDynamic.Major.Draw.Items)))
	}
	if arch := mods.ModuleDynamic.Major.Archive; arch.Title != "" {
		if cover == "" {
			cover = normalizeBiliAssetURL(arch.Cover)
		}
		vidNote := fmt.Sprintf("附视频: %s", arch.Title)
		if arch.DurationText != "" {
			vidNote += " (" + arch.DurationText + ")"
		}
		if arch.Bvid != "" {
			vidNote += " " + arch.Bvid
		}
		extraParts = append(extraParts, vidNote)
		if title == "" {
			title = arch.Title
		}
	}

	// Parse reposted original dynamic when present.
	var origBlock string
	if item.Orig != nil {
		om := item.Orig.Modules
		oAuthor := strings.TrimSpace(om.ModuleAuthor.Name)
		oText := strings.TrimSpace(om.ModuleDynamic.Major.Opus.Summary.Text)
		if oText == "" {
			oText = strings.TrimSpace(om.ModuleDynamic.Desc.Text)
		}
		if oText == "" && om.ModuleDynamic.Major.Archive.Title != "" {
			oText = "[视频] " + om.ModuleDynamic.Major.Archive.Title
			if om.ModuleDynamic.Major.Archive.Bvid != "" {
				oText += " (" + om.ModuleDynamic.Major.Archive.Bvid + ")"
			}
		}
		if oText != "" {
			if oAuthor != "" {
				origBlock = fmt.Sprintf("转发自 @%s: %s", oAuthor, collapseSpaces(oText))
			} else {
				origBlock = "转发原动态: " + collapseSpaces(oText)
			}
		}
		if cover == "" {
			if len(om.ModuleDynamic.Major.Opus.Pics) > 0 {
				cover = normalizeBiliAssetURL(om.ModuleDynamic.Major.Opus.Pics[0].URL)
			} else if om.ModuleDynamic.Major.Archive.Cover != "" {
				cover = normalizeBiliAssetURL(om.ModuleDynamic.Major.Archive.Cover)
			}
		}
	}

	if title == "" && text == "" && origBlock == "" {
		return nil, ErrEmptyContent
	}
	if title == "" {
		if text != "" {
			title = clipRunesCompact(collapseSpaces(text), 48)
		} else {
			title = clipRunesCompact(origBlock, 48)
		}
	}

	stats := fmt.Sprintf("%s赞 · %s评 · %s转发",
		formatCountCN(mods.ModuleStat.Like.Count),
		formatCountCN(mods.ModuleStat.Comment.Count),
		formatCountCN(mods.ModuleStat.Forward.Count))
	if len(extraParts) > 0 {
		stats += " · " + strings.Join(extraParts, " · ")
	}

	fullContent := text
	if origBlock != "" {
		if fullContent != "" {
			fullContent += "\n" + origBlock
		} else {
			fullContent = origBlock
		}
	}
	if len(extraParts) > 0 && !strings.Contains(fullContent, extraParts[0]) {
		fullContent = strings.TrimSpace(fullContent + "\n" + strings.Join(extraParts, " | "))
	}

	return &Result{
		Platform:     "bilibili",
		CanonicalURL: "https://www.bilibili.com/opus/" + id,
		Title:        title,
		Author:       strings.TrimSpace(mods.ModuleAuthor.Name),
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(fullContent), 280),
		RawText:      fmt.Sprintf("Bilibili Dynamic %s\nAuthor: %s\nTitle: %s\nStats: %s\nContent:\n%s", id, mods.ModuleAuthor.Name, title, stats, clipRunesCompact(fullContent, 6000)),
		CoverURL:     cover,
	}, nil
}

// --- 3. Bilibili Article (cv...) Extractor (Metadata + Full Body Text) ---

func (s *Service) extractBilibiliArticle(ctx context.Context, cli *http.Client, cvid string, deep bool, headers map[string]string) (*Result, error) {
	infoURL := fmt.Sprintf("%s/x/article/viewinfo?id=%s", s.bilibiliAPIBase, url.QueryEscape(cvid))
	pageURL := fmt.Sprintf("%s/read/cv%s", s.bilibiliWebBase, url.PathEscape(cvid))

	var (
		infoBody []byte
		infoCode int
		infoErr  error
		pageText string
	)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		infoBody, _, infoCode, infoErr = netutil.FetchLimited(ctx, cli, infoURL, 1<<20, headers)
	}()
	go func() {
		defer wg.Done()
		pTimeout := 3 * time.Second
		if deep {
			pTimeout = 6 * time.Second
		}
		pctx, cancel := context.WithTimeout(ctx, pTimeout)
		defer cancel()
		if pBody, ct, pCode, pErr := netutil.FetchLimited(pctx, cli, pageURL, 2<<20, headers); pErr == nil && pCode == 200 && len(pBody) > 0 {
			pageText = extractBilibiliArticleBody(pBody, ct)
		}
	}()
	wg.Wait()

	if infoErr != nil || infoCode >= 400 {
		return nil, fmt.Errorf("bilibili article fetch failed: %v (HTTP %d)", infoErr, infoCode)
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			Title      string   `json:"title"`
			AuthorName string   `json:"author_name"`
			BannerURL  string   `json:"banner_url"`
			ImageURLs  []string `json:"image_urls"`
			Stats      struct {
				View     int `json:"view"`
				Like     int `json:"like"`
				Coin     int `json:"coin"`
				Favorite int `json:"favorite"`
				Reply    int `json:"reply"`
			} `json:"stats"`
		} `json:"data"`
	}
	if err := json.Unmarshal(infoBody, &resp); err != nil || resp.Code != 0 || strings.TrimSpace(resp.Data.Title) == "" {
		return nil, ErrEmptyContent
	}
	d := resp.Data
	stats := fmt.Sprintf("专栏 · %s阅读 · %s赞 · %s币",
		formatCountCN(d.Stats.View), formatCountCN(d.Stats.Like), formatCountCN(d.Stats.Coin))
	if d.Stats.Favorite > 0 {
		stats += " · " + formatCountCN(d.Stats.Favorite) + "藏"
	}
	cover := normalizeBiliAssetURL(d.BannerURL)
	if cover == "" && len(d.ImageURLs) > 0 {
		cover = normalizeBiliAssetURL(d.ImageURLs[0])
	}

	var raw strings.Builder
	fmt.Fprintf(&raw, "Bilibili Article cv%s\nTitle: %s\nAuthor: %s\nStats: %s", cvid, d.Title, d.AuthorName, stats)
	if pageText != "" {
		raw.WriteString("\n\nContent:\n")
		raw.WriteString(clipRunesCompact(pageText, 7500))
	}

	return &Result{
		Platform:     "bilibili",
		CanonicalURL: "https://www.bilibili.com/read/cv" + cvid,
		Title:        strings.TrimSpace(d.Title),
		Author:       strings.TrimSpace(d.AuthorName),
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(pageText), 280),
		RawText:      raw.String(),
		CoverURL:     cover,
	}, nil
}

func extractBilibiliArticleBody(htmlBytes []byte, contentType string) string {
	decoded := decodeHTMLToUTF8(htmlBytes, contentType)
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader([]byte(decoded)))
	if err != nil {
		return ""
	}
	doc.Find("script, style, noscript, svg, nav, footer").Remove()
	for _, sel := range []string{"#article-content", ".article-holder", ".opus-module-content", ".read-article-holder"} {
		if node := doc.Find(sel).First(); node.Length() > 0 {
			if txt := collapseSpaces(node.Text()); len([]rune(txt)) >= 20 {
				return txt
			}
		}
	}
	if desc := metaContent(doc, "description", "og:description"); desc != "" {
		return desc
	}
	return ""
}

// --- 4. Bilibili Bangumi / PGC Extractor (ep / ss) ---

func (s *Service) extractBilibiliBangumi(ctx context.Context, cli *http.Client, kind, id string, headers map[string]string) (*Result, error) {
	param := "season_id=" + url.QueryEscape(id)
	canonPath := "ss" + id
	if kind == "ep" {
		param = "ep_id=" + url.QueryEscape(id)
		canonPath = "ep" + id
	}
	apiURL := s.bilibiliAPIBase + "/pgc/view/web/season?" + param
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 2<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("bilibili bangumi HTTP %d: %v", code, err)
	}
	var resp struct {
		Code   int `json:"code"`
		Result struct {
			Title       string `json:"title"`
			Evaluate    string `json:"evaluate"`
			Cover       string `json:"cover"`
			SeasonTitle string `json:"season_title"`
			NewEp       struct {
				Desc string `json:"desc"`
			} `json:"new_ep"`
			Rating struct {
				Score float64 `json:"score"`
				Count int     `json:"count"`
			} `json:"rating"`
			Stat struct {
				Views     int `json:"views"`
				Favorites int `json:"favorites"`
				Danmakus  int `json:"danmakus"`
			} `json:"stat"`
			Episodes []struct {
				ID        int64  `json:"id"`
				Title     string `json:"title"`
				LongTitle string `json:"long_title"`
				Cover     string `json:"cover"`
			} `json:"episodes"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 || strings.TrimSpace(resp.Result.Title) == "" {
		return nil, ErrEmptyContent
	}
	r := resp.Result
	title := strings.TrimSpace(r.Title)
	cover := normalizeBiliAssetURL(r.Cover)
	if kind == "ep" {
		epID, _ := strconv.ParseInt(id, 10, 64)
		for _, ep := range r.Episodes {
			if ep.ID == epID {
				epLabel := strings.TrimSpace(ep.Title)
				if ep.LongTitle != "" {
					if epLabel != "" {
						epLabel += " " + strings.TrimSpace(ep.LongTitle)
					} else {
						epLabel = strings.TrimSpace(ep.LongTitle)
					}
				}
				if epLabel != "" {
					title = fmt.Sprintf("%s 第%s话", title, epLabel)
				}
				if ep.Cover != "" {
					cover = normalizeBiliAssetURL(ep.Cover)
				}
				break
			}
		}
	}

	var statSegs []string
	if r.Rating.Score > 0 {
		statSegs = append(statSegs, fmt.Sprintf("评分 %.1f", r.Rating.Score))
	}
	if r.NewEp.Desc != "" {
		statSegs = append(statSegs, strings.TrimSpace(r.NewEp.Desc))
	}
	if r.Stat.Views > 0 {
		statSegs = append(statSegs, formatCountCN(r.Stat.Views)+"播放")
	}
	if r.Stat.Favorites > 0 {
		statSegs = append(statSegs, formatCountCN(r.Stat.Favorites)+"追番")
	}
	stats := strings.Join(statSegs, " · ")
	eval := collapseSpaces(r.Evaluate)

	return &Result{
		Platform:     "bilibili",
		CanonicalURL: "https://www.bilibili.com/bangumi/play/" + canonPath,
		Title:        title,
		Stats:        stats,
		Summary:      clipRunesCompact(eval, 280),
		RawText:      fmt.Sprintf("Bilibili Bangumi %s\nTitle: %s\nStats: %s\nSynopsis:\n%s", canonPath, title, stats, eval),
		CoverURL:     cover,
	}, nil
}

// --- 5. Bilibili Live Room Extractor ---

func (s *Service) extractBilibiliLive(ctx context.Context, cli *http.Client, roomID string, headers map[string]string) (*Result, error) {
	infoURL := fmt.Sprintf("%s/room/v1/Room/get_info?room_id=%s", s.bilibiliLiveAPIBase, url.QueryEscape(roomID))
	body, _, code, err := netutil.FetchLimited(ctx, cli, infoURL, 1<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("bilibili live HTTP %d: %v", code, err)
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			UID            int64  `json:"uid"`
			RoomID         int64  `json:"room_id"`
			Title          string `json:"title"`
			Description    string `json:"description"`
			LiveStatus     int    `json:"live_status"` // 0: offline, 1: live, 2: loop
			LiveTime       string `json:"live_time"`
			AreaName       string `json:"area_name"`
			ParentAreaName string `json:"parent_area_name"`
			Online         int    `json:"online"`
			Attention      int    `json:"attention"`
			UserCover      string `json:"user_cover"`
			Keyframe       string `json:"keyframe"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 || strings.TrimSpace(resp.Data.Title) == "" {
		return nil, ErrEmptyContent
	}
	d := resp.Data

	// Best-effort anchor name fetch.
	anchorName := ""
	if d.UID > 0 {
		mctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		mURL := fmt.Sprintf("%s/live_user/v1/Master/info?uid=%d", s.bilibiliLiveAPIBase, d.UID)
		if mBody, _, mCode, mErr := netutil.FetchLimited(mctx, cli, mURL, 256<<10, headers); mErr == nil && mCode == 200 {
			var mr struct {
				Code int `json:"code"`
				Data struct {
					Info struct {
						Uname string `json:"uname"`
					} `json:"info"`
				} `json:"data"`
			}
			if json.Unmarshal(mBody, &mr) == nil && mr.Code == 0 {
				anchorName = strings.TrimSpace(mr.Data.Info.Uname)
			}
		}
		cancel()
	}

	statusStr := "未开播"
	switch d.LiveStatus {
	case 1:
		statusStr = "直播中"
	case 2:
		statusStr = "轮播中"
	}
	var statSegs []string
	statSegs = append(statSegs, statusStr)
	if d.ParentAreaName != "" && d.AreaName != "" && d.ParentAreaName != d.AreaName {
		statSegs = append(statSegs, d.ParentAreaName+"/"+d.AreaName)
	} else if d.AreaName != "" {
		statSegs = append(statSegs, d.AreaName)
	}
	if d.LiveStatus == 1 && d.Online > 0 {
		statSegs = append(statSegs, formatCountCN(d.Online)+"人气")
	}
	if d.Attention > 0 {
		statSegs = append(statSegs, formatCountCN(d.Attention)+"粉丝")
	}
	stats := strings.Join(statSegs, " · ")

	cover := normalizeBiliAssetURL(d.Keyframe)
	if cover == "" {
		cover = normalizeBiliAssetURL(d.UserCover)
	}
	desc := collapseSpaces(stripHTMLTagsSimple(d.Description))
	canonID := roomID
	if d.RoomID > 0 {
		canonID = strconv.FormatInt(d.RoomID, 10)
	}
	pub := ""
	if d.LiveStatus == 1 && d.LiveTime != "" && !strings.HasPrefix(d.LiveTime, "0000") {
		pub = d.LiveTime
	}

	return &Result{
		Platform:     "bilibili",
		CanonicalURL: "https://live.bilibili.com/" + canonID,
		Title:        strings.TrimSpace(d.Title),
		Author:       anchorName,
		PublishedAt:  pub,
		Stats:        stats,
		Summary:      clipRunesCompact(desc, 220),
		RawText:      fmt.Sprintf("Bilibili Live Room %s\nTitle: %s\nStreamer: %s\nStatus: %s\nDescription: %s", canonID, d.Title, anchorName, stats, desc),
		CoverURL:     cover,
	}, nil
}

func stripHTMLTagsSimple(s string) string {
	if !strings.Contains(s, "<") {
		return s
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(s))
	if err != nil {
		return s
	}
	return doc.Text()
}

// --- 6. On-Demand Low-Bitrate DASH Audio Stream Fetcher (for Deep ASR Fallback, No Login) ---

// FetchBilibiliAudio downloads the lowest-bitrate DASH audio-only stream for a Bilibili video
// (capped at maxBytes, default 10 MiB) so read_source can transcribe unsubtitled short videos via local ASR.
func (s *Service) FetchBilibiliAudio(ctx context.Context, bvid string, cid int64, maxBytes int64) ([]byte, string, error) {
	bvid = strings.TrimSpace(bvid)
	if bvid == "" || cid <= 0 {
		return nil, "", ErrEmptyContent
	}
	if maxBytes <= 0 || maxBytes > 12<<20 {
		maxBytes = 10 << 20
	}
	opts, _, deepCli, _ := s.snapshotConfig()
	baseHeaders := map[string]string{
		"Referer": "https://www.bilibili.com/video/" + bvid + "/",
		"Origin":  "https://www.bilibili.com",
	}
	if strings.TrimSpace(opts.BilibiliCookie) != "" {
		baseHeaders["Cookie"] = strings.TrimSpace(opts.BilibiliCookie)
	}
	mixinKey, guestCK := s.ensureBilibiliSession(ctx, deepCli, baseHeaders)
	headers := cloneMap(baseHeaders)
	if merged := mergeBilibiliCookie(opts.BilibiliCookie, guestCK); merged != "" {
		headers["Cookie"] = merged
	}

	params := url.Values{}
	params.Set("bvid", bvid)
	params.Set("cid", strconv.FormatInt(cid, 10))
	params.Set("fnval", "16") // DASH
	params.Set("fnver", "0")
	params.Set("fourk", "0")
	params.Set("try_look", "1")

	var playURL string
	if mixinKey != "" {
		playURL = s.bilibiliAPIBase + "/x/player/wbi/playurl?" + signWBIParams(params, mixinKey, time.Now())
	} else {
		playURL = s.bilibiliAPIBase + "/x/player/playurl?" + params.Encode()
	}

	body, _, code, err := netutil.FetchLimited(ctx, deepCli, playURL, 2<<20, headers)
	if err != nil || code != 200 {
		return nil, "", fmt.Errorf("bilibili playurl HTTP %d: %v", code, err)
	}
	var pr struct {
		Code int `json:"code"`
		Data struct {
			Dash struct {
				Audio []struct {
					ID        int      `json:"id"`
					Bandwidth int      `json:"bandwidth"`
					BaseURL   string   `json:"baseUrl"`
					BaseURL2  string   `json:"base_url"`
					BackupURL []string `json:"backupUrl"`
				} `json:"audio"`
			} `json:"dash"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &pr); err != nil || pr.Code != 0 || len(pr.Data.Dash.Audio) == 0 {
		return nil, "", ErrEmptyContent
	}

	// Pick lowest-bandwidth audio stream (typically 30216 / 64kbps m4s) to minimize memory & download time.
	sort.Slice(pr.Data.Dash.Audio, func(i, j int) bool {
		bi, bj := pr.Data.Dash.Audio[i].Bandwidth, pr.Data.Dash.Audio[j].Bandwidth
		if bi == 0 {
			bi = 1 << 30
		}
		if bj == 0 {
			bj = 1 << 30
		}
		return bi < bj
	})
	chosen := pr.Data.Dash.Audio[0]
	var candidates []string
	if u := strings.TrimSpace(chosen.BaseURL); u != "" {
		candidates = append(candidates, u)
	}
	if u := strings.TrimSpace(chosen.BaseURL2); u != "" && (len(candidates) == 0 || candidates[0] != u) {
		candidates = append(candidates, u)
	}
	for _, bu := range chosen.BackupURL {
		if u := strings.TrimSpace(bu); u != "" {
			candidates = append(candidates, u)
		}
	}
	if len(candidates) == 0 {
		return nil, "", ErrEmptyContent
	}

	cdnHeaders := map[string]string{
		"Referer": "https://www.bilibili.com/",
		"Origin":  "https://www.bilibili.com",
	}
	var lastErr error
	for _, cand := range candidates {
		audioBytes, _, acode, aerr := netutil.FetchLimited(ctx, deepCli, cand, maxBytes, cdnHeaders)
		if aerr == nil && acode >= 200 && acode < 300 && len(audioBytes) > 0 {
			return audioBytes, bvid + ".m4a", nil
		}
		lastErr = aerr
		if lastErr == nil {
			lastErr = fmt.Errorf("audio cdn HTTP %d", acode)
		}
	}
	return nil, "", lastErr
}

// ErrVideoTooLarge indicates every available MP4 quality exceeds the configured max_video_mib limit.
var ErrVideoTooLarge = errors.New("parser: video exceeds maximum size limit")

// FetchBilibiliVideoMP4 downloads a standalone muxed MP4 stream (no ffmpeg or login required)
// with triple size-limit enforcement: playurl durl.size pre-check + quality downgrade,
// HTTP Content-Length pre-check, and io.LimitReader hard cap.
func (s *Service) FetchBilibiliVideoMP4(ctx context.Context, bvid string, cid int64, maxBytes int64) ([]byte, error) {
	bvid = strings.TrimSpace(bvid)
	if bvid == "" || cid <= 0 {
		return nil, ErrEmptyContent
	}
	if maxBytes <= 0 {
		maxBytes = 20 << 20
	}
	if maxBytes > 50<<20 {
		maxBytes = 50 << 20
	}

	opts, _, deepCli, _ := s.snapshotConfig()
	baseHeaders := map[string]string{
		"Referer": "https://www.bilibili.com/video/" + bvid + "/",
		"Origin":  "https://www.bilibili.com",
	}
	if strings.TrimSpace(opts.BilibiliCookie) != "" {
		baseHeaders["Cookie"] = strings.TrimSpace(opts.BilibiliCookie)
	}
	mixinKey, guestCK := s.ensureBilibiliSession(ctx, deepCli, baseHeaders)
	headers := cloneMap(baseHeaders)
	if merged := mergeBilibiliCookie(opts.BilibiliCookie, guestCK); merged != "" {
		headers["Cookie"] = merged
	}

	// Dedicated client sharing the SSRF-safe pooled transport with a generous download timeout.
	dlCli := &http.Client{
		Transport:     deepCli.Transport,
		Timeout:       45 * time.Second,
		CheckRedirect: deepCli.CheckRedirect,
	}

	type mp4Tier struct {
		platform string
		qn       string
		highQ    bool
	}
	// Prefer html5 progressive MP4 (muxed audio+video) from 720p -> 480p -> 360p, then pc durl fallback.
	tiers := []mp4Tier{
		{platform: "html5", qn: "64", highQ: true},
		{platform: "html5", qn: "32", highQ: false},
		{platform: "html5", qn: "16", highQ: false},
		{platform: "pc", qn: "32", highQ: false},
		{platform: "pc", qn: "16", highQ: false},
	}

	sawTooLarge := false
	seenURL := make(map[string]struct{})
	var lastErr error

	for _, tier := range tiers {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		params := url.Values{}
		params.Set("bvid", bvid)
		params.Set("cid", strconv.FormatInt(cid, 10))
		params.Set("qn", tier.qn)
		params.Set("fnval", "1") // progressive MP4 (audio+video muxed)
		params.Set("fnver", "0")
		params.Set("fourk", "0")
		params.Set("platform", tier.platform)
		if tier.highQ {
			params.Set("high_quality", "1")
		}

		var playURL string
		if mixinKey != "" {
			playURL = s.bilibiliAPIBase + "/x/player/wbi/playurl?" + signWBIParams(params, mixinKey, time.Now())
		} else {
			playURL = s.bilibiliAPIBase + "/x/player/playurl?" + params.Encode()
		}

		body, _, code, err := netutil.FetchLimited(ctx, deepCli, playURL, 1<<20, headers)
		if err != nil || code != 200 {
			lastErr = err
			continue
		}
		var pr struct {
			Code int `json:"code"`
			Data struct {
				Durl []struct {
					Size      int64    `json:"size"`
					URL       string   `json:"url"`
					BackupURL []string `json:"backup_url"`
				} `json:"durl"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &pr) != nil || pr.Code != 0 || len(pr.Data.Durl) == 0 {
			continue
		}

		d := pr.Data.Durl[0]
		// Pre-check declared byte size before downloading a single byte; downgrade to lower qn if too big.
		if d.Size > 0 && d.Size > maxBytes {
			sawTooLarge = true
			continue
		}

		var candidates []string
		if u := strings.TrimSpace(d.URL); u != "" {
			candidates = append(candidates, u)
		}
		for _, bu := range d.BackupURL {
			if u := strings.TrimSpace(bu); u != "" {
				candidates = append(candidates, u)
			}
		}
		if len(candidates) == 0 {
			continue
		}

		// Deduplicate identical stream URL returned across quality tiers (e.g. anonymous html5 returns fixed qn).
		if _, dup := seenURL[candidates[0]]; dup {
			continue
		}
		seenURL[candidates[0]] = struct{}{}

		cdnHeaders := map[string]string{
			"Referer":    "https://www.bilibili.com/",
			"Origin":     "https://www.bilibili.com",
			"User-Agent": defaultBrowserUA,
		}
		for _, cand := range candidates {
			data, derr := fetchBoundedMedia(ctx, dlCli, cand, maxBytes, cdnHeaders)
			if derr == nil && len(data) > 0 {
				return data, nil
			}
			if errors.Is(derr, ErrVideoTooLarge) {
				sawTooLarge = true
				break // Same tier's backups have the same size; move to lower quality tier.
			}
			lastErr = derr
		}
	}

	if sawTooLarge {
		return nil, ErrVideoTooLarge
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrEmptyContent
}

// fetchBoundedMedia checks Content-Length upfront and enforces a strict maxBytes io.LimitReader cap.
func fetchBoundedMedia(ctx context.Context, cli *http.Client, rawURL string, maxBytes int64, headers map[string]string) ([]byte, error) {
	if err := netutil.ValidateURL(rawURL); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("media HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBytes {
		return nil, ErrVideoTooLarge
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, ErrVideoTooLarge
	}
	return data, nil
}
