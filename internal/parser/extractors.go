package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html/charset"

	"github.com/iMMIQ/fatfish/internal/netutil"
)

const defaultBrowserUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

// --- Site Matchers ---

func matchBilibili(host, urlPath string) bool {
	if strings.Contains(host, "b23.tv") || strings.Contains(host, "bili2233.cn") {
		return true
	}
	if host == "t.bilibili.com" || strings.HasSuffix(host, ".bilibili.com") || host == "bilibili.com" {
		lp := strings.ToLower(urlPath)
		return strings.Contains(lp, "/video/") ||
			strings.Contains(lp, "/opus/") ||
			strings.Contains(lp, "/read/cv") ||
			host == "t.bilibili.com" ||
			bvidRe.MatchString(urlPath)
	}
	return false
}

func matchGitHub(host, urlPath string) bool {
	if host != "github.com" && host != "www.github.com" {
		return false
	}
	parts := splitPathParts(urlPath)
	if len(parts) < 2 {
		return false
	}
	// Skip non-repo top-level GitHub routes.
	switch strings.ToLower(parts[0]) {
	case "search", "explore", "topics", "trending", "collections", "events", "sponsors", "settings", "notifications", "login", "signup", "features", "pricing", "about", "orgs", "marketplace":
		return false
	}
	return true
}

func matchArxiv(host, urlPath string) bool {
	return (host == "arxiv.org" || strings.HasSuffix(host, ".arxiv.org")) && arxivIDRe.MatchString(urlPath)
}

func matchHuggingFace(host, urlPath string) bool {
	if host != "huggingface.co" && host != "www.huggingface.co" && host != "hf-mirror.com" && host != "www.hf-mirror.com" {
		return false
	}
	parts := splitPathParts(urlPath)
	return len(parts) >= 2
}

var tweetPathRe = regexp.MustCompile(`^/([^/]+)/status/(\d+)`)

func matchTwitter(host, urlPath string) bool {
	switch host {
	case "x.com", "www.x.com", "mobile.x.com",
		"twitter.com", "www.twitter.com", "mobile.twitter.com",
		"fxtwitter.com", "www.fxtwitter.com",
		"vxtwitter.com", "www.vxtwitter.com",
		"fixupx.com", "www.fixupx.com":
		return tweetPathRe.MatchString(urlPath)
	default:
		return false
	}
}

func matchWeixin(host string) bool {
	return host == "mp.weixin.qq.com"
}

// --- 1. Bilibili Extractor (Video / Dynamic Opus / Article) ---

var (
	biliOpusRe    = regexp.MustCompile(`/(?:opus/)?(\d{15,})`)
	biliArticleRe = regexp.MustCompile(`(?i)/read/cv(\d+)`)
)

func (s *Service) extractBilibili(ctx context.Context, cli *http.Client, u *url.URL, targetURL string, deep bool, cookie string) (*Result, error) {
	headers := map[string]string{
		"Referer": "https://www.bilibili.com/",
	}
	if strings.TrimSpace(cookie) != "" {
		headers["Cookie"] = strings.TrimSpace(cookie)
	}

	bvid := bvidRe.FindString(targetURL)
	var aid string
	if bvid == "" {
		if m := avidRe.FindStringSubmatch(u.Path); len(m) >= 2 {
			aid = m[1]
		}
	}
	if bvid != "" || aid != "" {
		return s.extractBilibiliVideo(ctx, cli, bvid, aid, deep, headers)
	}

	// Bilibili dynamic / opus
	if u.Hostname() == "t.bilibili.com" || strings.Contains(strings.ToLower(u.Path), "/opus/") {
		if m := biliOpusRe.FindStringSubmatch(u.Path); len(m) >= 2 {
			if res, err := s.extractBilibiliOpus(ctx, cli, m[1], targetURL, headers); err == nil {
				return res, nil
			}
		}
	}

	// Bilibili article (cv...)
	if m := biliArticleRe.FindStringSubmatch(u.Path); len(m) >= 2 {
		if res, err := s.extractBilibiliArticle(ctx, cli, m[1], targetURL, headers); err == nil {
			return res, nil
		}
	}

	return s.extractGenericHTML(ctx, cli, targetURL, deep)
}

func (s *Service) extractBilibiliVideo(ctx context.Context, cli *http.Client, bvid, aid string, deep bool, headers map[string]string) (*Result, error) {
	apiURL := s.bilibiliAPIBase + "/x/web-interface/view?"
	if bvid != "" {
		apiURL += "bvid=" + url.QueryEscape(bvid)
	} else {
		apiURL += "aid=" + url.QueryEscape(aid)
	}
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 2<<20, headers)
	if err != nil {
		return nil, err
	}
	if code >= 400 {
		return nil, fmt.Errorf("bilibili view HTTP %d", code)
	}
	var view struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Aid      int64  `json:"aid"`
			Cid      int64  `json:"cid"`
			Bvid     string `json:"bvid"`
			Title    string `json:"title"`
			Desc     string `json:"desc"`
			Duration int    `json:"duration"`
			PubDate  int64  `json:"pubdate"`
			TName    string `json:"tname"`
			Owner    struct {
				Name string `json:"name"`
			} `json:"owner"`
			Stat struct {
				View    int `json:"view"`
				Danmaku int `json:"danmaku"`
				Like    int `json:"like"`
				Coin    int `json:"coin"`
				Reply   int `json:"reply"`
			} `json:"stat"`
			Pages []struct {
				Cid  int64  `json:"cid"`
				Page int    `json:"page"`
				Part string `json:"part"`
			} `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		return nil, err
	}
	if view.Code != 0 || strings.TrimSpace(view.Data.Title) == "" {
		return nil, fmt.Errorf("bilibili view error code=%d msg=%s", view.Code, view.Message)
	}

	d := view.Data
	canon := "https://www.bilibili.com/video/" + d.Bvid
	if d.Bvid == "" && d.Aid > 0 {
		canon = fmt.Sprintf("https://www.bilibili.com/video/av%d", d.Aid)
	}
	stats := fmt.Sprintf("时长 %s · %s播放 · %s赞 · %s币",
		formatDuration(d.Duration), formatCountCN(d.Stat.View), formatCountCN(d.Stat.Like), formatCountCN(d.Stat.Coin))
	if d.TName != "" {
		stats = d.TName + " · " + stats
	}
	pubStr := ""
	if d.PubDate > 0 {
		pubStr = time.Unix(d.PubDate, 0).Format("2006-01-02")
	}

	var extra strings.Builder
	var raw strings.Builder
	fmt.Fprintf(&raw, "Bilibili %s\nTitle: %s\nUP: %s | Duration: %s | Views: %d | Likes: %d | Coins: %d\nDescription: %s\n",
		d.Bvid, d.Title, d.Owner.Name, formatDuration(d.Duration), d.Stat.View, d.Stat.Like, d.Stat.Coin, strings.TrimSpace(d.Desc))

	if len(d.Pages) > 1 {
		extra.WriteString("分P: ")
		raw.WriteString("Pages:\n")
		var pNames []string
		for i, p := range d.Pages {
			if i >= 10 {
				break
			}
			fmt.Fprintf(&raw, "  P%d: %s\n", p.Page, p.Part)
			if i < 5 {
				pNames = append(pNames, fmt.Sprintf("P%d %s", p.Page, p.Part))
			}
		}
		extra.WriteString(strings.Join(pNames, " / ") + "\n")
	}

	// Best-effort subtitles & top comments with bounded sub-timeout so proactive parse stays snappy.
	subTimeout := 2500 * time.Millisecond
	if deep {
		subTimeout = 6 * time.Second
	}
	subCtx, subCancel := context.WithTimeout(ctx, subTimeout)
	defer subCancel()

	cid := d.Cid
	if cid == 0 && len(d.Pages) > 0 {
		cid = d.Pages[0].Cid
	}
	if cid > 0 && d.Bvid != "" {
		pURL := fmt.Sprintf("%s/x/player/v2?bvid=%s&cid=%d", s.bilibiliAPIBase, url.QueryEscape(d.Bvid), cid)
		if pBody, _, pcode, perr := netutil.FetchLimited(subCtx, cli, pURL, 1<<20, headers); perr == nil && pcode == 200 {
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
					if sBody, _, scode, serr := netutil.FetchLimited(subCtx, cli, subURL, 2<<20, nil); serr == nil && scode == 200 {
						var sj struct {
							Body []struct {
								Content string `json:"content"`
							} `json:"body"`
						}
						if json.Unmarshal(sBody, &sj) == nil && len(sj.Body) > 0 {
							var lines []string
							for _, seg := range sj.Body {
								if txt := strings.TrimSpace(seg.Content); txt != "" {
									lines = append(lines, txt)
								}
							}
							if len(lines) > 0 {
								joined := strings.Join(lines, " ")
								raw.WriteString("Subtitles:\n  " + clipRunesCompact(joined, 2500) + "\n")
								extra.WriteString("字幕摘录: " + clipRunesCompact(joined, 300) + "\n")
							}
						}
					}
				}
			}
		}
	}

	if d.Aid > 0 {
		ps := 3
		if deep {
			ps = 5
		}
		cURL := fmt.Sprintf("%s/x/v2/reply?type=1&oid=%d&sort=2&ps=%d", s.bilibiliAPIBase, d.Aid, ps)
		if cBody, _, ccode, cerr := netutil.FetchLimited(subCtx, cli, cURL, 1<<20, headers); cerr == nil && ccode == 200 {
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
				raw.WriteString("Top Comments:\n")
				for i, r := range cr.Data.Replies {
					msgTxt := collapseSpaces(r.Content.Message)
					fmt.Fprintf(&raw, "  - %s (♥%d): %s\n", r.Member.Uname, r.Like, msgTxt)
					if i < 2 {
						fmt.Fprintf(&extra, "热评(%s): %s\n", r.Member.Uname, clipRunesCompact(msgTxt, 90))
					}
				}
			}
		}
	}

	desc := collapseSpaces(d.Desc)
	if desc == "-" {
		desc = ""
	}
	summary := clipRunesCompact(desc, 220)
	if extraTxt := strings.TrimSpace(extra.String()); extraTxt != "" {
		if summary != "" {
			summary += " | " + collapseSpaces(clipRunesCompact(extraTxt, 240))
		} else {
			summary = collapseSpaces(clipRunesCompact(extraTxt, 280))
		}
	}

	return &Result{
		Platform:     "bilibili",
		CanonicalURL: canon,
		Title:        strings.TrimSpace(d.Title),
		Author:       strings.TrimSpace(d.Owner.Name),
		PublishedAt:  pubStr,
		Stats:        stats,
		Summary:      summary,
		Extra:        strings.TrimSpace(extra.String()),
		RawText:      strings.TrimSpace(raw.String()),
	}, nil
}

func (s *Service) extractBilibiliOpus(ctx context.Context, cli *http.Client, id, targetURL string, headers map[string]string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/x/polymer/web-dynamic/v1/detail?id=%s", s.bilibiliAPIBase, url.QueryEscape(id))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 2<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("bilibili opus fetch failed: %v (HTTP %d)", err, code)
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			Item struct {
				Modules struct {
					ModuleAuthor struct {
						Name string `json:"name"`
					} `json:"module_author"`
					ModuleDynamic struct {
						Desc struct {
							Text string `json:"text"`
						} `json:"desc"`
						Major struct {
							Opus struct {
								Title   string `json:"title"`
								Summary struct {
									Text string `json:"text"`
								} `json:"summary"`
							} `json:"opus"`
						} `json:"major"`
					} `json:"module_dynamic"`
					ModuleStat struct {
						Like    struct{ Count int `json:"count"` } `json:"like"`
						Comment struct{ Count int `json:"count"` } `json:"comment"`
						Forward struct{ Count int `json:"count"` } `json:"forward"`
					} `json:"module_stat"`
				} `json:"modules"`
			} `json:"item"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 {
		return nil, ErrEmptyContent
	}
	mods := resp.Data.Item.Modules
	title := strings.TrimSpace(mods.ModuleDynamic.Major.Opus.Title)
	text := strings.TrimSpace(mods.ModuleDynamic.Major.Opus.Summary.Text)
	if text == "" {
		text = strings.TrimSpace(mods.ModuleDynamic.Desc.Text)
	}
	if title == "" && text == "" {
		return nil, ErrEmptyContent
	}
	if title == "" {
		title = clipRunesCompact(collapseSpaces(text), 48)
	}
	stats := fmt.Sprintf("%s赞 · %s评 · %s转发",
		formatCountCN(mods.ModuleStat.Like.Count),
		formatCountCN(mods.ModuleStat.Comment.Count),
		formatCountCN(mods.ModuleStat.Forward.Count))
	return &Result{
		Platform:     "bilibili",
		CanonicalURL: "https://www.bilibili.com/opus/" + id,
		Title:        title,
		Author:       strings.TrimSpace(mods.ModuleAuthor.Name),
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(text), 260),
		RawText:      fmt.Sprintf("Bilibili Dynamic %s\nAuthor: %s\nTitle: %s\nStats: %s\nContent:\n%s", id, mods.ModuleAuthor.Name, title, stats, clipRunesCompact(text, 4000)),
	}, nil
}

func (s *Service) extractBilibiliArticle(ctx context.Context, cli *http.Client, cvid, targetURL string, headers map[string]string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/x/article/viewinfo?id=%s", s.bilibiliAPIBase, url.QueryEscape(cvid))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 1<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("bilibili article fetch failed: %v (HTTP %d)", err, code)
	}
	var resp struct {
		Code int `json:"code"`
		Data struct {
			Title      string `json:"title"`
			AuthorName string `json:"author_name"`
			Stats      struct {
				View  int `json:"view"`
				Like  int `json:"like"`
				Coin  int `json:"coin"`
				Reply int `json:"reply"`
			} `json:"stats"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 || strings.TrimSpace(resp.Data.Title) == "" {
		return nil, ErrEmptyContent
	}
	d := resp.Data
	stats := fmt.Sprintf("%s阅读 · %s赞 · %s币",
		formatCountCN(d.Stats.View), formatCountCN(d.Stats.Like), formatCountCN(d.Stats.Coin))
	return &Result{
		Platform:     "bilibili",
		CanonicalURL: "https://www.bilibili.com/read/cv" + cvid,
		Title:        strings.TrimSpace(d.Title),
		Author:       strings.TrimSpace(d.AuthorName),
		Stats:        stats,
		RawText:      fmt.Sprintf("Bilibili Article cv%s\nTitle: %s\nAuthor: %s\nStats: %s", cvid, d.Title, d.AuthorName, stats),
	}, nil
}

// --- 2. GitHub Extractor (Repo / Issue / PR / Release) ---

func (s *Service) extractGitHub(ctx context.Context, cli *http.Client, u *url.URL, deep bool, token string) (*Result, error) {
	parts := splitPathParts(u.Path)
	if len(parts) < 2 {
		return s.extractGenericHTML(ctx, cli, u.String(), deep)
	}
	owner, repo := parts[0], parts[1]
	headers := map[string]string{
		"Accept":               "application/vnd.github+json",
		"X-GitHub-Api-Version": "2022-11-28",
	}
	if strings.TrimSpace(token) != "" {
		headers["Authorization"] = "Bearer " + strings.TrimSpace(token)
	}

	var res *Result
	var err error
	switch {
	case len(parts) >= 4 && parts[2] == "issues" && isDigits(parts[3]):
		res, err = s.extractGitHubIssue(ctx, cli, owner, repo, parts[3], headers)
	case len(parts) >= 4 && parts[2] == "pull" && isDigits(parts[3]):
		res, err = s.extractGitHubPR(ctx, cli, owner, repo, parts[3], headers)
	case len(parts) >= 5 && parts[2] == "releases" && parts[3] == "tag":
		tag := strings.Join(parts[4:], "/")
		res, err = s.extractGitHubRelease(ctx, cli, owner, repo, tag, headers)
	default:
		res, err = s.extractGitHubRepo(ctx, cli, owner, repo, deep, headers)
	}
	if err != nil {
		// Graceful fallback to generic HTML OG tags if GitHub API rate-limits (403/429).
		if fallback, ferr := s.extractGenericHTML(ctx, cli, u.String(), deep); ferr == nil {
			fallback.Platform = "github"
			return fallback, nil
		}
		return nil, err
	}
	return res, nil
}

func (s *Service) extractGitHubRepo(ctx context.Context, cli *http.Client, owner, repo string, deep bool, headers map[string]string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s", s.githubAPIBase, url.PathEscape(owner), url.PathEscape(repo))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 1<<20, headers)
	if err != nil {
		return nil, err
	}
	if code >= 400 {
		return nil, fmt.Errorf("github repo HTTP %d", code)
	}
	var data struct {
		FullName    string   `json:"full_name"`
		HTMLURL     string   `json:"html_url"`
		Description string   `json:"description"`
		Language    string   `json:"language"`
		Stars       int      `json:"stargazers_count"`
		Forks       int      `json:"forks_count"`
		OpenIssues  int      `json:"open_issues_count"`
		Topics      []string `json:"topics"`
		PushedAt    string   `json:"pushed_at"`
		License     struct {
			SPDXID string `json:"spdx_id"`
		} `json:"license"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if data.FullName == "" {
		return nil, ErrEmptyContent
	}
	var statParts []string
	if data.Language != "" {
		statParts = append(statParts, data.Language)
	}
	statParts = append(statParts, "★"+formatCountEN(data.Stars), "Forks "+formatCountEN(data.Forks))
	if data.License.SPDXID != "" && data.License.SPDXID != "NOASSERTION" {
		statParts = append(statParts, data.License.SPDXID)
	}
	stats := strings.Join(statParts, " · ")

	summary := strings.TrimSpace(data.Description)
	if len(data.Topics) > 0 {
		top := data.Topics
		if len(top) > 5 {
			top = top[:5]
		}
		if summary != "" {
			summary += " | Tags: " + strings.Join(top, ", ")
		} else {
			summary = "Tags: " + strings.Join(top, ", ")
		}
	}

	var raw strings.Builder
	fmt.Fprintf(&raw, "GitHub Repo: %s\nStats: %s | Open Issues: %d\nDescription: %s\n",
		data.FullName, stats, data.OpenIssues, data.Description)

	if deep {
		readmeURL := fmt.Sprintf("%s/repos/%s/%s/readme", s.githubAPIBase, url.PathEscape(owner), url.PathEscape(repo))
		rHeaders := cloneMap(headers)
		rHeaders["Accept"] = "application/vnd.github.raw+json"
		if rBody, _, rCode, rErr := netutil.FetchLimited(ctx, cli, readmeURL, 2<<20, rHeaders); rErr == nil && rCode == 200 && len(rBody) > 0 {
			raw.WriteString("\nREADME:\n")
			raw.WriteString(clipRunesCompact(string(rBody), 5000))
		}
	}

	canon := data.HTMLURL
	if canon == "" {
		canon = fmt.Sprintf("https://github.com/%s/%s", owner, repo)
	}
	return &Result{
		Platform:     "github",
		CanonicalURL: canon,
		Title:        data.FullName,
		Stats:        stats,
		PublishedAt:  clipDate(data.PushedAt),
		Summary:      clipRunesCompact(summary, 280),
		RawText:      strings.TrimSpace(raw.String()),
	}, nil
}

func (s *Service) extractGitHubIssue(ctx context.Context, cli *http.Client, owner, repo, num string, headers map[string]string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s/issues/%s", s.githubAPIBase, url.PathEscape(owner), url.PathEscape(repo), num)
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 1<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("github issue HTTP %d: %v", code, err)
	}
	var data struct {
		HTMLURL   string `json:"html_url"`
		Title     string `json:"title"`
		State     string `json:"state"`
		Body      string `json:"body"`
		Comments  int    `json:"comments"`
		CreatedAt string `json:"created_at"`
		User      struct{ Login string `json:"login"` } `json:"user"`
		Labels    []struct{ Name string `json:"name"` } `json:"labels"`
	}
	if err := json.Unmarshal(body, &data); err != nil || data.Title == "" {
		return nil, ErrEmptyContent
	}
	stats := fmt.Sprintf("Issue #%s · %s · %d comments", num, data.State, data.Comments)
	if len(data.Labels) > 0 {
		var lbls []string
		for i, l := range data.Labels {
			if i >= 4 {
				break
			}
			lbls = append(lbls, l.Name)
		}
		stats += " · [" + strings.Join(lbls, ", ") + "]"
	}
	cleanBody := collapseSpaces(data.Body)
	return &Result{
		Platform:     "github",
		CanonicalURL: fmt.Sprintf("https://github.com/%s/%s/issues/%s", owner, repo, num),
		Title:        fmt.Sprintf("%s/%s#%s: %s", owner, repo, num, data.Title),
		Author:       data.User.Login,
		PublishedAt:  clipDate(data.CreatedAt),
		Stats:        stats,
		Summary:      clipRunesCompact(cleanBody, 280),
		RawText:      fmt.Sprintf("GitHub Issue %s/%s#%s\nTitle: %s\nAuthor: %s | %s\n\n%s", owner, repo, num, data.Title, data.User.Login, stats, clipRunesCompact(data.Body, 4500)),
	}, nil
}

func (s *Service) extractGitHubPR(ctx context.Context, cli *http.Client, owner, repo, num string, headers map[string]string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s/pulls/%s", s.githubAPIBase, url.PathEscape(owner), url.PathEscape(repo), num)
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 1<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("github pr HTTP %d: %v", code, err)
	}
	var data struct {
		Title        string `json:"title"`
		State        string `json:"state"`
		Merged       bool   `json:"merged"`
		Body         string `json:"body"`
		Additions    int    `json:"additions"`
		Deletions    int    `json:"deletions"`
		ChangedFiles int    `json:"changed_files"`
		CreatedAt    string `json:"created_at"`
		User         struct{ Login string `json:"login"` } `json:"user"`
	}
	if err := json.Unmarshal(body, &data); err != nil || data.Title == "" {
		return nil, ErrEmptyContent
	}
	state := data.State
	if data.Merged {
		state = "merged"
	}
	stats := fmt.Sprintf("PR #%s · %s · +%d/-%d (%d files)", num, state, data.Additions, data.Deletions, data.ChangedFiles)
	return &Result{
		Platform:     "github",
		CanonicalURL: fmt.Sprintf("https://github.com/%s/%s/pull/%s", owner, repo, num),
		Title:        fmt.Sprintf("%s/%s#%s: %s", owner, repo, num, data.Title),
		Author:       data.User.Login,
		PublishedAt:  clipDate(data.CreatedAt),
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(data.Body), 280),
		RawText:      fmt.Sprintf("GitHub PR %s/%s#%s\nTitle: %s\nAuthor: %s | %s\n\n%s", owner, repo, num, data.Title, data.User.Login, stats, clipRunesCompact(data.Body, 4500)),
	}, nil
}

func (s *Service) extractGitHubRelease(ctx context.Context, cli *http.Client, owner, repo, tag string, headers map[string]string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/repos/%s/%s/releases/tags/%s", s.githubAPIBase, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(tag))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 1<<20, headers)
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("github release HTTP %d: %v", code, err)
	}
	var data struct {
		TagName     string `json:"tag_name"`
		Name        string `json:"name"`
		Body        string `json:"body"`
		PublishedAt string `json:"published_at"`
		Author      struct{ Login string `json:"login"` } `json:"author"`
	}
	if err := json.Unmarshal(body, &data); err != nil || (data.Name == "" && data.TagName == "") {
		return nil, ErrEmptyContent
	}
	title := strings.TrimSpace(data.Name)
	if title == "" {
		title = data.TagName
	}
	return &Result{
		Platform:     "github",
		CanonicalURL: fmt.Sprintf("https://github.com/%s/%s/releases/tag/%s", owner, repo, tag),
		Title:        fmt.Sprintf("%s/%s Release %s", owner, repo, title),
		Author:       data.Author.Login,
		PublishedAt:  clipDate(data.PublishedAt),
		Stats:        "Tag " + data.TagName,
		Summary:      clipRunesCompact(collapseSpaces(data.Body), 280),
		RawText:      fmt.Sprintf("GitHub Release %s/%s (%s)\nTitle: %s\nPublished: %s\n\n%s", owner, repo, data.TagName, title, clipDate(data.PublishedAt), clipRunesCompact(data.Body, 4500)),
	}, nil
}

// --- 3. arXiv Extractor ---

func (s *Service) extractArxiv(ctx context.Context, cli *http.Client, targetURL string, deep bool) (*Result, error) {
	m := arxivIDRe.FindStringSubmatch(targetURL)
	if len(m) < 2 {
		return s.extractGenericHTML(ctx, cli, targetURL, deep)
	}
	id := m[1]
	atomURL := s.arxivAPIBase + "/api/query?id_list=" + url.QueryEscape(id)
	body, _, code, err := netutil.FetchLimited(ctx, cli, atomURL, 2<<20, nil)
	if err != nil {
		return nil, err
	}
	if code >= 400 {
		return nil, fmt.Errorf("arxiv API HTTP %d", code)
	}
	var feed struct {
		Entries []struct {
			Title      string `xml:"title"`
			Summary    string `xml:"summary"`
			Published  string `xml:"published"`
			Authors    []struct{ Name string `xml:"name"` } `xml:"author"`
			Categories []struct {
				Term string `xml:"term,attr"`
			} `xml:"category"`
		} `xml:"entry"`
	}
	if err := xml.Unmarshal(body, &feed); err != nil || len(feed.Entries) == 0 {
		return s.extractGenericHTML(ctx, cli, targetURL, deep)
	}
	e := feed.Entries[0]
	title := collapseSpaces(e.Title)
	abstract := collapseSpaces(e.Summary)
	if title == "" {
		return nil, ErrEmptyContent
	}
	allAuthors := make([]string, 0, len(e.Authors))
	for _, a := range e.Authors {
		if nm := strings.TrimSpace(a.Name); nm != "" {
			allAuthors = append(allAuthors, nm)
		}
	}
	shortAuthors := strings.Join(allAuthors, ", ")
	if len(allAuthors) > 4 {
		shortAuthors = strings.Join(allAuthors[:4], ", ") + fmt.Sprintf(" et al. (%d人)", len(allAuthors))
	}
	var cats []string
	for i, c := range e.Categories {
		if i >= 3 {
			break
		}
		if c.Term != "" {
			cats = append(cats, c.Term)
		}
	}
	stats := "arXiv:" + id
	if len(cats) > 0 {
		stats += " · " + strings.Join(cats, ", ")
	}

	var raw strings.Builder
	fmt.Fprintf(&raw, "arXiv:%s (%s)\nTitle: %s\nAuthors: %s\nAbstract: %s",
		id, clipDate(e.Published), title, strings.Join(allAuthors, ", "), abstract)

	if deep {
		if _, lerr := exec.LookPath("pdftotext"); lerr == nil {
			pdfURL := s.arxivPDFBase + "/pdf/" + id + ".pdf"
			if pdfBytes, _, pcode, perr := netutil.FetchLimited(ctx, cli, pdfURL, 10<<20, nil); perr == nil && pcode == 200 && len(pdfBytes) > 0 {
				if pdfTxt, eerr := ExtractPDFPages(ctx, pdfBytes, 1, 4); eerr == nil && pdfTxt != "" {
					raw.WriteString("\n\nPDF Excerpt (pages 1-4):\n")
					raw.WriteString(clipRunesCompact(pdfTxt, 5000))
				}
			}
		}
	}

	return &Result{
		Platform:     "arxiv",
		CanonicalURL: "https://arxiv.org/abs/" + id,
		Title:        title,
		Author:       shortAuthors,
		PublishedAt:  clipDate(e.Published),
		Stats:        stats,
		Summary:      clipRunesCompact(abstract, 360),
		RawText:      strings.TrimSpace(raw.String()),
	}, nil
}

// --- 4. HuggingFace / hf-mirror Extractor ---

func (s *Service) extractHuggingFace(ctx context.Context, cli *http.Client, u *url.URL, deep bool) (*Result, error) {
	parts := splitPathParts(u.Path)
	if len(parts) < 2 {
		return s.extractGenericHTML(ctx, cli, u.String(), deep)
	}
	kind := "models"
	repoID := parts[0] + "/" + parts[1]
	rawPrefix := repoID
	if (parts[0] == "datasets" || parts[0] == "spaces") && len(parts) >= 3 {
		kind = parts[0]
		repoID = parts[1] + "/" + parts[2]
		rawPrefix = parts[0] + "/" + repoID
	}

	bases := []string{s.hfMirrorBase, s.hfOriginBase}
	var metaBody []byte
	var usedBase string
	var lastErr error
	for _, base := range bases {
		if base == "" {
			continue
		}
		b, _, code, err := netutil.FetchLimited(ctx, cli, fmt.Sprintf("%s/api/%s/%s", base, kind, repoID), 2<<20, nil)
		if err == nil && code == 200 {
			metaBody = b
			usedBase = base
			break
		}
		lastErr = err
	}
	if len(metaBody) == 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, ErrEmptyContent
	}

	var meta map[string]any
	_ = json.Unmarshal(metaBody, &meta)
	var statParts []string
	if pt, ok := meta["pipeline_tag"].(string); ok && pt != "" {
		statParts = append(statParts, pt)
	}
	if dl, ok := meta["downloads"].(float64); ok && dl > 0 {
		statParts = append(statParts, "dl "+formatCountEN(int(dl)))
	}
	if lk, ok := meta["likes"].(float64); ok && lk > 0 {
		statParts = append(statParts, "♥"+formatCountEN(int(lk)))
	}
	stats := strings.Join(statParts, " · ")

	var readmeText string
	readmeMax := 1200
	if deep {
		readmeMax = 5000
	}
	if readme, _, rcode, rerr := netutil.FetchLimited(ctx, cli, fmt.Sprintf("%s/%s/raw/main/README.md", usedBase, rawPrefix), 2<<20, nil); rerr == nil && rcode == 200 {
		readmeText = stripYAMLFrontmatter(string(readme))
	}

	var raw strings.Builder
	fmt.Fprintf(&raw, "HuggingFace %s: %s\n", strings.TrimSuffix(kind, "s"), repoID)
	if stats != "" {
		fmt.Fprintf(&raw, "Stats: %s\n", stats)
	}
	if readmeText != "" {
		raw.WriteString("\nREADME:\n")
		raw.WriteString(clipRunesCompact(readmeText, readmeMax))
	}

	return &Result{
		Platform:     "huggingface",
		CanonicalURL: "https://huggingface.co/" + rawPrefix,
		Title:        repoID,
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(readmeText), 260),
		RawText:      strings.TrimSpace(raw.String()),
	}, nil
}

// --- 5. X / Twitter Extractor (via configurable FxTwitter / VxTwitter JSON API) ---

func (s *Service) extractTwitter(ctx context.Context, cli *http.Client, u *url.URL) (*Result, error) {
	m := tweetPathRe.FindStringSubmatch(u.Path)
	if len(m) < 3 {
		return nil, ErrEmptyContent
	}
	screenUser, statusID := m[1], m[2]

	s.mu.RLock()
	primaryBase := s.twitterAPIBase
	fallbackBase := s.twitterFallbackBase
	s.mu.RUnlock()

	if res, err := s.fetchFxTwitter(ctx, cli, primaryBase, screenUser, statusID); err == nil {
		return res, nil
	}
	if fallbackBase != "" && fallbackBase != primaryBase {
		if res, err := s.fetchVxTwitter(ctx, cli, fallbackBase, screenUser, statusID); err == nil {
			return res, nil
		}
	}
	return nil, ErrBlockedBySite
}

func (s *Service) fetchFxTwitter(ctx context.Context, cli *http.Client, base, user, id string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/%s/status/%s", strings.TrimRight(base, "/"), url.PathEscape(user), url.PathEscape(id))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 2<<20, map[string]string{"Accept": "application/json"})
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("fxtwitter HTTP %d: %v", code, err)
	}
	var payload struct {
		Code  int `json:"code"`
		Tweet *struct {
			URL       string `json:"url"`
			Text      string `json:"text"`
			CreatedAt string `json:"created_at"`
			Likes     int    `json:"likes"`
			Retweets  int    `json:"retweets"`
			Replies   int    `json:"replies"`
			Views     int    `json:"views"`
			Author    struct {
				Name       string `json:"name"`
				ScreenName string `json:"screen_name"`
			} `json:"author"`
			Media *struct {
				Photos []any `json:"photos"`
				Videos []any `json:"videos"`
			} `json:"media"`
			Quote *struct {
				Text   string `json:"text"`
				Author struct {
					Name       string `json:"name"`
					ScreenName string `json:"screen_name"`
				} `json:"author"`
			} `json:"quote"`
		} `json:"tweet"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Tweet == nil {
		return nil, ErrEmptyContent
	}
	tw := payload.Tweet
	text := strings.TrimSpace(tw.Text)
	if text == "" && (tw.Media == nil || (len(tw.Media.Photos) == 0 && len(tw.Media.Videos) == 0)) {
		return nil, ErrEmptyContent
	}
	author := tw.Author.Name
	if tw.Author.ScreenName != "" {
		if author != "" {
			author = fmt.Sprintf("%s (@%s)", author, tw.Author.ScreenName)
		} else {
			author = "@" + tw.Author.ScreenName
		}
	}
	var statParts []string
	if tw.Likes > 0 {
		statParts = append(statParts, "♥"+formatCountEN(tw.Likes))
	}
	if tw.Retweets > 0 {
		statParts = append(statParts, "RT "+formatCountEN(tw.Retweets))
	}
	if tw.Replies > 0 {
		statParts = append(statParts, "💬"+formatCountEN(tw.Replies))
	}
	if tw.Views > 0 {
		statParts = append(statParts, formatCountEN(tw.Views)+" views")
	}
	if tw.Media != nil {
		if len(tw.Media.Photos) > 0 {
			statParts = append(statParts, fmt.Sprintf("附%d图", len(tw.Media.Photos)))
		}
		if len(tw.Media.Videos) > 0 {
			statParts = append(statParts, fmt.Sprintf("附%d视频", len(tw.Media.Videos)))
		}
	}
	stats := strings.Join(statParts, " · ")

	fullText := text
	if tw.Quote != nil && strings.TrimSpace(tw.Quote.Text) != "" {
		qAuthor := tw.Quote.Author.Name
		if tw.Quote.Author.ScreenName != "" {
			qAuthor += "(@" + tw.Quote.Author.ScreenName + ")"
		}
		fullText += fmt.Sprintf("\n[引用 %s]: %s", qAuthor, strings.TrimSpace(tw.Quote.Text))
	}

	canonUser := tw.Author.ScreenName
	if canonUser == "" {
		canonUser = user
	}
	return &Result{
		Platform:     "twitter",
		CanonicalURL: fmt.Sprintf("https://x.com/%s/status/%s", canonUser, id),
		Title:        fmt.Sprintf("%s 的推文", author),
		Author:       author,
		PublishedAt:  tw.CreatedAt,
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(fullText), 360),
		RawText:      fmt.Sprintf("X/Twitter Post by %s\nStats: %s\nDate: %s\n\n%s", author, stats, tw.CreatedAt, fullText),
	}, nil
}

func (s *Service) fetchVxTwitter(ctx context.Context, cli *http.Client, base, user, id string) (*Result, error) {
	apiURL := fmt.Sprintf("%s/%s/status/%s", strings.TrimRight(base, "/"), url.PathEscape(user), url.PathEscape(id))
	body, _, code, err := netutil.FetchLimited(ctx, cli, apiURL, 2<<20, map[string]string{"Accept": "application/json"})
	if err != nil || code >= 400 {
		return nil, fmt.Errorf("vxtwitter HTTP %d: %v", code, err)
	}
	var tw struct {
		Text          string   `json:"text"`
		UserName      string   `json:"user_name"`
		UserScreen    string   `json:"user_screen_name"`
		Date          string   `json:"date"`
		Likes         int      `json:"likes"`
		Retweets      int      `json:"retweets"`
		Replies       int      `json:"replies"`
		MediaURLs     []string `json:"mediaURLs"`
	}
	if err := json.Unmarshal(body, &tw); err != nil || (strings.TrimSpace(tw.Text) == "" && len(tw.MediaURLs) == 0) {
		return nil, ErrEmptyContent
	}
	author := tw.UserName
	if tw.UserScreen != "" {
		author = fmt.Sprintf("%s (@%s)", author, tw.UserScreen)
	}
	stats := fmt.Sprintf("♥%s · RT %s · 💬%s", formatCountEN(tw.Likes), formatCountEN(tw.Retweets), formatCountEN(tw.Replies))
	if len(tw.MediaURLs) > 0 {
		stats += fmt.Sprintf(" · 附%d媒体", len(tw.MediaURLs))
	}
	canonUser := tw.UserScreen
	if canonUser == "" {
		canonUser = user
	}
	return &Result{
		Platform:     "twitter",
		CanonicalURL: fmt.Sprintf("https://x.com/%s/status/%s", canonUser, id),
		Title:        fmt.Sprintf("%s 的推文", author),
		Author:       author,
		PublishedAt:  tw.Date,
		Stats:        stats,
		Summary:      clipRunesCompact(collapseSpaces(tw.Text), 360),
		RawText:      fmt.Sprintf("X/Twitter Post by %s\nStats: %s\nDate: %s\n\n%s", author, stats, tw.Date, tw.Text),
	}, nil
}

// --- 6. WeChat Official Account (mp.weixin.qq.com) Extractor ---

var (
	wxMsgTitleRe = regexp.MustCompile(`var\s+msg_title\s*=\s*['"]([^'"]+)['"]`)
	wxMsgDescRe  = regexp.MustCompile(`var\s+msg_desc\s*=\s*['"]([^'"]+)['"]`)
	wxNickRe     = regexp.MustCompile(`var\s+nickname\s*=\s*htmlDecode\(['"]([^'"]+)['"]\)`)
)

func (s *Service) extractWeixin(ctx context.Context, cli *http.Client, targetURL string, deep bool) (*Result, error) {
	maxBytes := int64(3 << 20)
	if deep {
		maxBytes = 8 << 20
	}
	headers := map[string]string{
		"User-Agent":      defaultBrowserUA,
		"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language": "zh-CN,zh;q=0.9",
	}
	data, ct, code, err := netutil.FetchLimited(ctx, cli, targetURL, maxBytes, headers)
	if err != nil {
		return nil, err
	}
	if code >= 400 {
		return nil, fmt.Errorf("weixin HTTP %d", code)
	}
	utf8HTML := decodeHTMLToUTF8(data, ct)
	if isWeixinCaptchaPage(utf8HTML) {
		return nil, ErrBlockedBySite
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(utf8HTML))
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(doc.Find("#activity-name").First().Text())
	if title == "" {
		title = metaContent(doc, "og:title", "twitter:title")
	}
	if title == "" {
		if m := wxMsgTitleRe.FindStringSubmatch(utf8HTML); len(m) >= 2 {
			title = strings.TrimSpace(m[1])
		}
	}
	author := strings.TrimSpace(doc.Find("#js_name").First().Text())
	if author == "" {
		author = metaContent(doc, "author")
	}
	if author == "" {
		if m := wxNickRe.FindStringSubmatch(utf8HTML); len(m) >= 2 {
			author = strings.TrimSpace(m[1])
		}
	}
	desc := metaContent(doc, "og:description", "description")
	if desc == "" {
		if m := wxMsgDescRe.FindStringSubmatch(utf8HTML); len(m) >= 2 {
			desc = strings.TrimSpace(m[1])
		}
	}

	contentSel := doc.Find("#js_content").First()
	contentSel.Find("script, style, noscript").Remove()
	bodyText := collapseSpaces(contentSel.Text())
	if title == "" && bodyText == "" {
		return nil, ErrEmptyContent
	}
	if isBlockedPageTitle(title) {
		return nil, ErrBlockedBySite
	}

	summary := collapseSpaces(bodyText)
	if summary == "" {
		summary = collapseSpaces(desc)
	}
	var raw strings.Builder
	fmt.Fprintf(&raw, "微信公众号文章\nTitle: %s\n", title)
	if author != "" {
		fmt.Fprintf(&raw, "Account: %s\n", author)
	}
	if desc != "" {
		fmt.Fprintf(&raw, "Description: %s\n", desc)
	}
	if bodyText != "" {
		raw.WriteString("\nContent:\n")
		raw.WriteString(clipRunesCompact(bodyText, 6000))
	}

	return &Result{
		Platform:     "weixin",
		CanonicalURL: CanonicalizeURL(targetURL),
		Title:        title,
		Author:       author,
		Summary:      clipRunesCompact(summary, 360),
		RawText:      strings.TrimSpace(raw.String()),
	}, nil
}

func isWeixinCaptchaPage(htmlStr string) bool {
	if strings.Contains(htmlStr, "js_content") && (strings.Contains(htmlStr, "activity-name") || strings.Contains(htmlStr, "msg_title")) {
		return false
	}
	for _, marker := range []string{"环境异常", "去验证", "wappoc_appmsgcaptcha", "访问过于频繁", "此内容因违规无法查看"} {
		if strings.Contains(htmlStr, marker) {
			return true
		}
	}
	return false
}

// --- 7. Generic Web Page Extractor (OG / Twitter Card / Main Content) ---

func (s *Service) extractGenericHTML(ctx context.Context, cli *http.Client, targetURL string, deep bool) (*Result, error) {
	maxBytes := int64(2 << 20)
	if deep {
		maxBytes = 8 << 20
	}
	headers := map[string]string{
		"User-Agent":      defaultBrowserUA,
		"Accept":          "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.5",
		"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
	}
	data, ct, code, err := netutil.FetchLimited(ctx, cli, targetURL, maxBytes, headers)
	if err != nil {
		return nil, err
	}
	if code >= 400 {
		return nil, fmt.Errorf("HTTP %d", code)
	}
	lowCT := strings.ToLower(ct)
	if lowCT != "" && !strings.Contains(lowCT, "text/html") && !strings.Contains(lowCT, "application/xhtml") && !strings.Contains(lowCT, "text/plain") {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedContent, ct)
	}

	utf8Text := decodeHTMLToUTF8(data, ct)
	if strings.Contains(lowCT, "text/plain") {
		clean := strings.TrimSpace(utf8Text)
		if clean == "" {
			return nil, ErrEmptyContent
		}
		return &Result{
			Platform:     "generic",
			CanonicalURL: CanonicalizeURL(targetURL),
			Title:        clipRunesCompact(collapseSpaces(clean), 60),
			Summary:      clipRunesCompact(collapseSpaces(clean), 320),
			RawText:      clipRunesCompact(clean, 8000),
		}, nil
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(utf8Text))
	if err != nil {
		return nil, err
	}
	title := metaContent(doc, "og:title", "twitter:title")
	if title == "" {
		title = collapseSpaces(doc.Find("title").First().Text())
	}
	if title == "" {
		title = collapseSpaces(doc.Find("h1").First().Text())
	}
	if isBlockedPageTitle(title) {
		return nil, ErrBlockedBySite
	}

	author := metaContent(doc, "author", "article:author", "og:site_name")
	desc := metaContent(doc, "og:description", "twitter:description", "description")

	doc.Find("script, style, noscript, svg, nav, footer, header, aside, iframe, form").Remove()
	mainText := ""
	for _, sel := range []string{"article", "main", "[role='main']", ".article-content", ".post-content", "#content"} {
		if node := doc.Find(sel).First(); node.Length() > 0 {
			if txt := collapseSpaces(node.Text()); len([]rune(txt)) >= 60 {
				mainText = txt
				break
			}
		}
	}
	if mainText == "" {
		mainText = collapseSpaces(doc.Find("body").Text())
	}
	if title == "" && desc == "" && len([]rune(mainText)) < 20 {
		return nil, ErrEmptyContent
	}
	if isBlockedBody(title, mainText) || isLowValueGenericPage(title, desc, mainText) {
		return nil, ErrBlockedBySite
	}

	summary := collapseSpaces(desc)
	if summary == "" {
		summary = mainText
	} else if mainText != "" && !strings.Contains(mainText, summary) {
		summary = summary + " | " + clipRunesCompact(mainText, 240)
	} else if mainText != "" && len([]rune(mainText)) > len([]rune(summary))+60 {
		summary = mainText
	}

	var raw strings.Builder
	if title != "" {
		fmt.Fprintf(&raw, "Title: %s\n", title)
	}
	if author != "" {
		fmt.Fprintf(&raw, "Source/Author: %s\n", author)
	}
	if desc != "" {
		fmt.Fprintf(&raw, "Description: %s\n", desc)
	}
	if mainText != "" {
		raw.WriteString("\nContent:\n")
		raw.WriteString(clipRunesCompact(mainText, 7500))
	}

	return &Result{
		Platform:     "generic",
		CanonicalURL: CanonicalizeURL(targetURL),
		Title:        clipRunesCompact(title, 120),
		Author:       clipRunesCompact(author, 60),
		Summary:      clipRunesCompact(summary, 320),
		RawText:      strings.TrimSpace(raw.String()),
	}, nil
}

// ExtractPDFPages extracts text from pdfBytes using pdftotext (shared by parser and read_source).
func ExtractPDFPages(ctx context.Context, pdfBytes []byte, firstPage, lastPage int) (string, error) {
	tmp, err := os.CreateTemp("", "fatfish-*.pdf")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(pdfBytes); err != nil {
		_ = tmp.Close()
		return "", err
	}
	_ = tmp.Close()

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

// --- Helpers ---

func decodeHTMLToUTF8(data []byte, contentType string) string {
	r, err := charset.NewReader(bytes.NewReader(data), contentType)
	if err != nil {
		return string(data)
	}
	decoded, err := io.ReadAll(r)
	if err != nil {
		return string(data)
	}
	return string(decoded)
}

func metaContent(doc *goquery.Document, keys ...string) string {
	for _, k := range keys {
		if v, ok := doc.Find(fmt.Sprintf("meta[property='%s']", k)).First().Attr("content"); ok {
			if t := collapseSpaces(v); t != "" {
				return t
			}
		}
		if v, ok := doc.Find(fmt.Sprintf("meta[name='%s']", k)).First().Attr("content"); ok {
			if t := collapseSpaces(v); t != "" {
				return t
			}
		}
	}
	return ""
}

func isBlockedPageTitle(title string) bool {
	low := strings.ToLower(strings.TrimSpace(title))
	if low == "" {
		return false
	}
	for _, bad := range []string{
		"just a moment",
		"attention required",
		"access denied",
		"403 forbidden",
		"404 not found",
		"page not found",
		"verify you are human",
		"security check",
		"are you a robot",
		"安全验证",
		"访问受限",
		"人机验证",
		"安全中心",
		"请先登录",
		"登录验证",
		"页面不存在",
	} {
		if strings.Contains(low, bad) {
			return true
		}
	}
	return false
}

// isLowValueGenericPage rejects login-walled or SPA shell pages that only yield a bare site slogan/brand title
// with no meaningful description or article content (e.g. "小红书 - 你的生活兴趣社区", "爱发电 · 连接创作者与粉丝的会员制平台").
func isLowValueGenericPage(title, desc, body string) bool {
	bodyRunes := len([]rune(strings.TrimSpace(body)))
	descClean := strings.TrimSpace(desc)
	descRunes := len([]rune(descClean))
	low := strings.ToLower(strings.TrimSpace(title))
	lowDesc := strings.ToLower(descClean)

	// Reject SPA shell pages whose <meta description> is merely a generic site-wide marketing slogan
	// while the rendered body is empty/minimal.
	if bodyRunes < 60 {
		for _, slogan := range []string{
			"将创作的自由还给创作者",
			"让创作者简单地获得稳定收入的粉丝赞助平台",
			"小红书，标记我的生活",
		} {
			if strings.Contains(lowDesc, slogan) {
				return true
			}
		}
	}
	if descRunes >= 15 || bodyRunes >= 40 {
		return false
	}
	if low == "" {
		return true
	}
	for _, shell := range []string{
		"小红书 - 你的生活兴趣社区",
		"小红书",
		"爱发电 · 连接创作者与粉丝的会员制平台",
		"爱发电",
		"知乎 - 有问题",
		"抖音-记录美好生活",
		"微博",
		"哔哩哔哩 (゜-゜)つロ 干杯",
		"登录",
		"注册",
		"sign in",
		"log in",
	} {
		if low == shell || strings.HasPrefix(low, shell) {
			return true
		}
	}
	return false
}

func isBlockedBody(title, body string) bool {
	if len([]rune(body)) > 200 {
		return false
	}
	low := strings.ToLower(title + " " + body)
	for _, bad := range []string{
		"enable javascript and cookies to continue",
		"checking your browser before accessing",
		"please verify you are a human",
		"cloudflare ray id",
	} {
		if strings.Contains(low, bad) {
			return true
		}
	}
	return false
}

func splitPathParts(p string) []string {
	trimmed := strings.Trim(p, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func isDigits(s string) bool {
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

func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}

func formatDuration(sec int) string {
	if sec <= 0 {
		return "00:00"
	}
	h := sec / 3600
	m := (sec % 3600) / 60
	s := sec % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

func formatCountCN(n int) string {
	if n >= 100000000 {
		return fmt.Sprintf("%.1f亿", float64(n)/100000000.0)
	}
	if n >= 10000 {
		return fmt.Sprintf("%.1f万", float64(n)/10000.0)
	}
	return strconv.Itoa(n)
}

func formatCountEN(n int) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000.0)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", float64(n)/1000.0)
	}
	return strconv.Itoa(n)
}

func clipDate(iso string) string {
	iso = strings.TrimSpace(iso)
	if len(iso) >= 10 {
		return iso[:10]
	}
	return iso
}

func stripYAMLFrontmatter(s string) string {
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "---") {
		return trimmed
	}
	rest := trimmed[3:]
	if idx := strings.Index(rest, "\n---"); idx >= 0 {
		return strings.TrimSpace(rest[idx+4:])
	}
	return trimmed
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
