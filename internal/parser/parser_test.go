package parser

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

func TestExtractURLsAndCanonicalize(t *testing.T) {
	msg := &model.Message{
		Parts: []model.Part{
			{
				Kind: model.PartCard,
				URL:  "https://b23.tv/abc123?spm_id_from=333.999",
			},
			{
				Kind: model.PartText,
				Text: "快看https://github.com/iMMIQ/fatfish?utm_source=qq哈哈，还有 https://example.com/a.jpg 和 https://github.com/ 以及 https://gchat.qpic.cn/pic/123 还有 BV1xx411c7mD！",
			},
		},
	}

	links := ExtractURLs(msg, 4, []string{"blocked.example.com"}, nil)
	if len(links) != 3 {
		t.Fatalf("expected 3 parsable links (card, github repo, bare BV), got %d: %+v", len(links), links)
	}
	if !links[0].FromCard || !strings.HasPrefix(links[0].URL, "https://b23.tv/abc123") {
		t.Fatalf("unexpected first link: %+v", links[0])
	}
	if links[1].URL != "https://github.com/iMMIQ/fatfish?utm_source=qq" {
		t.Fatalf("unexpected second link: %+v", links[1])
	}
	if links[2].URL != "https://www.bilibili.com/video/BV1xx411c7mD" {
		t.Fatalf("unexpected bare BV link: %+v", links[2])
	}

	// Balanced parens in Wikipedia URLs should be preserved; trailing unmatched punct stripped.
	wikiMsg := &model.Message{
		Parts: []model.Part{{Kind: model.PartText, Text: "(看这个 https://en.wikipedia.org/wiki/Go_(programming_language))"}},
	}
	wLinks := ExtractURLs(wikiMsg, 2, nil, nil)
	if len(wLinks) != 1 || wLinks[0].URL != "https://en.wikipedia.org/wiki/Go_(programming_language)" {
		t.Fatalf("expected balanced-paren wiki URL, got %+v", wLinks)
	}

	// Canonicalization strips tracking params while preserving WeChat mid.
	if got := CanonicalizeURL("https://www.bilibili.com/video/BV1xx411c7mD/?spm_id_from=333&vd_source=xyz#reply"); got != "https://www.bilibili.com/video/BV1xx411c7mD" {
		t.Fatalf("unexpected canonical bilibili URL: %q", got)
	}
	if got := CanonicalizeURL("https://mp.weixin.qq.com/s?__biz=MzA=&mid=12345&idx=1&sn=abc&chksm=drop&scene=21"); !strings.Contains(got, "mid=12345") || strings.Contains(got, "chksm=") || strings.Contains(got, "scene=") {
		t.Fatalf("unexpected canonical weixin URL: %q", got)
	}
	if got := CanonicalizeURL("https://arxiv.org/pdf/2401.12345v2.pdf"); got != "https://arxiv.org/abs/2401.12345" {
		t.Fatalf("unexpected canonical arxiv URL: %q", got)
	}
}

func TestTier1ExtractorsAndCache(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		switch {
		case r.URL.Path == "/x/web-interface/view":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"aid":      12345,
					"cid":      67890,
					"bvid":     "BV1xx411c7mD",
					"title":    "纯 Go 写一个超轻群聊助手",
					"desc":     "从零实现 KV Cache 与链接解析",
					"duration": 522,
					"tname":    "科技",
					"owner":    map[string]any{"name": "大肥鱼UP"},
					"stat":     map[string]any{"view": 125000, "like": 12300, "coin": 4500},
				},
			})
		case r.URL.Path == "/x/player/v2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"subtitle": map[string]any{
						"subtitles": []map[string]any{{"subtitle_url": "http://" + r.Host + "/sub.json"}},
					},
				},
			})
		case r.URL.Path == "/sub.json":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"body": []map[string]any{{"content": "今天聊聊链接主动解析"}, {"content": "支持B站和GitHub"}},
			})
		case r.URL.Path == "/x/v2/reply":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"replies": []map[string]any{{
						"member":  map[string]any{"uname": "热心群友"},
						"content": map[string]any{"message": "太实用了三连支持"},
						"like":    88,
					}},
				},
			})
		case r.URL.Path == "/repos/iMMIQ/fatfish":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"full_name":        "iMMIQ/fatfish",
				"html_url":         "https://github.com/iMMIQ/fatfish",
				"description":      "Pure Go multimodal group AI assistant",
				"language":         "Go",
				"stargazers_count": 2400,
				"forks_count":      180,
				"open_issues_count": 5,
				"topics":           []string{"go", "ai", "bot"},
				"pushed_at":        "2026-11-20T00:00:00Z",
				"license":          map[string]any{"spdx_id": "LGPL-3.0"},
			})
		case r.URL.Path == "/repos/iMMIQ/fatfish/readme":
			_, _ = w.Write([]byte("# fatfish\nA pure Go assistant."))
		case r.URL.Path == "/api/query":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0"?><feed><entry><title>KV Cache First Agents</title><summary>We present a prefix-stable runtime.</summary><published>2026-10-01T00:00:00Z</published><author><name>Alice</name></author><category term="cs.AI"/></entry></feed>`))
		case r.URL.Path == "/api/models/org/demo-llm":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"downloads":    42000,
				"likes":        350,
				"pipeline_tag": "text-generation",
			})
		case r.URL.Path == "/org/demo-llm/raw/main/README.md":
			_, _ = w.Write([]byte("---\nlicense: apache-2.0\n---\nCompact reasoning model for local inference."))
		case r.URL.Path == "/alice/status/999":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 200,
				"tweet": map[string]any{
					"text":       "Shipping pure Go link parsing today!",
					"created_at": "2026-11-27",
					"likes":      1500,
					"retweets":   220,
					"replies":    45,
					"views":      98000,
					"author":     map[string]any{"name": "Alice", "screen_name": "alice"},
				},
			})
		case r.URL.Path == "/article":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<html><head><title>深度好文</title><meta property="og:description" content="这是一篇关于系统架构的深度文章"><meta name="author" content="技术周刊"></head><body><article><h1>深度好文</h1><p>文章正文讲解了为什么需要把解析内核和策略层解耦，从而兼顾延迟、缓存命中率与群聊自然度。</p></article></body></html>`))
		case r.URL.Path == "/blocked":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<html><head><title>Just a moment...</title></head><body>Checking your browser before accessing</body></html>`))
		case r.URL.Path == "/wx-ok":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<html><body><h1 id="activity-name">公众号深度解析</h1><a id="js_name">科技观察</a><div id="js_content">这是微信公众号的正文内容，用于验证提取器能够拿到标题、公众号名称和正文摘要。</div></body></html>`))
		case r.URL.Path == "/wx-captcha":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<html><body><div>环境异常，请完成安全验证去验证</div></body></html>`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	svc := NewService(Options{
		AllowHosts:     []string{"127.0.0.1"},
		Timeout:        5 * time.Second,
		CacheTTL:       5 * time.Minute,
		TwitterAPIBase: srv.URL,
	})
	svc.bilibiliAPIBase = srv.URL
	svc.githubAPIBase = srv.URL
	svc.arxivAPIBase = srv.URL
	svc.arxivPDFBase = srv.URL
	svc.hfMirrorBase = srv.URL
	svc.hfOriginBase = srv.URL
	svc.twitterFallbackBase = ""

	ctx := context.Background()

	// 1. Bilibili video (subtitles + comments + stats)
	bRes, err := svc.Parse(ctx, "https://www.bilibili.com/video/BV1xx411c7mD?spm_id_from=333", ParseOptions{})
	if err != nil {
		t.Fatalf("parse bilibili: %v", err)
	}
	if bRes.Platform != "bilibili" || bRes.Author != "大肥鱼UP" || !strings.Contains(bRes.Stats, "12.5万播放") || !strings.Contains(bRes.Summary, "字幕摘录") {
		t.Fatalf("unexpected bilibili result: %+v", bRes)
	}

	// Cache hit on same canonical URL should perform zero additional HTTP requests.
	hitsBefore := hits.Load()
	bRes2, err := svc.Parse(ctx, "https://www.bilibili.com/video/BV1xx411c7mD?vd_source=other", ParseOptions{})
	if err != nil || bRes2.Title != bRes.Title || hits.Load() != hitsBefore {
		t.Fatalf("expected cache hit with zero extra requests, delta=%d err=%v", hits.Load()-hitsBefore, err)
	}

	// 2. GitHub repo (fast vs deep README upgrade)
	gRes, err := svc.Parse(ctx, "https://github.com/iMMIQ/fatfish", ParseOptions{Deep: false})
	if err != nil || gRes.Platform != "github" || !strings.Contains(gRes.Stats, "★2.4k") {
		t.Fatalf("unexpected github fast result: %+v err=%v", gRes, err)
	}
	gDeep, err := svc.Parse(ctx, "https://github.com/iMMIQ/fatfish", ParseOptions{Deep: true})
	if err != nil || !strings.Contains(gDeep.RawText, "README:") {
		t.Fatalf("expected github deep result with README, got %+v err=%v", gDeep, err)
	}

	// 3. arXiv
	aRes, err := svc.Parse(ctx, "https://arxiv.org/abs/2610.00001", ParseOptions{})
	if err != nil || aRes.Platform != "arxiv" || aRes.Title != "KV Cache First Agents" {
		t.Fatalf("unexpected arxiv result: %+v err=%v", aRes, err)
	}

	// 4. HuggingFace
	hRes, err := svc.Parse(ctx, "https://huggingface.co/org/demo-llm", ParseOptions{})
	if err != nil || hRes.Platform != "huggingface" || !strings.Contains(hRes.Summary, "Compact reasoning model") {
		t.Fatalf("unexpected hf result: %+v err=%v", hRes, err)
	}

	// 5. X / Twitter
	xRes, err := svc.Parse(ctx, "https://x.com/alice/status/999", ParseOptions{})
	if err != nil || xRes.Platform != "twitter" || !strings.Contains(xRes.Summary, "pure Go link parsing") {
		t.Fatalf("unexpected twitter result: %+v err=%v", xRes, err)
	}

	// 6. Generic HTML + Cloudflare wall rejection
	genRes, err := svc.Parse(ctx, srv.URL+"/article", ParseOptions{})
	if err != nil || genRes.Title != "深度好文" || !strings.Contains(genRes.Summary, "系统架构") {
		t.Fatalf("unexpected generic html result: %+v err=%v", genRes, err)
	}
	if _, err := svc.Parse(ctx, srv.URL+"/blocked", ParseOptions{}); !errors.Is(err, ErrBlockedBySite) {
		t.Fatalf("expected ErrBlockedBySite for cloudflare challenge, got %v", err)
	}

	// 7. WeChat extractor direct test (article + captcha wall)
	_, fastCli, _, _ := svc.snapshotConfig()
	wxRes, err := svc.extractWeixin(ctx, fastCli, srv.URL+"/wx-ok", false)
	if err != nil || wxRes.Platform != "weixin" || wxRes.Title != "公众号深度解析" || wxRes.Author != "科技观察" {
		t.Fatalf("unexpected weixin result: %+v err=%v", wxRes, err)
	}
	if _, err := svc.extractWeixin(ctx, fastCli, srv.URL+"/wx-captcha", false); !errors.Is(err, ErrBlockedBySite) {
		t.Fatalf("expected ErrBlockedBySite for weixin captcha, got %v", err)
	}

	// 8. Low-value login-wall / SPA slogan shell pages must be rejected instead of spamming generic titles.
	if !isLowValueGenericPage("小红书 - 你的生活兴趣社区", "", "") {
		t.Fatal("expected bare site login-wall title with empty body to be flagged as low-value")
	}
	if isLowValueGenericPage("小红书 - 你的生活兴趣社区", "这是一篇完整详细的笔记正文描述内容超过十五字", "") {
		t.Fatal("expected page with substantive description not to be flagged as low-value")
	}
	if !isLowValueGenericPage("爱发电 · 连接创作者与粉丝的会员制平台", "将创作的自由还给创作者！爱发电是让创作者简单地获得稳定收入的粉丝赞助平台。", "") {
		t.Fatal("expected SPA shell with generic site slogan description and empty body to be flagged as low-value")
	}
}
