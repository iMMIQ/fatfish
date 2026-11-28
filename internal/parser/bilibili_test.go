package parser

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

func TestBilibiliWBISigning(t *testing.T) {
	// Official-spec vector check for mixin key derivation and w_rid calculation.
	imgKey := "7cd084941338484aae1ad9425b84077c"
	subKey := "4932caff0ff746eab6f01bf08b70ac45"
	mk := deriveWBIMixinKey(imgKey, subKey)
	if len(mk) != 32 || mk != "ea1db124af3c7062474693fa704f4ff8" {
		t.Fatalf("unexpected WBI mixinKey: %q", mk)
	}

	params := url.Values{}
	params.Set("foo", "114")
	params.Set("bar", "514")
	params.Set("baz", "hello world!'()*")
	signed := signWBIParams(params, mk, time.Unix(1700000000, 0))
	if !strings.Contains(signed, "baz=hello%20world") {
		t.Fatalf("expected WBI sanitization and %%20 encoding, got %q", signed)
	}
	if !strings.Contains(signed, "wts=1700000000") || !strings.Contains(signed, "&w_rid=") {
		t.Fatalf("missing wts or w_rid in signed query: %q", signed)
	}
}

func TestBilibiliShortURLSkipsDestinationHTMLDownload(t *testing.T) {
	var destHTMLHits atomic.Int32
	destSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destHTMLHits.Add(1)
		_, _ = w.Write([]byte("<html><body>heavy page that must not be fetched</body></html>"))
	}))
	defer destSrv.Close()

	shortSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destSrv.URL+"/video/BV1xx411c7mD?p=2&t=125", http.StatusFound)
	}))
	defer shortSrv.Close()

	svc := NewService(Options{AllowHosts: []string{"127.0.0.1"}, Timeout: 5 * time.Second})
	_, fastCli, _, _ := svc.snapshotConfig()

	// Zero-RTT direct BV path on b23.tv
	direct, err := svc.expandShortURL(context.Background(), fastCli, "https://b23.tv/BV1xx411c7mD?p=3&t=60")
	if err != nil || direct != "https://www.bilibili.com/video/BV1xx411c7mD?p=3&t=60" {
		t.Fatalf("unexpected zero-RTT b23.tv expansion: %q err=%v", direct, err)
	}

	// Redirect expansion should stop at Location header without fetching destination HTML.
	got, err := svc.expandShortURL(context.Background(), fastCli, shortSrv.URL+"/abc")
	if err != nil {
		t.Fatalf("expandShortURL failed: %v", err)
	}
	if got != destSrv.URL+"/video/BV1xx411c7mD?p=2&t=125" {
		t.Fatalf("unexpected expanded URL: %q", got)
	}
	if destHTMLHits.Load() != 0 {
		t.Fatalf("expected 0 hits to destination HTML page, got %d", destHTMLHits.Load())
	}
}

func TestBilibiliBareAVAndCanonicalizePage(t *testing.T) {
	msg := &model.Message{
		Parts: []model.Part{{
			Kind: model.PartText,
			Text: "推荐看 av170001，画质支持 av1 和 av01 编码！",
		}},
	}
	links := ExtractURLs(msg, 4, nil, nil)
	if len(links) != 1 || links[0].URL != "https://www.bilibili.com/video/av170001" {
		t.Fatalf("expected only av170001 (ignoring av1/av01 codecs), got %+v", links)
	}

	if got := CanonicalizeURL("https://www.bilibili.com/video/BV1xx411c7mD?p=3&t=90&spm_id_from=333"); got != "https://www.bilibili.com/video/BV1xx411c7mD?p=3" {
		t.Fatalf("expected ?p=3 preserved and tracking/t stripped from cache key, got %q", got)
	}
	if got := CanonicalizeURL("https://www.bilibili.com/video/BV1xx411c7mD?p=1&t=90"); got != "https://www.bilibili.com/video/BV1xx411c7mD" {
		t.Fatalf("expected ?p=1 normalized away, got %q", got)
	}
}

func TestBilibiliVideoFullWBI_Pages_Timestamp_AISummary_PinnedReply(t *testing.T) {
	var navHits, spiHits, viewHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x/web-interface/nav":
			navHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": -101,
				"data": map[string]any{
					"wbi_img": map[string]any{
						"img_url": "https://i0.hdslb.com/bfs/wbi/7cd084941338484aae1ad9425b84077c.png",
						"sub_url": "https://i0.hdslb.com/bfs/wbi/4932caff0ff746eab6f01bf08b70ac45.png",
					},
				},
			})
		case "/x/frontend/finger/spi":
			spiHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{"b_3": "guest_b3_token", "b_4": "guest_b4_token"},
			})
		case "/x/web-interface/view":
			viewHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"aid":      998877,
					"cid":      1001,
					"bvid":     "BV1TestWbi999",
					"title":    "2026 显卡横评实测",
					"desc":     "全网最详细的能耗比横评",
					"dynamic":  "补测数据见置顶评论",
					"pic":      "//i0.hdslb.com/bfs/archive/cover.jpg",
					"duration": 900,
					"pubdate":  1760000000,
					"tname":    "数码",
					"owner":    map[string]any{"mid": 42, "name": "硬件老哥"},
					"staff": []map[string]any{
						{"title": "UP主", "name": "硬件老哥"},
						{"title": "测试", "name": "数据小哥"},
					},
					"stat": map[string]any{
						"view": 250000, "danmaku": 3200, "like": 18000, "coin": 9500, "favorite": 12000,
					},
					"argue_info": map[string]any{"argue_msg": "测试样本为工程样卡，零售版以正式发布为准"},
					"ugc_season": map[string]any{"title": "年度硬件横评合集"},
					"pages": []map[string]any{
						{"cid": 1001, "page": 1, "part": "理论跑分", "duration": 400},
						{"cid": 1002, "page": 2, "part": "游戏功耗实测", "duration": 500},
					},
				},
			})
		case "/x/web-interface/view/conclusion/get":
			if r.URL.Query().Get("w_rid") == "" || r.URL.Query().Get("cid") != "1002" {
				t.Errorf("conclusion request missing w_rid or wrong cid: %s", r.URL.RawQuery)
			}
			if !strings.Contains(r.Header.Get("Cookie"), "buvid3=guest_b3_token") {
				t.Errorf("expected anonymous buvid3 cookie on conclusion request, got %q", r.Header.Get("Cookie"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"code": 0,
					"model_result": map[string]any{
						"summary": "本期P2实测4K游戏功耗，新卡满载220W比上代低25%。",
						"outline": []map[string]any{
							{
								"title":     "4K游戏功耗对比",
								"timestamp": 120,
								"part_outline": []map[string]any{
									{"timestamp": 130, "content": "赛博朋克满载220W"},
								},
							},
						},
					},
				},
			})
		case "/x/player/wbi/v2":
			if r.URL.Query().Get("w_rid") == "" || r.URL.Query().Get("cid") != "1002" {
				t.Errorf("player/wbi/v2 missing w_rid or wrong cid: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"view_points": []map[string]any{
						{"from": 0, "to": 120, "content": "测试平台介绍"},
						{"from": 120, "to": 300, "content": "功耗实测数据"},
					},
					"subtitle": map[string]any{
						"subtitles": []map[string]any{
							{"lan": "en-US", "lan_doc": "English", "subtitle_url": "http://" + r.Host + "/sub_en.json"},
							{"lan": "zh-CN", "lan_doc": "中文（简体）", "subtitle_url": "http://" + r.Host + "/sub_zh.json"},
						},
					},
				},
			})
		case "/sub_zh.json":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"body": []map[string]any{
					{"from": 5.0, "to": 10.0, "content": "欢迎回到P2游戏实测环节"},
					{"from": 125.0, "to": 135.0, "content": "重点来了，这里测得满载功耗只有220瓦"},
					{"from": 460.0, "to": 480.0, "content": "最终总结推荐等双十一降价入手"},
				},
			})
		case "/x/v2/reply/wbi/main":
			if r.URL.Query().Get("w_rid") == "" {
				t.Errorf("reply/wbi/main missing w_rid")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"top": map[string]any{
						"upper": map[string]any{
							"member":  map[string]any{"uname": "硬件老哥"},
							"content": map[string]any{"message": "勘误：表格第3行功耗应为218W，完整Excel见简介"},
							"like":    520,
						},
					},
					"replies": []map[string]any{
						{"member": map[string]any{"uname": "路人甲"}, "content": map[string]any{"message": "第一"}, "like": 999},
						{"member": map[string]any{"uname": "热心课代表"}, "content": map[string]any{"message": "省流：220W性能追平上代旗舰，性价比拉满"}, "like": 400},
					},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	svc := NewService(Options{AllowHosts: []string{"127.0.0.1"}, Timeout: 5 * time.Second})
	svc.bilibiliAPIBase = srv.URL

	res, err := svc.Parse(context.Background(), "https://www.bilibili.com/video/BV1TestWbi999?p=2", ParseOptions{Deep: true})
	if err != nil {
		t.Fatalf("parse bilibili video failed: %v", err)
	}
	if res.CanonicalURL != "https://www.bilibili.com/video/BV1TestWbi999?p=2" {
		t.Fatalf("unexpected canonical URL: %q", res.CanonicalURL)
	}
	if !strings.Contains(res.Title, "P2: 游戏功耗实测") {
		t.Fatalf("expected P2 title annotation, got %q", res.Title)
	}
	if res.Author != "硬件老哥 / 数据小哥" {
		t.Fatalf("expected staff joint author, got %q", res.Author)
	}
	if res.CoverURL != "https://i0.hdslb.com/bfs/archive/cover.jpg" {
		t.Fatalf("unexpected cover URL: %q", res.CoverURL)
	}
	if res.AISummary == "" || !strings.Contains(res.AISummary, "220W") {
		t.Fatalf("expected AI summary, got %q", res.AISummary)
	}
	if !strings.Contains(res.PinnedComment, "勘误") {
		t.Fatalf("expected UP pinned comment, got %q", res.PinnedComment)
	}
	if strings.Contains(res.TopComments, "第一") || !strings.Contains(res.TopComments, "热心课代表") {
		t.Fatalf("expected low-value comment filtered and good comment kept, got %q", res.TopComments)
	}
	if !strings.Contains(res.RawText, "[02:05] 重点来了") || !strings.Contains(res.RawText, "最终总结") {
		t.Fatalf("expected timestamped Chinese subtitles in RawText, got:\n%s", res.RawText)
	}
	if res.ArgueMsg == "" || !strings.Contains(res.Stats, "1.2万藏") || !strings.Contains(res.Stats, "3200弹幕") {
		t.Fatalf("expected argue_msg, favorite and danmaku stats, got stats=%q argue=%q", res.Stats, res.ArgueMsg)
	}

	groupCard := res.FormatForGroupText()
	if !strings.Contains(groupCard, "提示:") || !strings.Contains(groupCard, "要点:") || !strings.Contains(groupCard, "UP置顶:") {
		t.Fatalf("unexpected structured group card:\n%s", groupCard)
	}

	// Second request with ?t=2m10s should hit cache (0 extra view hits) and dynamically inject timestamp focus!
	resTS, err := svc.Parse(context.Background(), "https://www.bilibili.com/video/BV1TestWbi999?p=2&t=2m10s", ParseOptions{Deep: true})
	if err != nil {
		t.Fatalf("cached parse with ?t failed: %v", err)
	}
	if viewHits.Load() != 1 || navHits.Load() != 1 || spiHits.Load() != 1 {
		t.Fatalf("expected cached session & video (view=%d nav=%d spi=%d)", viewHits.Load(), navHits.Load(), spiHits.Load())
	}
	if !strings.Contains(resTS.Summary, "[空降定位 02:10]") || !strings.Contains(resTS.Summary, "220瓦") {
		t.Fatalf("expected timestamp focus in summary, got %q", resTS.Summary)
	}
}

func TestBilibiliArticleBangumiLiveOpusAndAudio(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x/article/viewinfo":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"title":       "深度解析Go并发调度",
					"author_name": "后端专栏作者",
					"banner_url":  "//i0.hdslb.com/banner.png",
					"stats":       map[string]any{"view": 15000, "like": 800, "coin": 200, "favorite": 600},
				},
			})
		case "/read/cv12345":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<html><body><div id="article-content"><p>GMP 调度器通过本地队列和 work-stealing 实现极低延迟的高并发执行。</p></div></body></html>`))
		case "/pgc/view/web/season":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"result": map[string]any{
					"title":    "葬送的芙莉莲",
					"evaluate": "勇者一行打倒魔王之后的温情旅途。",
					"cover":    "https://i0.hdslb.com/frieren.jpg",
					"new_ep":   map[string]any{"desc": "已完结, 全28话"},
					"rating":   map[string]any{"score": 9.9, "count": 50000},
					"stat":     map[string]any{"views": 180000000, "favorites": 8500000},
					"episodes": []map[string]any{{"id": 777, "title": "1", "long_title": "冒险的终点"}},
				},
			})
		case "/room/v1/Room/get_info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"uid": 88, "room_id": 2233, "title": "深夜敲代码聊天",
					"live_status": 1, "live_time": "2026-11-28 21:00:00",
					"parent_area_name": "知识", "area_name": "科技科普",
					"online": 12800, "attention": 56000,
					"keyframe": "https://i0.hdslb.com/live.jpg",
				},
			})
		case "/live_user/v1/Master/info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{"info": map[string]any{"uname": "代码主播"}},
			})
		case "/x/polymer/web-dynamic/v1/detail":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"item": map[string]any{
						"modules": map[string]any{
							"module_author": map[string]any{"name": "转发者"},
							"module_dynamic": map[string]any{
								"desc": map[string]any{"text": "太强了这个必须转"},
							},
							"module_stat": map[string]any{
								"like":    map[string]any{"count": 30},
								"comment": map[string]any{"count": 5},
								"forward": map[string]any{"count": 2},
							},
						},
						"orig": map[string]any{
							"modules": map[string]any{
								"module_author": map[string]any{"name": "原作者"},
								"module_dynamic": map[string]any{
									"major": map[string]any{
										"archive": map[string]any{
											"bvid":  "BV1OrigVid123",
											"title": "自制微型火箭发射成功",
											"cover": "https://i0.hdslb.com/rocket.jpg",
										},
									},
								},
							},
						},
					},
				},
			})
		case "/x/player/playurl":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"dash": map[string]any{
						"audio": []map[string]any{
							{"id": 30280, "bandwidth": 192000, "baseUrl": "http://" + r.Host + "/audio_hi.m4s"},
							{"id": 30216, "bandwidth": 64000, "baseUrl": "http://" + r.Host + "/audio_lo.m4s"},
						},
					},
				},
			})
		case "/audio_lo.m4s":
			if !strings.Contains(r.Header.Get("Referer"), "bilibili.com") {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			_, _ = w.Write([]byte("FAKE_LOW_BITRATE_AUDIO_STREAM"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	svc := NewService(Options{AllowHosts: []string{"127.0.0.1"}, Timeout: 5 * time.Second})
	svc.bilibiliAPIBase = srv.URL
	svc.bilibiliLiveAPIBase = srv.URL
	svc.bilibiliWebBase = srv.URL
	ctx := context.Background()

	// 1. Article with full body
	art, err := svc.Parse(ctx, "https://www.bilibili.com/read/cv12345", ParseOptions{Deep: true})
	if err != nil || !strings.Contains(art.Summary, "GMP") || !strings.Contains(art.RawText, "work-stealing") {
		t.Fatalf("unexpected article result: %+v err=%v", art, err)
	}

	// 2. Bangumi episode
	bgm, err := svc.Parse(ctx, "https://www.bilibili.com/bangumi/play/ep777", ParseOptions{})
	if err != nil || !strings.Contains(bgm.Title, "冒险的终点") || !strings.Contains(bgm.Stats, "评分 9.9") {
		t.Fatalf("unexpected bangumi result: %+v err=%v", bgm, err)
	}

	// 3. Live room
	live, err := svc.Parse(ctx, "https://live.bilibili.com/2233", ParseOptions{})
	if err != nil || live.Author != "代码主播" || !strings.Contains(live.Stats, "直播中") || !strings.Contains(live.Stats, "知识/科技科普") {
		t.Fatalf("unexpected live result: %+v err=%v", live, err)
	}

	// 4. Reposted dynamic with attached original video
	dyn, err := svc.Parse(ctx, "https://www.bilibili.com/opus/123456789012345678", ParseOptions{})
	if err != nil || !strings.Contains(dyn.Summary, "转发自 @原作者") || !strings.Contains(dyn.Summary, "自制微型火箭发射成功") {
		t.Fatalf("unexpected repost dynamic result: %+v err=%v", dyn, err)
	}

	// 5. DASH lowest-bitrate audio fetch
	audio, fname, err := svc.FetchBilibiliAudio(ctx, "BV1NoSub12345", 555, 1<<20)
	if err != nil || string(audio) != "FAKE_LOW_BITRATE_AUDIO_STREAM" || !strings.HasSuffix(fname, ".m4a") {
		t.Fatalf("unexpected audio fetch: fname=%q body=%q err=%v", fname, string(audio), err)
	}
}

func TestFetchBilibiliVideoMP4SizeLimitsAndDowngrade(t *testing.T) {
	var hiHits, loHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x/player/playurl":
			qn := r.URL.Query().Get("qn")
			switch qn {
			case "64":
				// 720p declared as 40 MiB -> exceeds limit, must NOT be downloaded.
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 0,
					"data": map[string]any{
						"durl": []map[string]any{{
							"size": 40 << 20,
							"url":  "http://" + r.Host + "/vid_720.mp4",
						}},
					},
				})
			case "32":
				// 480p fits under limit.
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 0,
					"data": map[string]any{
						"durl": []map[string]any{{
							"size": 1024,
							"url":  "http://" + r.Host + "/vid_480.mp4",
						}},
					},
				})
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		case "/vid_720.mp4":
			hiHits.Add(1)
			_, _ = w.Write([]byte("SHOULD_NEVER_DOWNLOAD"))
		case "/vid_480.mp4":
			loHits.Add(1)
			_, _ = w.Write([]byte("MP4_480P_OK_PAYLOAD"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	svc := NewService(Options{AllowHosts: []string{"127.0.0.1"}, Timeout: 5 * time.Second})
	svc.bilibiliAPIBase = srv.URL

	// 1. Limit 1 MiB -> skips 40 MiB 720p (0 hits) and automatically downloads 480p.
	data, err := svc.FetchBilibiliVideoMP4(context.Background(), "BV1VidTest01", 888, 1<<20)
	if err != nil {
		t.Fatalf("FetchBilibiliVideoMP4 failed: %v", err)
	}
	if string(data) != "MP4_480P_OK_PAYLOAD" {
		t.Fatalf("unexpected mp4 data: %q", string(data))
	}
	if hiHits.Load() != 0 || loHits.Load() != 1 {
		t.Fatalf("expected 0 hits on oversized 720p and 1 hit on 480p, got hi=%d lo=%d", hiHits.Load(), loHits.Load())
	}

	// 2. Limit 10 bytes -> even 480p exceeds cap -> returns ErrVideoTooLarge without downloading body.
	loBefore := loHits.Load()
	_, err = svc.FetchBilibiliVideoMP4(context.Background(), "BV1VidTest01", 888, 10)
	if err != ErrVideoTooLarge {
		t.Fatalf("expected ErrVideoTooLarge, got %v", err)
	}
	if loHits.Load() != loBefore {
		t.Fatalf("expected 0 CDN downloads when declared size > maxBytes, got delta=%d", loHits.Load()-loBefore)
	}
}
