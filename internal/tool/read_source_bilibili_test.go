package tool

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/parser"
)

func TestClipBilibiliTranscriptBalanced(t *testing.T) {
	header := "Bilibili BV1LongTest01\nTitle: 超长深度评测\nAI Summary:\n  核心结论见末尾\n\nSubtitles (中文):\n"
	headMark := "HEAD_INTRO_MARKER "
	midMark := " MIDDLE_BENCHMARK_MARKER "
	tailMark := " FINAL_CONCLUSION_MARKER"
	body := headMark + strings.Repeat("这是视频铺垫内容和中间过程说明。", 400) + midMark + strings.Repeat("这是后半段测试数据展示过程。", 400) + tailMark
	full := header + body

	got := clipBilibiliTranscriptBalanced(full, 8000)
	if len([]rune(got)) > 8000 {
		t.Fatalf("expected balanced clip <= 8000 runes, got %d", len([]rune(got)))
	}
	if !strings.Contains(got, "核心结论见末尾") || !strings.Contains(got, "HEAD_INTRO_MARKER") || !strings.Contains(got, "FINAL_CONCLUSION_MARKER") {
		t.Fatalf("expected header + head + tail preserved, got len=%d", len([]rune(got)))
	}
}

func TestReadSourceBilibiliASRFallbackAndCache(t *testing.T) {
	var audioHits, asrHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x/web-interface/view":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"aid":      55555,
					"cid":      66666,
					"bvid":     "BV1NoSubASR1",
					"title":    "无字幕口播快讯",
					"desc":     "听一下这段实拍发言",
					"duration": 95,
					"owner":    map[string]any{"mid": 7, "name": "现场UP"},
					"stat":     map[string]any{"view": 1000, "like": 50, "coin": 10},
				},
			})
		case "/x/player/v2":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{}})
		case "/x/v2/reply":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{}})
		case "/x/player/playurl":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"dash": map[string]any{
						"audio": []map[string]any{
							{"id": 30216, "bandwidth": 64000, "baseUrl": "http://" + r.Host + "/a.m4s"},
						},
					},
				},
			})
		case "/a.m4s":
			audioHits.Add(1)
			_, _ = w.Write([]byte("RAW_AUDIO_BYTES"))
		case "/v1/audio/transcriptions":
			asrHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"text": "发布会宣布明天上午十点正式开放公测"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	psvc := parser.NewService(parser.Options{
		AllowHosts: []string{"127.0.0.1"},
		Timeout:    5 * time.Second,
		CacheTTL:   5 * time.Minute,
	})
	psvc.SetBilibiliAPIBaseForTest(srv.URL)

	hub := infer.NewHub(config.ModelsConfig{
		ASR: config.ModelEndpointConfig{
			BaseURL:    srv.URL + "/v1",
			APIKey:     "test",
			Model:      "qwen3-asr-1.7b",
			TimeoutSec: 5,
		},
	})

	rt := NewReadSourceTool(hub, []string{"127.0.0.1"})
	rt.SetParser(psvc)

	res1, err := rt.Execute(context.Background(), ToolContext{}, ReadSourceArgs{
		URL: "https://www.bilibili.com/video/BV1NoSubASR1",
	})
	if err != nil {
		t.Fatalf("read_source execute failed: %v", err)
	}
	if !strings.Contains(res1.TextForLLM, "Subtitles (Local ASR)") || !strings.Contains(res1.TextForLLM, "明天上午十点正式开放公测") {
		t.Fatalf("expected ASR transcript in read_source output, got:\n%s", res1.TextForLLM)
	}

	// Second call should hit deep cache with ASR transcript already populated (0 extra audio/ASR calls).
	res2, err := rt.Execute(context.Background(), ToolContext{}, ReadSourceArgs{
		URL: "https://www.bilibili.com/video/BV1NoSubASR1",
	})
	if err != nil || !strings.Contains(res2.TextForLLM, "明天上午十点正式开放公测") {
		t.Fatalf("second read_source failed: %v", err)
	}
	if audioHits.Load() != 1 || asrHits.Load() != 1 {
		t.Fatalf("expected ASR fallback cached after first call (audioHits=%d asrHits=%d)", audioHits.Load(), asrHits.Load())
	}
}
