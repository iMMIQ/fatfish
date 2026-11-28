package room

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/parser"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/transport"
)

type orderedMediaTransport struct {
	mockTransport
	mu         sync.Mutex
	events     []string
	textDelay  time.Duration
	videoBytes []byte
	doneCh     chan struct{}
	doneOnce   sync.Once
}

func (o *orderedMediaTransport) SendText(_ context.Context, _ model.ChannelID, text string, _ transport.SendOptions) (string, error) {
	if o.textDelay > 0 {
		time.Sleep(o.textDelay)
	}
	o.mu.Lock()
	o.events = append(o.events, "text:"+text)
	o.mu.Unlock()
	o.sentTexts.Add(1)
	return "txt1", nil
}

func (o *orderedMediaTransport) SendVideo(_ context.Context, _ model.ChannelID, videoBytes []byte, _ transport.SendOptions) (string, error) {
	o.mu.Lock()
	o.events = append(o.events, "video")
	o.videoBytes = append([]byte(nil), videoBytes...)
	o.mu.Unlock()
	o.doneOnce.Do(func() {
		if o.doneCh != nil {
			close(o.doneCh)
		}
	})
	return "vid1", nil
}

func TestBilibiliLinkPreviewSendsVideoInParallelAfterText(t *testing.T) {
	var videoDownloadStarted atomicBoolWithTime
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x/web-interface/view":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"aid":      112233,
					"cid":      445566,
					"bvid":     "BV1ParVid001",
					"title":    "并行视频发送实测",
					"desc":     "验证文字先发、视频并行下载后发",
					"duration": 45,
					"owner":    map[string]any{"mid": 9, "name": "测试UP"},
					"stat":     map[string]any{"view": 1000, "like": 80, "coin": 20},
				},
			})
		case "/x/player/playurl":
			videoDownloadStarted.Mark()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"durl": []map[string]any{{
						"size": 16,
						"url":  "http://" + r.Host + "/video.mp4",
					}},
				},
			})
		case "/video.mp4":
			_, _ = w.Write([]byte("MP4_TEST_CONTENT"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  rules:
    reply_gate: "false"
link_parser:
  enabled: true
  proactive_reply: false
  send_video: true
  max_video_mib: 20
  max_video_duration_sec: 300
  cooldown_sec: 0
  night_proactive: true
tools:
  ssrf_allow_hosts: ["127.0.0.1"]
`
	if err := os.WriteFile(cfgPath, []byte(yamlCfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgMgr, err := config.NewManager(cfgPath, policy.ValidateConfig)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	st, err := store.Open(dir, 512, 5)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	pol, err := policy.NewEngine(cfgMgr.Get().Policy)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	psvc := parser.NewService(parser.Options{
		AllowHosts: []string{"127.0.0.1"},
		Timeout:    5 * time.Second,
		CacheTTL:   5 * time.Minute,
	})
	psvc.SetBilibiliAPIBaseForTest(srv.URL)

	tr := &orderedMediaTransport{
		textDelay: 80 * time.Millisecond, // Delay text send so video download finishes FIRST and must wait for text!
		doneCh:    make(chan struct{}),
	}
	mgr := NewManager(Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Parser:    psvc,
		Transport: tr,
	})
	defer mgr.Drain(3 * time.Second)

	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "900001")
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m1",
		SenderID:      "u1",
		SenderName:    "群友A",
		Parts:         []model.Part{{Kind: model.PartText, Text: "看这个 https://www.bilibili.com/video/BV1ParVid001"}},
		CreatedAt:     time.Now(),
	})

	select {
	case <-tr.doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for parallel bilibili video delivery")
	}

	tr.mu.Lock()
	events := append([]string(nil), tr.events...)
	gotBytes := string(tr.videoBytes)
	tr.mu.Unlock()

	if len(events) != 2 {
		t.Fatalf("expected [text, video] events, got %v", events)
	}
	if events[0] == "video" || events[1] != "video" {
		t.Fatalf("expected text strictly before video, got order: %v", events)
	}
	if gotBytes != "MP4_TEST_CONTENT" {
		t.Fatalf("unexpected video bytes: %q", gotBytes)
	}
	if !videoDownloadStarted.Set() {
		t.Fatal("expected video download to be triggered")
	}
}

type atomicBoolWithTime struct {
	mu  sync.Mutex
	set bool
}

func (a *atomicBoolWithTime) Mark() {
	a.mu.Lock()
	a.set = true
	a.mu.Unlock()
}

func (a *atomicBoolWithTime) Set() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.set
}
