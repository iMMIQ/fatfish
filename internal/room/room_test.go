package room

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/store"
)

func TestFormatBubblesAndStripPeriod(t *testing.T) {
	pol := config.PolicyConfig{
		SplitNewlineBubbles: true,
		MaxReplyBubbles:     4,
		StripTrailingPeriod: true,
	}

	// Short casual lines split and drop isolated trailing Chinese period.
	b := FormatBubbles("哈哈哈哈。\n确实如此。\n走吧。", pol)
	if len(b) != 3 || b[0] != "哈哈哈哈" || b[1] != "确实如此" || b[2] != "走吧" {
		t.Fatalf("unexpected bubbles: %#v", b)
	}

	// Code fences must never be split into multiple bubbles.
	code := "```go\nfmt.Println(1)\nfmt.Println(2)\n```"
	b2 := FormatBubbles(code, pol)
	if len(b2) != 1 || b2[0] != code {
		t.Fatalf("code fence should not be split: %#v", b2)
	}

	// Smart reply decoration: direct reply omits quote, displaced adds quote, poke never quotes.
	pol.SmartReplyDecoration = true
	trig := triggerState{kind: "mention", msg: model.Message{PlatformMsgID: "m100"}}
	if r, _ := SmartReplyDecoration(trig, 0, pol); r != "" {
		t.Fatalf("direct reply should omit quote, got %q", r)
	}
	if r, _ := SmartReplyDecoration(trig, 2, pol); r != "m100" {
		t.Fatalf("displaced reply should quote m100, got %q", r)
	}
	trigPoke := triggerState{kind: "poke", msg: model.Message{PlatformMsgID: "m101"}}
	if r, _ := SmartReplyDecoration(trigPoke, 3, pol); r != "" {
		t.Fatalf("poke must never attach quote, got %q", r)
	}
}

func TestRoomDebounceIsolationAndPerceptionRaceFree(t *testing.T) {
	dir := t.TempDir()
	// Slow image server that triggers perception timeout while background goroutine still finishes.
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer slowSrv.Close()

	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  mention_wait_sec: 1.0
  media_wait_sec: 0.02
  input_debounce_sec: 0.5
  rules:
    reply_gate: "mentioned"
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
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	pol, err := policy.NewEngine(cfgMgr.Get().Policy)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	mgr := NewManager(Dependencies{
		Config: cfgMgr,
		Store:  st,
		Policy: pol,
	})
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "88888")
	r := newRoom(mgr, ch)

	// 1. Pure @mention from u1 arms mention_wait_sec (1.0s) and launches slow async perception.
	wait1, arm1 := r.onMessage(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m1",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts:         []model.Part{{Kind: model.PartImage, URL: slowSrv.URL + "/a.jpg"}},
		CreatedAt:     time.Now(),
	})
	if !arm1 || wait1 < 900*time.Millisecond {
		t.Fatalf("expected pure @mention to arm ~1s wait, got wait=%v arm=%v", wait1, arm1)
	}

	// 2. Unrelated user u2 message MUST NOT arm/shorten the pending debounce timer!
	wait2, arm2 := r.onMessage(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m2",
		SenderID:      "u2",
		SenderName:    "Bob",
		Parts:         []model.Part{{Kind: model.PartText, Text: "random chatter"}},
		CreatedAt:     time.Now(),
	})
	if arm2 || wait2 != 0 {
		t.Fatalf("unrelated user must not disturb active debounce timer: wait=%v arm=%v", wait2, arm2)
	}

	// 3. Same-sender follow-up from u1 coalesces and arms input_debounce_sec (0.5s).
	wait3, arm3 := r.onMessage(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m3",
		SenderID:      "u1",
		SenderName:    "Alice",
		Parts:         []model.Part{{Kind: model.PartText, Text: "here is my question"}},
		CreatedAt:     time.Now(),
	})
	if !arm3 || wait3 < 400*time.Millisecond {
		t.Fatalf("same-sender follow-up should coalesce with input_debounce_sec, got wait=%v arm=%v", wait3, arm3)
	}

	// Wait for background slow perception to finish (verifying zero data race under -race).
	time.Sleep(180 * time.Millisecond)
	_ = context.Background()
}
