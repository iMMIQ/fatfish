package room

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/agent"
	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/transport"
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

type mockTransport struct {
	sentTexts    atomic.Int32
	sentForwards atomic.Int32
	failForward  atomic.Bool
}

func (m *mockTransport) Start(context.Context, func(*model.Message)) error             { return nil }
func (m *mockTransport) StopInbound() error                                            { return nil }
func (m *mockTransport) Stop() error                                                   { return nil }
func (m *mockTransport) SelfID() string                                                { return "10001" }
func (m *mockTransport) Probe(context.Context) error                                   { return nil }
func (m *mockTransport) SendImage(context.Context, model.ChannelID, []byte, string, transport.SendOptions) (string, error) {
	return "img1", nil
}
func (m *mockTransport) SendForward(_ context.Context, _ model.ChannelID, _ []transport.ForwardNode, _ string) (string, error) {
	if m.failForward.Load() {
		return "", errors.New("forward unsupported")
	}
	m.sentForwards.Add(1)
	return "fwd1", nil
}
func (m *mockTransport) SendPoke(context.Context, model.ChannelID, string) error     { return nil }
func (m *mockTransport) SendReaction(context.Context, model.ChannelID, string, string) error {
	return nil
}
func (m *mockTransport) FetchAudio(context.Context, string, string) ([]byte, string, error) {
	return nil, "", nil
}
func (m *mockTransport) GetMessage(context.Context, model.ChannelID, string) (*model.Message, error) {
	return nil, nil
}
func (m *mockTransport) GetForward(context.Context, string, int) ([]model.Part, error) {
	return nil, nil
}
func (m *mockTransport) SendText(_ context.Context, _ model.ChannelID, _ string, _ transport.SendOptions) (string, error) {
	m.sentTexts.Add(1)
	return "p1", nil
}

// TestRoomNonBlockingResetDuringInFlightLLM verifies that /reset arriving while the LLM is generating
// immediately bumps epoch and cancels the in-flight turn so zero stale bubbles are delivered.
func TestRoomNonBlockingResetDuringInFlightLLM(t *testing.T) {
	llmStarted := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case llmStarted <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(2 * time.Second):
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message":       map[string]any{"content": "stale reply"},
			}},
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
transport:
  onebot:
    admin_ids: ["admin1"]
models:
  primary:
    base_url: "` + srv.URL + `"
    model: "test-m"
    timeout_sec: 5
policy:
  rules:
    reply_gate: "mentioned"
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
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	hub := infer.NewHub(cfgMgr.Get().Models)
	fb := frame.NewBuilder(st, cfgMgr.Get().Frame)
	runner := agent.NewRunner(hub, st, fb, nil, nil)
	tr := &mockTransport{}

	mgr := NewManager(Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Hub:       hub,
		Frames:    fb,
		Runner:    runner,
		Transport: tr,
	})
	defer mgr.Drain(2 * time.Second)

	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "99999")
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m1",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts:         []model.Part{{Kind: model.PartText, Text: "think slowly"}},
		CreatedAt:     time.Now(),
	})

	// Wait until the slow LLM call is in-flight.
	select {
	case <-llmStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for LLM call to start")
	}

	// Send /reset via DispatchCommand while LLM is blocked: must return promptly (<300ms)!
	replyCh := make(chan string, 1)
	mgr.DispatchCommand(model.AdminCommand{
		ChannelID: ch,
		SenderID:  "admin1",
		Name:      "reset",
		Reply:     replyCh,
	})
	select {
	case rep := <-replyCh:
		if rep == "" {
			t.Fatal("expected reset ack")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("/reset was blocked by in-flight LLM turn (actor starvation regression)")
	}

	time.Sleep(150 * time.Millisecond)
	if got := tr.sentTexts.Load(); got != 0 {
		t.Fatalf("expected 0 stale bubbles after mid-turn /reset, got %d", got)
	}
}

func TestNormalizeQQEmojiIDAndEnrichEvents(t *testing.T) {
	if got := normalizeQQEmojiID("76"); got != "76" {
		t.Fatalf("expected 76, got %q", got)
	}
	if got := normalizeQQEmojiID("狗头"); got != "179" {
		t.Fatalf("expected 179 for 狗头, got %q", got)
	}
	if got := normalizeQQEmojiID("invalid_emoji"); got != "" {
		t.Fatalf("expected empty for invalid emoji, got %q", got)
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := "data_dir: \"" + filepath.ToSlash(dir) + "\"\ntimezone: \"UTC\"\npolicy:\n  nicknames: [\"TestBot\"]\n"
	if err := os.WriteFile(cfgPath, []byte(yamlCfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgMgr, _ := config.NewManager(cfgPath, policy.ValidateConfig)
	st, err := store.Open(dir, 512, 5)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	mgr := NewManager(Dependencies{Config: cfgMgr, Store: st, Policy: pol})
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "77777")
	ctx := context.Background()

	_ = st.UpsertMember(ctx, model.Member{ChannelID: ch, MemberID: "u1", DisplayName: "Alice", UpdatedAt: time.Now()})
	_ = st.UpsertMember(ctx, model.Member{ChannelID: ch, MemberID: "u2", DisplayName: "Bob", UpdatedAt: time.Now()})
	_ = st.AppendMessage(ctx, &model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m99",
		SenderID:      "u1",
		SenderName:    "Alice",
		Parts:         []model.Part{{Kind: model.PartText, Text: "secret typo"}},
		CreatedAt:     time.Now(),
	})

	r := newRoom(mgr, ch)
	// 1. Poke between other members resolves both sender and target display names.
	pokeMsg := &model.Message{
		ChannelID:  ch,
		SenderID:   "u1",
		SenderName: "u1",
		PokedBot:   false,
		Parts:      []model.Part{{Kind: model.PartPoke, TargetID: "u2", Text: "[戳一戳]"}},
		CreatedAt:  time.Now(),
	}
	r.enrichInboundEvent(ctx, pokeMsg)
	if pokeMsg.SenderName != "Alice" || pokeMsg.Parts[0].Text != "[戳了 Bob]" {
		t.Fatalf("unexpected enriched poke: sender=%q part=%+v", pokeMsg.SenderName, pokeMsg.Parts[0])
	}

	// 2. Silent recall notice enriches recalled message snippet and does not trigger a turn.
	recallMsg := &model.Message{
		ChannelID:    ch,
		SenderID:     "u1",
		SenderName:   "u1",
		SilentNotice: true,
		Parts:        []model.Part{{Kind: model.PartNotice, RefMsgID: "m99", Text: "[撤回了一条消息]"}},
		CreatedAt:    time.Now(),
	}
	wait, arm := r.onMessage(recallMsg)
	if arm || wait != 0 {
		t.Fatalf("silent recall notice must not trigger reply: wait=%v arm=%v", wait, arm)
	}
	if len(r.pendingMsgs) != 1 || r.pendingMsgs[0].SenderName != "Alice" || r.pendingMsgs[0].Parts[0].Text != `[撤回了刚才的消息: "secret typo"]` {
		t.Fatalf("unexpected pending recall notice: %+v", r.pendingMsgs)
	}
}

func TestRoomProactiveLinkParseAndFatfishGate(t *testing.T) {
	// Mock target web server hosting two articles.
	webSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		switch r.URL.Path {
		case "/post-1":
			_, _ = w.Write([]byte(`<html><head><title>Go 1.26 新特性解析</title><meta property="og:description" content="详细介绍了纯 Go 性能优化与内存改进"></head><body><article><p>Go 1.26 带来了显著的 GC 延迟下降和更快的编译速度。</p></article></body></html>`))
		case "/post-2":
			_, _ = w.Write([]byte(`<html><head><title>随手广告链接</title><meta property="og:description" content="普通推广页"></head><body><article><p>欢迎光临，这是一条没有讨论价值的普通推广页面。</p></article></body></html>`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer webSrv.Close()

	// Mock LLM server distinguishing fast gate ("fast-m") vs primary ("primary-m").
	var gateAction atomic.Value
	gateAction.Store("reply")
	var primaryCalls atomic.Int32
	var gateCalls atomic.Int32
	var mu sync.Mutex
	var lastPrimaryPrompt string
	var lastGatePrompt string

	llmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model == "fast-m" {
			gateCalls.Add(1)
			if len(req.Messages) > 1 {
				if s, ok := req.Messages[len(req.Messages)-1].Content.(string); ok {
					mu.Lock()
					lastGatePrompt = s
					mu.Unlock()
				}
			}
			act := gateAction.Load().(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"finish_reason": "stop",
					"message":       map[string]any{"content": `{"action":"` + act + `","emoji_id":""}`},
				}},
			})
			return
		}
		primaryCalls.Add(1)
		if len(req.Messages) > 0 {
			if s, ok := req.Messages[len(req.Messages)-1].Content.(string); ok {
				mu.Lock()
				lastPrimaryPrompt = s
				mu.Unlock()
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message":       map[string]any{"content": "这篇主要讲 Go 1.26 的 GC 延迟下降和编译提速，挺实用的"},
			}},
		})
	}))
	defer llmSrv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
models:
  primary:
    base_url: "` + llmSrv.URL + `"
    model: "primary-m"
    timeout_sec: 5
  fast:
    base_url: "` + llmSrv.URL + `"
    model: "fast-m"
    timeout_sec: 5
policy:
  input_debounce_sec: 0.05
  explicit_debounce_sec: 0.05
  rules:
    reply_gate: "mentioned"
link_parser:
  enabled: true
  proactive_reply: true
  cooldown_sec: 0
  dedup_ttl_sec: 1800
  wait_sec: 2.0
  timeout_sec: 5
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
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	hub := infer.NewHub(cfgMgr.Get().Models)
	fb := frame.NewBuilder(st, cfgMgr.Get().Frame)
	runner := agent.NewRunner(hub, st, fb, nil, nil)
	tr := &mockTransport{}

	mgr := NewManager(Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Hub:       hub,
		Frames:    fb,
		Runner:    runner,
		Transport: tr,
	})
	defer mgr.Drain(2 * time.Second)

	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "54321")

	// 1. Group member drops a link without @bot -> always sends plain-text preview, then fast gate says "reply" -> primary also comments.
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m1",
		SenderID:      "u1",
		SenderName:    "Alice",
		Parts:         []model.Part{{Kind: model.PartText, Text: "看看这个 " + webSrv.URL + "/post-1"}},
		CreatedAt:     time.Now(),
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && tr.sentTexts.Load() < 2 {
		time.Sleep(25 * time.Millisecond)
	}
	if tr.sentTexts.Load() != 2 || gateCalls.Load() != 1 || primaryCalls.Load() != 1 {
		t.Fatalf("expected 1 preview + 1 gate + 1 primary reply (2 sent), got gate=%d primary=%d sent=%d",
			gateCalls.Load(), primaryCalls.Load(), tr.sentTexts.Load())
	}
	mu.Lock()
	gp, pp := lastGatePrompt, lastPrimaryPrompt
	mu.Unlock()
	if !strings.Contains(gp, "Go 1.26 新特性解析") {
		t.Fatalf("fast gate should see parsed link title, got: %s", gp)
	}
	if !strings.Contains(pp, `<external_data source="link_parse:`) || !strings.Contains(pp, "Go 1.26 新特性解析") {
		t.Fatalf("primary prompt should include external_data wrapped link parse, got: %s", pp)
	}

	// 2. Duplicate same URL within dedup_ttl_sec must NOT re-send preview or re-trigger gate/reply.
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m2",
		SenderID:      "u2",
		SenderName:    "Bob",
		Parts:         []model.Part{{Kind: model.PartText, Text: webSrv.URL + "/post-1?utm_source=dup"}},
		CreatedAt:     time.Now(),
	})
	time.Sleep(250 * time.Millisecond)
	if gateCalls.Load() != 1 || primaryCalls.Load() != 1 || tr.sentTexts.Load() != 2 {
		t.Fatalf("duplicate URL should be deduped, got gate=%d primary=%d sent=%d", gateCalls.Load(), primaryCalls.Load(), tr.sentTexts.Load())
	}

	// 3. New URL when fatfish fast gate decides "ignore" -> preview STILL sent (+1 sent), 0 extra primary call, L0 enriched.
	gateAction.Store("ignore")
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m3",
		SenderID:      "u2",
		SenderName:    "Bob",
		Parts:         []model.Part{{Kind: model.PartText, Text: webSrv.URL + "/post-2"}},
		CreatedAt:     time.Now(),
	})
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && gateCalls.Load() < 2 {
		time.Sleep(25 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	if gateCalls.Load() != 2 || primaryCalls.Load() != 1 || tr.sentTexts.Load() != 3 {
		t.Fatalf("gate=ignore should still send preview but skip primary commentary: gate=%d primary=%d sent=%d",
			gateCalls.Load(), primaryCalls.Load(), tr.sentTexts.Load())
	}
	saved, err := st.GetMessageByPlatformID(context.Background(), ch, "m3")
	if err != nil || saved == nil || !strings.Contains(saved.PlainText(), "随手广告链接") {
		t.Fatalf("ignored link should still be patched into L0 history, got: %+v err=%v", saved, err)
	}

	// 4. Explicit @bot with link must run ONCE (mention turn, skipping standalone preview & proactive gate) -> zero double reply.
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m4",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts:         []model.Part{{Kind: model.PartText, Text: "总结一下 " + webSrv.URL + "/post-1"}},
		CreatedAt:     time.Now(),
	})
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && tr.sentTexts.Load() < 4 {
		time.Sleep(25 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	if gateCalls.Load() != 2 || primaryCalls.Load() != 2 || tr.sentTexts.Load() != 4 {
		t.Fatalf("explicit @bot + link must bypass proactive gate and send exactly 1 reply: gate=%d primary=%d sent=%d",
			gateCalls.Load(), primaryCalls.Load(), tr.sentTexts.Load())
	}
}

func TestRoomOutboundRateLimitBubbleCollapseAndTieredBackoff(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  max_msgs_per_min: 8
  mute_backoff_sec: 1800
  rate_limit_backoff_sec: 75
  explicit_debounce_sec: 0.9
  poke:
    mode: "all"
    cooldown_sec: 15
  rules:
    reply_gate: "mentioned || poked"
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
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	tr := &mockTransport{}
	mgr := NewManager(Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Frames:    frame.NewBuilder(st, cfgMgr.Get().Frame),
		Transport: tr,
	})
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "77777")
	ctx := context.Background()
	r := newRoom(mgr, ch)
	cfg := cfgMgr.Get()

	// 1. Pre-fill 6 recent outbound messages in 60s window (remaining = 2).
	// A 3-bubble plan must collapse into 1 single message to preserve rate-limit headroom.
	now := time.Now()
	for i := 0; i < 6; i++ {
		r.recordOutbound(now)
	}
	seg, _, err := mgr.frames.EnsureActiveSegment(ctx, ch, 1, "h1", nil)
	if err != nil {
		t.Fatalf("EnsureActiveSegment: %v", err)
	}
	plan := model.TurnPlan{
		ChannelID: ch,
		Epoch:     1,
		SegmentID: seg.ID,
		RawText:   "line1\nline2\nline3",
		Bubbles:   []string{"line1", "line2", "line3"},
	}
	ok := r.deliverPlan(ctx, seg, plan, nil, -1, false, agent.TurnStats{ModelUsed: "test"}, triggerState{kind: "mention"}, cfg)
	if !ok {
		t.Fatal("expected deliverPlan to commit")
	}
	if got := tr.sentTexts.Load(); got != 1 {
		t.Fatalf("expected 3 bubbles to collapse into 1 outbound send when headroom=2, got %d", got)
	}

	// 2. Tiered backoff: with 7 recent outbound msgs in 60s, hit #1 uses short rate_limit_backoff_sec (~75s),
	// hit #2 escalates to ~300s, hit #3 escalates to full mute_backoff_sec (1800s).
	r.engageMute(ctx, cfg)
	d1 := time.Until(time.Unix(0, r.muteUntilNano.Load()))
	if d1 < 60*time.Second || d1 > 90*time.Second {
		t.Fatalf("expected hit #1 rate-limit backoff ~75s, got %v", d1)
	}
	r.engageMute(ctx, cfg)
	d2 := time.Until(time.Unix(0, r.muteUntilNano.Load()))
	if d2 < 280*time.Second || d2 > 310*time.Second {
		t.Fatalf("expected hit #2 rate-limit backoff ~300s, got %v", d2)
	}
	r.engageMute(ctx, cfg)
	d3 := time.Until(time.Unix(0, r.muteUntilNano.Load()))
	if d3 < 1700*time.Second {
		t.Fatalf("expected hit #3 full mute backoff ~1800s, got %v", d3)
	}

	// 3. Low outbound volume hit immediately uses full mute_backoff_sec (real admin mute).
	r2 := newRoom(mgr, model.MakeChannelID("onebot", "10001", model.ChannelGroup, "77778"))
	r2.engageMute(ctx, cfg)
	dFull := time.Until(time.Unix(0, r2.muteUntilNano.Load()))
	if dFull < 1700*time.Second {
		t.Fatalf("expected low-outbound risk hit to use full 1800s mute backoff, got %v", dFull)
	}

	// 4. Poke debounce & cooldown: first poke debounces (~0.9s); second poke within 15s cooldown is suppressed.
	r3 := newRoom(mgr, model.MakeChannelID("onebot", "10001", model.ChannelGroup, "77779"))
	waitP1, armP1 := r3.onMessage(&model.Message{
		ChannelID:  r3.ch,
		SenderID:   "u1",
		SenderName: "Alice",
		PokedBot:   true,
		Parts:      []model.Part{{Kind: model.PartPoke, TargetID: "10001", Text: "[戳一戳]"}},
		CreatedAt:  time.Now(),
	})
	if !armP1 || waitP1 < 800*time.Millisecond {
		t.Fatalf("expected direct poke to coalesce with explicit_debounce_sec (~0.9s), got wait=%v arm=%v", waitP1, armP1)
	}
	r3.lastPokeReply = time.Now()
	r3.pendingTrigger = nil
	waitP2, armP2 := r3.onMessage(&model.Message{
		ChannelID:  r3.ch,
		SenderID:   "u2",
		SenderName: "Bob",
		PokedBot:   true,
		Parts:      []model.Part{{Kind: model.PartPoke, TargetID: "10001", Text: "[戳一戳]"}},
		CreatedAt:  time.Now(),
	})
	if armP2 || waitP2 != 0 {
		t.Fatalf("expected poke within 15s cooldown to be suppressed, got wait=%v arm=%v", waitP2, armP2)
	}
}

func TestSocialTargetingSuppressesThreadAndSameNameBot(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  nicknames: ["大肥鱼"]
  ignored_bots: ["9990001"]
  thread_continuation_sec: 60
  thread_min_interval_sec: 0
  thread_max_turns: 3
  proactive_cooldown_sec: 0
  rules:
    reply_gate: "mentioned || replied_to_bot || poked || (in_thread && thread_turns < 3) || (cooldown_ok && !mentioned_other && !replied_to_other && hit_nickname)"
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
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	mgr := NewManager(Dependencies{
		Config: cfgMgr,
		Store:  st,
		Policy: pol,
		Frames: frame.NewBuilder(st, cfgMgr.Get().Frame),
	})
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "88888")
	_ = st.UpsertMember(context.Background(), model.Member{
		ChannelID:   ch,
		MemberID:    "9990001",
		DisplayName: "大肥鱼",
		UpdatedAt:   time.Now(),
	})

	r := newRoom(mgr, ch)
	r.threadUserID = "u1"
	r.threadTurns = 1
	r.threadLastAt = time.Now().Add(-10 * time.Second)

	// 1. User in active thread explicitly @-mentions another bot named "大肥鱼" -> must NOT trigger thread or hit_nickname!
	_, arm1 := r.onMessage(&model.Message{
		ChannelID:  ch,
		SenderID:   "u1",
		SenderName: "Alice",
		Parts: []model.Part{
			{Kind: model.PartMention, TargetID: "9990001", Text: "@9990001"},
			{Kind: model.PartText, Text: "帮我查一下天气"},
		},
		CreatedAt: time.Now(),
	})
	if arm1 {
		t.Fatal("expected @OtherSameNameBot to NOT trigger thread continuation or nickname match")
	}

	// 2. Same user quoting someone else's message -> must NOT trigger thread continuation!
	_, arm2 := r.onMessage(&model.Message{
		ChannelID:    ch,
		SenderID:     "u1",
		SenderName:   "Alice",
		ReplyToMsgID: "other_msg_1",
		Parts: []model.Part{
			{Kind: model.PartQuote, RefMsgID: "other_msg_1", Text: "Bob: hi"},
			{Kind: model.PartText, Text: "确实"},
		},
		CreatedAt: time.Now(),
	})
	if arm2 {
		t.Fatal("expected quoting another user to NOT trigger thread continuation")
	}

	// 3. Same user talking normally without @other or quote -> SHOULD continue thread!
	_, arm3 := r.onMessage(&model.Message{
		ChannelID:  ch,
		SenderID:   "u1",
		SenderName: "Alice",
		Parts:      []model.Part{{Kind: model.PartText, Text: "那接下来怎么办"}},
		CreatedAt:  time.Now(),
	})
	if !arm3 {
		t.Fatal("expected direct follow-up from active thread user to trigger continuation")
	}

	// 4. Same user poking the bot during poke cooldown must NEVER bypass cooldown via in_thread!
	r.pendingTrigger = nil
	r.lastPokeReply = time.Now()
	_, armPokeBypass := r.onMessage(&model.Message{
		ChannelID:  ch,
		SenderID:   "u1",
		SenderName: "Alice",
		PokedBot:   true,
		Parts:      []model.Part{{Kind: model.PartPoke, TargetID: "10001", Text: "[戳了你]"}},
		CreatedAt:  time.Now(),
	})
	if armPokeBypass {
		t.Fatal("expected poke during cooldown to NOT bypass via in_thread")
	}

	// 5. Committed poke outcome must NOT create or refresh a text thread continuation.
	r.threadUserID = ""
	r.threadTurns = 0
	r.applyTurnOutcome(turnOutcome{
		committed: true,
		trig: triggerState{
			kind: "poke",
			msg:  model.Message{SenderID: "u9"},
		},
	})
	if r.threadUserID != "" || r.threadTurns != 0 {
		t.Fatalf("expected poke reply not to open text thread, got user=%q turns=%d", r.threadUserID, r.threadTurns)
	}
}

// TestRoomNonBlockingPerceptionAndParallelMultiImage verifies that:
// 1. Multiple images in one message are fetched/processed concurrently inside runPerception.
// 2. Room.run() remains immediately responsive to commands (/status) while perception is in flight.
func TestRoomNonBlockingPerceptionAndParallelMultiImage(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var pngBuf bytes.Buffer
	_ = png.Encode(&pngBuf, img)
	pngBytes := pngBuf.Bytes()

	var inFlight, maxInFlight atomic.Int32
	imgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		inFlight.Add(-1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	}))
	defer imgSrv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
policy:
  media_wait_sec: 1.5
  explicit_debounce_sec: 0.05
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
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	mgr := NewManager(Dependencies{
		Config: cfgMgr,
		Store:  st,
		Policy: pol,
	})
	defer mgr.Drain(2 * time.Second)

	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "66666")
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m_multi_img",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts: []model.Part{
			{Kind: model.PartText, Text: "看这三张图"},
			{Kind: model.PartImage, URL: imgSrv.URL + "/1.png"},
			{Kind: model.PartImage, URL: imgSrv.URL + "/2.png"},
			{Kind: model.PartImage, URL: imgSrv.URL + "/3.png"},
		},
		CreatedAt: time.Now(),
	})

	// Send /status immediately: must return well before the 100ms image perception finishes!
	replyCh := make(chan string, 1)
	start := time.Now()
	mgr.DispatchCommand(model.AdminCommand{
		ChannelID: ch,
		SenderID:  "u1",
		Name:      "status",
		Reply:     replyCh,
	})
	select {
	case rep := <-replyCh:
		if rep == "" {
			t.Fatal("expected status reply")
		}
		if elapsed := time.Since(start); elapsed > 80*time.Millisecond {
			t.Fatalf("Room.run() was blocked during perception: /status took %v", elapsed)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Room.run() blocked on perception instead of servicing /status immediately")
	}

	// Wait for parallel perception to complete and verify all 3 images were fetched concurrently.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		saved, err := st.GetMessageByPlatformID(context.Background(), ch, "m_multi_img")
		if err == nil && saved != nil {
			ready := 0
			for _, p := range saved.Parts {
				if p.Kind == model.PartImage && p.SHA256 != "" && p.MediaID != "" {
					ready++
				}
			}
			if ready == 3 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if maxInFlight.Load() < 2 {
		t.Fatalf("expected multi-image perception to run concurrently (maxInFlight >= 2), got %d", maxInFlight.Load())
	}
}
