package room

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	sentTexts atomic.Int32
}

func (m *mockTransport) Start(context.Context, func(*model.Message)) error             { return nil }
func (m *mockTransport) StopInbound() error                                            { return nil }
func (m *mockTransport) Stop() error                                                   { return nil }
func (m *mockTransport) SelfID() string                                                { return "10001" }
func (m *mockTransport) Probe(context.Context) error                                   { return nil }
func (m *mockTransport) SendImage(context.Context, model.ChannelID, []byte, string, transport.SendOptions) (string, error) {
	return "img1", nil
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
