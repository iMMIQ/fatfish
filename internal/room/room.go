package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iMMIQ/fatfish/internal/agent"
	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/memory"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/netutil"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/sticker"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/tool"
	"github.com/iMMIQ/fatfish/internal/transport"
)

// Manager owns all Per-Channel Room Actors and routes events non-blockingly.
type Manager struct {
	cfgMgr   *config.Manager
	store    *store.Store
	policy   *policy.Engine
	hub      *infer.Hub
	frames   *frame.Builder
	runner   *agent.Runner
	memory   *memory.Service
	stickers *sticker.Engine
	registry *tool.Registry
	tr       transport.Transport
	httpCli  *http.Client
	log      *slog.Logger

	mu       sync.RWMutex
	rooms    map[model.ChannelID]*Room
	draining atomic.Bool
	wg       sync.WaitGroup
}

type Dependencies struct {
	Config    *config.Manager
	Store     *store.Store
	Policy    *policy.Engine
	Hub       *infer.Hub
	Frames    *frame.Builder
	Runner    *agent.Runner
	Memory    *memory.Service
	Stickers  *sticker.Engine
	Registry  *tool.Registry
	Transport transport.Transport
	Logger    *slog.Logger
}

func NewManager(deps Dependencies) *Manager {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	allowHosts := []string{"127.0.0.1"}
	if deps.Config != nil && deps.Config.Get() != nil {
		allowHosts = deps.Config.Get().Tools.SSRFAllowHosts
	}
	return &Manager{
		cfgMgr:   deps.Config,
		store:    deps.Store,
		policy:   deps.Policy,
		hub:      deps.Hub,
		frames:   deps.Frames,
		runner:   deps.Runner,
		memory:   deps.Memory,
		stickers: deps.Stickers,
		registry: deps.Registry,
		tr:       deps.Transport,
		httpCli:  netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 10 * time.Second, AllowHosts: allowHosts}),
		log:      deps.Logger,
		rooms:    make(map[model.ChannelID]*Room),
	}
}

// Dispatch routes an inbound message into the target channel's mailbox without blocking the WS reader.
func (m *Manager) Dispatch(msg *model.Message) {
	if msg == nil || m.draining.Load() {
		return
	}
	if !m.policy.ChannelAllowed(msg.ChannelID) {
		return
	}
	r := m.getOrCreate(msg.ChannelID)
	select {
	case r.mailbox <- model.InboundEvent{Message: msg}:
	default:
		m.log.Warn("room mailbox full, dropping event", "channel", msg.ChannelID)
	}
}

// DispatchSystem routes a scheduled or administrative system trigger into the target Room Actor.
func (m *Manager) DispatchSystem(ev model.SystemTriggerEvent) {
	if m.draining.Load() {
		return
	}
	r := m.getOrCreate(ev.ChannelID)
	select {
	case r.mailbox <- model.InboundEvent{System: &ev}:
	default:
		m.log.Warn("room mailbox full, dropping system trigger", "channel", ev.ChannelID)
	}
}

// ActiveChannels returns currently live Room channel IDs for console diagnostics.
func (m *Manager) ActiveChannels() []model.ChannelID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.ChannelID, 0, len(m.rooms))
	for id := range m.rooms {
		out = append(out, id)
	}
	return out
}

// Drain stops accepting new events and waits up to timeout for in-flight turns & deliveries to finish.
func (m *Manager) Drain(timeout time.Duration) {
	m.draining.Store(true)
	m.mu.RLock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, r := range m.rooms {
		rooms = append(rooms, r)
	}
	m.mu.RUnlock()

	for _, r := range rooms {
		r.stop()
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		m.log.Warn("room drain timed out", "timeout", timeout)
	}
}

func (m *Manager) getOrCreate(ch model.ChannelID) *Room {
	m.mu.RLock()
	r, ok := m.rooms[ch]
	m.mu.RUnlock()
	if ok {
		return r
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok = m.rooms[ch]; ok {
		return r
	}
	r = newRoom(m, ch)
	m.rooms[ch] = r
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		r.run()
	}()
	return r
}

// Room is a single-goroutine Per-Channel Actor serializing state, perception waits, policy checks,
// debouncing, thread tracking, +1 repeater, and epoch-guarded delivery.
type Room struct {
	mgr     *Manager
	ch      model.ChannelID
	mailbox chan model.InboundEvent
	stopCh  chan struct{}
	stopOne sync.Once

	epoch          atomic.Int64
	muteUntil      time.Time
	lastProactive  time.Time
	lastPlusOne    time.Time
	seenInSegment  map[string]bool // tracks members already introduced in active segment
	lastSegID      int64
	pendingMsgs    []model.Message // messages accumulated since last LLM turn
	pendingTrigger *triggerState

	// Thread continuation state
	threadUserID string
	threadTurns  int
	threadLastAt time.Time

	// +1 repeater state
	plusOnePhrase string
	plusOneUsers  map[string]struct{}

	// Stale tracking
	msgCount uint64
}

type triggerState struct {
	msg        model.Message
	kind       string // "mention" | "reply" | "poke" | "thread" | "proactive" | "dm" | "cron"
	proactive  bool
	enqueuedAt time.Time
	enqMsgCnt  uint64
}

func newRoom(mgr *Manager, ch model.ChannelID) *Room {
	r := &Room{
		mgr:           mgr,
		ch:            ch,
		mailbox:       make(chan model.InboundEvent, 256),
		stopCh:        make(chan struct{}),
		seenInSegment: make(map[string]bool),
		plusOneUsers:  make(map[string]struct{}),
	}
	if st, err := mgr.store.EnsureChannel(context.Background(), ch); err == nil {
		r.epoch.Store(st.Epoch)
		r.muteUntil = st.MuteUntil
	} else {
		r.epoch.Store(1)
	}
	return r
}

func (r *Room) stop() {
	r.stopOne.Do(func() { close(r.stopCh) })
}

func (r *Room) run() {
	var debounceTimer *time.Timer
	var debounceCh <-chan time.Time

	armDebounce := func(d time.Duration) {
		if d <= 0 {
			d = 50 * time.Millisecond
		}
		if debounceTimer == nil {
			debounceTimer = time.NewTimer(d)
			debounceCh = debounceTimer.C
		} else {
			if !debounceTimer.Stop() {
				select {
				case <-debounceTimer.C:
				default:
				}
			}
			debounceTimer.Reset(d)
		}
	}

	for {
		select {
		case <-r.stopCh:
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return

		case ev := <-r.mailbox:
			if ev.Message != nil {
				wait := r.onMessage(ev.Message)
				if r.pendingTrigger != nil {
					armDebounce(wait)
				}
			} else if ev.System != nil {
				r.onSystemTrigger(*ev.System)
			} else if ev.Command != nil {
				r.onAdminCommand(*ev.Command)
			}

		case <-debounceCh:
			debounceCh = nil
			debounceTimer = nil
			if r.pendingTrigger != nil {
				trig := r.pendingTrigger
				r.pendingTrigger = nil
				r.executeTurn(*trig)
			}
		}
	}
}

func (r *Room) onAdminCommand(cmd model.AdminCommand) {
	ctx := context.Background()
	switch cmd.Name {
	case "reset":
		newEp, err := r.mgr.store.BumpChannelEpoch(ctx, r.ch)
		if err == nil {
			r.epoch.Store(newEp)
		} else {
			r.epoch.Add(1)
		}
		r.pendingMsgs = nil
		r.pendingTrigger = nil
		r.seenInSegment = make(map[string]bool)
		r.threadUserID = ""
		r.threadTurns = 0
		if cmd.Reply != nil {
			cmd.Reply <- fmt.Sprintf("已重置当前频道上下文 (epoch=%d)", r.epoch.Load())
		}
	case "tr":
		if cmd.Reply != nil {
			out, err := r.mgr.hub.Translate(ctx, cmd.Args, "Simplified Chinese")
			if err != nil {
				cmd.Reply <- "翻译失败: " + err.Error()
			} else {
				cmd.Reply <- out
			}
		}
	}
}

func (r *Room) onMessage(msg *model.Message) time.Duration {
	ctx := context.Background()
	cfg := r.mgr.cfgMgr.Get()
	r.msgCount++

	// Resolve RepliedToBot from local L0 timeline if message quotes a previous bot message.
	if msg.ReplyToMsgID != "" && !msg.RepliedToBot {
		if prev, err := r.mgr.store.GetMessageByPlatformID(ctx, r.ch, msg.ReplyToMsgID); err == nil && prev != nil {
			if prev.FromBot {
				msg.RepliedToBot = true
			}
			for i := range msg.Parts {
				if msg.Parts[i].Kind == model.PartQuote && msg.Parts[i].Text == "" {
					msg.Parts[i].Text = fmt.Sprintf("%s: %s", prev.SenderName, prev.PlainText())
				}
			}
		}
	}

	// Update immutable member identity & alias ledger.
	_ = r.mgr.store.UpsertMember(ctx, model.Member{
		ChannelID:   r.ch,
		MemberID:    msg.SenderID,
		DisplayName: msg.SenderName,
		UpdatedAt:   msg.CreatedAt,
	})

	// Persist L0 raw message immediately so seq is strictly monotonic.
	_ = r.mgr.store.AppendMessage(ctx, msg)

	// Ignore our own outbound echoes and configured ignored bots for trigger logic.
	if msg.FromBot || r.mgr.policy.IsIgnoredBot(msg.SenderID) {
		return 0
	}

	// Built-in slash commands: /reset, /tr
	plain := strings.TrimSpace(msg.PlainText())
	if plain == "/reset" && (cfg.IsAdmin(msg.SenderID) || !r.ch.IsGroup()) {
		replyCh := make(chan string, 1)
		r.onAdminCommand(model.AdminCommand{ChannelID: r.ch, SenderID: msg.SenderID, Name: "reset", Reply: replyCh})
		if r.mgr.tr != nil {
			_, _ = r.mgr.tr.SendText(ctx, r.ch, <-replyCh, transport.SendOptions{})
		}
		return 0
	}
	if strings.HasPrefix(plain, "/tr ") {
		arg := strings.TrimSpace(strings.TrimPrefix(plain, "/tr "))
		go func() {
			out, err := r.mgr.hub.Translate(context.Background(), arg, "Simplified Chinese")
			if err == nil && out != "" && r.mgr.tr != nil {
				_, _ = r.mgr.tr.SendText(context.Background(), r.ch, out, transport.SendOptions{ReplyToMsgID: msg.PlatformMsgID})
			}
		}()
		return 0
	}

	// Launch bounded async perception tasks (image canonicalization + voice ASR).
	hasMedia, hasAudio := false, false
	for _, p := range msg.Parts {
		if p.Kind == model.PartImage {
			hasMedia = true
		}
		if p.Kind == model.PartAudio {
			hasAudio = true
		}
	}
	var perceptionDone chan struct{}
	if hasMedia || hasAudio {
		perceptionDone = make(chan struct{})
		go func() {
			defer close(perceptionDone)
			r.runPerception(msg, cfg)
		}()
	}

	// Check mute backoff before deciding whether to respond.
	if time.Now().Before(r.muteUntil) {
		r.pendingMsgs = append(r.pendingMsgs, *msg)
		return 0
	}

	// Zero-latency +1 repeater (group only, short identical text by >= min_users distinct users).
	if r.ch.IsGroup() && cfg.Policy.PlusOne.Enabled && !msg.MentionedBot && !msg.RepliedToBot && !msg.PokedBot {
		if r.tryPlusOne(ctx, msg, cfg) {
			r.pendingMsgs = append(r.pendingMsgs, *msg)
			return 0
		}
	}

	// Evaluate policy rules.
	nowLocal := time.Now().In(cfg.Location())
	hour := nowLocal.Hour()
	isNight := hour < 7 || hour >= 23
	inThread := r.threadUserID == msg.SenderID &&
		time.Since(r.threadLastAt) <= time.Duration(cfg.Policy.ThreadContinuationSec)*time.Second &&
		time.Since(r.threadLastAt) >= time.Duration(cfg.Policy.ThreadMinIntervalSec*float64(time.Second))
	cooldownOK := time.Since(r.lastProactive) >= time.Duration(cfg.Policy.ProactiveCooldownSec)*time.Second

	env := policy.PolicyEnv{
		IsGroup:      r.ch.IsGroup(),
		FromBot:      msg.FromBot,
		IsAdmin:      cfg.IsAdmin(msg.SenderID),
		Mentioned:    msg.MentionedBot,
		RepliedToBot: msg.RepliedToBot,
		Poked:        msg.PokedBot,
		HitNickname:  r.mgr.policy.HitNickname(plain),
		InThread:     inThread,
		ThreadTurns:  r.threadTurns,
		CooldownOK:   cooldownOK,
		IsNight:      isNight,
		Hour:         hour,
		TextLen:      len([]rune(plain)),
		Rand:         rand.Float64(),
	}

	shouldReply := !r.ch.IsGroup() // Always reply in DMs
	if r.ch.IsGroup() {
		if ok, err := r.mgr.policy.EvalReplyGate(r.ch, env); err == nil {
			shouldReply = ok
		}
	}

	// Wait bounded for perception if this message triggers or joins an active turn.
	if perceptionDone != nil && (shouldReply || r.pendingTrigger != nil) {
		waitDur := time.Duration(cfg.Policy.MediaWaitSec * float64(time.Second))
		if hasAudio {
			waitDur = time.Duration(cfg.Policy.ASRWaitSec * float64(time.Second))
		}
		if waitDur <= 0 {
			waitDur = 3 * time.Second
		}
		select {
		case <-perceptionDone:
		case <-time.After(waitDur):
			// Proceed with placeholder; background perception will patch L0+FTS5 on completion.
		}
	}

	r.pendingMsgs = append(r.pendingMsgs, *msg)
	if len(r.pendingMsgs) > 60 {
		r.pendingMsgs = r.pendingMsgs[len(r.pendingMsgs)-60:]
	}

	if !shouldReply {
		return 0
	}

	trigKind := "proactive"
	proactive := true
	switch {
	case !r.ch.IsGroup():
		trigKind = "dm"
		proactive = false
	case msg.MentionedBot:
		trigKind = "mention"
		proactive = false
	case msg.RepliedToBot:
		trigKind = "reply"
		proactive = false
	case msg.PokedBot:
		trigKind = "poke"
		proactive = false
	case inThread:
		trigKind = "thread"
		proactive = false
	}

	r.pendingTrigger = &triggerState{
		msg:        *msg,
		kind:       trigKind,
		proactive:  proactive,
		enqueuedAt: time.Now(),
		enqMsgCnt:  r.msgCount,
	}

	// Pure @mention with no text waits mention_wait_sec for follow-up message from same sender.
	if msg.MentionedBot && plain == "" {
		return time.Duration(cfg.Policy.MentionWaitSec * float64(time.Second))
	}
	if !proactive {
		return time.Duration(cfg.Policy.InputDebounceSec * float64(time.Second))
	}
	return time.Duration(cfg.Policy.InputDebounceSec * float64(time.Second))
}

func (r *Room) runPerception(msg *model.Message, cfg *config.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	changed := false
	retention := time.Duration(cfg.Frame.MediaRetentionHours) * time.Hour

	for i := range msg.Parts {
		p := &msg.Parts[i]
		switch p.Kind {
		case model.PartImage:
			if p.SHA256 != "" || p.URL == "" {
				continue
			}
			raw, _, code, err := netutil.FetchLimited(ctx, r.mgr.httpCli, p.URL, int64(cfg.Frame.MaxImageMiB)<<20, nil)
			if err != nil || code >= 400 {
				continue
			}
			blob, err := r.mgr.store.Blobs().PutImage(raw)
			if err != nil {
				continue
			}
			asset, err := r.mgr.store.RegisterMediaAsset(ctx, r.ch, msg.SenderID, blob, retention)
			if err != nil {
				continue
			}
			p.MediaID = asset.MediaID
			p.SHA256 = asset.SHA256
			p.MimeType = asset.MimeType
			p.Width = asset.Width
			p.Height = asset.Height
			changed = true

		case model.PartAudio:
			if p.Transcript != "" || p.FileID == "" || r.mgr.tr == nil {
				continue
			}
			audioBytes, fname, err := r.mgr.tr.FetchAudio(ctx, p.FileID, "wav")
			if err != nil {
				p.ASRStatus = "failed"
				changed = true
				continue
			}
			text, err := r.mgr.hub.Transcribe(ctx, fname, audioBytes)
			if err != nil {
				p.ASRStatus = "failed"
				changed = true
				continue
			}
			p.Transcript = text
			p.ASRStatus = "ok"
			changed = true
		}
	}

	if changed {
		_ = r.mgr.store.PatchMessageParts(ctx, msg)
	}
}

func (r *Room) tryPlusOne(ctx context.Context, msg *model.Message, cfg *config.Config) bool {
	txt := strings.TrimSpace(msg.PlainText())
	if txt == "" || len([]rune(txt)) > 24 || strings.Contains(txt, "\n") {
		r.plusOnePhrase = ""
		r.plusOneUsers = make(map[string]struct{})
		return false
	}
	if txt != r.plusOnePhrase {
		r.plusOnePhrase = txt
		r.plusOneUsers = map[string]struct{}{msg.SenderID: {}}
		return false
	}
	r.plusOneUsers[msg.SenderID] = struct{}{}
	if len(r.plusOneUsers) < cfg.Policy.PlusOne.MinUsers {
		return false
	}
	if time.Since(r.lastPlusOne) < time.Duration(cfg.Policy.PlusOne.CooldownSec)*time.Second {
		return false
	}
	roll := rand.Float64()
	if roll < cfg.Policy.PlusOne.RepeatProb {
		r.lastPlusOne = time.Now()
		r.plusOnePhrase = ""
		r.plusOneUsers = make(map[string]struct{})
		if r.mgr.tr != nil {
			_, _ = r.mgr.tr.SendText(ctx, r.ch, txt, transport.SendOptions{})
		}
		return true
	}
	return false
}

func (r *Room) onSystemTrigger(ev model.SystemTriggerEvent) {
	trigMsg := model.Message{
		ChannelID:  r.ch,
		SenderID:   "system",
		SenderName: "System",
		Parts:      []model.Part{{Kind: model.PartText, Text: "[Scheduled Trigger] " + ev.Prompt}},
		CreatedAt:  ev.CreatedAt,
	}
	r.pendingMsgs = append(r.pendingMsgs, trigMsg)
	r.executeTurn(triggerState{
		msg:        trigMsg,
		kind:       "cron",
		proactive:  false,
		enqueuedAt: time.Now(),
		enqMsgCnt:  r.msgCount,
	})
}

func (r *Room) executeTurn(trig triggerState) {
	ctx := context.Background()
	cfg := r.mgr.cfgMgr.Get()
	startEpoch := r.epoch.Load()

	// Stale-drop check #1 (before generation).
	if r.isStale(trig, cfg) {
		return
	}

	// Proactive fast-model JSON Gate (reply | react:<emoji> | ignore).
	if trig.proactive && r.mgr.hub != nil {
		decision := r.runProactiveGate(ctx, trig)
		if decision == "ignore" {
			return
		}
		if strings.HasPrefix(decision, "react:") {
			emoji := strings.TrimPrefix(decision, "react:")
			if r.mgr.tr != nil && trig.msg.PlatformMsgID != "" && emoji != "" {
				_ = r.mgr.tr.SendReaction(ctx, r.ch, trig.msg.PlatformMsgID, emoji)
			}
			r.lastProactive = time.Now()
			return
		}
	}

	batch := append([]model.Message(nil), r.pendingMsgs...)
	if len(batch) == 0 {
		batch = []model.Message{trig.msg}
	}
	r.pendingMsgs = nil

	// Ensure active segment (with Segment-Pinned Memory Snapshot frozen ONCE at segment creation).
	toolsHash := ""
	if r.mgr.registry != nil {
		toolsHash = r.mgr.registry.Hash()
	}
	scopeHash := frame.ScopeHash(cfg.PersonaHash, toolsHash, cfg.Models.Primary.Model)

	activeMembers := uniqueSenders(batch)
	seg, closedSeg, err := r.mgr.frames.EnsureActiveSegment(ctx, r.ch, startEpoch, scopeHash, func(sctx context.Context) (frame.SnapshotInput, error) {
		if r.mgr.memory == nil {
			return frame.SnapshotInput{}, nil
		}
		return r.mgr.memory.BuildSnapshot(sctx, r.ch, activeMembers)
	})
	if err != nil {
		r.mgr.log.Error("ensure segment failed", "channel", r.ch, "err", err)
		return
	}
	if closedSeg != nil {
		go r.summarizeClosedSegment(closedSeg)
	}
	if seg.ID != r.lastSegID {
		r.lastSegID = seg.ID
		r.seenInSegment = make(map[string]bool)
		for _, id := range activeMembers {
			r.seenInSegment[id] = true
		}
	}

	// Mid-segment new member compact one-time delta lines (keeps msg[1] byte-identical!).
	var newMemberDelta []string
	for _, m := range batch {
		if m.FromBot || m.SenderID == "system" || r.seenInSegment[m.SenderID] {
			continue
		}
		r.seenInSegment[m.SenderID] = true
		if r.mgr.memory != nil {
			if delta := r.mgr.memory.MemberCardDelta(ctx, r.ch, m.SenderID, m.SenderName); delta != "" {
				newMemberDelta = append(newMemberDelta, delta)
			}
		}
	}

	userPayload := frame.FormatUserTurn(batch, cfg.Location(), newMemberDelta)
	out, err := r.mgr.runner.Run(ctx, agent.TurnInput{
		ChannelID:    r.ch,
		Epoch:        startEpoch,
		Segment:      seg,
		PersonaText:  cfg.PersonaText,
		UserPayload:  userPayload,
		SenderID:     trig.msg.SenderID,
		SenderName:   trig.msg.SenderName,
		IsAdmin:      cfg.IsAdmin(trig.msg.SenderID),
		TriggerMsgID: trig.msg.PlatformMsgID,
		TriggerKind:  trig.kind,
	})
	if err != nil {
		r.mgr.log.Error("agent turn failed", "channel", r.ch, "err", err)
		return
	}

	// Abort if /reset happened or channel moved on while LLM was thinking.
	if r.epoch.Load() != startEpoch || r.isStale(trig, cfg) {
		return
	}

	// Build TurnPlan with natural output formatting.
	bubbles := FormatBubbles(out.Text, cfg.Policy)
	replyTo, mentionUser := SmartReplyDecoration(trig, int(r.msgCount-trig.enqMsgCnt), cfg.Policy)
	plan := model.TurnPlan{
		ChannelID:     r.ch,
		Epoch:         startEpoch,
		SegmentID:     seg.ID,
		TriggerMsgID:  trig.msg.PlatformMsgID,
		TriggerUserID: trig.msg.SenderID,
		TriggerKind:   trig.kind,
		RawText:       out.Text,
		Bubbles:       bubbles,
		DirectMedia:   out.DirectMedia,
		StickerIntent: out.StickerIntent,
		ReplyToMsgID:  replyTo,
		MentionUserID: mentionUser,
	}

	r.deliverPlan(ctx, seg, plan, out.NewFrames, trig, cfg)
}

func (r *Room) deliverPlan(ctx context.Context, seg *store.SegmentRecord, plan model.TurnPlan, newFrames []frame.StoredFramePayload, trig triggerState, cfg *config.Config) {
	delID, _ := r.mgr.store.CreateDelivery(ctx, plan)
	var sent []model.SentItem
	status := model.DeliverySent

	if r.mgr.tr != nil {
		// 1. Send text bubbles with natural pacing and epoch/risk guards.
		for i, b := range plan.Bubbles {
			if r.epoch.Load() != plan.Epoch {
				status = model.DeliveryAbortedEpoch
				break
			}
			if i > 0 {
				delay := bubbleDelay(b, cfg.Policy.BubbleTypingDelayMax)
				select {
				case <-time.After(delay):
				case <-r.stopCh:
					status = model.DeliveryPartial
					break
				}
				if r.epoch.Load() != plan.Epoch {
					status = model.DeliveryAbortedEpoch
					break
				}
			}
			opts := transport.SendOptions{}
			if i == 0 {
				opts.ReplyToMsgID = plan.ReplyToMsgID
				opts.MentionUserID = plan.MentionUserID
			}
			pid, err := r.mgr.tr.SendText(ctx, r.ch, b, opts)
			if err != nil {
				if errors.Is(err, transport.ErrRiskMuted) {
					status = model.DeliveryAbortedRisk
					r.muteUntil = time.Now().Add(time.Duration(cfg.Policy.MuteBackoffSec) * time.Second)
					_ = r.mgr.store.SetChannelMuteUntil(ctx, r.ch, r.muteUntil)
					r.mgr.log.Warn("risk/mute detected, engaging backoff", "channel", r.ch, "until", r.muteUntil)
				} else if len(sent) > 0 {
					status = model.DeliveryPartial
				} else {
					status = model.DeliveryFailed
				}
				break
			}
			sent = append(sent, model.SentItem{
				Kind:          "text",
				PlatformMsgID: pid,
				Text:          b,
				SentAt:        time.Now().UnixMilli(),
			})
			// Record bot reply in L0 timeline.
			_ = r.mgr.store.AppendMessage(ctx, &model.Message{
				ChannelID:     r.ch,
				PlatformMsgID: pid,
				SenderID:      r.mgr.tr.SelfID(),
				SenderName:    "fatfish",
				FromBot:       true,
				Parts:         []model.Part{{Kind: model.PartText, Text: b}},
				CreatedAt:     time.Now(),
			})
		}

		// 2. Send DirectMedia (e.g. PNG charts from compute) if text succeeded.
		if status == model.DeliverySent {
			for _, dm := range plan.DirectMedia {
				if r.epoch.Load() != plan.Epoch {
					status = model.DeliveryAbortedEpoch
					break
				}
				pid, err := r.mgr.tr.SendImage(ctx, r.ch, dm.Data, dm.MimeType, transport.SendOptions{})
				if err != nil {
					if errors.Is(err, transport.ErrRiskMuted) {
						status = model.DeliveryAbortedRisk
						r.muteUntil = time.Now().Add(time.Duration(cfg.Policy.MuteBackoffSec) * time.Second)
						_ = r.mgr.store.SetChannelMuteUntil(ctx, r.ch, r.muteUntil)
					}
					break
				}
				sent = append(sent, model.SentItem{
					Kind:          "image",
					PlatformMsgID: pid,
					MediaID:       dm.MediaID,
					SentAt:        time.Now().UnixMilli(),
				})
			}
		}

		// 3. Zero-latency post-ACK async reaction sticker (only if not muted/aborted).
		if status == model.DeliverySent && plan.StickerIntent != "" && r.mgr.stickers != nil {
			ctxQ := trig.msg.PlainText() + " -> " + plan.RawText
			r.mgr.stickers.DispatchAsync(
				context.Background(),
				r.ch,
				plan.Epoch,
				plan.StickerIntent,
				ctxQ,
				func() int64 { return r.epoch.Load() },
				func(sctx context.Context, st model.Sticker, imgBytes []byte) error {
					_, err := r.mgr.tr.SendImage(sctx, r.ch, imgBytes, "image/png", transport.SendOptions{})
					return err
				},
			)
		}
	}

	_ = r.mgr.store.UpdateDelivery(ctx, delID, status, sent)

	// Invariant (SPEC Section 3): ONLY delivered turns enter the FrozenFrame timeline.
	if len(sent) > 0 || r.mgr.tr == nil {
		// If partial delivery occurred, trim assistant frame text to what was actually sent.
		if status != model.DeliverySent && len(newFrames) > 0 && newFrames[len(newFrames)-1].Role == "assistant" {
			var parts []string
			for _, s := range sent {
				if s.Text != "" {
					parts = append(parts, s.Text)
				}
			}
			newFrames[len(newFrames)-1].Text = strings.Join(parts, "\n")
		}
		_ = r.mgr.frames.AppendFrozenFrames(ctx, seg, newFrames...)

		// Update thread continuation & proactive cooldown timers.
		if trig.proactive {
			r.lastProactive = time.Now()
		}
		if trig.msg.SenderID != "" && trig.msg.SenderID != "system" {
			if r.threadUserID == trig.msg.SenderID {
				r.threadTurns++
			} else {
				r.threadUserID = trig.msg.SenderID
				r.threadTurns = 1
			}
			r.threadLastAt = time.Now()
		}
	}
}

func (r *Room) isStale(trig triggerState, cfg *config.Config) bool {
	if !trig.proactive {
		return false
	}
	drop, err := r.mgr.policy.EvalDropStale(r.ch, policy.PolicyEnv{
		Proactive: true,
		NewMsgs:   int(r.msgCount - trig.enqMsgCnt),
		WaitSec:   time.Since(trig.enqueuedAt).Seconds(),
	})
	return err == nil && drop
}

func (r *Room) runProactiveGate(ctx context.Context, trig triggerState) string {
	recent, _ := r.mgr.store.RecentMessages(ctx, r.ch, 10)
	var lines []string
	for _, m := range recent {
		lines = append(lines, fmt.Sprintf("%s: %s", m.SenderName, m.PlainText()))
	}
	resp, err := r.mgr.hub.Chat(ctx, "fast", infer.ChatRequest{
		Messages: []infer.ChatMessage{
			{Role: "system", Content: `Decide whether the bot should chime in naturally in this group chat. Output JSON: {"action":"reply"|"ignore"|"react","emoji_id":"optional"}`},
			{Role: "user", Content: strings.Join(lines, "\n")},
		},
		MaxTokens:          128,
		ResponseFormat:     map[string]any{"type": "json_object"},
		ChatTemplateKwargs: map[string]any{"enable_thinking": false},
	})
	if err != nil {
		return "ignore"
	}
	var res struct {
		Action  string `json:"action"`
		EmojiID string `json:"emoji_id"`
	}
	if err := json.Unmarshal([]byte(resp.Content), &res); err != nil {
		return "ignore"
	}
	switch strings.ToLower(strings.TrimSpace(res.Action)) {
	case "reply":
		return "reply"
	case "react":
		return "react:" + res.EmojiID
	default:
		return "ignore"
	}
}

func (r *Room) summarizeClosedSegment(seg *store.SegmentRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	frames, err := r.mgr.store.ListFrames(ctx, seg.ID)
	if err != nil || len(frames) == 0 || r.mgr.hub == nil {
		return
	}
	var lines []string
	for _, f := range frames {
		var p frame.StoredFramePayload
		if json.Unmarshal([]byte(f.PayloadJSON), &p) == nil && p.Text != "" {
			lines = append(lines, f.Role+": "+p.Text)
		}
	}
	if len(lines) == 0 {
		return
	}
	resp, err := r.mgr.hub.Chat(ctx, "fast", infer.ChatRequest{
		Messages: []infer.ChatMessage{
			{Role: "system", Content: "Summarize this completed conversation segment in <=180 words for continuity in the next segment."},
			{Role: "user", Content: strings.Join(lines, "\n")},
		},
	})
	if err == nil && strings.TrimSpace(resp.Content) != "" {
		_ = r.mgr.store.CloseSegment(ctx, seg.ID, strings.TrimSpace(resp.Content))
	}
}

// --- Natural Output Formatting Helpers (exported for unit testing) ---

// FormatBubbles splits prose newlines into <= maxBubbles short bubbles while keeping fenced code
// blocks and Markdown tables intact, and optionally strips isolated trailing periods.
func FormatBubbles(text string, pol config.PolicyConfig) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	stripPeriod := func(s string) string {
		if !pol.StripTrailingPeriod {
			return s
		}
		r := []rune(s)
		if len(r) > 1 && len(r) <= 25 && r[len(r)-1] == '。' && r[len(r)-2] != '。' {
			return string(r[:len(r)-1])
		}
		return s
	}

	if !pol.SplitNewlineBubbles || strings.Contains(text, "```") || strings.Contains(text, "|---") {
		return []string{stripPeriod(text)}
	}
	rawLines := strings.Split(text, "\n")
	var bubbles []string
	for _, line := range rawLines {
		t := strings.TrimSpace(line)
		if t == "" {
			continue
		}
		bubbles = append(bubbles, t)
	}
	maxB := pol.MaxReplyBubbles
	if maxB <= 0 {
		maxB = 4
	}
	if len(bubbles) > maxB {
		tail := strings.Join(bubbles[maxB-1:], "\n")
		bubbles = append(bubbles[:maxB-1], tail)
	}
	for i := range bubbles {
		bubbles[i] = stripPeriod(bubbles[i])
	}
	return bubbles
}

// SmartReplyDecoration implements SPEC 5.3 smart_reply_decoration:
// omit quote/@ when directly below target, quote-only when displaced, never attach fake quote on poke.
func SmartReplyDecoration(trig triggerState, msgsSinceTrigger int, pol config.PolicyConfig) (replyToMsgID, mentionUserID string) {
	if !pol.SmartReplyDecoration {
		return trig.msg.PlatformMsgID, ""
	}
	if trig.kind == "poke" || trig.kind == "cron" || trig.kind == "dm" {
		return "", ""
	}
	if msgsSinceTrigger <= 0 {
		return "", "" // Directly below target: clean reply without redundant quote
	}
	return trig.msg.PlatformMsgID, ""
}

func bubbleDelay(bubble string, maxSec float64) time.Duration {
	if maxSec <= 0 {
		maxSec = 1.0
	}
	sec := 0.3 + float64(len([]rune(bubble)))*0.02
	if sec > maxSec {
		sec = maxSec
	}
	return time.Duration(sec * float64(time.Second))
}

func uniqueSenders(msgs []model.Message) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, m := range msgs {
		if m.FromBot || m.SenderID == "" || m.SenderID == "system" {
			continue
		}
		if _, ok := seen[m.SenderID]; !ok {
			seen[m.SenderID] = struct{}{}
			out = append(out, m.SenderID)
		}
	}
	return out
}
