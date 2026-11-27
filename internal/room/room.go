package room

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
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
	httpMu   sync.RWMutex
	httpCli  *http.Client
	log      *slog.Logger

	mu       sync.RWMutex
	rooms    map[model.ChannelID]*Room
	draining atomic.Bool
	wg       sync.WaitGroup

	percSem        chan struct{} // Bounded async perception concurrency (SPEC §5.1)
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
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
	var allowHosts []string
	if deps.Config != nil && deps.Config.Get() != nil {
		allowHosts = deps.Config.Get().Tools.SSRFAllowHosts
	}
	sdCtx, sdCancel := context.WithCancel(context.Background())
	return &Manager{
		cfgMgr:         deps.Config,
		store:          deps.Store,
		policy:         deps.Policy,
		hub:            deps.Hub,
		frames:         deps.Frames,
		runner:         deps.Runner,
		memory:         deps.Memory,
		stickers:       deps.Stickers,
		registry:       deps.Registry,
		tr:             deps.Transport,
		httpCli:        netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 10 * time.Second, AllowHosts: allowHosts}),
		log:            deps.Logger,
		rooms:          make(map[model.ChannelID]*Room),
		percSem:        make(chan struct{}, 16),
		shutdownCtx:    sdCtx,
		shutdownCancel: sdCancel,
	}
}

// UpdateHTTPClient hot-reloads the SSRF-safe HTTP client when ssrf_allow_hosts changes.
func (m *Manager) UpdateHTTPClient(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 10 * time.Second, AllowHosts: allowHosts})
	m.httpMu.Lock()
	m.httpCli = cli
	m.httpMu.Unlock()
}

func (m *Manager) httpClient() *http.Client {
	m.httpMu.RLock()
	cli := m.httpCli
	m.httpMu.RUnlock()
	return cli
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
	if m.policy != nil && !m.policy.ChannelAllowed(ev.ChannelID) {
		return
	}
	r := m.getOrCreate(ev.ChannelID)
	select {
	case r.mailbox <- model.InboundEvent{System: &ev}:
	default:
		m.log.Warn("room mailbox full, dropping system trigger", "channel", ev.ChannelID)
	}
}

// DispatchCommand sends an administrative command (e.g. reset, status, tr) into the target Room Actor.
func (m *Manager) DispatchCommand(cmd model.AdminCommand) {
	if m.draining.Load() {
		return
	}
	r := m.getOrCreate(cmd.ChannelID)
	select {
	case r.mailbox <- model.InboundEvent{Command: &cmd}:
	default:
		m.log.Warn("room mailbox full, dropping admin command", "channel", cmd.ChannelID, "cmd", cmd.Name)
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

// Drain stops accepting new events and waits up to timeout for in-flight turns, deliveries,
// and background room tasks to finish before cancelling remaining contexts.
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
		m.shutdownCancel()
	case <-time.After(timeout):
		m.log.Warn("room drain timed out, cancelling remaining workers", "timeout", timeout)
		m.shutdownCancel()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
		}
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
// Blocking LLM inference and bubble delivery execute on a single managed turn worker so the actor
// keeps draining mailbox events (/reset epoch cancellation, live msgCount for post-gen stale drop,
// and background perception patches).
type Room struct {
	mgr         *Manager
	ch          model.ChannelID
	mailbox     chan model.InboundEvent
	percPatchCh chan perceptionPatch
	turnDoneCh  chan turnOutcome
	stopCh      chan struct{}
	stopOne     sync.Once

	epoch          atomic.Int64
	muteUntilNano  atomic.Int64 // UnixNano; atomic so actor & turn worker share safely
	msgCount       atomic.Uint64
	lastProactive  time.Time
	lastPlusOne    time.Time
	seenInSegment  map[string]bool // tracks members already introduced in active segment
	lastSegID      int64
	pendingMsgs    []model.Message // messages accumulated since last LLM turn
	pendingTrigger *triggerState

	// Managed single turn worker state (owned strictly by Room.run())
	turnActive bool
	turnCancel context.CancelFunc

	// Thread continuation state
	threadUserID string
	threadTurns  int
	threadLastAt time.Time

	// +1 repeater state
	plusOnePhrase string
	plusOneUsers  map[string]struct{}

	// Burst user tracking (rolling 30s window for PolicyEnv.BurstUsers)
	recentSenders []senderStamp
}

type perceptionPatch struct {
	seq   int64
	parts []model.Part
}

type turnOutcome struct {
	epoch            int64
	trig             triggerState
	committed        bool
	proactiveActed   bool
	uncommittedBatch []model.Message
	segID            int64
	seenMembers      []string
}

type senderStamp struct {
	userID string
	at     time.Time
}

type perceptionResult struct {
	parts   []model.Part
	changed bool
	imageKB int
}

type plusOneAction int

const (
	plusOneNone plusOneAction = iota
	plusOneEcho
	plusOneBreak
)

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
		percPatchCh:   make(chan perceptionPatch, 64),
		turnDoneCh:    make(chan turnOutcome, 1),
		stopCh:        make(chan struct{}),
		seenInSegment: make(map[string]bool),
		plusOneUsers:  make(map[string]struct{}),
	}
	r.epoch.Store(1)
	return r
}

func (r *Room) isMuted(now time.Time) bool {
	until := r.muteUntilNano.Load()
	return until > 0 && now.UnixNano() < until
}

func (r *Room) engageMute(ctx context.Context, cfg *config.Config) {
	sec := cfg.Policy.MuteBackoffSec
	if sec <= 0 {
		sec = 1800
	}
	until := time.Now().Add(time.Duration(sec) * time.Second)
	r.muteUntilNano.Store(until.UnixNano())
	_ = r.mgr.store.SetChannelMuteUntil(ctx, r.ch, until)
	r.mgr.log.Warn("risk/mute detected, engaging backoff", "channel", r.ch, "until", until)
}

func (r *Room) stop() {
	r.stopOne.Do(func() { close(r.stopCh) })
}

func (r *Room) run() {
	// Load persistent channel epoch & mute backoff inside the Room goroutine (never blocking WS reader Dispatch).
	if r.mgr.store != nil {
		if st, err := r.mgr.store.EnsureChannel(r.mgr.shutdownCtx, r.ch); err == nil {
			r.epoch.Store(st.Epoch)
			if !st.MuteUntil.IsZero() {
				r.muteUntilNano.Store(st.MuteUntil.UnixNano())
			}
		}
	}

	var debounceTimer *time.Timer
	var debounceCh <-chan time.Time

	armDebounce := func(d time.Duration) {
		if d <= 0 {
			d = 10 * time.Millisecond
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

	launchPendingTurn := func() {
		if r.turnActive || r.pendingTrigger == nil {
			return
		}
		trig := *r.pendingTrigger
		r.pendingTrigger = nil
		r.startTurn(trig)
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
				wait, arm := r.onMessage(ev.Message)
				if arm && r.pendingTrigger != nil {
					armDebounce(wait)
				}
			} else if ev.System != nil {
				if r.onSystemTrigger(*ev.System) && !r.turnActive {
					launchPendingTurn()
				}
			} else if ev.Command != nil {
				r.onAdminCommand(*ev.Command)
			}

		case patch := <-r.percPatchCh:
			for i := range r.pendingMsgs {
				if r.pendingMsgs[i].Seq == patch.seq {
					r.pendingMsgs[i].Parts = patch.parts
					break
				}
			}

		case out := <-r.turnDoneCh:
			r.turnActive = false
			r.turnCancel = nil
			r.applyTurnOutcome(out)
			if r.pendingTrigger != nil && debounceTimer == nil {
				armDebounce(10 * time.Millisecond)
			}

		case <-debounceCh:
			debounceCh = nil
			debounceTimer = nil
			launchPendingTurn()
		}
	}
}

func (r *Room) onAdminCommand(cmd model.AdminCommand) {
	ctx := r.mgr.shutdownCtx
	switch cmd.Name {
	case "reset":
		newEp, err := r.mgr.store.BumpChannelEpoch(ctx, r.ch)
		if err == nil {
			r.epoch.Store(newEp)
		} else {
			r.epoch.Add(1)
		}
		if r.turnCancel != nil {
			r.turnCancel()
		}
		r.pendingMsgs = nil
		r.pendingTrigger = nil
		r.seenInSegment = make(map[string]bool)
		r.threadUserID = ""
		r.threadTurns = 0
		if cmd.Reply != nil {
			cmd.Reply <- fmt.Sprintf("已重置当前频道上下文 (epoch=%d)", r.epoch.Load())
		}
	case "status":
		if cmd.Reply != nil {
			cmd.Reply <- fmt.Sprintf("channel=%s epoch=%d active_turn=%v pending=%d muted=%v",
				r.ch, r.epoch.Load(), r.turnActive, len(r.pendingMsgs), r.isMuted(time.Now()))
		}
	case "tr":
		if cmd.Reply != nil {
			trCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if r.mgr.hub == nil {
				cmd.Reply <- "翻译失败: inference hub unavailable"
				return
			}
			out, err := r.mgr.hub.Translate(trCtx, cmd.Args, "Simplified Chinese")
			if err != nil {
				cmd.Reply <- "翻译失败: " + err.Error()
			} else {
				cmd.Reply <- out
			}
		}
	}
}

func (r *Room) onMessage(msg *model.Message) (time.Duration, bool) {
	ctx := r.mgr.shutdownCtx
	cfg := r.mgr.cfgMgr.Get()
	r.msgCount.Add(1)

	// Ignore our own outbound echoes (message_sent) before persisting L0 since outbound paths already recorded them.
	if msg.FromBot {
		return 0, false
	}

	// Resolve RepliedToBot and bounded quote chain (depth <= 3) via local L0 + transport.GetMessage fallback.
	if msg.ReplyToMsgID != "" {
		r.resolveQuoteChain(ctx, msg, 3)
	}

	// Update immutable member identity & alias ledger (preserving platform role).
	_ = r.mgr.store.UpsertMember(ctx, model.Member{
		ChannelID:   r.ch,
		MemberID:    msg.SenderID,
		DisplayName: msg.SenderName,
		Role:        msg.SenderRole,
		UpdatedAt:   msg.CreatedAt,
	})

	// Persist L0 raw message immediately so seq is strictly monotonic.
	_ = r.mgr.store.AppendMessage(ctx, msg)

	// Ignore configured ignored bots for trigger logic (after persisting to L0).
	if r.mgr.policy.IsIgnoredBot(msg.SenderID) {
		return 0, false
	}

	// Track distinct senders in a rolling 30s window for PolicyEnv.BurstUsers.
	now := time.Now()
	r.recentSenders = append(r.recentSenders, senderStamp{userID: msg.SenderID, at: now})
	burstUsers := r.countBurstUsers(now, 30*time.Second)

	// Built-in slash commands: /reset, /tr
	plain := strings.TrimSpace(msg.PlainText())
	if plain == "/reset" && (cfg.IsAdmin(msg.SenderID) || !r.ch.IsGroup()) {
		replyCh := make(chan string, 1)
		r.onAdminCommand(model.AdminCommand{ChannelID: r.ch, SenderID: msg.SenderID, Name: "reset", Reply: replyCh})
		if r.mgr.tr != nil {
			if _, err := r.mgr.tr.SendText(ctx, r.ch, <-replyCh, transport.SendOptions{}); errors.Is(err, transport.ErrRiskMuted) {
				r.engageMute(ctx, cfg)
			}
		}
		return 0, false
	}
	if strings.HasPrefix(plain, "/tr ") {
		if r.mgr.hub == nil || r.isMuted(now) {
			return 0, false
		}
		arg := strings.TrimSpace(strings.TrimPrefix(plain, "/tr "))
		replyTo := msg.PlatformMsgID
		r.mgr.wg.Add(1)
		go func() {
			defer r.mgr.wg.Done()
			tctx, cancel := context.WithTimeout(r.mgr.shutdownCtx, 30*time.Second)
			defer cancel()
			out, err := r.mgr.hub.Translate(tctx, arg, "Simplified Chinese")
			if err == nil && out != "" && r.mgr.tr != nil {
				if _, serr := r.mgr.tr.SendText(tctx, r.ch, out, transport.SendOptions{ReplyToMsgID: replyTo}); errors.Is(serr, transport.ErrRiskMuted) {
					r.engageMute(tctx, cfg)
				}
			}
		}()
		return 0, false
	}

	// Launch bounded async perception tasks (image canonicalization + voice ASR + forward nodes)
	// on a DEEP COPY of msg so the Room Actor goroutine never races with background perception!
	hasMedia, hasAudio, hasForward := false, false, false
	for _, p := range msg.Parts {
		switch p.Kind {
		case model.PartImage:
			hasMedia = true
		case model.PartAudio:
			hasAudio = true
		case model.PartForward:
			hasForward = true
		}
	}
	var perceptionCh chan perceptionResult
	var deliveredInline atomic.Bool
	if hasMedia || hasAudio || hasForward {
		perceptionCh = make(chan perceptionResult, 1)
		msgCopy := cloneMessage(*msg)
		r.mgr.wg.Add(1)
		go func() {
			defer r.mgr.wg.Done()
			select {
			case r.mgr.percSem <- struct{}{}:
				defer func() { <-r.mgr.percSem }()
			case <-r.mgr.shutdownCtx.Done():
				perceptionCh <- perceptionResult{}
				return
			}
			res := r.runPerception(&msgCopy, cfg)
			perceptionCh <- res
			// If perception completed after onMessage moved on (silent msg or timeout),
			// patch r.pendingMsgs via actor channel so subsequent turns see the image/transcript!
			if res.changed && !deliveredInline.Load() {
				select {
				case r.percPatchCh <- perceptionPatch{seq: msgCopy.Seq, parts: res.parts}:
				default:
				}
			}
		}()
	}

	// Check mute backoff before deciding whether to respond.
	if r.isMuted(now) {
		r.pendingMsgs = append(r.pendingMsgs, cloneMessage(*msg))
		return 0, false
	}

	// Poke mode filtering & optional reverse poke (SPEC §5.3 / PokeConfig).
	isPokeOnly := len(msg.Parts) == 1 && msg.Parts[0].Kind == model.PartPoke
	pokedForPolicy := msg.PokedBot
	if isPokeOnly {
		switch strings.ToLower(strings.TrimSpace(cfg.Policy.Poke.Mode)) {
		case "ignore":
			pokedForPolicy = false
		case "all":
			pokedForPolicy = true
		default: // "bot_only"
			pokedForPolicy = msg.PokedBot
		}
		if pokedForPolicy && cfg.Policy.Poke.ReverseProb > 0 && rand.Float64() < cfg.Policy.Poke.ReverseProb && r.mgr.tr != nil {
			r.mgr.wg.Add(1)
			go func(target string) {
				defer r.mgr.wg.Done()
				pctx, cancel := context.WithTimeout(r.mgr.shutdownCtx, 2*time.Second)
				defer cancel()
				_ = r.mgr.tr.SendPoke(pctx, r.ch, target)
			}(msg.SenderID)
		}
	}

	// Zero-latency +1 repeater & playful chain break (group only, >= min_users distinct users).
	plusOneBreakTriggered := false
	if r.ch.IsGroup() && cfg.Policy.PlusOne.Enabled && !msg.MentionedBot && !msg.RepliedToBot && !pokedForPolicy {
		switch r.tryPlusOne(ctx, msg, cfg) {
		case plusOneEcho:
			r.pendingMsgs = append(r.pendingMsgs, cloneMessage(*msg))
			return 0, false
		case plusOneBreak:
			plusOneBreakTriggered = true
		}
	}

	// Evaluate policy rules using per-channel overrides for cooldown & thread_max_turns.
	nowLocal := now.In(cfg.Location())
	hour := nowLocal.Hour()
	isNight := hour < 7 || hour >= 23
	threadMax := r.mgr.policy.ThreadMaxTurns(r.ch)
	inThread := r.threadUserID == msg.SenderID &&
		r.threadTurns < threadMax &&
		time.Since(r.threadLastAt) <= time.Duration(cfg.Policy.ThreadContinuationSec)*time.Second &&
		time.Since(r.threadLastAt) >= time.Duration(cfg.Policy.ThreadMinIntervalSec*float64(time.Second))
	cooldownSec := r.mgr.policy.ProactiveCooldownSec(r.ch)
	cooldownOK := time.Since(r.lastProactive) >= time.Duration(cooldownSec)*time.Second

	env := policy.PolicyEnv{
		IsGroup:      r.ch.IsGroup(),
		FromBot:      msg.FromBot,
		IsAdmin:      cfg.IsAdmin(msg.SenderID),
		Mentioned:    msg.MentionedBot,
		RepliedToBot: msg.RepliedToBot,
		Poked:        pokedForPolicy,
		HitNickname:  r.mgr.policy.HitNickname(plain),
		InThread:     inThread,
		ThreadTurns:  r.threadTurns,
		CooldownOK:   cooldownOK,
		IsNight:      isNight,
		Hour:         hour,
		BurstUsers:   burstUsers,
		TextLen:      len([]rune(plain)),
		Rand:         rand.Float64(),
	}

	shouldReply := !r.ch.IsGroup() || plusOneBreakTriggered // Always reply in DMs or +1 chain break
	if r.ch.IsGroup() && !plusOneBreakTriggered {
		if ok, err := r.mgr.policy.EvalReplyGate(r.ch, env); err == nil {
			shouldReply = ok
		}
	}

	// Wait bounded for perception if this message triggers or joins an active turn.
	if perceptionCh != nil && (shouldReply || r.pendingTrigger != nil) {
		waitDur := time.Duration(cfg.Policy.MediaWaitSec * float64(time.Second))
		if hasAudio {
			waitDur = time.Duration(cfg.Policy.ASRWaitSec * float64(time.Second))
		}
		if waitDur <= 0 {
			waitDur = 3 * time.Second
		}
		select {
		case res := <-perceptionCh:
			deliveredInline.Store(true)
			if res.changed {
				msg.Parts = res.parts
				plain = strings.TrimSpace(msg.PlainText())
			}
		case <-time.After(waitDur):
			// Proceed with placeholder; background perception patches L0+FTS5 & r.pendingMsgs on completion.
		}
	}

	r.pendingMsgs = append(r.pendingMsgs, cloneMessage(*msg))
	if len(r.pendingMsgs) > 60 {
		r.pendingMsgs = r.pendingMsgs[len(r.pendingMsgs)-60:]
	}

	if !shouldReply {
		// Same-sender burst follow-up coalesces into an active pending trigger (e.g. pure @bot followed by text).
		if r.pendingTrigger != nil && r.pendingTrigger.msg.SenderID == msg.SenderID {
			return time.Duration(cfg.Policy.InputDebounceSec * float64(time.Second)), true
		}
		// Unrelated user message must NEVER disturb or shorten an active debounce timer.
		return 0, false
	}

	trigKind := "proactive"
	proactive := true
	switch {
	case !r.ch.IsGroup():
		trigKind = "dm"
		proactive = false
	case plusOneBreakTriggered:
		trigKind = "plus_one_break"
		proactive = false
	case msg.MentionedBot:
		trigKind = "mention"
		proactive = false
	case msg.RepliedToBot:
		trigKind = "reply"
		proactive = false
	case pokedForPolicy:
		trigKind = "poke"
		proactive = false
	case inThread:
		trigKind = "thread"
		proactive = false
	}

	// Enforce hard proactive cooldown in Go regardless of custom reply_gate expressions (SPEC §5.3).
	if proactive && !cooldownOK {
		return 0, false
	}
	// Never let a lower-priority proactive trigger overwrite a queued explicit trigger (e.g. pure @bot wait).
	if proactive && r.pendingTrigger != nil && !r.pendingTrigger.proactive {
		return 0, false
	}

	r.pendingTrigger = &triggerState{
		msg:        cloneMessage(*msg),
		kind:       trigKind,
		proactive:  proactive,
		enqueuedAt: time.Now(),
		enqMsgCnt:  r.msgCount.Load(),
	}

	// Pure @mention with no text waits mention_wait_sec for follow-up message from same sender.
	if msg.MentionedBot && plain == "" {
		return time.Duration(cfg.Policy.MentionWaitSec * float64(time.Second)), true
	}
	// Explicit @mention (with content) / reply / poke / +1 break wakes immediately (SPEC §5.3).
	if msg.MentionedBot || msg.RepliedToBot || pokedForPolicy || plusOneBreakTriggered {
		return 0, true
	}
	// DMs, thread continuations, and proactive triggers debounce over input_debounce_sec.
	return time.Duration(cfg.Policy.InputDebounceSec * float64(time.Second)), true
}

func (r *Room) resolveQuoteChain(ctx context.Context, msg *model.Message, maxDepth int) {
	if maxDepth <= 0 {
		maxDepth = 3
	}
	curID := msg.ReplyToMsgID
	var chain []string
	for depth := 0; depth < maxDepth && curID != ""; depth++ {
		prev, err := r.mgr.store.GetMessageByPlatformID(ctx, r.ch, curID)
		if (err != nil || prev == nil) && r.mgr.tr != nil {
			gctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			prev, err = r.mgr.tr.GetMessage(gctx, r.ch, curID)
			cancel()
		}
		if err != nil || prev == nil {
			break
		}
		if depth == 0 && prev.FromBot {
			msg.RepliedToBot = true
		}
		txt := strings.TrimSpace(prev.PlainText())
		if txt != "" {
			chain = append(chain, fmt.Sprintf("%s: %s", prev.SenderName, txt))
		}
		if prev.ReplyToMsgID == "" || prev.ReplyToMsgID == curID {
			break
		}
		curID = prev.ReplyToMsgID
	}
	if len(chain) > 0 {
		joined := strings.Join(chain, " <- ")
		for i := range msg.Parts {
			if msg.Parts[i].Kind == model.PartQuote && msg.Parts[i].Text == "" {
				msg.Parts[i].Text = joined
			}
		}
	}
}

func (r *Room) countBurstUsers(now time.Time, window time.Duration) int {
	cutoff := now.Add(-window)
	kept := r.recentSenders[:0]
	seen := make(map[string]struct{})
	for _, s := range r.recentSenders {
		if s.at.After(cutoff) {
			kept = append(kept, s)
			seen[s.userID] = struct{}{}
		}
	}
	r.recentSenders = kept
	return len(seen)
}

func cloneMessage(m model.Message) model.Message {
	cp := m
	if len(m.Parts) > 0 {
		cp.Parts = make([]model.Part, len(m.Parts))
		copy(cp.Parts, m.Parts)
	}
	return cp
}

func (r *Room) runPerception(msg *model.Message, cfg *config.Config) perceptionResult {
	ctx, cancel := context.WithTimeout(r.mgr.shutdownCtx, 20*time.Second)
	defer cancel()

	changed := false
	maxImageKB := 0
	retention := time.Duration(cfg.Frame.MediaRetentionHours) * time.Hour

	for i := range msg.Parts {
		p := &msg.Parts[i]
		switch p.Kind {
		case model.PartImage:
			if p.SHA256 != "" {
				continue
			}
			var raw []byte
			var err error
			if strings.HasPrefix(p.URL, "http://") || strings.HasPrefix(p.URL, "https://") {
				var code int
				raw, _, code, err = netutil.FetchLimited(ctx, r.mgr.httpClient(), p.URL, int64(cfg.Frame.MaxImageMiB)<<20, nil)
				if err != nil || code >= 400 {
					raw = nil
				}
			}
			// Fallback to NapCat local file path when configured locally.
			if len(raw) == 0 && p.FileID != "" && filepath.IsAbs(p.FileID) {
				if data, rerr := os.ReadFile(p.FileID); rerr == nil && int64(len(data)) <= int64(cfg.Frame.MaxImageMiB)<<20 {
					raw = data
				}
			}
			if len(raw) == 0 {
				continue
			}
			kb := len(raw) / 1024
			if kb > maxImageKB {
				maxImageKB = kb
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

			// Optional auto-collect candidate sticker when enabled and policy expr passes (SPEC §5.2).
			if cfg.Sticker.AutoCollect && r.mgr.policy != nil {
				nowLocal := time.Now().In(cfg.Location())
				hour := nowLocal.Hour()
				if ok, perr := r.mgr.policy.EvalAutoCollectSticker(r.ch, policy.PolicyEnv{
					IsGroup: r.ch.IsGroup(),
					FromBot: msg.FromBot,
					IsAdmin: cfg.IsAdmin(msg.SenderID),
					IsNight: hour < 7 || hour >= 23,
					Hour:    hour,
					ImageKB: kb,
					Rand:    rand.Float64(),
				}); perr == nil && ok {
					r.autoCollectSticker(ctx, cfg, raw, blob.Ext)
				}
			}

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

		case model.PartForward:
			if len(p.Nodes) > 0 || p.FileID == "" || r.mgr.tr == nil {
				continue
			}
			if nodes, ferr := r.mgr.tr.GetForward(ctx, p.FileID, 20); ferr == nil && len(nodes) > 0 {
				p.Nodes = nodes
				changed = true
			}
		}
	}

	if changed {
		_ = r.mgr.store.PatchMessageParts(ctx, msg)
	}
	partsOut := append([]model.Part(nil), msg.Parts...)
	return perceptionResult{parts: partsOut, changed: changed, imageKB: maxImageKB}
}

func (r *Room) autoCollectSticker(ctx context.Context, cfg *config.Config, raw []byte, fallbackExt string) {
	sum := sha256.Sum256(raw)
	hash := hex.EncodeToString(sum[:])
	pack := cfg.Sticker.DefaultPack
	if pack == "" {
		pack = "default"
	}
	ext := fallbackExt
	if len(raw) >= 4 && string(raw[:4]) == "GIF8" {
		ext = ".gif"
	} else if ext == "" {
		ext = ".png"
	}
	targetDir := filepath.Join(cfg.Sticker.PackDir, pack)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return
	}
	name := hash[:16] + ext
	dst := filepath.Join(targetDir, name)
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		_ = os.WriteFile(dst, raw, 0o644)
	}
	_ = r.mgr.store.UpsertSticker(ctx, model.Sticker{
		StickerID: "stk_" + hash[:12],
		PackID:    pack,
		SHA256:    hash,
		RelPath:   filepath.Join(pack, name),
		Status:    "needs_review",
		CreatedAt: time.Now(),
	}, nil, nil)
}

func (r *Room) tryPlusOne(ctx context.Context, msg *model.Message, cfg *config.Config) plusOneAction {
	txt := strings.TrimSpace(msg.PlainText())
	if txt == "" || len([]rune(txt)) > 24 || strings.Contains(txt, "\n") {
		r.plusOnePhrase = ""
		r.plusOneUsers = make(map[string]struct{})
		return plusOneNone
	}
	if txt != r.plusOnePhrase {
		r.plusOnePhrase = txt
		r.plusOneUsers = map[string]struct{}{msg.SenderID: {}}
		return plusOneNone
	}
	r.plusOneUsers[msg.SenderID] = struct{}{}
	if len(r.plusOneUsers) < cfg.Policy.PlusOne.MinUsers {
		return plusOneNone
	}
	if time.Since(r.lastPlusOne) < time.Duration(cfg.Policy.PlusOne.CooldownSec)*time.Second {
		return plusOneNone
	}
	roll := rand.Float64()
	// Reset tracking once the threshold is reached so 4th/5th identical messages don't re-roll repeatedly.
	r.plusOnePhrase = ""
	r.plusOneUsers = make(map[string]struct{})
	if roll < cfg.Policy.PlusOne.RepeatProb {
		r.lastPlusOne = time.Now()
		if r.mgr.tr != nil {
			pid, err := r.mgr.tr.SendText(ctx, r.ch, txt, transport.SendOptions{})
			if errors.Is(err, transport.ErrRiskMuted) {
				r.engageMute(ctx, cfg)
			} else if err == nil {
				_ = r.mgr.store.AppendMessage(ctx, &model.Message{
					ChannelID:     r.ch,
					PlatformMsgID: pid,
					SenderID:      r.mgr.tr.SelfID(),
					SenderName:    "fatfish",
					FromBot:       true,
					Parts:         []model.Part{{Kind: model.PartText, Text: txt}},
					CreatedAt:     time.Now(),
				})
			}
		}
		return plusOneEcho
	}
	if cfg.Policy.PlusOne.BreakProb > 0 && roll < cfg.Policy.PlusOne.RepeatProb+cfg.Policy.PlusOne.BreakProb {
		r.lastPlusOne = time.Now()
		return plusOneBreak
	}
	return plusOneNone
}

func (r *Room) onSystemTrigger(ev model.SystemTriggerEvent) bool {
	if r.isMuted(time.Now()) {
		r.mgr.log.Info("skipping scheduled trigger while muted", "channel", r.ch, "schedule_id", ev.ScheduleID)
		return false
	}
	trigMsg := model.Message{
		ChannelID:  r.ch,
		SenderID:   "system",
		SenderName: "System",
		Parts:      []model.Part{{Kind: model.PartText, Text: "[Scheduled Trigger] " + ev.Prompt}},
		CreatedAt:  ev.CreatedAt,
	}
	r.pendingMsgs = append(r.pendingMsgs, trigMsg)
	r.pendingTrigger = &triggerState{
		msg:        trigMsg,
		kind:       "cron",
		proactive:  false,
		enqueuedAt: time.Now(),
		enqMsgCnt:  r.msgCount.Load(),
	}
	return true
}

// startTurn hands off blocking Gate/LLM/delivery execution to a single managed worker goroutine
// so Room.run() continues draining r.mailbox (/reset, live msgCount, perception patches).
func (r *Room) startTurn(trig triggerState) {
	cfg := r.mgr.cfgMgr.Get()
	startEpoch := r.epoch.Load()
	batch := append([]model.Message(nil), r.pendingMsgs...)
	if len(batch) == 0 {
		batch = []model.Message{trig.msg}
	}
	r.pendingMsgs = nil

	seenCopy := make(map[string]bool, len(r.seenInSegment))
	for k, v := range r.seenInSegment {
		seenCopy[k] = v
	}
	lastSegID := r.lastSegID

	turnCtx, cancel := context.WithCancel(r.mgr.shutdownCtx)
	r.turnActive = true
	r.turnCancel = cancel

	r.mgr.wg.Add(1)
	go func() {
		defer r.mgr.wg.Done()
		defer cancel()
		out := r.runTurnWorker(turnCtx, trig, batch, startEpoch, lastSegID, seenCopy, cfg)
		select {
		case r.turnDoneCh <- out:
		case <-r.stopCh:
		}
	}()
}

func (r *Room) applyTurnOutcome(out turnOutcome) {
	if out.proactiveActed {
		r.lastProactive = time.Now()
	}
	if out.segID > 0 {
		if out.segID != r.lastSegID {
			r.lastSegID = out.segID
			r.seenInSegment = make(map[string]bool)
		}
		for _, id := range out.seenMembers {
			r.seenInSegment[id] = true
		}
	}
	if out.committed {
		if out.trig.proactive {
			r.lastProactive = time.Now()
		}
		if out.trig.msg.SenderID != "" && out.trig.msg.SenderID != "system" {
			// Reset threadTurns to 1 on a fresh explicit mention/reply/poke so a user is never permanently locked out.
			if r.threadUserID == out.trig.msg.SenderID && out.trig.kind == "thread" {
				r.threadTurns++
			} else {
				r.threadUserID = out.trig.msg.SenderID
				r.threadTurns = 1
			}
			r.threadLastAt = time.Now()
		}
		return
	}
	// Restore uncommitted batch if epoch didn't change (e.g. LLM error / stale drop / 0 sent) so background context isn't lost.
	if len(out.uncommittedBatch) > 0 && r.epoch.Load() == out.epoch {
		merged := make([]model.Message, 0, len(out.uncommittedBatch)+len(r.pendingMsgs))
		merged = append(merged, out.uncommittedBatch...)
		merged = append(merged, r.pendingMsgs...)
		if len(merged) > 60 {
			merged = merged[len(merged)-60:]
		}
		r.pendingMsgs = merged
	}
}

func (r *Room) runTurnWorker(
	ctx context.Context,
	trig triggerState,
	batch []model.Message,
	startEpoch int64,
	lastSegID int64,
	seenInSegment map[string]bool,
	cfg *config.Config,
) turnOutcome {
	outcome := turnOutcome{
		epoch:            startEpoch,
		trig:             trig,
		uncommittedBatch: batch,
	}

	// Stale-drop check #1 (before generation).
	if r.epoch.Load() != startEpoch || r.isStale(trig, cfg) {
		r.mgr.log.Info("turn dropped stale", "channel", r.ch, "trigger", trig.kind, "stage", "pre_gen")
		return outcome
	}

	// Proactive fast-model JSON Gate (reply | react:<emoji> | ignore).
	if trig.proactive && r.mgr.hub != nil {
		decision := r.runProactiveGate(ctx, trig, cfg)
		if decision == "ignore" {
			return outcome
		}
		if strings.HasPrefix(decision, "react:") {
			emoji := strings.TrimSpace(strings.TrimPrefix(decision, "react:"))
			if r.mgr.tr != nil && trig.msg.PlatformMsgID != "" && emoji != "" {
				if err := r.mgr.tr.SendReaction(ctx, r.ch, trig.msg.PlatformMsgID, emoji); errors.Is(err, transport.ErrRiskMuted) {
					r.engageMute(ctx, cfg)
				} else if err == nil {
					outcome.proactiveActed = true
					r.mgr.log.Info("proactive gate reacted", "channel", r.ch, "emoji", emoji)
				}
			}
			return outcome
		}
	}

	// Ensure active segment (with Segment-Pinned Memory Snapshot frozen ONCE at segment creation).
	toolsHash := ""
	if r.mgr.registry != nil {
		toolsHash = r.mgr.registry.Hash()
	}
	scopeHash := frame.ScopeHash(cfg.PersonaHash, toolsHash, cfg.Models.Primary.Model)

	activeMembers := uniqueSenders(batch)
	seg, closedSeg, err := r.mgr.frames.EnsureActiveSegmentWithClose(
		ctx, r.ch, startEpoch, scopeHash,
		func(sctx context.Context) (frame.SnapshotInput, error) {
			if r.mgr.memory == nil {
				return frame.SnapshotInput{}, nil
			}
			return r.mgr.memory.BuildSnapshot(sctx, r.ch, activeMembers)
		},
		func(sctx context.Context, closed *store.SegmentRecord) string {
			// Summarize closed segment inline within a short 6s budget so Segment N+1 gets Segment N's summary immediately.
			sumCtx, cancel := context.WithTimeout(sctx, 6*time.Second)
			defer cancel()
			return r.generateSegmentSummary(sumCtx, closed)
		},
	)
	if err != nil {
		r.mgr.log.Error("ensure segment failed", "channel", r.ch, "err", err)
		return outcome
	}
	if closedSeg != nil {
		r.mgr.wg.Add(1)
		go func(cseg *store.SegmentRecord) {
			defer r.mgr.wg.Done()
			r.summarizeClosedSegment(cseg)
		}(closedSeg)
	}
	outcome.segID = seg.ID
	if seg.ID != lastSegID {
		seenInSegment = make(map[string]bool)
		for _, id := range activeMembers {
			seenInSegment[id] = true
		}
	}

	// Mid-segment new member compact one-time delta lines (keeps msg[1] byte-identical!).
	var newMemberDelta []string
	for _, m := range batch {
		if m.FromBot || m.SenderID == "system" || seenInSegment[m.SenderID] {
			continue
		}
		seenInSegment[m.SenderID] = true
		if r.mgr.memory != nil {
			if delta := r.mgr.memory.MemberCardDelta(ctx, r.ch, m.SenderID, m.SenderName); delta != "" {
				newMemberDelta = append(newMemberDelta, delta)
			}
		}
	}
	for id := range seenInSegment {
		outcome.seenMembers = append(outcome.seenMembers, id)
	}

	userPayload := frame.FormatUserTurnWithLimits(
		batch, cfg.Location(), newMemberDelta, trig.msg.PlatformMsgID,
		cfg.Frame.MaxBackgroundImages, cfg.Frame.MaxImagesPerRequest,
	)
	if trig.kind == "plus_one_break" {
		userPayload.Text += "\n[Hint: Multiple members repeated the same phrase; playfully break the chain in one brief casual line.]"
	}
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
		if ctx.Err() == nil {
			r.mgr.log.Error("agent turn failed", "channel", r.ch, "err", err)
		}
		return outcome
	}

	// Abort if /reset happened or channel moved on while LLM was thinking.
	// Because Room.run() keeps draining mailbox concurrently, r.epoch and r.msgCount are LIVE here!
	if r.epoch.Load() != startEpoch || r.isStale(trig, cfg) {
		r.mgr.log.Info("turn dropped stale", "channel", r.ch, "trigger", trig.kind, "stage", "post_gen",
			"new_msgs", int(r.msgCount.Load()-trig.enqMsgCnt))
		return outcome
	}

	// Build TurnPlan with natural output formatting using LIVE message displacement count.
	bubbles := FormatBubbles(out.Text, cfg.Policy)
	replyTo, mentionUser := SmartReplyDecoration(trig, int(r.msgCount.Load()-trig.enqMsgCnt), cfg.Policy)
	pokeAfter := false
	if cfg.Policy.Poke.Mode != "ignore" &&
		cfg.Policy.Poke.AfterReplyProb > 0 &&
		trig.msg.SenderID != "" && trig.msg.SenderID != "system" &&
		len([]rune(out.Text)) > 0 &&
		(cfg.Policy.Poke.AfterReplyMaxChars <= 0 || len([]rune(out.Text)) <= cfg.Policy.Poke.AfterReplyMaxChars) &&
		rand.Float64() < cfg.Policy.Poke.AfterReplyProb {
		pokeAfter = true
	}
	plan := model.TurnPlan{
		ChannelID:      r.ch,
		Epoch:          startEpoch,
		SegmentID:      seg.ID,
		TriggerMsgID:   trig.msg.PlatformMsgID,
		TriggerUserID:  trig.msg.SenderID,
		TriggerKind:    trig.kind,
		RawText:        out.Text,
		Bubbles:        bubbles,
		DirectMedia:    out.DirectMedia,
		StickerIntent:  out.StickerIntent,
		ReplyToMsgID:   replyTo,
		MentionUserID:  mentionUser,
		PokeAfterReply: pokeAfter,
	}

	committed := r.deliverPlan(ctx, seg, plan, out.NewFrames, out.TrimUpToIndex, out.Stats, trig, cfg)
	if committed {
		outcome.committed = true
		outcome.uncommittedBatch = nil
	}
	return outcome
}

func (r *Room) deliverPlan(ctx context.Context, seg *store.SegmentRecord, plan model.TurnPlan, newFrames []frame.StoredFramePayload, trimUpToIdx int, stats agent.TurnStats, trig triggerState, cfg *config.Config) bool {
	delID, _ := r.mgr.store.CreateDelivery(ctx, plan)
	var sent []model.SentItem
	status := model.DeliverySent
	stickerDispatched := false

	if r.mgr.tr != nil {
		// Transition ledger planned -> sending before outbound transport calls (SPEC §3).
		_ = r.mgr.store.UpdateDelivery(ctx, delID, model.DeliverySending, nil)

		// 1. Send text bubbles with natural pacing and live epoch/risk guards.
	bubbleLoop:
		for i, b := range plan.Bubbles {
			if r.epoch.Load() != plan.Epoch {
				status = model.DeliveryAbortedEpoch
				break bubbleLoop
			}
			if i > 0 {
				delay := bubbleDelay(b, cfg.Policy.BubbleTypingDelayMax)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					if r.epoch.Load() != plan.Epoch {
						status = model.DeliveryAbortedEpoch
					} else {
						status = model.DeliveryPartial
					}
					break bubbleLoop
				}
				if r.epoch.Load() != plan.Epoch {
					status = model.DeliveryAbortedEpoch
					break bubbleLoop
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
					r.engageMute(ctx, cfg)
				} else if len(sent) > 0 {
					status = model.DeliveryPartial
				} else {
					status = model.DeliveryFailed
				}
				break bubbleLoop
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
			retention := time.Duration(cfg.Frame.MediaRetentionHours) * time.Hour
			for _, dm := range plan.DirectMedia {
				if r.epoch.Load() != plan.Epoch {
					status = model.DeliveryAbortedEpoch
					break
				}
				pid, err := r.mgr.tr.SendImage(ctx, r.ch, dm.Data, dm.MimeType, transport.SendOptions{})
				if err != nil {
					if errors.Is(err, transport.ErrRiskMuted) {
						status = model.DeliveryAbortedRisk
						r.engageMute(ctx, cfg)
					} else if len(sent) > 0 {
						status = model.DeliveryPartial
					} else {
						status = model.DeliveryFailed
					}
					break
				}
				mediaID := dm.MediaID
				sha := dm.SHA256
				mime := dm.MimeType
				if mediaID == "" && len(dm.Data) > 0 {
					if blob, berr := r.mgr.store.Blobs().PutImage(dm.Data); berr == nil {
						if asset, aerr := r.mgr.store.RegisterMediaAsset(ctx, r.ch, r.mgr.tr.SelfID(), blob, retention); aerr == nil {
							mediaID = asset.MediaID
							sha = asset.SHA256
							mime = asset.MimeType
						}
					}
				}
				sent = append(sent, model.SentItem{
					Kind:          "image",
					PlatformMsgID: pid,
					MediaID:       mediaID,
					SentAt:        time.Now().UnixMilli(),
				})
				// Record delivered media into public L0 timeline (SPEC §3).
				_ = r.mgr.store.AppendMessage(ctx, &model.Message{
					ChannelID:     r.ch,
					PlatformMsgID: pid,
					SenderID:      r.mgr.tr.SelfID(),
					SenderName:    "fatfish",
					FromBot:       true,
					Parts: []model.Part{{
						Kind:     model.PartImage,
						MediaID:  mediaID,
						SHA256:   sha,
						MimeType: mime,
						Text:     dm.Caption,
					}},
					CreatedAt: time.Now(),
				})
			}
		}

		// 3. Zero-latency post-ACK async reaction sticker (only if not muted/aborted).
		if status == model.DeliverySent && plan.StickerIntent != "" && r.mgr.stickers != nil {
			stickerDispatched = true
			ctxQ := trig.msg.PlainText() + " -> " + plan.RawText
			r.mgr.stickers.DispatchAsync(
				r.mgr.shutdownCtx,
				r.ch,
				plan.Epoch,
				plan.StickerIntent,
				ctxQ,
				func() int64 { return r.epoch.Load() },
				func(sctx context.Context, st model.Sticker, imgBytes []byte) error {
					mime := sticker.MimeTypeFor(st)
					pid, err := r.mgr.tr.SendImage(sctx, r.ch, imgBytes, mime, transport.SendOptions{})
					if err != nil {
						if errors.Is(err, transport.ErrRiskMuted) {
							r.engageMute(sctx, cfg)
						}
						return err
					}
					// Record sent reaction sticker into public L0 media timeline (SPEC §9).
					_ = r.mgr.store.AppendMessage(sctx, &model.Message{
						ChannelID:     r.ch,
						PlatformMsgID: pid,
						SenderID:      r.mgr.tr.SelfID(),
						SenderName:    "fatfish",
						FromBot:       true,
						Parts: []model.Part{{
							Kind:     model.PartImage,
							SHA256:   st.SHA256,
							MimeType: mime,
							Text:     "[sticker:" + st.StickerID + "]",
						}},
						CreatedAt: time.Now(),
					})
					return nil
				},
			)
		}

		// 4. Optional playful poke after concise reply (SPEC §5.3 / PokeConfig).
		if status == model.DeliverySent && plan.PokeAfterReply && plan.TriggerUserID != "" {
			delay := time.Duration(cfg.Policy.Poke.AfterReplyDelaySec * float64(time.Second))
			if delay <= 0 {
				delay = 500 * time.Millisecond
			}
			r.mgr.wg.Add(1)
			go func(target string, expEpoch int64) {
				defer r.mgr.wg.Done()
				select {
				case <-time.After(delay):
				case <-r.mgr.shutdownCtx.Done():
					return
				}
				if r.epoch.Load() != expEpoch {
					return
				}
				pctx, cancel := context.WithTimeout(r.mgr.shutdownCtx, 2*time.Second)
				defer cancel()
				_ = r.mgr.tr.SendPoke(pctx, r.ch, target)
			}(plan.TriggerUserID, plan.Epoch)
		}
	}

	_ = r.mgr.store.UpdateDelivery(ctx, delID, status, sent)

	hitPct := 0.0
	if stats.PromptTokens > 0 {
		hitPct = float64(stats.CachedTokens) * 100.0 / float64(stats.PromptTokens)
	}
	r.mgr.log.Info("turn delivered",
		"channel", r.ch,
		"trigger", trig.kind,
		"status", status,
		"model", stats.ModelUsed,
		"fallback", stats.FallbackUsed,
		"iters", stats.Iterations,
		"tools", strings.Join(stats.ToolNames, ","),
		"prompt_tok", stats.PromptTokens,
		"cached_tok", stats.CachedTokens,
		"cache_hit_pct", fmt.Sprintf("%.1f%%", hitPct),
		"comp_tok", stats.CompletionTokens,
		"latency_ms", stats.TotalLatencyMs,
		"bubbles", len(sent),
		"sticker", plan.StickerIntent != "",
	)

	// Invariant (SPEC Section 3): ONLY delivered turns enter the FrozenFrame timeline
	// and commit any deferred whole-turn frame trim!
	if len(sent) > 0 || stickerDispatched || r.mgr.tr == nil {
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
		_ = r.mgr.frames.AppendFrozenFramesWithTrim(ctx, seg, trimUpToIdx, newFrames...)
		return true
	}
	return false
}

func (r *Room) isStale(trig triggerState, cfg *config.Config) bool {
	if !trig.proactive {
		return false
	}
	nowLocal := time.Now().In(cfg.Location())
	hour := nowLocal.Hour()
	drop, err := r.mgr.policy.EvalDropStale(r.ch, policy.PolicyEnv{
		IsGroup:   r.ch.IsGroup(),
		IsNight:   hour < 7 || hour >= 23,
		Hour:      hour,
		Proactive: true,
		NewMsgs:   int(r.msgCount.Load() - trig.enqMsgCnt),
		WaitSec:   time.Since(trig.enqueuedAt).Seconds(),
	})
	return err == nil && drop
}

func (r *Room) runProactiveGate(ctx context.Context, trig triggerState, cfg *config.Config) string {
	recent, _ := r.mgr.store.RecentMessages(ctx, r.ch, 20)
	budget := cfg.Policy.GateTokenBudget
	if budget <= 0 {
		budget = 4096
	}
	var lines []string
	usedTok := 0
	for i := len(recent) - 1; i >= 0; i-- {
		m := recent[i]
		line := fmt.Sprintf("%s: %s", m.SenderName, m.PlainText())
		tok := frame.EstimateTextTokens(line)
		if usedTok+tok > budget && len(lines) > 0 {
			break
		}
		lines = append(lines, line)
		usedTok += tok
	}
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
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
	raw := extractJSONBlock(resp.Content)
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		return "ignore"
	}
	switch strings.ToLower(strings.TrimSpace(res.Action)) {
	case "reply":
		return "reply"
	case "react":
		if strings.TrimSpace(res.EmojiID) == "" {
			return "ignore"
		}
		return "react:" + strings.TrimSpace(res.EmojiID)
	default:
		return "ignore"
	}
}

func extractJSONBlock(s string) string {
	s = strings.TrimSpace(s)
	start := strings.IndexByte(s, '{')
	end := strings.LastIndexByte(s, '}')
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}

func (r *Room) generateSegmentSummary(ctx context.Context, seg *store.SegmentRecord) string {
	if seg == nil || r.mgr.hub == nil || r.mgr.store == nil {
		return ""
	}
	frames, err := r.mgr.store.ListFrames(ctx, seg.ID)
	if err != nil || len(frames) == 0 {
		return ""
	}
	var lines []string
	for _, f := range frames {
		var p frame.StoredFramePayload
		if json.Unmarshal([]byte(f.PayloadJSON), &p) == nil && p.Text != "" {
			lines = append(lines, f.Role+": "+p.Text)
		}
	}
	if len(lines) == 0 {
		return ""
	}
	resp, err := r.mgr.hub.Chat(ctx, "fast", infer.ChatRequest{
		Messages: []infer.ChatMessage{
			{Role: "system", Content: "Summarize this completed conversation segment in <=180 words for continuity in the next segment."},
			{Role: "user", Content: strings.Join(lines, "\n")},
		},
	})
	if err != nil {
		return ""
	}
	return strings.TrimSpace(resp.Content)
}

func (r *Room) summarizeClosedSegment(seg *store.SegmentRecord) {
	ctx, cancel := context.WithTimeout(r.mgr.shutdownCtx, 60*time.Second)
	defer cancel()
	// Backfill segment summary if inline summarization timed out or was skipped.
	if strings.TrimSpace(seg.SummaryText) == "" {
		if summary := r.generateSegmentSummary(ctx, seg); summary != "" {
			_ = r.mgr.store.CloseSegment(ctx, seg.ID, summary)
		}
	}

	// Online L1 Episode & L2 Profile/Lore distillation on segment close (SPEC §6.2 & §8).
	// Duplicate/overlapping windows are guarded by HasOverlappingEpisode inside DistillWindow.
	if r.mgr.memory != nil {
		endTS := seg.LastActiveAt
		if endTS.Before(seg.CreatedAt) {
			endTS = time.Now()
		}
		segMsgs, merr := r.mgr.store.MessagesInRange(ctx, seg.ChannelID, seg.CreatedAt.Add(-time.Minute), endTS.Add(time.Minute), 200)
		if merr == nil && len(segMsgs) >= 3 {
			if _, derr := r.mgr.memory.DistillWindow(ctx, seg.ChannelID, segMsgs, "fast"); derr != nil {
				r.mgr.log.Warn("online segment distill failed", "channel", seg.ChannelID, "segment", seg.ID, "err", derr)
			}
		}
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

	if !pol.SplitNewlineBubbles || strings.Contains(text, "```") || containsMarkdownTable(text) {
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

func containsMarkdownTable(s string) bool {
	return strings.Contains(s, "|---") || strings.Contains(s, "| ---") || strings.Contains(s, "|:---")
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
