package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/vec"
)

// Service manages strictly channel-scoped L0/L1/L2 memory, vector search, and offline/online distillation.
type Service struct {
	store *store.Store
	hub   *infer.Hub
	log   *slog.Logger
	dim   int

	mu         sync.RWMutex
	epIndex    *vec.FlatF16Index // key: "ep:<id>"
	epChannels map[string]model.ChannelID
	profileMu  sync.Mutex // Serializes L2 profile read-modify-write across concurrent distill workers
}

func NewService(st *store.Store, hub *infer.Hub, dim int, logger *slog.Logger) *Service {
	if dim <= 0 {
		dim = 2048
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		store:      st,
		hub:        hub,
		log:        logger,
		dim:        dim,
		epIndex:    vec.NewFlatF16Index(dim, 0.0), // diverse episodes -> alpha=0
		epChannels: make(map[string]model.ChannelID),
	}
}

// LoadIndex loads resident float16 episode vectors from SQLite into RAM.
func (s *Service) LoadIndex(ctx context.Context) (int, error) {
	eps, blobs, err := s.store.AllEpisodesWithEmbeds(ctx)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epIndex = vec.NewFlatF16Index(s.dim, 0.0)
	s.epChannels = make(map[string]model.ChannelID, len(eps))
	loaded := 0
	for i, ep := range eps {
		key := episodeKey(ep.ID)
		s.epChannels[key] = ep.ChannelID
		if len(blobs[i]) == s.dim*2 && s.epIndex.UpsertPreencoded(key, blobs[i]) {
			loaded++
		}
	}
	return loaded, nil
}

// BuildSnapshot constructs a channel-scoped SnapshotInput for a new Frame Segment.
func (s *Service) BuildSnapshot(ctx context.Context, ch model.ChannelID, activeMemberIDs []string) (model.SnapshotInput, error) {
	return s.BuildSnapshotWithTime(ctx, ch, activeMemberIDs, time.Now(), time.Local)
}

// BuildSnapshotWithTime constructs a channel-scoped SnapshotInput frozen ONCE at segment creation,
// including a human-readable local time anchor and recent 48h group highlights without breaking KV cache.
func (s *Service) BuildSnapshotWithTime(ctx context.Context, ch model.ChannelID, activeMemberIDs []string, now time.Time, loc *time.Location) (model.SnapshotInput, error) {
	if loc == nil {
		loc = time.UTC
	}
	if now.IsZero() {
		now = time.Now()
	}
	localNow := now.In(loc)

	lore, _ := s.store.ListLore(ctx, ch, 15)
	notes, _ := s.store.ListNotes(ctx, ch, true)
	names, _ := s.store.GetMemberNames(ctx, ch)

	memberSet := make(map[string]struct{}, len(activeMemberIDs))
	for _, id := range activeMemberIDs {
		memberSet[id] = struct{}{}
	}
	profiles, _ := s.store.ListProfiles(ctx, ch, 30)
	var cards []string
	for _, p := range profiles {
		if len(memberSet) > 0 {
			if _, ok := memberSet[p.MemberID]; !ok {
				continue
			}
		}
		if card := FormatMemberCard(p, names[p.MemberID]); card != "" {
			cards = append(cards, card)
		}
	}

	var highlights []string
	if eps, err := s.store.RecentEpisodesSince(ctx, ch, now.Add(-48*time.Hour), 3); err == nil && len(eps) > 0 {
		for _, ep := range eps {
			ts := ep.EndTS.In(loc).Format("01-02 15:04")
			highlights = append(highlights, fmt.Sprintf("- [%s] %s: %s", ts, ep.Topic, truncateText(ep.Summary, 90)))
		}
	} else if digests, err := s.store.ListDailyDigests(ctx, ch, 1); err == nil && len(digests) > 0 {
		d := digests[0]
		if now.Sub(d.CreatedAt) <= 72*time.Hour && strings.TrimSpace(d.Report) != "" {
			highlights = append(highlights, fmt.Sprintf("- [%s 群日报] %s", d.DayKey, truncateText(d.Report, 140)))
		}
	}

	return model.SnapshotInput{
		SessionTime:      formatSessionTime(localNow),
		Lore:             lore,
		PinnedNotes:      notes,
		RecentHighlights: highlights,
		MemberCards:      cards,
	}, nil
}

func formatSessionTime(t time.Time) string {
	weekdays := [...]string{"周日", "周一", "周二", "周三", "周四", "周五", "周六"}
	h := t.Hour()
	period := "白天"
	switch {
	case h < 5:
		period = "凌晨"
	case h < 8:
		period = "早上"
	case h < 11:
		period = "上午"
	case h < 13:
		period = "中午"
	case h < 17:
		period = "下午"
	case h < 19:
		period = "傍晚"
	case h < 23:
		period = "晚上"
	default:
		period = "深夜"
	}
	return fmt.Sprintf("%s %s %s %s", t.Format("2006-01-02"), weekdays[t.Weekday()], period, t.Format("15:04"))
}

// FormatMemberCard formats corroborated (non-tentative) facts ranked by time-decayed confidence,
// interaction style note, and recent jokes cleanly for prompt injection.
func FormatMemberCard(p model.MemberProfile, displayName string) string {
	nowMs := time.Now().UnixMilli()
	type scoredFact struct {
		text  string
		score float64
	}
	var ranked []scoredFact
	for _, f := range p.Facts {
		if !f.Tentative && f.Confidence >= 0.5 && strings.TrimSpace(f.Text) != "" {
			ranked = append(ranked, scoredFact{
				text:  f.Text,
				score: decayedFactScore(f, nowMs),
			})
		}
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].score > ranked[j].score })
	facts := make([]string, 0, 5)
	for _, rf := range ranked {
		facts = append(facts, rf.text)
		if len(facts) >= 5 {
			break
		}
	}
	style := strings.TrimSpace(p.StyleNote)
	if len(facts) == 0 && len(p.Jokes) == 0 && style == "" {
		return ""
	}
	label := p.MemberID
	if displayName != "" && displayName != p.MemberID {
		label = fmt.Sprintf("%s(%s)", displayName, p.MemberID)
	}
	parts := make([]string, 0, 3)
	if style != "" {
		parts = append(parts, "互动风格: "+style)
	}
	if len(facts) > 0 {
		parts = append(parts, strings.Join(facts, "; "))
	}
	if len(p.Jokes) > 0 {
		start := len(p.Jokes) - 2
		if start < 0 {
			start = 0
		}
		var jokes []string
		for _, j := range p.Jokes[start:] {
			if t := strings.TrimSpace(j.Text); t != "" {
				jokes = append(jokes, t)
			}
		}
		if len(jokes) > 0 {
			parts = append(parts, "梗: "+strings.Join(jokes, " / "))
		}
	}
	return fmt.Sprintf("- %s: %s", label, strings.Join(parts, " | "))
}

// MemberCardDelta returns a compact one-time context line for a newly appearing mid-segment member.
func (s *Service) MemberCardDelta(ctx context.Context, ch model.ChannelID, memberID, displayName string) string {
	p, err := s.store.GetProfile(ctx, ch, memberID)
	if err != nil || p == nil {
		return ""
	}
	return FormatMemberCard(*p, displayName)
}

// RecallOptions configures channel-scoped memory search.
type RecallOptions struct {
	ChannelID model.ChannelID
	Mode      string // "recent" | "raw" | "memory"
	Query     string
	Sender    string // Optional nickname/alias or member ID
	SinceMs   int64
	UntilMs   int64
	Limit     int
}

// RecallResult returns formatted channel-scoped historical evidence.
type RecallResult struct {
	Summary string
}

// Recall executes strictly channel-scoped L0 or L1/L2 hybrid retrieval.
func (s *Service) Recall(ctx context.Context, opts RecallOptions) (RecallResult, error) {
	if opts.Limit <= 0 || opts.Limit > 50 {
		opts.Limit = 15
	}
	switch strings.ToLower(strings.TrimSpace(opts.Mode)) {
	case "recent", "":
		msgs, err := s.store.RecentMessages(ctx, opts.ChannelID, opts.Limit)
		if err != nil {
			return RecallResult{}, err
		}
		return RecallResult{Summary: formatMessages(msgs)}, nil

	case "raw":
		var senderIDs []string
		if strings.TrimSpace(opts.Sender) != "" {
			senderIDs, _ = s.store.ResolveMembersByAlias(ctx, opts.ChannelID, opts.Sender)
		}
		msgs, err := s.store.SearchMessagesFTS(ctx, opts.ChannelID, opts.Query, senderIDs, opts.SinceMs, opts.UntilMs, opts.Limit)
		if err != nil {
			return RecallResult{}, err
		}
		if len(msgs) == 0 {
			return RecallResult{Summary: "(no matching raw messages in this channel)"}, nil
		}
		return RecallResult{Summary: formatMessages(msgs)}, nil

	case "memory":
		return s.recallHybridMemory(ctx, opts)

	default:
		return RecallResult{}, fmt.Errorf("memory: unsupported recall mode %q (use recent|raw|memory)", opts.Mode)
	}
}

func (s *Service) recallHybridMemory(ctx context.Context, opts RecallOptions) (RecallResult, error) {
	var sections []string

	// 1. Episode hybrid recall: FTS5 + Vector -> RRF -> optional Rerank.
	ftsEps, _ := s.store.SearchEpisodesFTS(ctx, opts.ChannelID, opts.Query, 20)
	ftsHits := make([]vec.Hit, 0, len(ftsEps))
	epMap := make(map[string]model.Episode, len(ftsEps))
	for i, ep := range ftsEps {
		k := episodeKey(ep.ID)
		epMap[k] = ep
		ftsHits = append(ftsHits, vec.Hit{ID: k, Score: float32(20 - i)})
	}

	var vecHits []vec.Hit
	s.mu.RLock()
	hasEpVecs := s.epIndex.Len() > 0
	s.mu.RUnlock()
	if s.hub != nil && strings.TrimSpace(opts.Query) != "" && hasEpVecs {
		if embs, err := s.hub.Embed(ctx, []string{opts.Query}); err == nil && len(embs) == 1 && len(embs[0]) == s.dim {
			s.mu.RLock()
			vecHits = s.epIndex.Search(embs[0], 20, func(id string) bool {
				return s.epChannels[id] == opts.ChannelID
			})
			s.mu.RUnlock()
		}
	}

	fused := vec.WeightedRRF([][]vec.Hit{ftsHits, vecHits}, []float32{1.0, 1.0}, 60, 15)
	candidates := make([]model.Episode, 0, len(fused))
	for _, h := range fused {
		var ep *model.Episode
		if e, ok := epMap[h.ID]; ok {
			epCopy := e
			ep = &epCopy
		} else if id, err := parseEpisodeKey(h.ID); err == nil {
			if e, err := s.store.GetEpisodeByID(ctx, id); err == nil && e.ChannelID == opts.ChannelID {
				ep = e
			}
		}
		if ep == nil {
			continue
		}
		if opts.SinceMs > 0 && ep.EndTS.UnixMilli() < opts.SinceMs {
			continue
		}
		if opts.UntilMs > 0 && ep.StartTS.UnixMilli() > opts.UntilMs {
			continue
		}
		candidates = append(candidates, *ep)
	}

	// Optional precision rerank (gracefully skipped if rerank circuit is open).
	if s.hub != nil && len(candidates) > 1 && strings.TrimSpace(opts.Query) != "" {
		docs := make([]string, len(candidates))
		for i, ep := range candidates {
			docs[i] = ep.Topic + ": " + ep.Summary
		}
		if rr, err := s.hub.Rerank(ctx, opts.Query, docs); err == nil && len(rr) > 0 {
			sort.Slice(rr, func(i, j int) bool { return rr[i].Score > rr[j].Score })
			reordered := make([]model.Episode, 0, len(candidates))
			for _, r := range rr {
				if r.Index >= 0 && r.Index < len(candidates) {
					reordered = append(reordered, candidates[r.Index])
				}
			}
			candidates = reordered
		}
	}

	if len(candidates) > opts.Limit {
		candidates = candidates[:opts.Limit]
	}
	if len(candidates) > 0 {
		var lines []string
		for _, ep := range candidates {
			lines = append(lines, fmt.Sprintf("- [%s] %s: %s", ep.StartTS.Format("2006-01-02 15:04"), ep.Topic, ep.Summary))
		}
		sections = append(sections, "[Episodes]\n"+strings.Join(lines, "\n"))
	}

	// 2. Relevant L2 Profiles (filter by sender if supplied).
	if strings.TrimSpace(opts.Sender) != "" {
		ids, _ := s.store.ResolveMembersByAlias(ctx, opts.ChannelID, opts.Sender)
		names, _ := s.store.GetMemberNames(ctx, opts.ChannelID)
		var lines []string
		for _, id := range ids {
			if p, err := s.store.GetProfile(ctx, opts.ChannelID, id); err == nil && p != nil {
				if card := FormatMemberCard(*p, names[id]); card != "" {
					lines = append(lines, card)
				}
			}
		}
		if len(lines) > 0 {
			sections = append(sections, "[Member Profiles]\n"+strings.Join(lines, "\n"))
		}
	}

	// 3. Lore & Pinned Notes & Recent Digests.
	if lore, err := s.store.ListLore(ctx, opts.ChannelID, 10); err == nil && len(lore) > 0 {
		var lines []string
		qLow := strings.ToLower(opts.Query)
		for _, l := range lore {
			if qLow == "" || strings.Contains(strings.ToLower(l.Term+l.Meaning), qLow) {
				lines = append(lines, fmt.Sprintf("- %s: %s", l.Term, l.Meaning))
			}
		}
		if len(lines) > 0 {
			sections = append(sections, "[Lore]\n"+strings.Join(lines, "\n"))
		}
	}

	if notes, err := s.store.ListNotes(ctx, opts.ChannelID, false); err == nil && len(notes) > 0 {
		var lines []string
		for _, n := range notes {
			lines = append(lines, fmt.Sprintf("- %s: %s", n.Key, n.Content))
		}
		sections = append(sections, "[Notes]\n"+strings.Join(lines, "\n"))
	}

	if digests, err := s.store.ListDailyDigests(ctx, opts.ChannelID, 3); err == nil && len(digests) > 0 {
		var lines []string
		for _, d := range digests {
			lines = append(lines, fmt.Sprintf("- [%s] %s", d.DayKey, truncateText(d.Report, 200)))
		}
		sections = append(sections, "[Recent Daily Digests]\n"+strings.Join(lines, "\n"))
	}

	if len(sections) == 0 {
		// Cold-start fallback: when L1/L2 memory is empty for this channel/sender,
		// surface recent L0 raw messages so the bot still recognizes active members.
		var senderIDs []string
		if strings.TrimSpace(opts.Sender) != "" {
			senderIDs, _ = s.store.ResolveMembersByAlias(ctx, opts.ChannelID, opts.Sender)
		}
		fbLimit := opts.Limit
		if fbLimit > 10 {
			fbLimit = 10
		}
		var rawMsgs []model.Message
		if strings.TrimSpace(opts.Query) != "" || len(senderIDs) > 0 {
			rawMsgs, _ = s.store.SearchMessagesFTS(ctx, opts.ChannelID, opts.Query, senderIDs, opts.SinceMs, opts.UntilMs, fbLimit)
			// If query keywords were conversational (e.g. "do you know me") and matched nothing,
			// retry filtered strictly by senderIDs to fetch their recent history in this channel.
			if len(rawMsgs) == 0 && len(senderIDs) > 0 && strings.TrimSpace(opts.Query) != "" {
				rawMsgs, _ = s.store.SearchMessagesFTS(ctx, opts.ChannelID, "", senderIDs, opts.SinceMs, opts.UntilMs, fbLimit)
			}
		}
		if len(rawMsgs) > 0 {
			return RecallResult{Summary: "[Raw Message Fallback]\n" + formatMessages(rawMsgs)}, nil
		}
		return RecallResult{Summary: "(no memory records found for this channel)"}, nil
	}
	return RecallResult{Summary: strings.Join(sections, "\n\n")}, nil
}

// --- L0 -> L1/L2 Distillation with Anti-Sarcasm Corroboration ---

// EpisodeExtraction is the structured JSON emitted by the distill model.
type EpisodeExtraction struct {
	Topic          string                     `json:"topic"`
	Summary        string                     `json:"summary"`
	Participants   []model.EpisodeParticipant `json:"participants"`
	NotableQuotes  []string                   `json:"notable_quotes"`
	CandidateFacts []struct {
		MemberID string `json:"member_id"`
		Fact     string `json:"fact"`
		Serious  bool   `json:"serious"`
	} `json:"candidate_facts"`
	CandidateJokes []struct {
		MemberID string `json:"member_id"`
		Joke     string `json:"joke"`
	} `json:"candidate_jokes"`
	MemberStyles []struct {
		MemberID string `json:"member_id"`
		Style    string `json:"style"`
	} `json:"member_styles"`
	CandidateLore []struct {
		Term    string `json:"term"`
		Meaning string `json:"meaning"`
	} `json:"candidate_lore"`
}

// PartitionWindows groups messages into meaningful conversational episodes separated by >maxGap silence
// or >=maxMsgs valid messages, skipping noisy short fragments (<minMsgs) and media-only spam windows.
func PartitionWindows(msgs []model.Message, maxGap time.Duration, maxMsgs, minMsgs int) [][]model.Message {
	if maxGap <= 0 {
		maxGap = 30 * time.Minute
	}
	if maxMsgs <= 0 {
		maxMsgs = 100
	}
	if minMsgs <= 0 {
		minMsgs = 8
	}
	minTextMsgs := minMsgs/2 + 1
	if minTextMsgs > 5 {
		minTextMsgs = 5
	}
	hasSubstantiveText := func(m model.Message) bool {
		for _, p := range m.Parts {
			if p.Kind == model.PartText && len([]rune(strings.TrimSpace(p.Text))) > 1 {
				return true
			}
			if p.Kind == model.PartAudio && len([]rune(strings.TrimSpace(p.Transcript))) > 1 {
				return true
			}
			if p.Kind == model.PartCard && strings.TrimSpace(p.Title) != "" {
				return true
			}
		}
		return false
	}

	var windows [][]model.Message
	var cur []model.Message
	curText := 0
	flush := func() {
		if len(cur) >= minMsgs && curText >= minTextMsgs {
			windows = append(windows, cur)
		}
		cur = nil
		curText = 0
	}
	for _, m := range msgs {
		txt := strings.TrimSpace(m.PlainText())
		// Filter out single-char/pure-noise lines unless carrying media.
		if len([]rune(txt)) <= 1 && len(m.Parts) == 1 && m.Parts[0].Kind == model.PartText {
			continue
		}
		if len(cur) > 0 {
			gap := m.CreatedAt.Sub(cur[len(cur)-1].CreatedAt)
			if gap > maxGap || len(cur) >= maxMsgs {
				flush()
			}
		}
		cur = append(cur, m)
		if hasSubstantiveText(m) {
			curText++
		}
	}
	flush()
	return windows
}

// DistillOptions configures offline L0 -> L1/L2 memory distillation.
type DistillOptions struct {
	ChannelID   model.ChannelID
	MaxEpisodes int
	Concurrency int
	Role        string
	SinceDays   int // When >0, only distill messages from the last N days
}

// DistillChannel performs resumable offline distillation for one channel up to opts.MaxEpisodes.
func (s *Service) DistillChannel(ctx context.Context, opts DistillOptions) (int, error) {
	ch := opts.ChannelID
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	role := opts.Role
	if role == "" {
		role = "distill"
	}
	afterSeq, err := s.store.GetDistillCheckpoint(ctx, ch)
	if err != nil {
		return 0, err
	}
	var sinceMs int64
	if opts.SinceDays > 0 {
		sinceMs = time.Now().AddDate(0, 0, -opts.SinceDays).UnixMilli()
	}
	msgs, err := s.store.MessagesAfterSeqSince(ctx, ch, afterSeq, sinceMs, 100000)
	if err != nil {
		return 0, err
	}
	windows := PartitionWindows(msgs, 30*time.Minute, 100, 8)
	if opts.MaxEpisodes > 0 && len(windows) > opts.MaxEpisodes {
		windows = windows[:opts.MaxEpisodes]
	}
	if len(windows) == 0 {
		return 0, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var mu sync.Mutex
	created := 0
	winDone := make([]bool, len(windows))
	contiguousIdx := 0
	var contiguousSeq int64 = afterSeq

	for idx, w := range windows {
		winIdx := idx
		win := w
		winNo := idx + 1
		totalWins := len(windows)
		g.Go(func() error {
			ep, err := s.distillWindowWithRetry(gctx, ch, win, role)
			if err != nil {
				s.log.Warn("distill window failed", "channel", ch, "window", winNo, "err", err)
				return nil
			}
			mu.Lock()
			created++
			winDone[winIdx] = true
			for contiguousIdx < len(windows) && winDone[contiguousIdx] {
				endSeq := windows[contiguousIdx][len(windows[contiguousIdx])-1].Seq
				if endSeq > contiguousSeq {
					contiguousSeq = endSeq
				}
				contiguousIdx++
			}
			_ = ep
			if created%10 == 0 || winNo == totalWins {
				if contiguousSeq > afterSeq {
					_ = s.store.SetDistillCheckpoint(ctx, ch, contiguousSeq)
				}
				s.log.Info("distill progress", "channel", ch, "done", created, "total_windows", totalWins, "checkpoint_seq", contiguousSeq)
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return created, err
	}
	if contiguousSeq > afterSeq {
		_ = s.store.SetDistillCheckpoint(ctx, ch, contiguousSeq)
	}
	return created, nil
}

// distillWindowWithRetry backs off when the distill circuit breaker is open instead of
// stampeding through remaining windows, and retries transient timeouts once.
func (s *Service) distillWindowWithRetry(ctx context.Context, ch model.ChannelID, win []model.Message, role string) (*model.Episode, error) {
	const maxAttempts = 4
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ep, err := s.DistillWindow(ctx, ch, win, role)
		if err == nil {
			return ep, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var wait time.Duration
		if errors.Is(err, infer.ErrCircuitOpen) {
			wait = 31 * time.Second
		} else if attempt <= 2 {
			wait = 5 * time.Second
		} else {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// DistillWindow summarizes a single message window into an L1 Episode and updates L2 profiles/lore
// using anti-sarcasm corroboration (>=2 distinct episodes before promoting tentative facts).
func (s *Service) DistillWindow(ctx context.Context, ch model.ChannelID, win []model.Message, role string) (*model.Episode, error) {
	if len(win) == 0 {
		return nil, fmt.Errorf("memory: empty window")
	}
	startSeq := win[0].Seq
	endSeq := win[len(win)-1].Seq
	if startSeq > 0 && endSeq >= startSeq {
		if dup, err := s.store.HasOverlappingEpisode(ctx, ch, startSeq, endSeq); err == nil && dup {
			return nil, nil
		}
	}
	transcript := formatMessages(win)
	sysPrompt := `Extract a concise structured JSON summary of this chat episode in the primary language of the chat (e.g. Simplified Chinese).
Rules:
1. Key every participant strictly by their immutable member_id shown in parentheses, NEVER by nickname.
2. Separate jokes/sarcasm/shitposting into candidate_jokes or candidate_lore; NEVER record sarcasm as candidate_facts.
3. Never extract candidate_facts, candidate_jokes, or member_styles for the bot itself.
4. For member_styles, capture brief interaction vibe with the group/bot when clear (e.g. "爱互怼的熟人", "爱聊技术的老哥", "话少但爱发图"), at most 12 chars.
5. Keep arrays compact (at most 5 items each) so the JSON stays concise and valid.
6. Output ONLY valid compact JSON matching:
{"topic":"...","summary":"...","participants":[{"id":"...","role_in_topic":"..."}],"notable_quotes":["..."],"candidate_facts":[{"member_id":"...","fact":"...","serious":true}],"candidate_jokes":[{"member_id":"...","joke":"..."}],"member_styles":[{"member_id":"...","style":"..."}],"candidate_lore":[{"term":"...","meaning":"..."}]}`

	resp, err := s.hub.Chat(ctx, role, infer.ChatRequest{
		Messages: []infer.ChatMessage{
			{Role: "system", Content: sysPrompt},
			{Role: "user", Content: transcript},
		},
		MaxTokens:          2048,
		ResponseFormat:     map[string]any{"type": "json_object"},
		ChatTemplateKwargs: map[string]any{"enable_thinking": false},
	})
	if err != nil {
		return nil, err
	}
	var ext EpisodeExtraction
	rawJSON := extractJSONObject(resp.Content)
	if err := json.Unmarshal([]byte(rawJSON), &ext); err != nil {
		// Graceful fallback to plain summary if model returned non-JSON.
		ext.Topic = "Conversation"
		ext.Summary = truncateText(strings.TrimSpace(resp.Content), 300)
	}
	if ext.Topic == "" {
		ext.Topic = "Conversation"
	}

	ep := &model.Episode{
		ChannelID:    ch,
		StartSeq:     win[0].Seq,
		EndSeq:       win[len(win)-1].Seq,
		StartTS:      win[0].CreatedAt,
		EndTS:        win[len(win)-1].CreatedAt,
		Topic:        ext.Topic,
		Summary:      ext.Summary,
		Participants: ext.Participants,
		Notable:      ext.NotableQuotes,
		CreatedAt:    time.Now(),
	}

	var embedBlob []byte
	var rawVec []float32
	if s.hub != nil {
		if embs, err := s.hub.Embed(ctx, []string{ep.Topic + "\n" + ep.Summary}); err == nil && len(embs) == 1 && len(embs[0]) == s.dim {
			rawVec = embs[0]
			vec.NormalizeInPlace(rawVec)
			embedBlob = vec.EncodeFloat16(rawVec)
		}
	}
	if err := s.store.SaveEpisode(ctx, ep, embedBlob); err != nil {
		return nil, err
	}
	if len(rawVec) == s.dim {
		s.mu.Lock()
		key := episodeKey(ep.ID)
		s.epChannels[key] = ch
		s.epIndex.Upsert(key, rawVec)
		s.mu.Unlock()
	}

	// Identify bot self IDs so the bot never pollutes member profiles with its own persona/history.
	botIDs := make(map[string]bool)
	if _, botID, _, _, ok := model.ParseChannelID(ch); ok && botID != "" {
		botIDs[botID] = true
	}
	for _, m := range win {
		if m.FromBot && m.SenderID != "" {
			botIDs[m.SenderID] = true
		}
	}

	// Merge L2 Candidate Facts & Jokes with anti-sarcasm corroboration (>=2 distinct episodes).
	for _, cf := range ext.CandidateFacts {
		if cf.MemberID == "" || botIDs[cf.MemberID] || strings.TrimSpace(cf.Fact) == "" {
			continue
		}
		_ = s.MergeCandidateFact(ctx, ch, cf.MemberID, cf.Fact, ep.ID, cf.Serious)
	}
	for _, cj := range ext.CandidateJokes {
		if cj.MemberID == "" || botIDs[cj.MemberID] || strings.TrimSpace(cj.Joke) == "" {
			continue
		}
		_ = s.AddProfileJoke(ctx, ch, cj.MemberID, cj.Joke, ep.ID)
	}
	for _, ms := range ext.MemberStyles {
		if ms.MemberID == "" || botIDs[ms.MemberID] || strings.TrimSpace(ms.Style) == "" {
			continue
		}
		_ = s.UpdateProfileStyle(ctx, ch, ms.MemberID, ms.Style)
	}
	for _, cl := range ext.CandidateLore {
		if strings.TrimSpace(cl.Term) == "" || strings.TrimSpace(cl.Meaning) == "" {
			continue
		}
		_ = s.store.UpsertLore(ctx, model.LoreEntry{
			ChannelID:  ch,
			Term:       strings.TrimSpace(cl.Term),
			Meaning:    strings.TrimSpace(cl.Meaning),
			Confidence: 0.6,
		})
	}
	return ep, nil
}

// MergeCandidateFact enforces SPEC 8 anti-sarcasm corroboration:
// a fact from a single episode remains tentative until corroborated by >=2 distinct episodes.
func (s *Service) MergeCandidateFact(ctx context.Context, ch model.ChannelID, memberID, factText string, episodeID int64, serious bool) error {
	s.profileMu.Lock()
	defer s.profileMu.Unlock()

	p, err := s.store.GetProfile(ctx, ch, memberID)
	if err != nil {
		return err
	}
	norm := strings.ToLower(strings.TrimSpace(factText))
	now := time.Now().UnixMilli()
	matched := false
	for i := range p.Facts {
		if strings.ToLower(strings.TrimSpace(p.Facts[i].Text)) == norm ||
			strings.Contains(strings.ToLower(p.Facts[i].Text), norm) ||
			strings.Contains(norm, strings.ToLower(p.Facts[i].Text)) {
			matched = true
			if !containsInt64(p.Facts[i].EvidenceEpisodes, episodeID) {
				p.Facts[i].EvidenceEpisodes = append(p.Facts[i].EvidenceEpisodes, episodeID)
			}
			if len(p.Facts[i].EvidenceEpisodes) >= 2 {
				p.Facts[i].Tentative = false
				p.Facts[i].Confidence = minFloat(0.95, 0.55+0.15*float64(len(p.Facts[i].EvidenceEpisodes)))
			}
			p.Facts[i].UpdatedAt = now
			break
		}
	}
	if !matched {
		conf := 0.35
		if serious {
			conf = 0.45
		}
		p.Facts = append(p.Facts, model.ProfileFact{
			Text:             strings.TrimSpace(factText),
			Confidence:       conf,
			EvidenceEpisodes: []int64{episodeID},
			Tentative:        true, // Requires >=2 distinct episodes to graduate!
			UpdatedAt:        now,
		})
	}
	p.Facts = CompactProfileFacts(p.Facts, now)
	return s.store.SaveProfile(ctx, p)
}

const (
	maxConfirmedProfileFacts = 20
	maxTentativeProfileFacts = 15
	tentativeExpireDays      = 60.0
	factDecayHalfLifeDays    = 90.0
)

func decayedFactScore(f model.ProfileFact, nowMs int64) float64 {
	if f.UpdatedAt <= 0 || nowMs <= f.UpdatedAt {
		return f.Confidence
	}
	ageDays := float64(nowMs-f.UpdatedAt) / (24 * 3600 * 1000)
	return f.Confidence * math.Exp(-math.Ln2*ageDays/factDecayHalfLifeDays)
}

// CompactProfileFacts applies time decay, expires uncorroborated tentative facts older than 60d,
// and caps stored facts per member (<=20 confirmed, <=15 tentative) so L2 profiles never bloat unboundedly.
func CompactProfileFacts(facts []model.ProfileFact, nowMs int64) []model.ProfileFact {
	var confirmed, tentative []model.ProfileFact
	for _, f := range facts {
		if strings.TrimSpace(f.Text) == "" {
			continue
		}
		if f.Tentative {
			if f.UpdatedAt > 0 && nowMs > f.UpdatedAt {
				ageDays := float64(nowMs-f.UpdatedAt) / (24 * 3600 * 1000)
				if ageDays > tentativeExpireDays {
					continue // Expire stale single-episode tentative claim
				}
			}
			tentative = append(tentative, f)
		} else {
			confirmed = append(confirmed, f)
		}
	}

	sort.Slice(confirmed, func(i, j int) bool {
		si := decayedFactScore(confirmed[i], nowMs)
		sj := decayedFactScore(confirmed[j], nowMs)
		if si == sj {
			return confirmed[i].UpdatedAt > confirmed[j].UpdatedAt
		}
		return si > sj
	})
	if len(confirmed) > maxConfirmedProfileFacts {
		confirmed = confirmed[:maxConfirmedProfileFacts]
	}

	sort.Slice(tentative, func(i, j int) bool {
		return tentative[i].UpdatedAt > tentative[j].UpdatedAt
	})
	if len(tentative) > maxTentativeProfileFacts {
		tentative = tentative[:maxTentativeProfileFacts]
	}

	out := make([]model.ProfileFact, 0, len(confirmed)+len(tentative))
	out = append(out, confirmed...)
	out = append(out, tentative...)
	return out
}

// DistillIncremental runs a lightweight bounded incremental distillation pass (<=5 new episodes,
// concurrency=1) for a single channel inside the live serve process, updating RAM vectors immediately.
func (s *Service) DistillIncremental(ctx context.Context, ch model.ChannelID) (int, error) {
	return s.DistillChannel(ctx, DistillOptions{
		ChannelID:   ch,
		MaxEpisodes: 5,
		Concurrency: 1,
		Role:        "distill",
	})
}

// ReloadIndex refreshes the in-memory float16 episode index from SQLite (picks up external CLI distill runs).
func (s *Service) ReloadIndex(ctx context.Context) (int, error) {
	return s.LoadIndex(ctx)
}

func (s *Service) AddProfileJoke(ctx context.Context, ch model.ChannelID, memberID, jokeText string, episodeID int64) error {
	s.profileMu.Lock()
	defer s.profileMu.Unlock()

	p, err := s.store.GetProfile(ctx, ch, memberID)
	if err != nil {
		return err
	}
	p.Jokes = append(p.Jokes, model.ProfileJoke{
		Text:      strings.TrimSpace(jokeText),
		EpisodeID: episodeID,
		UpdatedAt: time.Now().UnixMilli(),
	})
	if len(p.Jokes) > 12 {
		p.Jokes = p.Jokes[len(p.Jokes)-12:]
	}
	return s.store.SaveProfile(ctx, p)
}

// UpdateProfileStyle updates the concise interaction vibe note for a channel member.
func (s *Service) UpdateProfileStyle(ctx context.Context, ch model.ChannelID, memberID, style string) error {
	s.profileMu.Lock()
	defer s.profileMu.Unlock()

	p, err := s.store.GetProfile(ctx, ch, memberID)
	if err != nil {
		return err
	}
	p.StyleNote = truncateText(strings.TrimSpace(style), 24)
	return s.store.SaveProfile(ctx, p)
}

// ReindexEmbeddings embeds all episodes lacking float16 embeddings and reloads RAM index.
func (s *Service) ReindexEmbeddings(ctx context.Context) (int, error) {
	if s.hub == nil {
		return 0, fmt.Errorf("memory: inference hub not configured")
	}
	eps, blobs, err := s.store.AllEpisodesWithEmbeds(ctx)
	if err != nil {
		return 0, err
	}
	var toEmbed []model.Episode
	var texts []string
	for i, ep := range eps {
		if len(blobs[i]) != s.dim*2 {
			toEmbed = append(toEmbed, ep)
			texts = append(texts, ep.Topic+"\n"+ep.Summary)
		}
	}
	if len(toEmbed) > 0 {
		embs, err := s.hub.Embed(ctx, texts)
		if err != nil {
			return 0, err
		}
		for i, ep := range toEmbed {
			if i < len(embs) && len(embs[i]) == s.dim {
				vec.NormalizeInPlace(embs[i])
				_ = s.store.UpdateEpisodeEmbed(ctx, ep.ID, vec.EncodeFloat16(embs[i]))
			}
		}
	}
	return s.LoadIndex(ctx)
}

func episodeKey(id int64) string { return "ep:" + strconv.FormatInt(id, 10) }

func parseEpisodeKey(k string) (int64, error) {
	return strconv.ParseInt(strings.TrimPrefix(k, "ep:"), 10, 64)
}

func formatMessages(msgs []model.Message) string {
	lines := make([]string, 0, len(msgs))
	for _, m := range msgs {
		txt := m.PlainText()
		if txt == "" {
			continue
		}
		lines = append(lines, fmt.Sprintf("[%s] %s(%s): %s", m.CreatedAt.Format("2006-01-02 15:04"), m.SenderName, m.SenderID, txt))
	}
	return strings.Join(lines, "\n")
}

func extractJSONObject(s string) string {
	start := strings.IndexByte(s, '{')
	end := strings.LastIndexByte(s, '}')
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}

func containsInt64(xs []int64, v int64) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func truncateText(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "..."
}
