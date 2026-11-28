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
	"unicode"

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

	mu            sync.RWMutex
	epIndices     map[model.ChannelID]*vec.FlatF16Index // per-channel compact float16 index (zero cross-channel scan)
	epLoadedCnt   int
	epLoadedMaxID int64
	ignoredBots   map[string]struct{}
	profileShards [64]sync.Mutex // Striped per-(channel,member) locks for L2 profile RMW
}

func (s *Service) profileLock(ch model.ChannelID, memberID string) *sync.Mutex {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(ch); i++ {
		h ^= uint64(ch[i])
		h *= 1099511628211
	}
	h ^= 0xff
	h *= 1099511628211
	for i := 0; i < len(memberID); i++ {
		h ^= uint64(memberID[i])
		h *= 1099511628211
	}
	return &s.profileShards[h%uint64(len(s.profileShards))]
}

func NewService(st *store.Store, hub *infer.Hub, dim int, logger *slog.Logger) *Service {
	if dim <= 0 {
		dim = 2048
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		store:       st,
		hub:         hub,
		log:         logger,
		dim:         dim,
		epIndices:   make(map[model.ChannelID]*vec.FlatF16Index),
		ignoredBots: make(map[string]struct{}),
	}
}

// SetIgnoredBots configures external bot sender IDs excluded from L2 profile/lore distillation.
func (s *Service) SetIgnoredBots(ids []string) {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if t := strings.TrimSpace(id); t != "" {
			m[t] = struct{}{}
		}
	}
	s.mu.Lock()
	s.ignoredBots = m
	s.mu.Unlock()
}

func (s *Service) ignoredBotsCopy() map[string]bool {
	s.mu.RLock()
	out := make(map[string]bool, len(s.ignoredBots))
	for id := range s.ignoredBots {
		out[id] = true
	}
	s.mu.RUnlock()
	return out
}

// LoadIndex streams resident float16 episode vectors from SQLite directly into per-channel RAM
// indices outside the write lock (avoiding full Episode JSON decoding and 2x peak RAM),
// then swaps pointers in O(1) so concurrent Recall queries never stall.
func (s *Service) LoadIndex(ctx context.Context) (int, error) {
	newIndices := make(map[model.ChannelID]*vec.FlatF16Index)
	loaded := 0
	var maxID int64
	_, err := s.store.StreamEpisodeEmbeds(ctx, func(id int64, ch model.ChannelID, blob []byte) {
		if len(blob) != s.dim*2 {
			return
		}
		idx := newIndices[ch]
		if idx == nil {
			idx = vec.NewFlatF16Index(s.dim, 0.0)
			newIndices[ch] = idx
		}
		if idx.UpsertPreencoded(episodeKey(id), blob) {
			loaded++
			if id > maxID {
				maxID = id
			}
		}
	})
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.epIndices = newIndices
	s.epLoadedCnt = loaded
	s.epLoadedMaxID = maxID
	s.mu.Unlock()
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

	var (
		rawLore  []model.LoreEntry
		notes    []model.Note
		names    map[string]string
		profiles []model.MemberProfile
		recentEp []model.Episode
		digests  []model.DailyDigest
	)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		rawLore, _ = s.store.ListLore(ctx, ch, 30)
	}()
	go func() {
		defer wg.Done()
		notes, _ = s.store.ListNotes(ctx, ch, true)
	}()
	go func() {
		defer wg.Done()
		names, _ = s.store.GetMemberNames(ctx, ch)
	}()
	go func() {
		defer wg.Done()
		profiles, _ = s.store.ListProfiles(ctx, ch, 30)
	}()
	go func() {
		defer wg.Done()
		if eps, err := s.store.RecentEpisodesSince(ctx, ch, now.Add(-48*time.Hour), 3); err == nil && len(eps) > 0 {
			recentEp = eps
		} else if ds, err := s.store.ListDailyDigests(ctx, ch, 1); err == nil {
			digests = ds
		}
	}()
	wg.Wait()

	var lore, fallbackLore []model.LoreEntry
	for _, l := range rawLore {
		if l.Confidence >= 0.55 {
			lore = append(lore, l)
		} else {
			fallbackLore = append(fallbackLore, l)
		}
	}
	if len(lore) < 5 {
		for _, l := range fallbackLore {
			lore = append(lore, l)
			if len(lore) >= 15 {
				break
			}
		}
	}
	if len(lore) > 15 {
		lore = lore[:15]
	}

	ignored := s.ignoredBotsCopy()
	memberSet := make(map[string]struct{}, len(activeMemberIDs))
	for _, id := range activeMemberIDs {
		if !ignored[id] {
			memberSet[id] = struct{}{}
		}
	}
	actIDs := make([]string, 0, len(profiles))
	for _, p := range profiles {
		if ignored[p.MemberID] {
			continue
		}
		if len(memberSet) > 0 {
			if _, ok := memberSet[p.MemberID]; !ok {
				continue
			}
		}
		actIDs = append(actIDs, p.MemberID)
	}
	actCounts, _ := s.store.MemberActivityCounts(ctx, ch, actIDs, now.AddDate(0, 0, -30))
	var cards []string
	for _, p := range profiles {
		if ignored[p.MemberID] {
			continue
		}
		if len(memberSet) > 0 {
			if _, ok := memberSet[p.MemberID]; !ok {
				continue
			}
		}
		if card := FormatMemberCardWithFamiliarity(p, names[p.MemberID], familiarityLabel(actCounts[p.MemberID])); card != "" {
			cards = append(cards, card)
		}
	}

	var highlights []string
	if len(recentEp) > 0 {
		for _, ep := range recentEp {
			ts := ep.EndTS.In(loc).Format("01-02 15:04")
			highlights = append(highlights, fmt.Sprintf("- [%s] %s: %s", ts, ep.Topic, truncateText(ep.Summary, 90)))
		}
	} else if len(digests) > 0 {
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
	return FormatMemberCardWithFamiliarity(p, displayName, "")
}

// FormatMemberCardWithFamiliarity extends FormatMemberCard with an optional activity-based familiarity tag.
func FormatMemberCardWithFamiliarity(p model.MemberProfile, displayName, familiarity string) string {
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
	fam := strings.TrimSpace(familiarity)
	if len(facts) == 0 && len(p.Jokes) == 0 && style == "" && fam == "" {
		return ""
	}
	label := p.MemberID
	if displayName != "" && displayName != p.MemberID {
		label = fmt.Sprintf("%s(%s)", displayName, p.MemberID)
	}
	parts := make([]string, 0, 4)
	if fam != "" {
		parts = append(parts, fam)
	}
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

func familiarityLabel(msgCount30d int) string {
	switch {
	case msgCount30d >= 80:
		return "熟人常客"
	case msgCount30d >= 20:
		return "活跃群友"
	case msgCount30d > 0 && msgCount30d <= 3:
		return "新面孔"
	default:
		return ""
	}
}

// MemberCardDelta returns a compact one-time context line for a newly appearing mid-segment member.
func (s *Service) MemberCardDelta(ctx context.Context, ch model.ChannelID, memberID, displayName string) string {
	if s.ignoredBotsCopy()[memberID] {
		return ""
	}
	p, err := s.store.GetProfile(ctx, ch, memberID)
	if err != nil || p == nil {
		return ""
	}
	counts, _ := s.store.MemberActivityCounts(ctx, ch, []string{memberID}, time.Now().AddDate(0, 0, -30))
	return FormatMemberCardWithFamiliarity(*p, displayName, familiarityLabel(counts[memberID]))
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
	// Run all independent retrieval legs (FTS5, Embed+Vec, Profiles, Lore, Notes, Digests) in parallel.
	var (
		ftsEps       []model.Episode
		vecHits      []vec.Hit
		profileLines []string
		lore         []model.LoreEntry
		notes        []model.Note
		digests      []model.DailyDigest
	)

	s.mu.RLock()
	chIdx := s.epIndices[opts.ChannelID]
	hasEpVecs := chIdx != nil && chIdx.Len() > 0
	s.mu.RUnlock()

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		ftsEps, _ = s.store.SearchEpisodesFTS(ctx, opts.ChannelID, opts.Query, 20)
	}()
	go func() {
		defer wg.Done()
		lore, _ = s.store.ListLore(ctx, opts.ChannelID, 10)
	}()
	go func() {
		defer wg.Done()
		notes, _ = s.store.ListNotes(ctx, opts.ChannelID, false)
	}()
	go func() {
		defer wg.Done()
		digests, _ = s.store.ListDailyDigests(ctx, opts.ChannelID, 3)
	}()
	if s.hub != nil && strings.TrimSpace(opts.Query) != "" && hasEpVecs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if embs, err := s.hub.Embed(ctx, []string{opts.Query}); err == nil && len(embs) == 1 && len(embs[0]) == s.dim {
				s.mu.RLock()
				idx := s.epIndices[opts.ChannelID]
				s.mu.RUnlock()
				if idx != nil {
					vecHits = idx.Search(embs[0], 20, nil)
				}
			}
		}()
	}
	if strings.TrimSpace(opts.Sender) != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids, _ := s.store.ResolveMembersByAlias(ctx, opts.ChannelID, opts.Sender)
			names, _ := s.store.GetMemberNames(ctx, opts.ChannelID)
			for _, id := range ids {
				if p, err := s.store.GetProfile(ctx, opts.ChannelID, id); err == nil && p != nil {
					if card := FormatMemberCard(*p, names[id]); card != "" {
						profileLines = append(profileLines, card)
					}
				}
			}
		}()
	}
	wg.Wait()

	var sections []string

	// 1. Episode hybrid recall: FTS5 + Vector -> RRF -> batch hydrate -> optional Rerank.
	ftsHits := make([]vec.Hit, 0, len(ftsEps))
	epMap := make(map[string]model.Episode, len(ftsEps))
	for i, ep := range ftsEps {
		k := episodeKey(ep.ID)
		epMap[k] = ep
		ftsHits = append(ftsHits, vec.Hit{ID: k, Score: float32(20 - i)})
	}

	fused := vec.WeightedRRF([][]vec.Hit{ftsHits, vecHits}, []float32{1.0, 1.0}, 60, 15)
	var missingIDs []int64
	for _, h := range fused {
		if _, ok := epMap[h.ID]; !ok {
			if id, err := parseEpisodeKey(h.ID); err == nil {
				missingIDs = append(missingIDs, id)
			}
		}
	}
	if len(missingIDs) > 0 {
		if fetched, err := s.store.GetEpisodesByIDs(ctx, opts.ChannelID, missingIDs); err == nil {
			for id, ep := range fetched {
				epMap[episodeKey(id)] = ep
			}
		}
	}

	candidates := make([]model.Episode, 0, len(fused))
	for _, h := range fused {
		ep, ok := epMap[h.ID]
		if !ok {
			continue
		}
		if opts.SinceMs > 0 && ep.EndTS.UnixMilli() < opts.SinceMs {
			continue
		}
		if opts.UntilMs > 0 && ep.StartTS.UnixMilli() > opts.UntilMs {
			continue
		}
		candidates = append(candidates, ep)
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

	// 2. Relevant L2 Profiles (pre-fetched in parallel).
	if len(profileLines) > 0 {
		sections = append(sections, "[Member Profiles]\n"+strings.Join(profileLines, "\n"))
	}

	// 3. Lore & Pinned Notes & Recent Digests (pre-fetched in parallel).
	if len(lore) > 0 {
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

	if len(notes) > 0 {
		var lines []string
		for _, n := range notes {
			lines = append(lines, fmt.Sprintf("- %s: %s", n.Key, n.Content))
		}
		sections = append(sections, "[Notes]\n"+strings.Join(lines, "\n"))
	}

	if len(digests) > 0 {
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
	// Identify bot self IDs and configured ignored_bots so bots never pollute member profiles or lore.
	botIDs := s.ignoredBotsCopy()
	if _, botID, _, _, ok := model.ParseChannelID(ch); ok && botID != "" {
		botIDs[botID] = true
	}
	for _, m := range win {
		if m.FromBot && m.SenderID != "" {
			botIDs[m.SenderID] = true
		}
	}

	// Exclude noisy external bots (e.g. quote bots) from distill transcript when human messages exist.
	distillMsgs := win
	if len(botIDs) > 0 {
		var filtered []model.Message
		for _, m := range win {
			if !m.FromBot && botIDs[m.SenderID] {
				continue
			}
			filtered = append(filtered, m)
		}
		if len(filtered) >= 3 {
			distillMsgs = filtered
		}
	}
	transcript := formatMessages(distillMsgs)
	sysPrompt := `Extract a concise structured JSON summary of this chat episode STRICTLY in Simplified Chinese (keep English only for proper acronyms/tickers).
Rules:
1. Key every participant strictly by their immutable member_id shown in parentheses, NEVER by nickname.
2. candidate_facts: ONLY enduring personal traits, job/school, base city, hobbies, recurring habits, or concrete travel/venue preferences & real experiences (e.g. "常驻杭州", "持有希尔顿钻卡", "去过某温泉/酒店并给出具体好坏评价"). NEVER record one-off stock/crypto prices, transient news headlines, single trade orders, bot commands, or sarcasm/jokes.
3. In summary: when members discuss trips, hotels, restaurants, airlines, visas, or tax-refund pitfalls, keep concrete destination names, specific hotel/shop names, approximate prices, and real pros/cons or pitfall warnings so future travel recall can reuse them.
4. Separate jokes/sarcasm/shitposting into candidate_jokes or candidate_lore; NEVER record sarcasm as candidate_facts.
5. Never extract candidate_facts, candidate_jokes, or member_styles for the bot itself or other bots.
6. candidate_lore: ONLY genuine recurring group slang, inside jokes, or nicknames (at most 3 items). NEVER record standalone numbers/prices, ordinary dictionary words, or generic product/library names.
7. For member_styles, capture brief interaction vibe with the group/bot when clear (e.g. "爱互怼的熟人", "爱聊技术的老哥", "话少但爱发图"), at most 12 chars.
8. Keep arrays compact (at most 5 items each) so the JSON stays concise and valid.
9. Keep all summaries, facts, jokes, and lore SFW and neutral: if chat contains explicit sexual, vulgar, or politically sensitive details, abstract them neutrally in Chinese (e.g. "成人玩笑", "敏感话题调侃") and NEVER copy explicit sexual/vulgar/political terms verbatim into lore, summary, or facts.
9. Output ONLY valid compact JSON matching:
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
		idx := s.epIndices[ch]
		if idx == nil {
			idx = vec.NewFlatF16Index(s.dim, 0.0)
			s.epIndices[ch] = idx
		}
		idx.Upsert(episodeKey(ep.ID), rawVec)
		s.epLoadedCnt++
		if ep.ID > s.epLoadedMaxID {
			s.epLoadedMaxID = ep.ID
		}
		s.mu.Unlock()
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
		term := strings.TrimSpace(cl.Term)
		meaning := strings.TrimSpace(cl.Meaning)
		if !isValidLoreTerm(term) || meaning == "" {
			continue
		}
		_ = s.store.UpsertLore(ctx, model.LoreEntry{
			ChannelID:  ch,
			Term:       term,
			Meaning:    meaning,
			Confidence: 0.45, // Starts tentative; corroborated episodes increment +0.15 up to 0.95.
		})
	}
	return ep, nil
}

// MergeCandidateFact enforces SPEC 8 anti-sarcasm corroboration:
// a fact from a single episode remains tentative until corroborated by >=2 distinct episodes.
// Uses per-(channel, member) striped locks and executes any network Embed lookup OUTSIDE the lock
// so concurrent distill workers never serialize behind a global mutex.
func (s *Service) MergeCandidateFact(ctx context.Context, ch model.ChannelID, memberID, factText string, episodeID int64, serious bool) error {
	lk := s.profileLock(ch, memberID)
	trimmed := strings.TrimSpace(factText)
	norm := strings.ToLower(trimmed)

	findLexical := func(facts []model.ProfileFact) int {
		for i := range facts {
			existing := strings.ToLower(strings.TrimSpace(facts[i].Text))
			if existing == norm || strings.Contains(existing, norm) || strings.Contains(norm, existing) || factBigramDice(existing, norm) >= 0.45 {
				return i
			}
		}
		return -1
	}

	// Phase 1: check under shard lock; only unlock if semantic embedding is needed.
	lk.Lock()
	p, err := s.store.GetProfile(ctx, ch, memberID)
	if err != nil {
		lk.Unlock()
		return err
	}
	matchIdx := findLexical(p.Facts)
	var semanticMatchedText string
	if matchIdx < 0 && s.hub != nil && len(p.Facts) > 0 {
		snapTexts := make([]string, len(p.Facts))
		for i, f := range p.Facts {
			snapTexts[i] = strings.TrimSpace(f.Text)
		}
		lk.Unlock()

		// Run remote embedding completely lock-free.
		texts := make([]string, 0, len(snapTexts)+1)
		texts = append(texts, trimmed)
		texts = append(texts, snapTexts...)
		embCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		embs, eerr := s.hub.Embed(embCtx, texts)
		cancel()
		if eerr == nil && len(embs) == len(texts) {
			bestSim := 0.0
			for i, txt := range snapTexts {
				if sim := cosineSim(embs[0], embs[i+1]); sim > bestSim {
					bestSim = sim
					if sim >= 0.78 {
						semanticMatchedText = txt
					}
				}
			}
		}

		// Re-acquire shard lock and re-read fresh profile to preserve strict RMW consistency.
		lk.Lock()
		p, err = s.store.GetProfile(ctx, ch, memberID)
		if err != nil {
			lk.Unlock()
			return err
		}
		matchIdx = findLexical(p.Facts)
		if matchIdx < 0 && semanticMatchedText != "" {
			for i := range p.Facts {
				if strings.TrimSpace(p.Facts[i].Text) == semanticMatchedText {
					matchIdx = i
					break
				}
			}
		}
	}
	defer lk.Unlock()

	now := time.Now().UnixMilli()
	if matchIdx >= 0 {
		if !containsInt64(p.Facts[matchIdx].EvidenceEpisodes, episodeID) {
			p.Facts[matchIdx].EvidenceEpisodes = append(p.Facts[matchIdx].EvidenceEpisodes, episodeID)
		}
		if len(p.Facts[matchIdx].EvidenceEpisodes) >= 2 {
			p.Facts[matchIdx].Tentative = false
			p.Facts[matchIdx].Confidence = minFloat(0.95, 0.55+0.15*float64(len(p.Facts[matchIdx].EvidenceEpisodes)))
		}
		p.Facts[matchIdx].UpdatedAt = now
	} else {
		conf := 0.35
		if serious {
			conf = 0.45
		}
		p.Facts = append(p.Facts, model.ProfileFact{
			Text:             trimmed,
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

// DistillIncremental runs a lightweight bounded incremental distillation pass (<=5 new episodes
// from the last 30 days, concurrency=1) for a single channel inside the live serve process,
// updating RAM vectors immediately without wasting GPU chewing months-old dormant channels.
func (s *Service) DistillIncremental(ctx context.Context, ch model.ChannelID) (int, error) {
	return s.DistillChannel(ctx, DistillOptions{
		ChannelID:   ch,
		MaxEpisodes: 5,
		Concurrency: 1,
		Role:        "distill",
		SinceDays:   30,
	})
}

func isValidLoreTerm(term string) bool {
	r := []rune(strings.TrimSpace(term))
	if len(r) < 2 || len(r) > 24 {
		return false
	}
	hasLetterOrHan := false
	for _, ch := range r {
		if unicode.IsLetter(ch) {
			hasLetterOrHan = true
			break
		}
	}
	return hasLetterOrHan
}

// ReloadIndex refreshes the in-memory float16 episode index from SQLite (picks up external CLI distill runs),
// skipping the full stream in O(1) when episode count and max ID are unchanged.
func (s *Service) ReloadIndex(ctx context.Context) (int, error) {
	cnt, maxID, err := s.store.EpisodeEmbedStats(ctx)
	if err == nil {
		s.mu.RLock()
		same := cnt == s.epLoadedCnt && maxID == s.epLoadedMaxID
		curCnt := s.epLoadedCnt
		s.mu.RUnlock()
		if same {
			return curCnt, nil
		}
	}
	return s.LoadIndex(ctx)
}

func (s *Service) AddProfileJoke(ctx context.Context, ch model.ChannelID, memberID, jokeText string, episodeID int64) error {
	lk := s.profileLock(ch, memberID)
	lk.Lock()
	defer lk.Unlock()

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
	lk := s.profileLock(ch, memberID)
	lk.Lock()
	defer lk.Unlock()

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

func factBigramDice(a, b string) float64 {
	ba := charBigrams(a)
	bb := charBigrams(b)
	if len(ba) == 0 || len(bb) == 0 {
		return 0
	}
	inter := 0
	for k := range ba {
		if _, ok := bb[k]; ok {
			inter++
		}
	}
	return float64(2*inter) / float64(len(ba)+len(bb))
}

func charBigrams(s string) map[string]struct{} {
	var runes []rune
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			runes = append(runes, r)
		}
	}
	if len(runes) < 2 {
		return nil
	}
	out := make(map[string]struct{}, len(runes)-1)
	for i := 0; i < len(runes)-1; i++ {
		out[string(runes[i:i+2])] = struct{}{}
	}
	return out
}

func cosineSim(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		fa, fb := float64(a[i]), float64(b[i])
		dot += fa * fb
		na += fa * fa
		nb += fb * fb
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
