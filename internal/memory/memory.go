package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/iMMIQ/fatfish/internal/frame"
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
func (s *Service) BuildSnapshot(ctx context.Context, ch model.ChannelID, activeMemberIDs []string) (frame.SnapshotInput, error) {
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
	return frame.SnapshotInput{
		Lore:        lore,
		PinnedNotes: notes,
		MemberCards: cards,
	}, nil
}

// FormatMemberCard formats corroborated (non-tentative) facts and jokes cleanly for prompt injection.
func FormatMemberCard(p model.MemberProfile, displayName string) string {
	var facts []string
	for _, f := range p.Facts {
		if !f.Tentative && f.Confidence >= 0.5 && strings.TrimSpace(f.Text) != "" {
			facts = append(facts, f.Text)
		}
		if len(facts) >= 5 {
			break
		}
	}
	if len(facts) == 0 && len(p.Jokes) == 0 {
		return ""
	}
	label := p.MemberID
	if displayName != "" && displayName != p.MemberID {
		label = fmt.Sprintf("%s(%s)", displayName, p.MemberID)
	}
	parts := make([]string, 0, 2)
	if len(facts) > 0 {
		parts = append(parts, strings.Join(facts, "; "))
	}
	if len(p.Jokes) > 0 {
		parts = append(parts, "memes: "+p.Jokes[len(p.Jokes)-1].Text)
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
	if s.hub != nil && strings.TrimSpace(opts.Query) != "" && s.epIndex.Len() > 0 {
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
		if ep, ok := epMap[h.ID]; ok {
			candidates = append(candidates, ep)
			continue
		}
		if id, err := parseEpisodeKey(h.ID); err == nil {
			if ep, err := s.store.GetEpisodeByID(ctx, id); err == nil && ep.ChannelID == opts.ChannelID {
				candidates = append(candidates, *ep)
			}
		}
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
	CandidateLore []struct {
		Term    string `json:"term"`
		Meaning string `json:"meaning"`
	} `json:"candidate_lore"`
}

// PartitionWindows groups messages into meaningful conversational episodes separated by >maxGap silence
// or >=maxMsgs valid messages, skipping noisy short fragments (<minMsgs).
func PartitionWindows(msgs []model.Message, maxGap time.Duration, maxMsgs, minMsgs int) [][]model.Message {
	if maxGap <= 0 {
		maxGap = 30 * time.Minute
	}
	if maxMsgs <= 0 {
		maxMsgs = 150
	}
	if minMsgs <= 0 {
		minMsgs = 8
	}
	var windows [][]model.Message
	var cur []model.Message
	for _, m := range msgs {
		txt := strings.TrimSpace(m.PlainText())
		// Filter out single-char/pure-noise lines unless carrying media.
		if len([]rune(txt)) <= 1 && len(m.Parts) == 1 && m.Parts[0].Kind == model.PartText {
			continue
		}
		if len(cur) > 0 {
			gap := m.CreatedAt.Sub(cur[len(cur)-1].CreatedAt)
			if gap > maxGap || len(cur) >= maxMsgs {
				if len(cur) >= minMsgs {
					windows = append(windows, cur)
				}
				cur = nil
			}
		}
		cur = append(cur, m)
	}
	if len(cur) >= minMsgs {
		windows = append(windows, cur)
	}
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
	msgs, err := s.store.MessagesAfterSeq(ctx, ch, afterSeq, 100000)
	if err != nil {
		return 0, err
	}
	if opts.SinceDays > 0 {
		cutoff := time.Now().AddDate(0, 0, -opts.SinceDays)
		filtered := msgs[:0]
		for _, m := range msgs {
			if !m.CreatedAt.Before(cutoff) {
				filtered = append(filtered, m)
			}
		}
		msgs = filtered
	}
	windows := PartitionWindows(msgs, 30*time.Minute, 150, 8)
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
	var maxSeqDone int64 = afterSeq

	for idx, w := range windows {
		win := w
		winNo := idx + 1
		totalWins := len(windows)
		g.Go(func() error {
			ep, err := s.DistillWindow(gctx, ch, win, role)
			if err != nil {
				s.log.Warn("distill window failed", "channel", ch, "window", winNo, "err", err)
				return nil
			}
			mu.Lock()
			created++
			if ep.EndSeq > maxSeqDone {
				maxSeqDone = ep.EndSeq
			}
			if created%10 == 0 || winNo == totalWins {
				_ = s.store.SetDistillCheckpoint(ctx, ch, maxSeqDone)
				s.log.Info("distill progress", "channel", ch, "done", created, "total_windows", totalWins, "checkpoint_seq", maxSeqDone)
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return created, err
	}
	if maxSeqDone > afterSeq {
		_ = s.store.SetDistillCheckpoint(ctx, ch, maxSeqDone)
	}
	return created, nil
}

// DistillWindow summarizes a single message window into an L1 Episode and updates L2 profiles/lore
// using anti-sarcasm corroboration (>=2 distinct episodes before promoting tentative facts).
func (s *Service) DistillWindow(ctx context.Context, ch model.ChannelID, win []model.Message, role string) (*model.Episode, error) {
	if len(win) == 0 {
		return nil, fmt.Errorf("memory: empty window")
	}
	transcript := formatMessages(win)
	sysPrompt := `Extract a concise structured JSON summary of this chat episode in the primary language of the chat (e.g. Simplified Chinese).
Rules:
1. Key every participant strictly by their immutable member_id shown in parentheses, NEVER by nickname.
2. Separate jokes/sarcasm/shitposting into candidate_jokes or candidate_lore; NEVER record sarcasm as candidate_facts.
3. Keep arrays compact (at most 5 items each) so the JSON stays concise and valid.
4. Output ONLY valid compact JSON matching:
{"topic":"...","summary":"...","participants":[{"id":"...","role_in_topic":"..."}],"notable_quotes":["..."],"candidate_facts":[{"member_id":"...","fact":"...","serious":true}],"candidate_jokes":[{"member_id":"...","joke":"..."}],"candidate_lore":[{"term":"...","meaning":"..."}]}`

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

	// Merge L2 Candidate Facts & Jokes with anti-sarcasm corroboration (>=2 distinct episodes).
	for _, cf := range ext.CandidateFacts {
		if cf.MemberID == "" || strings.TrimSpace(cf.Fact) == "" {
			continue
		}
		_ = s.MergeCandidateFact(ctx, ch, cf.MemberID, cf.Fact, ep.ID, cf.Serious)
	}
	for _, cj := range ext.CandidateJokes {
		if cj.MemberID == "" || strings.TrimSpace(cj.Joke) == "" {
			continue
		}
		_ = s.AddProfileJoke(ctx, ch, cj.MemberID, cj.Joke, ep.ID)
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
	return s.store.SaveProfile(ctx, p)
}

func (s *Service) AddProfileJoke(ctx context.Context, ch model.ChannelID, memberID, jokeText string, episodeID int64) error {
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
