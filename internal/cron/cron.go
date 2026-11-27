package cron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	robfigcron "github.com/robfig/cron/v3"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/vec"
)

// Dispatcher delivers SystemTriggerEvent into a Room Actor mailbox.
// internal/cron NEVER calls internal/transport directly (SPEC Section 2 invariant).
type Dispatcher interface {
	DispatchSystem(ev model.SystemTriggerEvent)
}

// MemoryHooks lets the scheduler run lightweight hourly incremental distillation
// and refresh the live RAM episode vector index without introducing package cycles.
type MemoryHooks interface {
	DistillIncremental(ctx context.Context, ch model.ChannelID) (int, error)
	ReloadIndex(ctx context.Context) (int, error)
}

// ChannelFilter restricts background memory maintenance to enabled channels.
type ChannelFilter interface {
	ChannelAllowed(ch model.ChannelID) bool
}

// Scheduler polls persistent SQLite schedules and posts SystemTriggerEvents into Rooms,
// and runs periodic maintenance (hourly incremental memory distill, daily digests, WAL/media cleanup).
type Scheduler struct {
	store          *store.Store
	hub            *infer.Hub
	dispatcher     Dispatcher
	memHooks       MemoryHooks
	chanFilter     ChannelFilter
	loc            *time.Location
	maxPerChannel  int
	minIntervalMin int
	embedDim       int
	log            *slog.Logger
}

// SetMemoryHooks attaches optional in-process incremental memory distillation and channel filtering.
func (s *Scheduler) SetMemoryHooks(hooks MemoryHooks, filter ChannelFilter) {
	s.memHooks = hooks
	s.chanFilter = filter
}

func NewScheduler(st *store.Store, hub *infer.Hub, disp Dispatcher, loc *time.Location, maxPerChannel, minIntervalMin, embedDim int, logger *slog.Logger) *Scheduler {
	if loc == nil {
		loc = time.UTC
	}
	if maxPerChannel <= 0 {
		maxPerChannel = 10
	}
	if minIntervalMin <= 0 {
		minIntervalMin = 5
	}
	if embedDim <= 0 {
		embedDim = 2048
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		store:          st,
		hub:            hub,
		dispatcher:     disp,
		loc:            loc,
		maxPerChannel:  maxPerChannel,
		minIntervalMin: minIntervalMin,
		embedDim:       embedDim,
		log:            logger,
	}
}

var cronParser = robfigcron.NewParser(robfigcron.Minute | robfigcron.Hour | robfigcron.Dom | robfigcron.Month | robfigcron.Dow)

// ValidateCron validates a standard 5-field cron expression and enforces the minimum interval policy (>=5m).
func (s *Scheduler) ValidateCron(expr string) (robfigcron.Schedule, error) {
	sched, err := cronParser.Parse(strings.TrimSpace(expr))
	if err != nil {
		return nil, fmt.Errorf("cron: invalid 5-field expression %q: %w", expr, err)
	}
	now := time.Now().In(s.loc)
	t1 := sched.Next(now)
	t2 := sched.Next(t1)
	minGap := time.Duration(s.minIntervalMin) * time.Minute
	if t2.Sub(t1) < minGap {
		return nil, fmt.Errorf("cron: interval %v is shorter than minimum allowed %v", t2.Sub(t1), minGap)
	}
	return sched, nil
}

// CreateSchedule creates a validated one-shot or recurring schedule for a channel.
func (s *Scheduler) CreateSchedule(ctx context.Context, ch model.ChannelID, creatorID, kind, cronExpr, runAtRFC3339, prompt string) (*model.Schedule, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, errors.New("cron: prompt is required")
	}
	var nextRun time.Time
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "once":
		if strings.TrimSpace(runAtRFC3339) == "" {
			return nil, errors.New("cron: run_at is required for once schedule")
		}
		t, err := parseFlexibleTime(runAtRFC3339, s.loc)
		if err != nil {
			return nil, fmt.Errorf("cron: invalid run_at %q: %w", runAtRFC3339, err)
		}
		if t.Before(time.Now().Add(-10 * time.Second)) {
			return nil, errors.New("cron: run_at must be in the future")
		}
		nextRun = t
		kind = "once"
		cronExpr = ""
	case "cron":
		sched, err := s.ValidateCron(cronExpr)
		if err != nil {
			return nil, err
		}
		nextRun = sched.Next(time.Now().In(s.loc))
		kind = "cron"
	default:
		return nil, fmt.Errorf("cron: invalid kind %q (use once|cron)", kind)
	}

	sc := &model.Schedule{
		ChannelID: ch,
		CreatorID: creatorID,
		Kind:      kind,
		CronExpr:  strings.TrimSpace(cronExpr),
		NextRunAt: nextRun,
		Prompt:    prompt,
		Enabled:   true,
	}
	if err := s.store.CreateSchedule(ctx, sc, s.maxPerChannel); err != nil {
		return nil, err
	}
	return sc, nil
}

// Start runs the background scheduler loop until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	distillTicker := time.NewTicker(1 * time.Hour)
	maintTicker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	defer distillTicker.Stop()
	defer maintTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.tick(ctx, now)
			s.maybeRunDailyDigests(ctx, now)
		case <-distillTicker.C:
			s.RunIncrementalDistill(ctx)
		case now := <-maintTicker.C:
			s.runMaintenance(ctx, now)
		}
	}
}

// RunIncrementalDistill refreshes RAM episode vectors (picking up any external CLI runs)
// and incrementally distills new messages across all allowed channels.
func (s *Scheduler) RunIncrementalDistill(ctx context.Context) {
	if s.memHooks == nil {
		return
	}
	if n, err := s.memHooks.ReloadIndex(ctx); err == nil {
		s.log.Info("reloaded episode vector index", "count", n)
	}
	chs, err := s.store.ListChannels(ctx)
	if err != nil {
		return
	}
	total := 0
	for _, c := range chs {
		if ctx.Err() != nil {
			return
		}
		if s.chanFilter != nil && !s.chanFilter.ChannelAllowed(c.ChannelID) {
			continue
		}
		n, err := s.memHooks.DistillIncremental(ctx, c.ChannelID)
		if err != nil {
			s.log.Warn("incremental distill failed", "channel", c.ChannelID, "err", err)
			continue
		}
		total += n
	}
	if total > 0 {
		s.log.Info("hourly incremental distill finished", "new_episodes", total)
	}
}

// maybeRunDailyDigests generates yesterday's channel digests once per day after 00:15 local time.
func (s *Scheduler) maybeRunDailyDigests(ctx context.Context, now time.Time) {
	local := now.In(s.loc)
	if local.Hour() != 0 || local.Minute() < 15 {
		return
	}
	yesterday := local.AddDate(0, 0, -1)
	dayKey := yesterday.Format("2006-01-02")
	metaKey := "daily_digest_done:" + dayKey
	if done, err := s.store.GetMeta(ctx, metaKey); err == nil && done == "1" {
		return
	}
	_ = s.store.SetMeta(ctx, metaKey, "1")

	chs, err := s.store.ListChannels(ctx)
	if err != nil {
		return
	}
	generated := 0
	for _, c := range chs {
		if ctx.Err() != nil {
			return
		}
		if s.chanFilter != nil && !s.chanFilter.ChannelAllowed(c.ChannelID) {
			continue
		}
		if d, err := s.GenerateDailyDigest(ctx, c.ChannelID, yesterday); err == nil && d != nil {
			generated++
		}
	}
	if generated > 0 {
		s.log.Info("generated daily digests", "day", dayKey, "channels", generated)
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	due, err := s.store.DueSchedules(ctx, now)
	if err != nil {
		s.log.Warn("cron poll failed", "err", err)
		return
	}
	for _, sc := range due {
		if sc.Kind == "once" {
			_ = s.store.UpdateScheduleNextRun(ctx, sc.ID, sc.NextRunAt, false)
		} else {
			sched, err := cronParser.Parse(sc.CronExpr)
			if err != nil {
				_ = s.store.UpdateScheduleNextRun(ctx, sc.ID, sc.NextRunAt, false)
				continue
			}
			next := sched.Next(now.In(s.loc))
			_ = s.store.UpdateScheduleNextRun(ctx, sc.ID, next, true)
		}

		if s.dispatcher != nil {
			s.dispatcher.DispatchSystem(model.SystemTriggerEvent{
				ChannelID:  sc.ChannelID,
				ScheduleID: sc.ID,
				Kind:       "schedule",
				Prompt:     sc.Prompt,
				CreatorID:  sc.CreatorID,
				CreatedAt:  now,
			})
		}
	}
}

func (s *Scheduler) runMaintenance(ctx context.Context, now time.Time) {
	if pruned, err := s.store.PruneExpiredMedia(ctx, now); err == nil && pruned > 0 {
		s.log.Info("pruned expired media assets", "count", pruned)
	}
	if err := s.store.Checkpoint(ctx, "TRUNCATE"); err != nil {
		s.log.Warn("wal checkpoint failed", "err", err)
	}
}

// GenerateDailyDigest builds a channel daily digest report for dayKey (YYYY-MM-DD in scheduler tz).
func (s *Scheduler) GenerateDailyDigest(ctx context.Context, ch model.ChannelID, day time.Time) (*model.DailyDigest, error) {
	dayLocal := day.In(s.loc)
	start := time.Date(dayLocal.Year(), dayLocal.Month(), dayLocal.Day(), 0, 0, 0, 0, s.loc)
	end := start.Add(24*time.Hour - time.Nanosecond)
	dayKey := start.Format("2006-01-02")

	msgs, err := s.store.MessagesInRange(ctx, ch, start, end, 2000)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}

	// Compute lightweight channel stats.
	counts := make(map[string]int)
	names := make(map[string]string)
	var sampleLines []string
	for _, m := range msgs {
		if !m.FromBot {
			counts[m.SenderID]++
			names[m.SenderID] = m.SenderName
		}
		if txt := strings.TrimSpace(m.PlainText()); txt != "" && len(sampleLines) < 200 {
			sampleLines = append(sampleLines, fmt.Sprintf("[%s] %s: %s", m.CreatedAt.In(s.loc).Format("15:04"), m.SenderName, txt))
		}
	}
	type rankItem struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	var ranks []rankItem
	for id, c := range counts {
		ranks = append(ranks, rankItem{ID: id, Name: names[id], Count: c})
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i].Count > ranks[j].Count })
	if len(ranks) > 5 {
		ranks = ranks[:5]
	}
	statsBytes, _ := json.Marshal(map[string]any{
		"total_messages": len(msgs),
		"unique_senders": len(counts),
		"top_speakers":   ranks,
	})

	report := fmt.Sprintf("Daily Summary (%s): %d messages from %d participants.", dayKey, len(msgs), len(counts))
	if s.hub != nil && len(sampleLines) > 0 {
		if resp, err := s.hub.Chat(ctx, "fast", infer.ChatRequest{
			Messages: []infer.ChatMessage{
				{Role: "system", Content: "Summarize today's channel discussions concisely in bullet points (key topics, highlights, decisions)."},
				{Role: "user", Content: strings.Join(sampleLines, "\n")},
			},
		}); err == nil && strings.TrimSpace(resp.Content) != "" {
			report = strings.TrimSpace(resp.Content)
		}
	}

	var embedBlob []byte
	if s.hub != nil {
		if embs, err := s.hub.Embed(ctx, []string{dayKey + " " + report}); err == nil && len(embs) == 1 && len(embs[0]) == s.embedDim {
			vec.NormalizeInPlace(embs[0])
			embedBlob = vec.EncodeFloat16(embs[0])
		}
	}

	digest := model.DailyDigest{
		ChannelID: ch,
		DayKey:    dayKey,
		Report:    report,
		StatsJSON: string(statsBytes),
		CreatedAt: time.Now(),
	}
	if err := s.store.SaveDailyDigest(ctx, digest, embedBlob); err != nil {
		return nil, err
	}
	return &digest, nil
}

func parseFlexibleTime(s string, loc *time.Location) (time.Time, error) {
	s = strings.TrimSpace(s)
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return time.Now().Add(d), nil
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time format %q", s)
}
