package cron

import (
	"context"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
)

type captureDisp struct {
	events []model.SystemTriggerEvent
}

func (c *captureDisp) DispatchSystem(ev model.SystemTriggerEvent) {
	c.events = append(c.events, ev)
}

func TestSchedulerValidationAndDispatch(t *testing.T) {
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	disp := &captureDisp{}
	sc := NewScheduler(st, nil, disp, time.UTC, 2, 5, 4, nil)
	ctx := context.Background()
	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")

	// Reject cron expression with <5m interval (* * * * *).
	if _, err := sc.CreateSchedule(ctx, ch, "u1", "cron", "* * * * *", "", "spam"); err == nil {
		t.Fatal("expected <5m cron interval to be rejected")
	}

	// Accept valid 10-minute cron.
	if _, err := sc.CreateSchedule(ctx, ch, "u1", "cron", "*/10 * * * *", "", "check status"); err != nil {
		t.Fatalf("expected */10 cron to succeed: %v", err)
	}

	// Accept one-shot due soon and verify tick dispatches SystemTriggerEvent (never transport).
	one, err := sc.CreateSchedule(ctx, ch, "u1", "once", "", "1s", "remind me")
	if err != nil {
		t.Fatalf("create once: %v", err)
	}
	// Exceeding maxPerChannel=2 must fail.
	if _, err := sc.CreateSchedule(ctx, ch, "u1", "once", "", "1h", "third"); err == nil {
		t.Fatal("expected maxPerChannel limit to reject 3rd schedule")
	}

	sc.tick(ctx, one.NextRunAt.Add(time.Second))
	if len(disp.events) != 1 || disp.events[0].Prompt != "remind me" {
		t.Fatalf("expected 1 dispatched SystemTriggerEvent, got %+v", disp.events)
	}
}

type mockMemHooks struct {
	reloads   int
	distilled []model.ChannelID
}

func (m *mockMemHooks) DistillIncremental(_ context.Context, ch model.ChannelID) (int, error) {
	m.distilled = append(m.distilled, ch)
	return 1, nil
}
func (m *mockMemHooks) ReloadIndex(_ context.Context) (int, error) {
	m.reloads++
	return 10, nil
}

type mockFilter struct{ denied model.ChannelID }

func (f mockFilter) ChannelAllowed(ch model.ChannelID) bool { return ch != f.denied }

func TestIncrementalDistillAndDailyDigestHooks(t *testing.T) {
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	chAllowed := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")
	chDenied := model.MakeChannelID("onebot", "1", model.ChannelGroup, "200")
	_, _ = st.EnsureChannel(ctx, chAllowed)
	_, _ = st.EnsureChannel(ctx, chDenied)

	// Append a message from "yesterday" so daily digest has content.
	now := time.Date(2026, 11, 28, 0, 20, 0, 0, time.UTC)
	_ = st.AppendMessage(ctx, &model.Message{
		ChannelID:  chAllowed,
		SenderID:   "u1",
		SenderName: "Alice",
		Parts:      []model.Part{{Kind: model.PartText, Text: "yesterday discussion"}},
		CreatedAt:  now.Add(-6 * time.Hour),
	})

	sc := NewScheduler(st, nil, &captureDisp{}, time.UTC, 10, 5, 4, nil)
	hooks := &mockMemHooks{}
	sc.SetMemoryHooks(hooks, mockFilter{denied: chDenied})

	// Incremental distill must reload index and skip denied channel.
	sc.RunIncrementalDistill(ctx)
	if hooks.reloads != 1 || len(hooks.distilled) != 1 || hooks.distilled[0] != chAllowed {
		t.Fatalf("unexpected incremental distill run: reloads=%d distilled=%+v", hooks.reloads, hooks.distilled)
	}

	// Daily digest fires once after 00:15 and is idempotent on repeat ticks.
	sc.maybeRunDailyDigests(ctx, now)
	sc.maybeRunDailyDigests(ctx, now.Add(time.Minute))
	digests, err := st.ListDailyDigests(ctx, chAllowed, 5)
	if err != nil || len(digests) != 1 || digests[0].DayKey != "2026-11-27" {
		t.Fatalf("expected 1 daily digest for 2026-11-27, got %+v err=%v", digests, err)
	}
}
