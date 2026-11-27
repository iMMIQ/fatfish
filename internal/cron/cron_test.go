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
