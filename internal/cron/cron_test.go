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

type mockPriceFetcher struct {
	quotes map[string]PriceQuote
	calls  int
}

func (m *mockPriceFetcher) FetchLivePrices(_ context.Context, _ []string) map[string]PriceQuote {
	m.calls++
	return m.quotes
}

func (m *mockPriceFetcher) ResolveAlertSymbol(_ context.Context, raw string) (string, string, string, error) {
	if raw == "BTC" {
		return "crypto:BTC", "BTC/USDT", "crypto", nil
	}
	return "usNVDA", "英伟达", "us", nil
}

func TestTradingCalendarAndZeroLLMPriceAlerts(t *testing.T) {
	// Holiday & weekend calendar checks
	cnHoliday := time.Date(2026, 10, 1, 10, 0, 0, 0, shanghaiLoc)
	if IsTradingDay("cn", cnHoliday) || IsMarketOpen("cn", cnHoliday) {
		t.Fatal("2026-10-01 must be recognized as CN holiday")
	}
	cnOpen := time.Date(2026, 11, 27, 10, 30, 0, 0, shanghaiLoc) // Friday
	if !IsTradingDay("cn", cnOpen) || !IsMarketOpen("cn", cnOpen) {
		t.Fatal("Friday 10:30 CST must be CN market open")
	}
	cnClosed := time.Date(2026, 11, 27, 21, 0, 0, 0, shanghaiLoc)
	if IsMarketOpen("cn", cnClosed) {
		t.Fatal("Friday 21:00 CST must be CN market closed")
	}
	if !IsMarketOpen("crypto", cnClosed) {
		t.Fatal("crypto market must be 24/7 open")
	}

	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	disp := &captureDisp{}
	sc := NewScheduler(st, nil, disp, shanghaiLoc, 10, 5, 4, nil)
	pf := &mockPriceFetcher{
		quotes: map[string]PriceQuote{
			"crypto:BTC": {Symbol: "crypto:BTC", Name: "BTC/USDT", Price: 101500, ChangePct: 4.2, Market: "crypto"},
		},
	}
	sc.SetPriceFetcher(pf)

	ctx := context.Background()
	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")

	// With 0 alerts, CheckPriceAlerts must make ZERO network fetches.
	sc.CheckPriceAlerts(ctx, cnClosed)
	if pf.calls != 0 {
		t.Fatalf("expected 0 fetches when no alerts exist, got %d", pf.calls)
	}

	// Create a one-shot above alert and a recurring pct_up alert with 30m cooldown.
	a1, err := sc.CreatePriceAlert(ctx, ch, "u1", "BTC", "above", 100000, 0, "大饼破10万了")
	if err != nil || a1.Symbol != "crypto:BTC" {
		t.Fatalf("CreatePriceAlert a1 failed: %+v err=%v", a1, err)
	}
	a2, err := sc.CreatePriceAlert(ctx, ch, "u1", "BTC", "pct_up", 3.0, 30, "日内大涨")
	if err != nil {
		t.Fatalf("CreatePriceAlert a2 failed: %v", err)
	}

	// First check fires both alerts.
	sc.CheckPriceAlerts(ctx, cnClosed)
	if len(disp.events) != 2 || disp.events[0].Kind != "price_alert" {
		t.Fatalf("expected 2 price_alert dispatches, got %+v", disp.events)
	}
	// 5 minutes later: a1 is disabled (one-shot), a2 is in 30m cooldown -> 0 new dispatches.
	sc.CheckPriceAlerts(ctx, cnClosed.Add(5*time.Minute))
	if len(disp.events) != 2 {
		t.Fatalf("expected cooldown & one-shot disable to suppress re-trigger, got %d events", len(disp.events))
	}
	// 35 minutes later: a2 cooldown elapsed -> fires again (total 3).
	sc.CheckPriceAlerts(ctx, cnClosed.Add(35*time.Minute))
	if len(disp.events) != 3 {
		t.Fatalf("expected recurring alert #%d to re-fire after cooldown, got %d events", a2.ID, len(disp.events))
	}

	// Verify market_days_only skips firing on CN holiday while advancing schedule.
	scHol, err := sc.CreateScheduleWithMarket(ctx, ch, "u1", "cron", "0 15 * * 1-5", "", "A股收盘速报", "cn")
	if err != nil {
		t.Fatalf("CreateScheduleWithMarket: %v", err)
	}
	_ = st.UpdateScheduleNextRun(ctx, scHol.ID, cnHoliday, true)
	prevEv := len(disp.events)
	sc.tick(ctx, cnHoliday.Add(time.Minute))
	if len(disp.events) != prevEv {
		t.Fatalf("expected market_days_only=cn cron to skip 2026-10-01 holiday, got %d events", len(disp.events)-prevEv)
	}
}
