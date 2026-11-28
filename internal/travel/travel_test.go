package travel

import (
	"bytes"
	"context"
	"encoding/json"
	"image/png"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
)

type captureDispatcher struct {
	mu     sync.Mutex
	events []model.SystemTriggerEvent
}

func (d *captureDispatcher) DispatchSystem(ev model.SystemTriggerEvent) {
	d.mu.Lock()
	d.events = append(d.events, ev)
	d.mu.Unlock()
}

func (d *captureDispatcher) Events() []model.SystemTriggerEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]model.SystemTriggerEvent(nil), d.events...)
}

type allowAllFilter struct{}

func (allowAllFilter) ChannelAllowed(model.ChannelID) bool { return true }

func TestAtlasLookupAndTransit(t *testing.T) {
	if len(AllCities()) < 80 {
		t.Fatalf("expected rich global city atlas, got %d", len(AllCities()))
	}
	for _, q := range []string{"hangzhou", "东京", "LIS", "Reykjavik", "冰岛", "圣托里尼", "纽约"} {
		if _, ok := FindCity(q); !ok {
			t.Fatalf("FindCity(%q) failed", q)
		}
	}

	hz, _ := CityByID("hangzhou")
	sh, _ := CityByID("shanghai")
	mode, dur, dist := EstimateTransit(hz, sh)
	if mode != "train" || dist < 100 || dist > 300 || dur < time.Hour {
		t.Fatalf("unexpected hangzhou->shanghai transit: mode=%s dur=%v dist=%.0f", mode, dur, dist)
	}

	lis, _ := CityByID("lisbon")
	mode2, dur2, dist2 := EstimateTransit(hz, lis)
	if mode2 != "flight" || dist2 < 9000 || dur2 < 10*time.Hour {
		t.Fatalf("unexpected hangzhou->lisbon transit: mode=%s dur=%v dist=%.0f", mode2, dur2, dist2)
	}
}

func TestTravelStateMachineWeatherDiaryIsolationAndVisuals(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(t.TempDir(), 1280, 10)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// Seed two group channels for strict isolation verification.
	chA := model.MakeChannelID("onebot", "100001", model.ChannelGroup, "111")
	chB := model.MakeChannelID("onebot", "100001", model.ChannelGroup, "222")
	_, _ = st.EnsureChannel(ctx, chA)
	_, _ = st.EnsureChannel(ctx, chB)

	// Mock Open-Meteo server
	weatherSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"current": map[string]any{
				"temperature_2m":       14.2,
				"apparent_temperature": 12.1,
				"weather_code":         61,
				"wind_speed_10m":       18.5,
			},
		})
	}))
	defer weatherSrv.Close()

	// Mock primary LLM server for diary generation
	var llmCalls int
	llmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		llmCalls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message": map[string]any{
					"content": "刚出车站被毛毛雨淋一脸，路边买了两个热蛋挞先回回血",
				},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{"prompt_tokens": 120, "completion_tokens": 24, "total_tokens": 144},
		})
	}))
	defer llmSrv.Close()

	hub := infer.NewHub(config.ModelsConfig{
		Primary: config.ModelEndpointConfig{
			BaseURL:    llmSrv.URL,
			APIKey:     "test-key",
			Model:      "test-primary",
			TimeoutSec: 10,
		},
	})

	shLoc, _ := time.LoadLocation("Asia/Shanghai")
	cfg := config.TravelConfig{
		Enabled:                 true,
		HomeCity:                "hangzhou",
		RouteStyle:              "realistic",
		StayMinDays:             3,
		StayMaxDays:             4,
		JournalIntervalHours:    4.0,
		WeatherIntervalHours:    2.0,
		JournalModel:            "primary",
		AllowSuggestions:        true,
		ProactiveEnabled:        true,
		ProactiveChannels:       []string{"*"},
		ProactiveQuietStartHour: 23,
		ProactiveQuietEndHour:   9,
		ProactiveMinGapHours:    12,
	}

	fontPath := filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf")
	svc := NewService(st, hub, cfg, shLoc, "你是大肥鱼，嘴硬短句群友。", fontPath, []string{"127.0.0.1"}, nil)
	svc.weatherURL = weatherSrv.URL
	disp := &captureDispatcher{}
	svc.SetHooks(disp, allowAllFilter{})

	// 1. Initial tick at 14:00 CST -> initializes at Hangzhou, fetches weather, writes primary-model diary
	t0 := time.Date(2026, 6, 10, 14, 0, 0, 0, shLoc)
	if err := svc.Tick(ctx, t0); err != nil {
		t.Fatalf("initial tick: %v", err)
	}
	state, err := st.GetTravelState(ctx)
	if err != nil || state == nil {
		t.Fatalf("expected travel state: %v", err)
	}
	if state.CurrentCityID != "hangzhou" || state.Status != "staying" {
		t.Fatalf("unexpected initial state: %+v", state)
	}
	if !strings.Contains(state.WeatherSummary, "小雨") {
		t.Fatalf("expected Open-Meteo weather summary, got %q", state.WeatherSummary)
	}
	if llmCalls != 1 {
		t.Fatalf("expected 1 primary model diary call, got %d", llmCalls)
	}

	// 2. Member in chA suggests Lisbon -> must remain strictly scoped to chA!
	sg, city, err := svc.SuggestNext(ctx, chA, "u_alice", "Alice", "里斯本", "吃蛋挞")
	if err != nil || city.ID != "lisbon" || sg.ID == 0 {
		t.Fatalf("SuggestNext failed: sg=%+v city=%+v err=%v", sg, city, err)
	}

	// 3. Force departure to Lisbon (in_transit) and verify BotClock + Snapshot
	stTransit, _, err := svc.ForceTravel(ctx, "lisbon", false)
	if err != nil || stTransit.Status != "in_transit" || stTransit.NextCityID != "lisbon" {
		t.Fatalf("ForceTravel transit failed: %+v err=%v", stTransit, err)
	}
	_, _, inTransit, ok := svc.BotClock(time.Now())
	if !ok || !inTransit {
		t.Fatalf("expected BotClock inTransit=true, ok=%v inTransit=%v", ok, inTransit)
	}

	// 4. Advance clock past TransitETA during awake hours (15:00 CST) -> arrives at Lisbon & dispatches proactive triggers
	arriveAt := stTransit.TransitETA.Add(5 * time.Minute)
	arriveAt = time.Date(arriveAt.Year(), arriveAt.Month(), arriveAt.Day(), 15, 0, 0, 0, shLoc)
	if arriveAt.Before(stTransit.TransitETA) {
		arriveAt = arriveAt.Add(24 * time.Hour)
	}
	if err := svc.Tick(ctx, arriveAt); err != nil {
		t.Fatalf("arrival tick: %v", err)
	}
	stateAfter, _ := st.GetTravelState(ctx)
	if stateAfter.Status != "staying" || stateAfter.CurrentCityID != "lisbon" || stateAfter.LegCount != 2 {
		t.Fatalf("expected arrived at lisbon leg 2, got %+v", stateAfter)
	}

	// Verify strict channel isolation in [msg 1] snapshots & proactive prompts!
	snapA := svc.SnapshotForChannel(ctx, chA, arriveAt, shLoc)
	snapB := svc.SnapshotForChannel(ctx, chB, arriveAt, shLoc)
	if !strings.Contains(snapA, "里斯本") || !strings.Contains(snapA, "Alice") {
		t.Fatalf("expected chA snapshot to include Lisbon and Alice attribution:\n%s", snapA)
	}
	if !strings.Contains(snapB, "里斯本") || strings.Contains(snapB, "Alice") {
		t.Fatalf("strict channel isolation violated! chB snapshot leaked Alice:\n%s", snapB)
	}

	events := disp.Events()
	if len(events) != 2 {
		t.Fatalf("expected 2 proactive arrival triggers (chA + chB), got %d", len(events))
	}
	for _, ev := range events {
		if ev.Kind != "travel" {
			t.Fatalf("expected kind=travel, got %q", ev.Kind)
		}
		if ev.ChannelID == chA && !strings.Contains(ev.Prompt, "Alice") {
			t.Fatalf("expected chA proactive prompt to credit Alice: %s", ev.Prompt)
		}
		if ev.ChannelID == chB && strings.Contains(ev.Prompt, "Alice") {
			t.Fatalf("chB proactive prompt leaked Alice: %s", ev.Prompt)
		}
	}

	// 5. Render pure-Go world footprint map & boarding pass PNGs and verify valid decoded images!
	mapBytes, err := svc.RenderMapPNG(ctx)
	if err != nil {
		t.Fatalf("RenderMapPNG failed: %v", err)
	}
	mapImg, err := png.Decode(bytes.NewReader(mapBytes))
	if err != nil || mapImg.Bounds().Dx() < 800 {
		t.Fatalf("invalid rendered map PNG: err=%v bounds=%v", err, mapImg.Bounds())
	}

	passBytes, err := svc.RenderBoardingPassPNG(ctx)
	if err != nil {
		t.Fatalf("RenderBoardingPassPNG failed: %v", err)
	}
	passImg, err := png.Decode(bytes.NewReader(passBytes))
	if err != nil || passImg.Bounds().Dx() < 800 {
		t.Fatalf("invalid rendered boarding pass PNG: err=%v bounds=%v", err, passImg.Bounds())
	}
}
