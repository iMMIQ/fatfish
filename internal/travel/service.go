package travel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/netutil"
	"github.com/iMMIQ/fatfish/internal/store"
)

// Dispatcher routes low-frequency travel milestone events into Room Actors (never touching transport directly).
type Dispatcher interface {
	DispatchSystem(ev model.SystemTriggerEvent)
}

// ChannelFilter checks whether a channel is enabled in policy.
type ChannelFilter interface {
	ChannelAllowed(ch model.ChannelID) bool
}

// Service manages the bot's global, continuous world-travel state machine, real weather sync,
// primary-model persona micro-diary generation, channel-scoped destination suggestions, and PNG visuals.
type Service struct {
	store *store.Store
	hub   *infer.Hub
	log   *slog.Logger

	mu          sync.RWMutex
	cfg         config.TravelConfig
	groupLoc    *time.Location
	personaHint string
	fontPath    string
	weatherURL  string // overridable for unit tests
	httpCli     *http.Client
	dispatcher  Dispatcher
	chanFilter  ChannelFilter

	// Cached lightweight clock state for zero-I/O Room PolicyEnv checks.
	cachedState *model.TravelState
}

// NewService constructs a world-travel Service.
func NewService(st *store.Store, hub *infer.Hub, cfg config.TravelConfig, groupLoc *time.Location, personaText, fontPath string, allowHosts []string, logger *slog.Logger) *Service {
	if groupLoc == nil {
		groupLoc = time.UTC
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		store:       st,
		hub:         hub,
		log:         logger,
		cfg:         cfg,
		groupLoc:    groupLoc,
		personaHint: compactPersonaHint(personaText),
		fontPath:    fontPath,
		httpCli:     netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 10 * time.Second, AllowHosts: allowHosts}),
	}
	// Warm cached state from SQLite if already initialized.
	if st != nil {
		if existing, err := st.GetTravelState(context.Background()); err == nil && existing != nil {
			s.cachedState = existing
		}
	}
	return s
}

// SetHooks attaches optional Room dispatcher & policy channel filter for proactive milestone triggers.
func (s *Service) SetHooks(disp Dispatcher, filter ChannelFilter) {
	s.mu.Lock()
	s.dispatcher = disp
	s.chanFilter = filter
	s.mu.Unlock()
}

// UpdateConfig hot-reloads travel configuration, persona voice hint, and SSRF client safely.
func (s *Service) UpdateConfig(cfg config.TravelConfig, groupLoc *time.Location, personaText, fontPath string, allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 10 * time.Second, AllowHosts: allowHosts})
	s.mu.Lock()
	s.cfg = cfg
	if groupLoc != nil {
		s.groupLoc = groupLoc
	}
	s.personaHint = compactPersonaHint(personaText)
	if fontPath != "" {
		s.fontPath = fontPath
	}
	s.httpCli = cli
	s.mu.Unlock()
}

// Enabled reports whether world travel is currently enabled.
func (s *Service) Enabled() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	en := s.cfg.Enabled
	s.mu.RUnlock()
	return en
}

func (s *Service) snapshotConfig() (config.TravelConfig, *time.Location, string, string, *http.Client, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg, s.groupLoc, s.personaHint, s.fontPath, s.httpCli, s.weatherURL
}

func (s *Service) setCachedState(st *model.TravelState) {
	if st == nil {
		return
	}
	cp := *st
	s.mu.Lock()
	s.cachedState = &cp
	s.mu.Unlock()
}

// BotClock returns the bot's real-time local hour, sleeping status (01:00..07:30 local),
// and transit status without blocking on SQLite. Returns ok=false when travel is disabled or uninitialized.
func (s *Service) BotClock(now time.Time) (localHour int, sleeping bool, inTransit bool, ok bool) {
	if s == nil {
		return 0, false, false, false
	}
	s.mu.RLock()
	en := s.cfg.Enabled
	st := s.cachedState
	s.mu.RUnlock()
	if !en || st == nil {
		return 0, false, false, false
	}
	city, found := CityByID(st.CurrentCityID)
	if !found {
		return 0, false, false, false
	}
	loc := loadCityLocation(city.Timezone)
	lt := now.In(loc)
	h, m := lt.Hour(), lt.Minute()
	sleeping = (h >= 1 && h < 7) || (h == 7 && m < 30)
	inTransit = st.Status == "in_transit"
	return h, sleeping, inTransit, true
}

// EnsureState loads or initializes the global travel state machine at HomeCity.
func (s *Service) EnsureState(ctx context.Context, now time.Time) (*model.TravelState, error) {
	st, err := s.store.GetTravelState(ctx)
	if err != nil {
		return nil, err
	}
	if st != nil {
		s.setCachedState(st)
		return st, nil
	}
	cfg, _, _, _, _, _ := s.snapshotConfig()
	home, ok := FindCity(cfg.HomeCity)
	if !ok {
		home, _ = CityByID("hangzhou")
	}
	stayDays := randomStayDays(cfg.StayMinDays, cfg.StayMaxDays)
	st = &model.TravelState{
		Status:        "staying",
		CurrentCityID: home.ID,
		ArrivedAt:     now,
		DepartAt:      now.Add(time.Duration(stayDays) * 24 * time.Hour),
		WeatherCode:   -1,
		LegCount:      1,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := s.store.SaveTravelState(ctx, st); err != nil {
		return nil, err
	}
	_ = s.store.AddTravelVisit(ctx, &model.TravelVisit{
		CityID:    home.ID,
		CityName:  home.NameZH,
		Country:   home.CountryZH,
		Lat:       home.Lat,
		Lon:       home.Lon,
		ArrivedAt: now,
	})
	s.setCachedState(st)
	return st, nil
}

// Tick advances the travel state machine, refreshes real Open-Meteo weather, generates
// persona-voiced micro-diary entries using the configured model (default: primary), and
// dispatches low-frequency milestone triggers to whitelisted channels.
func (s *Service) Tick(ctx context.Context, now time.Time) error {
	if !s.Enabled() {
		return nil
	}
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return err
	}
	cfg, _, _, _, _, _ := s.snapshotConfig()

	justArrived := false
	justDeparted := false
	extremeAlert := ""

	// 1. State transitions
	if st.Status == "in_transit" && !st.TransitETA.IsZero() && !now.Before(st.TransitETA) {
		prevCity, _ := CityByID(st.CurrentCityID)
		nextCity, ok := CityByID(st.NextCityID)
		if !ok {
			nextCity = s.pickNextCity(ctx, prevCity, cfg.RouteStyle)
		}
		_, _, distKm := EstimateTransit(prevCity, nextCity)
		stayDays := randomStayDays(cfg.StayMinDays, cfg.StayMaxDays)
		st.Status = "staying"
		st.CurrentCityID = nextCity.ID
		st.NextCityID = ""
		st.ArrivedAt = now
		st.DepartAt = now.Add(time.Duration(stayDays) * 24 * time.Hour)
		st.TransitETA = time.Time{}
		st.WeatherSummary = ""
		st.WeatherCode = -1
		st.WeatherUpdatedAt = time.Time{}
		st.LegCount++
		st.TotalDistanceKm += distKm

		_ = s.store.AddTravelVisit(ctx, &model.TravelVisit{
			CityID:        nextCity.ID,
			CityName:      nextCity.NameZH,
			Country:       nextCity.CountryZH,
			Lat:           nextCity.Lat,
			Lon:           nextCity.Lon,
			TransportMode: st.TransportMode,
			DistanceKm:    distKm,
			ArrivedAt:     now,
		})
		_ = s.store.MarkTravelSuggestionsByCity(ctx, nextCity.ID, "selected", "visited", now)
		_ = s.store.MarkTravelSuggestionsByCity(ctx, nextCity.ID, "pending", "visited", now)
		justArrived = true
		s.log.Info("travel arrived", "city", nextCity.NameZH, "country", nextCity.CountryZH, "leg", st.LegCount, "dist_km", distKm)
	} else if st.Status == "staying" && !st.DepartAt.IsZero() && !now.Before(st.DepartAt) {
		curCity, _ := CityByID(st.CurrentCityID)
		nextCity := s.pickNextCity(ctx, curCity, cfg.RouteStyle)
		if nextCity.ID != "" && nextCity.ID != curCity.ID {
			mode, dur, _ := EstimateTransit(curCity, nextCity)
			st.Status = "in_transit"
			st.NextCityID = nextCity.ID
			st.TransportMode = mode
			st.DepartAt = now
			st.TransitETA = now.Add(dur)
			_ = s.store.UpdateLatestTravelVisitDeparture(ctx, curCity.ID, now)
			_ = s.store.MarkTravelSuggestionsByCity(ctx, nextCity.ID, "pending", "selected", time.Time{})
			justDeparted = true
			s.log.Info("travel departed", "from", curCity.NameZH, "to", nextCity.NameZH, "mode", mode, "eta", st.TransitETA.Format(time.RFC3339))
		}
	}

	// 2. Refresh Open-Meteo real weather when staying and stale
	curCity, _ := CityByID(st.CurrentCityID)
	weatherInterval := time.Duration(cfg.WeatherIntervalHours * float64(time.Hour))
	if st.Status == "staying" && (st.WeatherUpdatedAt.IsZero() || now.Sub(st.WeatherUpdatedAt) >= weatherInterval) {
		if w, werr := s.refreshWeatherLocked(ctx, st, curCity, now); werr == nil && w != nil {
			if w.Extreme && !justArrived {
				extremeAlert = w.ExtremeReason
			}
		}
	}

	// 3. Generate persona-voiced micro-diary entry using configured model (default: primary)
	journalInterval := time.Duration(cfg.JournalIntervalHours * float64(time.Hour))
	localNow := now.In(loadCityLocation(curCity.Timezone))
	botAwake := !(localNow.Hour() >= 1 && localNow.Hour() < 7)

	var diaryEntry *model.TravelJournalEntry
	switch {
	case justArrived:
		diaryEntry = s.generateJournalEntry(ctx, st, curCity, "arrival", now)
	case justDeparted:
		diaryEntry = s.generateJournalEntry(ctx, st, curCity, "departure", now)
	case extremeAlert != "" && (st.LastJournalAt.IsZero() || now.Sub(st.LastJournalAt) >= 2*time.Hour):
		diaryEntry = s.generateJournalEntry(ctx, st, curCity, "extreme_weather", now)
	case botAwake && (st.LastJournalAt.IsZero() || now.Sub(st.LastJournalAt) >= journalInterval):
		diaryEntry = s.generateJournalEntry(ctx, st, curCity, "diary", now)
	}
	if diaryEntry != nil {
		if err := s.store.AddTravelJournal(ctx, diaryEntry); err == nil {
			st.LastJournalAt = now
		}
	}

	if err := s.store.SaveTravelState(ctx, st); err != nil {
		return err
	}
	s.setCachedState(st)

	// 4. Low-frequency proactive milestone check-in (arrival or extreme weather only)
	if justArrived {
		s.maybeDispatchProactive(ctx, st, curCity, "arrival", "", now)
	} else if extremeAlert != "" {
		s.maybeDispatchProactive(ctx, st, curCity, "extreme_weather", extremeAlert, now)
	}
	return nil
}

// RefreshWeatherNow forces an immediate Open-Meteo refresh for the current city.
func (s *Service) RefreshWeatherNow(ctx context.Context) (*WeatherInfo, error) {
	st, err := s.EnsureState(ctx, time.Now())
	if err != nil {
		return nil, err
	}
	city, ok := CityByID(st.CurrentCityID)
	if !ok {
		return nil, fmt.Errorf("travel: unknown current city %q", st.CurrentCityID)
	}
	w, err := s.refreshWeatherLocked(ctx, st, city, time.Now())
	if err != nil {
		return nil, err
	}
	if err := s.store.SaveTravelState(ctx, st); err != nil {
		return nil, err
	}
	s.setCachedState(st)
	return w, nil
}

func (s *Service) refreshWeatherLocked(ctx context.Context, st *model.TravelState, city model.TravelCity, now time.Time) (*WeatherInfo, error) {
	_, _, _, _, cli, baseURL := s.snapshotConfig()
	wCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	w, err := FetchWeather(wCtx, cli, baseURL, city.Lat, city.Lon)
	if err != nil {
		s.log.Warn("travel weather refresh failed", "city", city.NameZH, "err", err)
		return nil, err
	}
	st.WeatherSummary = w.Summary
	st.WeatherCode = w.Code
	st.WeatherTempC = w.TempC
	st.WeatherUpdatedAt = now
	return w, nil
}

// TriggerDiaryNow immediately writes a new micro-diary entry using the configured model.
func (s *Service) TriggerDiaryNow(ctx context.Context) (*model.TravelJournalEntry, error) {
	now := time.Now()
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return nil, err
	}
	city, ok := CityByID(st.CurrentCityID)
	if !ok {
		return nil, fmt.Errorf("travel: unknown current city %q", st.CurrentCityID)
	}
	entry := s.generateJournalEntry(ctx, st, city, "diary", now)
	if entry == nil {
		return nil, errors.New("travel: failed to generate diary entry")
	}
	if err := s.store.AddTravelJournal(ctx, entry); err != nil {
		return nil, err
	}
	st.LastJournalAt = now
	_ = s.store.SaveTravelState(ctx, st)
	s.setCachedState(st)
	return entry, nil
}

// ForceTravel moves the bot to a target city immediately (instant=true) or starts transit right now (instant=false).
func (s *Service) ForceTravel(ctx context.Context, query string, instant bool) (*model.TravelState, model.TravelCity, error) {
	target, ok := FindCity(query)
	if !ok {
		return nil, model.TravelCity{}, fmt.Errorf("travel: city %q not found in global atlas", query)
	}
	now := time.Now()
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return nil, model.TravelCity{}, err
	}
	cur, _ := CityByID(st.CurrentCityID)
	mode, dur, distKm := EstimateTransit(cur, target)
	cfg, _, _, _, _, _ := s.snapshotConfig()

	if cur.ID != "" && cur.ID != target.ID {
		_ = s.store.UpdateLatestTravelVisitDeparture(ctx, cur.ID, now)
	}

	if instant {
		stayDays := randomStayDays(cfg.StayMinDays, cfg.StayMaxDays)
		st.Status = "staying"
		st.CurrentCityID = target.ID
		st.NextCityID = ""
		st.TransportMode = mode
		st.ArrivedAt = now
		st.DepartAt = now.Add(time.Duration(stayDays) * 24 * time.Hour)
		st.TransitETA = time.Time{}
		st.WeatherSummary = ""
		st.WeatherCode = -1
		st.WeatherUpdatedAt = time.Time{}
		if cur.ID != target.ID {
			st.LegCount++
			st.TotalDistanceKm += distKm
		}
		_ = s.store.AddTravelVisit(ctx, &model.TravelVisit{
			CityID:        target.ID,
			CityName:      target.NameZH,
			Country:       target.CountryZH,
			Lat:           target.Lat,
			Lon:           target.Lon,
			TransportMode: mode,
			DistanceKm:    distKm,
			ArrivedAt:     now,
		})
		_ = s.store.MarkTravelSuggestionsByCity(ctx, target.ID, "pending", "visited", now)
		_ = s.store.MarkTravelSuggestionsByCity(ctx, target.ID, "selected", "visited", now)
		_, _ = s.refreshWeatherLocked(ctx, st, target, now)
		if entry := s.generateJournalEntry(ctx, st, target, "arrival", now); entry != nil {
			if aerr := s.store.AddTravelJournal(ctx, entry); aerr == nil {
				st.LastJournalAt = now
			}
		}
	} else {
		st.Status = "in_transit"
		st.NextCityID = target.ID
		st.TransportMode = mode
		st.DepartAt = now
		st.TransitETA = now.Add(dur)
		_ = s.store.MarkTravelSuggestionsByCity(ctx, target.ID, "pending", "selected", time.Time{})
		if entry := s.generateJournalEntry(ctx, st, cur, "departure", now); entry != nil {
			if aerr := s.store.AddTravelJournal(ctx, entry); aerr == nil {
				st.LastJournalAt = now
			}
		}
	}

	if err := s.store.SaveTravelState(ctx, st); err != nil {
		return nil, model.TravelCity{}, err
	}
	s.setCachedState(st)
	return st, target, nil
}

// SuggestNext records a channel-scoped member suggestion for the next destination.
func (s *Service) SuggestNext(ctx context.Context, ch model.ChannelID, memberID, memberName, query, note string) (*model.TravelSuggestion, model.TravelCity, error) {
	cfg, _, _, _, _, _ := s.snapshotConfig()
	if !cfg.AllowSuggestions {
		return nil, model.TravelCity{}, errors.New("travel: destination suggestions are currently disabled")
	}
	city, ok := FindCity(query)
	if !ok {
		return nil, model.TravelCity{}, fmt.Errorf("未在城市图谱中找到 %q（可尝试具体城市名如 东京/里斯本/雷克雅未克/纽约）", query)
	}
	st, err := s.EnsureState(ctx, time.Now())
	if err == nil && st != nil && st.CurrentCityID == city.ID && st.Status == "staying" {
		return nil, city, fmt.Errorf("现在人就在 %s 呢，换个下一站吧", city.NameZH)
	}
	sg := &model.TravelSuggestion{
		ChannelID:  ch,
		MemberID:   memberID,
		MemberName: strings.TrimSpace(memberName),
		CityID:     city.ID,
		CityName:   city.NameZH,
		Note:       truncateRunes(strings.TrimSpace(note), 60),
		Status:     "pending",
		CreatedAt:  time.Now(),
	}
	if err := s.store.AddTravelSuggestion(ctx, sg); err != nil {
		return nil, model.TravelCity{}, err
	}
	return sg, city, nil
}

// SnapshotForChannel formats the compact [Current Journey] section frozen ONCE per segment into [msg 1].
// Channel-scoped suggestion attribution is ONLY included when a member in this exact channel suggested the stop.
func (s *Service) SnapshotForChannel(ctx context.Context, ch model.ChannelID, now time.Time, groupLoc *time.Location) string {
	if !s.Enabled() {
		return ""
	}
	st, err := s.EnsureState(ctx, now)
	if err != nil || st == nil {
		return ""
	}
	if groupLoc == nil {
		_, groupLoc, _, _, _, _ = s.snapshotConfig()
	}
	cur, ok := CityByID(st.CurrentCityID)
	if !ok {
		return ""
	}

	var lines []string
	curLoc := loadCityLocation(cur.Timezone)
	localNow := now.In(curLoc)
	diffStr := formatTimezoneDelta(now, groupLoc, curLoc)

	if st.Status == "in_transit" {
		nextCity, hasNext := CityByID(st.NextCityID)
		destLabel := st.NextCityID
		etaStr := st.TransitETA.In(curLoc).Format("15:04")
		if hasNext {
			destLabel = fmt.Sprintf("%s·%s", nextCity.CountryZH, nextCity.NameZH)
			etaStr = st.TransitETA.In(loadCityLocation(nextCity.Timezone)).Format("15:04")
		}
		rem := time.Until(st.TransitETA)
		if rem < 0 {
			rem = 0
		}
		lines = append(lines, fmt.Sprintf("状态: 正在从 %s·%s 乘坐%s前往 %s（预计目的地当地时间 %s 到达，剩余约 %s）",
			cur.CountryZH, cur.NameZH, transportModeZH(st.TransportMode), destLabel, etaStr, formatShortDuration(rem)))
		if hasNext && ch != "" {
			if sgs, err := s.store.ListChannelSuggestionsForCity(ctx, ch, nextCity.ID); err == nil && len(sgs) > 0 {
				lines = append(lines, formatRecommenderHint(sgs[0]))
			}
		}
	} else {
		stayDay := int(now.Sub(st.ArrivedAt).Hours()/24) + 1
		if stayDay < 1 {
			stayDay = 1
		}
		lines = append(lines, fmt.Sprintf("位置: %s·%s (%s，当地时间 %s %s，%s) | 停留第%d天",
			cur.CountryZH, cur.NameZH, cur.Currency, localNow.Format("15:04"), periodZH(localNow.Hour()), diffStr, stayDay))
		if st.WeatherSummary != "" {
			lines = append(lines, "当地天气: "+st.WeatherSummary)
		}
		if ch != "" {
			if sgs, err := s.store.ListChannelSuggestionsForCity(ctx, ch, cur.ID); err == nil && len(sgs) > 0 {
				lines = append(lines, formatRecommenderHint(sgs[0]))
			}
		}
	}

	if entries, err := s.store.ListTravelJournal(ctx, 2); err == nil && len(entries) > 0 {
		lines = append(lines, "近期随手记:")
		for _, e := range entries {
			ts := e.LocalTime
			if ts == "" {
				ts = e.CreatedAt.In(curLoc).Format("01-02 15:04")
			}
			lines = append(lines, fmt.Sprintf("- [%s %s] %s", e.CityName, ts, e.Content))
		}
	}
	return strings.Join(lines, "\n")
}

// DetailedStatus formats a comprehensive status report for the `travel(action="status")` tool.
func (s *Service) DetailedStatus(ctx context.Context, ch model.ChannelID, now time.Time) (string, error) {
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return "", err
	}
	_, groupLoc, _, _, _, _ := s.snapshotConfig()
	cur, ok := CityByID(st.CurrentCityID)
	if !ok {
		return "", fmt.Errorf("travel: unknown city %q", st.CurrentCityID)
	}
	curLoc := loadCityLocation(cur.Timezone)
	localNow := now.In(curLoc)
	diffStr := formatTimezoneDelta(now, groupLoc, curLoc)

	citiesCnt, countriesCnt, _ := s.store.CountTravelStats(ctx)
	daysOnRoad := int(now.Sub(st.StartedAt).Hours()/24) + 1
	if daysOnRoad < 1 {
		daysOnRoad = 1
	}

	var b strings.Builder
	if st.Status == "in_transit" {
		next, hasNext := CityByID(st.NextCityID)
		dest := st.NextCityID
		etaStr := st.TransitETA.In(curLoc).Format("01-02 15:04")
		if hasNext {
			dest = fmt.Sprintf("%s·%s (%s)", next.CountryZH, next.NameZH, next.AirportCode)
			etaStr = st.TransitETA.In(loadCityLocation(next.Timezone)).Format("01-02 15:04")
		}
		fmt.Fprintf(&b, "当前状态: 移动中（%s）\n出发地: %s·%s (%s)\n目的地: %s\n预计到达(目的地时间): %s（剩余约 %s）\n",
			transportModeZH(st.TransportMode), cur.CountryZH, cur.NameZH, cur.AirportCode, dest, etaStr, formatShortDuration(time.Until(st.TransitETA)))
	} else {
		stayDay := int(now.Sub(st.ArrivedAt).Hours()/24) + 1
		if stayDay < 1 {
			stayDay = 1
		}
		fmt.Fprintf(&b, "当前位置: %s·%s (%s / %s)\n当地时间: %s %s (%s)\n当前状态: 停留第 %d 天（预计 %s 前往下一站）\n当地天气: %s\n",
			cur.CountryZH, cur.NameZH, cur.Name, cur.AirportCode,
			localNow.Format("2006-01-02 15:04"), periodZH(localNow.Hour()), diffStr,
			stayDay, st.DepartAt.In(curLoc).Format("01-02 15:04"),
			emptyOr(st.WeatherSummary, "暂未同步"))
	}
	fmt.Fprintf(&b, "旅途统计: 已旅行 %d 天 | 足迹 %d 城 / %d 国 | 累计里程 %.0f km\n",
		daysOnRoad, citiesCnt, countriesCnt, st.TotalDistanceKm)

	if ch != "" {
		if sgs, err := s.store.ListChannelSuggestionsForCity(ctx, ch, cur.ID); err == nil && len(sgs) > 0 {
			fmt.Fprintf(&b, "%s\n", formatRecommenderHint(sgs[0]))
		}
	}

	if entries, err := s.store.ListTravelJournal(ctx, 5); err == nil && len(entries) > 0 {
		b.WriteString("\n最近旅行随手记:\n")
		for _, e := range entries {
			fmt.Fprintf(&b, "- [%s %s] %s\n", e.CityName, e.LocalTime, e.Content)
		}
	}
	if pending, err := s.store.ListTravelSuggestions(ctx, "pending", 8); err == nil && len(pending) > 0 {
		var names []string
		seen := make(map[string]bool)
		for _, sg := range pending {
			if seen[sg.CityID] {
				continue
			}
			seen[sg.CityID] = true
			if sg.ChannelID == ch && sg.MemberName != "" {
				names = append(names, fmt.Sprintf("%s(%s推荐)", sg.CityName, sg.MemberName))
			} else {
				names = append(names, sg.CityName)
			}
		}
		if len(names) > 0 {
			fmt.Fprintf(&b, "\n心愿单候选站: %s\n", strings.Join(names, "、"))
		}
	}
	return strings.TrimSpace(b.String()), nil
}

// HistorySummary formats visited cities and highlights for `travel(action="history")`.
func (s *Service) HistorySummary(ctx context.Context, limit int) (string, error) {
	if limit <= 0 || limit > 30 {
		limit = 15
	}
	visits, err := s.store.ListTravelVisits(ctx, limit, false)
	if err != nil {
		return "", err
	}
	if len(visits) == 0 {
		return "还没有记录到任何旅行足迹。", nil
	}
	citiesCnt, countriesCnt, _ := s.store.CountTravelStats(ctx)
	st, _ := s.store.GetTravelState(ctx)
	totalKm := 0.0
	if st != nil {
		totalKm = st.TotalDistanceKm
	}
	var b strings.Builder
	fmt.Fprintf(&b, "环球足迹总览: %d 个城市 / %d 个国家 / 累计 %.0f km\n最近站点:\n", citiesCnt, countriesCnt, totalKm)
	for i, v := range visits {
		leg := ""
		if v.DistanceKm > 0 {
			leg = fmt.Sprintf("（%s %.0fkm）", transportModeZH(v.TransportMode), v.DistanceKm)
		}
		fmt.Fprintf(&b, "%d. %s·%s — %s 抵达%s\n", i+1, v.Country, v.CityName, v.ArrivedAt.Format("2006-01-02"), leg)
	}
	return strings.TrimSpace(b.String()), nil
}

// pickNextCity chooses the next destination, favoring pending member suggestions (~75% chance)
// and otherwise picking an unvisited city according to routeStyle ("realistic" vs "wander").
func (s *Service) pickNextCity(ctx context.Context, current model.TravelCity, routeStyle string) model.TravelCity {
	// 1. Check pending member suggestions first (high engagement!).
	if sgs, err := s.store.ListTravelSuggestions(ctx, "pending", 20); err == nil && len(sgs) > 0 && rand.Float64() < 0.78 {
		for _, sg := range sgs {
			if sg.CityID != current.ID {
				if c, ok := CityByID(sg.CityID); ok {
					return c
				}
			}
		}
	}

	visited := make(map[string]bool)
	if visits, err := s.store.ListTravelVisits(ctx, 200, false); err == nil {
		// Allow revisiting very old cities only after visiting 40+ unique places.
		capRecent := 40
		for i, v := range visits {
			if i >= capRecent {
				break
			}
			visited[v.CityID] = true
		}
	}
	visited[current.ID] = true

	var candidates []model.TravelCity
	for _, c := range builtinCities {
		if !visited[c.ID] {
			candidates = append(candidates, c)
		}
	}
	if len(candidates) == 0 {
		for _, c := range builtinCities {
			if c.ID != current.ID {
				candidates = append(candidates, c)
			}
		}
	}
	if len(candidates) == 0 {
		return current
	}

	if strings.EqualFold(strings.TrimSpace(routeStyle), "wander") {
		return candidates[rand.Intn(len(candidates))]
	}

	// Realistic route: 65% stay within 3500km (regional progression), 35% intercontinental jump.
	var near, far []model.TravelCity
	for _, c := range candidates {
		d := HaversineKm(current.Lat, current.Lon, c.Lat, c.Lon)
		if d <= 3500 {
			near = append(near, c)
		} else {
			far = append(far, c)
		}
	}
	if len(near) > 0 && (len(far) == 0 || rand.Float64() < 0.65) {
		return near[rand.Intn(len(near))]
	}
	if len(far) > 0 {
		return far[rand.Intn(len(far))]
	}
	return candidates[rand.Intn(len(candidates))]
}

// generateJournalEntry invokes the configured model (default: primary chat model) to write one
// concise, grounded, persona-voiced micro-diary line without any AI tourist-brochure flavor.
func (s *Service) generateJournalEntry(ctx context.Context, st *model.TravelState, city model.TravelCity, kind string, now time.Time) *model.TravelJournalEntry {
	cfg, _, personaHint, _, _, _ := s.snapshotConfig()
	loc := loadCityLocation(city.Timezone)
	localNow := now.In(loc)
	localTimeStr := localNow.Format("01-02 15:04")

	var recentLines []string
	if prev, err := s.store.ListTravelJournal(ctx, 4); err == nil {
		for _, p := range prev {
			recentLines = append(recentLines, fmt.Sprintf("- [%s %s] %s", p.CityName, p.LocalTime, p.Content))
		}
	}

	content := ""
	if s.hub != nil {
		role := strings.TrimSpace(cfg.JournalModel)
		if role == "" {
			role = "primary"
		}
		nextLabel := ""
		if st.NextCityID != "" {
			if nc, ok := CityByID(st.NextCityID); ok {
				nextLabel = fmt.Sprintf("%s·%s", nc.CountryZH, nc.NameZH)
			}
		}

		sysPrompt := "你是正在独自环球旅行中的角色，用第一人称写一条极短的私人旅行随手记（18~55字，口语、接地气、略带吐槽或具体生活体感）。" +
			"\n要求：\n1. 绝不要写旅游攻略、百科科普或鸡汤感叹（禁止“风景如画”“推荐打卡”“令人流连忘返”）。" +
			"\n2. 写具体微小细节：吃到了什么、迷路、物价、天气体感、倒时差、候机/坐车、路边小事。" +
			"\n3. 不加引号、不加日期前缀、不加标签、不提及任何具体人名或群聊，只输出这一句正文。"
		if personaHint != "" {
			sysPrompt += "\n角色口吻参考：" + personaHint
		}

		var ub strings.Builder
		fmt.Fprintf(&ub, "当前城市：%s·%s（特点：%s）\n当地时间：%s %s\n天气：%s\n事件类型：%s\n",
			city.CountryZH, city.NameZH, city.Vibe,
			localNow.Format("2006-01-02 15:04"), periodZH(localNow.Hour()),
			emptyOr(st.WeatherSummary, "未知"), kind)
		if kind == "departure" && nextLabel != "" {
			fmt.Fprintf(&ub, "正在出发：乘坐%s前往 %s\n", transportModeZH(st.TransportMode), nextLabel)
		}
		if len(recentLines) > 0 {
			ub.WriteString("最近几条随手记（请避免重复题材）：\n" + strings.Join(recentLines, "\n") + "\n")
		}
		ub.WriteString("请直接输出这一条新的随手记：")

		temp := 0.9
		callCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
		resp, err := s.hub.Chat(callCtx, role, infer.ChatRequest{
			Messages: []infer.ChatMessage{
				{Role: "system", Content: sysPrompt},
				{Role: "user", Content: ub.String()},
			},
			Temperature: &temp,
			MaxTokens:   140,
		})
		cancel()
		if err != nil {
			s.log.Warn("travel journal model call failed, using fallback", "role", role, "err", err)
		} else if resp != nil {
			content = cleanDiaryOutput(resp.Content)
		}
	}

	if content == "" {
		content = fallbackDiaryLine(st, city, kind, localNow)
	}

	return &model.TravelJournalEntry{
		CityID:    city.ID,
		CityName:  city.NameZH,
		Country:   city.CountryZH,
		Status:    st.Status,
		Weather:   st.WeatherSummary,
		LocalTime: localTimeStr,
		Kind:      kind,
		Content:   content,
		CreatedAt: now,
	}
}

func (s *Service) maybeDispatchProactive(ctx context.Context, st *model.TravelState, city model.TravelCity, kind, extra string, now time.Time) {
	s.mu.RLock()
	cfg := s.cfg
	groupLoc := s.groupLoc
	disp := s.dispatcher
	filter := s.chanFilter
	s.mu.RUnlock()

	if !cfg.ProactiveEnabled || disp == nil || len(cfg.ProactiveChannels) == 0 {
		return
	}
	if !st.LastProactiveAt.IsZero() && now.Sub(st.LastProactiveAt) < time.Duration(cfg.ProactiveMinGapHours*float64(time.Hour)) {
		return
	}
	if inQuietHours(now.In(groupLoc).Hour(), cfg.ProactiveQuietStartHour, cfg.ProactiveQuietEndHour) {
		return
	}

	targets := s.resolveProactiveChannels(ctx, cfg.ProactiveChannels, filter)
	if len(targets) == 0 {
		return
	}

	sentAny := false
	for _, ch := range targets {
		prompt := s.buildProactivePrompt(ctx, ch, st, city, kind, extra)
		disp.DispatchSystem(model.SystemTriggerEvent{
			ChannelID: ch,
			Kind:      "travel",
			Prompt:    prompt,
			CreatorID: "travel",
			CreatedAt: now,
		})
		sentAny = true
	}
	if sentAny {
		st.LastProactiveAt = now
		_ = s.store.SaveTravelState(ctx, st)
		s.setCachedState(st)
	}
}

func (s *Service) buildProactivePrompt(ctx context.Context, ch model.ChannelID, st *model.TravelState, city model.TravelCity, kind, extra string) string {
	var b strings.Builder
	switch kind {
	case "extreme_weather":
		fmt.Fprintf(&b, "你正在环球旅行，目前在 %s·%s，当地刚遇到 %s（天气：%s）。如果觉得值得在群里随口吐槽一句就发 1~2 条超短口语（绝不写攻略或汇报腔），没必要打扰就输出 [[skip]]。",
			city.CountryZH, city.NameZH, extra, st.WeatherSummary)
	default:
		fmt.Fprintf(&b, "你刚乘坐%s落地 %s·%s（当地天气：%s）。按群友平时随口冒泡的语气发 1~2 条很短的话（比如吐槽航班/天气/刚出机场的体感，像真人发群消息，绝不要写“大家好我到了XX”之类的导游腔）；若觉得此刻不适合打扰群里可直接输出 [[skip]]。",
			transportModeZH(st.TransportMode), city.CountryZH, city.NameZH, emptyOr(st.WeatherSummary, "未知"))
	}
	if sgs, err := s.store.ListChannelSuggestionsForCity(ctx, ch, city.ID); err == nil && len(sgs) > 0 && sgs[0].MemberName != "" {
		fmt.Fprintf(&b, "（顺带一提，之前本群的 %s 推荐你来这里，可以顺口 cue 一下或回旋镖吐槽一句。）", sgs[0].MemberName)
	}
	return b.String()
}

func (s *Service) resolveProactiveChannels(ctx context.Context, configured []string, filter ChannelFilter) []model.ChannelID {
	chs, err := s.store.ListChannels(ctx)
	if err != nil {
		return nil
	}
	wantAll := false
	allowSet := make(map[string]bool, len(configured))
	for _, raw := range configured {
		v := strings.TrimSpace(raw)
		if v == "*" {
			wantAll = true
		} else if v != "" {
			allowSet[v] = true
		}
	}
	var out []model.ChannelID
	for _, c := range chs {
		if !c.ChannelID.IsGroup() {
			continue
		}
		if filter != nil && !filter.ChannelAllowed(c.ChannelID) {
			continue
		}
		if wantAll || allowSet[string(c.ChannelID)] || allowSet[c.ChannelID.NativeID()] || allowSet["group:"+c.ChannelID.NativeID()] {
			out = append(out, c.ChannelID)
		}
	}
	return out
}

func inQuietHours(hour, start, end int) bool {
	if start == end {
		return false
	}
	if start > end {
		return hour >= start || hour < end
	}
	return hour >= start && hour < end
}

func randomStayDays(minDays, maxDays int) int {
	if minDays <= 0 {
		minDays = 3
	}
	if maxDays < minDays {
		maxDays = minDays
	}
	if maxDays == minDays {
		return minDays
	}
	return minDays + rand.Intn(maxDays-minDays+1)
}

func compactPersonaHint(persona string) string {
	persona = strings.TrimSpace(persona)
	if persona == "" {
		return ""
	}
	lines := strings.Split(persona, "\n")
	var kept []string
	totalRunes := 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		r := len([]rune(l))
		if totalRunes+r > 260 {
			break
		}
		kept = append(kept, l)
		totalRunes += r
	}
	return strings.Join(kept, " ")
}

func cleanDiaryOutput(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.ReplaceAll(s, "\r\n", "\n")
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		// Take first non-empty line
		for _, part := range strings.Split(s, "\n") {
			p := strings.TrimSpace(part)
			if p != "" {
				s = p
				break
			}
		}
	}
	s = strings.Trim(s, `"'“”「」[] `)
	return truncateRunes(s, 80)
}

func fallbackDiaryLine(st *model.TravelState, city model.TravelCity, kind string, localNow time.Time) string {
	switch kind {
	case "arrival":
		if st.WeatherSummary != "" {
			return fmt.Sprintf("刚落地%s，外面%s，先找地方把行李扔下喘口气", city.NameZH, st.WeatherSummary)
		}
		return fmt.Sprintf("刚到%s，折腾一路有点晕，先找口热饭吃", city.NameZH)
	case "departure":
		if nc, ok := CityByID(st.NextCityID); ok {
			return fmt.Sprintf("收拾完行李出发去%s，在候车/登机口边发呆边等", nc.NameZH)
		}
		return "准备出发去下一站，又在赶路摸鱼中"
	case "extreme_weather":
		return fmt.Sprintf("%s这天气有点离谱（%s），直接窝室内摆烂了", city.NameZH, st.WeatherSummary)
	default:
		if localNow.Hour() < 11 {
			return fmt.Sprintf("在%s刚起，街边买杯喝的慢悠悠晃荡", city.NameZH)
		}
		if localNow.Hour() >= 20 {
			return fmt.Sprintf("%s天黑了，溜达完准备回去躺平刷手机", city.NameZH)
		}
		return fmt.Sprintf("在%s随便逛了逛，%s，顺手摸会儿鱼", city.NameZH, emptyOr(city.Vibe, "街上挺热闹"))
	}
}

func formatRecommenderHint(sg model.TravelSuggestion) string {
	if sg.MemberName == "" {
		return ""
	}
	if sg.Note != "" {
		return fmt.Sprintf("彩蛋: 本群群友 %s 之前推荐来这里（理由: %s）", sg.MemberName, sg.Note)
	}
	return fmt.Sprintf("彩蛋: 本群群友 %s 之前推荐来这里", sg.MemberName)
}

func formatTimezoneDelta(now time.Time, groupLoc, cityLoc *time.Location) string {
	_, gOff := now.In(groupLoc).Zone()
	_, cOff := now.In(cityLoc).Zone()
	diffMin := (cOff - gOff) / 60
	if diffMin == 0 {
		return "与群里无时差"
	}
	sign := "快"
	if diffMin < 0 {
		sign = "慢"
		diffMin = -diffMin
	}
	h := diffMin / 60
	m := diffMin % 60
	if m == 0 {
		return fmt.Sprintf("比群里%s%d小时", sign, h)
	}
	return fmt.Sprintf("比群里%s%d小时%d分", sign, h, m)
}

func formatShortDuration(d time.Duration) string {
	if d <= 0 {
		return "即将到达"
	}
	mins := int(d.Minutes())
	if mins < 60 {
		return fmt.Sprintf("%d分钟", mins)
	}
	h := mins / 60
	m := mins % 60
	if m == 0 {
		return fmt.Sprintf("%d小时", h)
	}
	return fmt.Sprintf("%d小时%d分", h, m)
}

func periodZH(h int) string {
	switch {
	case h < 5:
		return "凌晨"
	case h < 8:
		return "清晨"
	case h < 11:
		return "上午"
	case h < 13:
		return "中午"
	case h < 17:
		return "下午"
	case h < 19:
		return "傍晚"
	case h < 23:
		return "晚上"
	default:
		return "深夜"
	}
}

func emptyOr(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
