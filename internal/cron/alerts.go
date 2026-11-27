package cron

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

// PriceQuote carries the minimal live quote needed by the zero-LLM price alert watcher.
type PriceQuote struct {
	Symbol    string
	Name      string
	Price     float64
	ChangePct float64
	Market    string // "cn" | "hk" | "us" | "crypto" | "fx" | "fund"
	Status    string
}

// PriceFetcher decouples internal/cron from internal/tool (preserving the strict DAG).
type PriceFetcher interface {
	FetchLivePrices(ctx context.Context, symbols []string) map[string]PriceQuote
	ResolveAlertSymbol(ctx context.Context, raw string) (symbol, displayName, market string, err error)
}

// SetPriceFetcher attaches the pure-Go live price provider for background alert evaluation.
func (s *Scheduler) SetPriceFetcher(pf PriceFetcher) {
	s.mu.Lock()
	s.priceFetcher = pf
	s.mu.Unlock()
}

func (s *Scheduler) getPriceFetcher() PriceFetcher {
	s.mu.RLock()
	pf := s.priceFetcher
	s.mu.RUnlock()
	return pf
}

// CreatePriceAlert validates and persists a zero-LLM market price/change alert.
func (s *Scheduler) CreatePriceAlert(ctx context.Context, ch model.ChannelID, creatorID, rawSymbol, condition string, threshold float64, cooldownMin int, prompt string) (*model.PriceAlert, error) {
	rawSymbol = strings.TrimSpace(rawSymbol)
	if rawSymbol == "" {
		return nil, errors.New("price_alert: symbol is required")
	}
	cond := strings.ToLower(strings.TrimSpace(condition))
	switch cond {
	case "above", "below", "pct_up", "pct_down", "pct_abs":
	default:
		return nil, fmt.Errorf("price_alert: invalid condition %q (use above|below|pct_up|pct_down|pct_abs)", condition)
	}
	if math.IsNaN(threshold) || math.IsInf(threshold, 0) {
		return nil, errors.New("price_alert: threshold must be a finite number")
	}
	if (cond == "above" || cond == "below") && threshold <= 0 {
		return nil, errors.New("price_alert: price threshold must be > 0")
	}
	if (cond == "pct_up" || cond == "pct_down" || cond == "pct_abs") && threshold <= 0 {
		threshold = math.Abs(threshold)
		if threshold == 0 {
			return nil, errors.New("price_alert: percentage threshold must be > 0")
		}
	}
	if cooldownMin < 0 {
		cooldownMin = 0
	}

	sym := rawSymbol
	name := rawSymbol
	market := "us"
	if pf := s.getPriceFetcher(); pf != nil {
		if rSym, rName, rMkt, err := pf.ResolveAlertSymbol(ctx, rawSymbol); err == nil && rSym != "" {
			sym, name, market = rSym, rName, rMkt
		}
	}

	alert := &model.PriceAlert{
		ChannelID:   ch,
		CreatorID:   creatorID,
		Symbol:      sym,
		DisplayName: name,
		Market:      market,
		Condition:   cond,
		Threshold:   threshold,
		CooldownMin: cooldownMin,
		Prompt:      strings.TrimSpace(prompt),
		Enabled:     true,
	}
	if err := s.store.CreatePriceAlert(ctx, alert, 20); err != nil {
		return nil, err
	}
	return alert, nil
}

// CheckPriceAlerts polls active alerts in pure Go (zero LLM calls unless a condition hits).
func (s *Scheduler) CheckPriceAlerts(ctx context.Context, now time.Time) {
	pf := s.getPriceFetcher()
	if pf == nil || s.store == nil {
		return
	}
	alerts, err := s.store.AllEnabledPriceAlerts(ctx)
	if err != nil || len(alerts) == 0 {
		return
	}

	// Filter to eligible alerts (allowed channel, market open, cooldown elapsed) and dedup symbols.
	var eligible []model.PriceAlert
	symSet := make(map[string]struct{})
	var syms []string
	for _, a := range alerts {
		if s.chanFilter != nil && !s.chanFilter.ChannelAllowed(a.ChannelID) {
			continue
		}
		if !IsMarketOpen(a.Market, now) {
			continue
		}
		if a.CooldownMin > 0 && !a.LastTriggeredAt.IsZero() && now.Sub(a.LastTriggeredAt) < time.Duration(a.CooldownMin)*time.Minute {
			continue
		}
		eligible = append(eligible, a)
		if _, exists := symSet[a.Symbol]; !exists {
			symSet[a.Symbol] = struct{}{}
			syms = append(syms, a.Symbol)
		}
	}
	if len(eligible) == 0 {
		return
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	quotes := pf.FetchLivePrices(fetchCtx, syms)
	cancel()
	if len(quotes) == 0 {
		return
	}

	for _, a := range eligible {
		q, ok := quotes[a.Symbol]
		if !ok || q.Price <= 0 {
			continue
		}
		if !evalAlertCondition(a.Condition, a.Threshold, q.Price, q.ChangePct) {
			continue
		}
		keepEnabled := a.CooldownMin > 0
		if err := s.store.MarkPriceAlertTriggered(ctx, a.ID, now, keepEnabled); err != nil {
			s.log.Warn("mark price alert triggered failed", "id", a.ID, "err", err)
			continue
		}
		if s.dispatcher != nil {
			name := a.DisplayName
			if q.Name != "" {
				name = q.Name
			}
			msg := fmt.Sprintf("价格预警 #%d 触发: %s (%s) 现价 %.4g (当日 %+.2f%%)，命中条件 %s %.4g。",
				a.ID, name, a.Symbol, q.Price, q.ChangePct, formatConditionCN(a.Condition), a.Threshold)
			if a.Prompt != "" {
				msg += " 备注: " + a.Prompt
			}
			s.dispatcher.DispatchSystem(model.SystemTriggerEvent{
				ChannelID: a.ChannelID,
				Kind:      "price_alert",
				Prompt:    msg,
				CreatorID: a.CreatorID,
				CreatedAt: now,
			})
		}
	}
}

func evalAlertCondition(cond string, threshold, price, changePct float64) bool {
	switch cond {
	case "above":
		return price >= threshold
	case "below":
		return price <= threshold
	case "pct_up":
		return changePct >= threshold
	case "pct_down":
		return changePct <= -math.Abs(threshold)
	case "pct_abs":
		return math.Abs(changePct) >= math.Abs(threshold)
	default:
		return false
	}
}

func formatConditionCN(cond string) string {
	switch cond {
	case "above":
		return "涨破/达到"
	case "below":
		return "跌破/触及"
	case "pct_up":
		return "日涨幅达到+"
	case "pct_down":
		return "日跌幅达到-"
	case "pct_abs":
		return "日振幅达到±"
	default:
		return cond
	}
}

// --- Trading Calendar & Market Hours (CN / HK / US / Crypto / FX) ---

var (
	shanghaiLoc = mustLoadLoc("Asia/Shanghai", 8*3600)
	newYorkLoc  = mustLoadLoc("America/New_York", -5*3600)
)

func mustLoadLoc(name string, fallbackOffset int) *time.Location {
	if loc, err := time.LoadLocation(name); err == nil {
		return loc
	}
	return time.FixedZone(name, fallbackOffset)
}

// Major fixed/known market holiday dates (YYYY-MM-DD) for 2025-2027 so scheduled briefings & alerts stay quiet.
var cnHolidays = map[string]bool{
	// 2025
	"2025-01-01": true, "2025-01-28": true, "2025-01-29": true, "2025-01-30": true, "2025-01-31": true,
	"2025-02-03": true, "2025-02-04": true, "2025-04-04": true, "2025-05-01": true, "2025-05-02": true,
	"2025-05-05": true, "2025-06-02": true, "2025-10-01": true, "2025-10-02": true, "2025-10-03": true,
	"2025-10-06": true, "2025-10-07": true, "2025-10-08": true,
	// 2026
	"2026-01-01": true, "2026-01-02": true, "2026-02-16": true, "2026-02-17": true, "2026-02-18": true,
	"2026-02-19": true, "2026-02-20": true, "2026-02-23": true, "2026-04-06": true, "2026-05-01": true,
	"2026-05-04": true, "2026-05-05": true, "2026-06-19": true, "2026-09-25": true,
	"2026-10-01": true, "2026-10-02": true, "2026-10-05": true, "2026-10-06": true, "2026-10-07": true,
	// 2027
	"2027-01-01": true, "2027-02-05": true, "2027-02-08": true, "2027-02-09": true, "2027-02-10": true,
	"2027-02-11": true, "2027-04-05": true, "2027-05-03": true, "2027-06-09": true,
	"2027-10-01": true, "2027-10-04": true, "2027-10-05": true, "2027-10-06": true, "2027-10-07": true,
}

var hkHolidays = map[string]bool{
	"2025-01-01": true, "2025-01-29": true, "2025-01-30": true, "2025-01-31": true, "2025-04-04": true,
	"2025-04-18": true, "2025-04-21": true, "2025-05-01": true, "2025-05-05": true, "2025-07-01": true,
	"2025-10-01": true, "2025-10-07": true, "2025-10-29": true, "2025-12-25": true, "2025-12-26": true,
	"2026-01-01": true, "2026-02-17": true, "2026-02-18": true, "2026-02-19": true, "2026-04-03": true,
	"2026-04-06": true, "2026-04-07": true, "2026-05-01": true, "2026-05-25": true, "2026-06-19": true,
	"2026-07-01": true, "2026-09-26": true, "2026-10-01": true, "2026-10-19": true, "2026-12-25": true,
	"2027-01-01": true, "2027-02-08": true, "2027-02-09": true, "2027-03-26": true, "2027-03-29": true,
	"2027-04-05": true, "2027-05-01": true, "2027-07-01": true, "2027-10-01": true, "2027-12-27": true,
}

var usHolidays = map[string]bool{
	"2025-01-01": true, "2025-01-20": true, "2025-02-17": true, "2025-04-18": true, "2025-05-26": true,
	"2025-06-19": true, "2025-07-04": true, "2025-09-01": true, "2025-11-27": true, "2025-12-25": true,
	"2026-01-01": true, "2026-01-19": true, "2026-02-16": true, "2026-04-03": true, "2026-05-25": true,
	"2026-06-19": true, "2026-07-03": true, "2026-09-07": true, "2026-11-26": true, "2026-12-25": true,
	"2027-01-01": true, "2027-01-18": true, "2027-02-15": true, "2027-03-26": true, "2027-05-31": true,
	"2027-06-18": true, "2027-07-05": true, "2027-09-06": true, "2027-11-25": true, "2027-12-24": true,
}

// IsTradingDay checks whether the local market date is a weekday and not a known holiday.
func IsTradingDay(market string, now time.Time) bool {
	switch strings.ToLower(strings.TrimSpace(market)) {
	case "", "crypto":
		return true
	case "cn", "fund":
		t := now.In(shanghaiLoc)
		if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
			return false
		}
		return !cnHolidays[t.Format("2006-01-02")]
	case "hk":
		t := now.In(shanghaiLoc)
		if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
			return false
		}
		return !hkHolidays[t.Format("2006-01-02")]
	case "us":
		t := now.In(newYorkLoc)
		if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
			return false
		}
		return !usHolidays[t.Format("2006-01-02")]
	case "fx":
		u := now.UTC()
		if u.Weekday() == time.Saturday {
			return false
		}
		if u.Weekday() == time.Sunday && u.Hour() < 22 {
			return false
		}
		return true
	default:
		return true
	}
}

// IsMarketOpen checks whether the target market is currently in trading hours (with small auction/pre-market buffer).
func IsMarketOpen(market string, now time.Time) bool {
	m := strings.ToLower(strings.TrimSpace(market))
	if !IsTradingDay(m, now) {
		return false
	}
	switch m {
	case "crypto":
		return true
	case "fx":
		return true
	case "cn", "fund":
		t := now.In(shanghaiLoc)
		mins := t.Hour()*60 + t.Minute()
		// 09:15-11:35, 12:55-15:05 CST
		return (mins >= 9*60+15 && mins <= 11*60+35) || (mins >= 12*60+55 && mins <= 15*60+5)
	case "hk":
		t := now.In(shanghaiLoc)
		mins := t.Hour()*60 + t.Minute()
		// 09:20-12:05, 12:55-16:10 HKT
		return (mins >= 9*60+20 && mins <= 12*60+5) || (mins >= 12*60+55 && mins <= 16*60+10)
	case "us":
		t := now.In(newYorkLoc)
		mins := t.Hour()*60 + t.Minute()
		// Regular + extended buffer: 09:00-16:30 ET
		return mins >= 9*60 && mins <= 16*60+30
	default:
		return true
	}
}
