package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	"github.com/iMMIQ/fatfish/internal/cron"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/netutil"
)

// --- 6. quote (Pure-Go Multi-Asset Market, Indicators, Chart, Market Pulse, News & Fundamentals) ---

type QuoteArgs struct {
	Mode      string `json:"mode,omitempty" jsonschema:"enum=quote,enum=market,enum=news,enum=fundamentals,enum=search,description=Operation mode: quote (default realtime+K-line+indicators), market (A/HK/US indices+breadth+sectors+capital flow), news (realtime CLS/Eastmoney finance telegraph), fundamentals (F10 financials+valuation), search (resolve Chinese/English name to ticker)"`
	Symbol    string `json:"symbol,omitempty" jsonschema:"description=Single or comma-separated tickers or names (up to 5), e.g. 'usNVDA', 'sh600519,sz000858', '中芯国际', '510300', 'hk00700', '恒生科技', '标普500', 'BTC', '黄金', 'USDCNY', 'fund:110011'"`
	Days      int    `json:"days,omitempty" jsonschema:"description=Number of K-line bars to fetch (default 20, max 250; use >=60 for MA60 or 250 for 1Y/YTD)"`
	Period    string `json:"period,omitempty" jsonschema:"enum=day,enum=week,enum=month,description=K-line period (default day)"`
	ChartMode string `json:"chart_mode,omitempty" jsonschema:"enum=none,enum=kline,enum=compare,description=Optional direct PNG chart output without copying OHLCV: kline (candlestick+MA+volume) or compare (normalized % return comparison for 2-5 symbols)"`
	Query     string `json:"query,omitempty" jsonschema:"description=Optional search keyword for mode=search or keyword filter for mode=news"`
}

type QuoteTool struct {
	mu       sync.RWMutex
	client   *http.Client
	renderer *ChartRenderer
}

func NewQuoteTool(allowHosts []string, renderer *ChartRenderer) *QuoteTool {
	return &QuoteTool{
		client: netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
			Timeout:    12 * time.Second,
			AllowHosts: allowHosts,
		}),
		renderer: renderer,
	}
}

func (t *QuoteTool) UpdateAllowHosts(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{Timeout: 12 * time.Second, AllowHosts: allowHosts})
	t.mu.Lock()
	t.client = cli
	t.mu.Unlock()
}

func (t *QuoteTool) httpClient() *http.Client {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	return c
}

func (t *QuoteTool) Name() string   { return "quote" }
func (t *QuoteTool) ReadOnly() bool { return true }
func (t *QuoteTool) Description() string {
	return "Pure-Go financial intelligence tool: realtime quotes + K-lines + auto technical indicators + direct PNG K-line/comparison charts for A-share/ETF/BSE, HK, US, indices, crypto (BTC/ETH/SOL), FX (USDCNY/DXY), commodities (gold/oil), and CN mutual funds; also supports fuzzy name search, market pulse (breadth/sectors/southbound), realtime finance news telegraph, and company F10 fundamentals."
}

type quoteSnapshot struct {
	Symbol       string
	Name         string
	Code         string
	Market       string // "cn" | "hk" | "us" | "crypto" | "fx" | "fund"
	Price        float64
	PrevClose    float64
	Open         float64
	High         float64
	Low          float64
	Change       float64
	ChangePct    float64
	Volume       string
	Turnover     string
	TurnoverRate string
	PE           string
	PB           string
	TotalMktCap  string
	FloatMktCap  string
	High52W      string
	Low52W       string
	FundingRate  string
	Timestamp    string
	Status       string
	ExtraNote    string
	Candles      []Candle
}

func (t *QuoteTool) Execute(ctx context.Context, _ ToolContext, args QuoteArgs) (ToolResult, error) {
	mode := strings.ToLower(strings.TrimSpace(args.Mode))
	if mode == "" {
		mode = "quote"
	}

	switch mode {
	case "market":
		out, err := t.execMarketPulse(ctx)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: netutil.WrapExternalData("quote:market", out)}, nil

	case "news":
		kw := strings.TrimSpace(args.Query)
		if kw == "" {
			kw = strings.TrimSpace(args.Symbol)
		}
		out, err := t.execFinanceNews(ctx, kw)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: netutil.WrapExternalData("quote:news", out)}, nil

	case "search":
		kw := strings.TrimSpace(args.Query)
		if kw == "" {
			kw = strings.TrimSpace(args.Symbol)
		}
		if kw == "" {
			return ToolResult{}, errors.New("quote(mode=search): query or symbol is required")
		}
		hits, err := t.searchSymbols(ctx, kw, 8)
		if err != nil {
			return ToolResult{}, err
		}
		if len(hits) == 0 {
			return ToolResult{TextForLLM: fmt.Sprintf("No matching symbols found for %q.", kw)}, nil
		}
		var b strings.Builder
		fmt.Fprintf(&b, "Search results for %q:\n", kw)
		for _, h := range hits {
			fmt.Fprintf(&b, "- %s | %s (%s)\n", h.Symbol, h.Name, h.Market)
		}
		return ToolResult{TextForLLM: netutil.WrapExternalData("quote:search", b.String())}, nil

	case "fundamentals":
		rawSym := strings.TrimSpace(args.Symbol)
		if rawSym == "" {
			rawSym = strings.TrimSpace(args.Query)
		}
		if rawSym == "" {
			return ToolResult{}, errors.New("quote(mode=fundamentals): symbol is required")
		}
		out, err := t.execFundamentals(ctx, rawSym)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: netutil.WrapExternalData("quote:fundamentals:"+rawSym, out)}, nil

	default:
		return t.execQuotes(ctx, args)
	}
}

func (t *QuoteTool) execQuotes(ctx context.Context, args QuoteArgs) (ToolResult, error) {
	rawInput := strings.TrimSpace(args.Symbol)
	if rawInput == "" {
		rawInput = strings.TrimSpace(args.Query)
	}
	if rawInput == "" {
		return ToolResult{}, errors.New("quote: symbol is required")
	}

	rawList := splitSymbols(rawInput)
	if len(rawList) == 0 {
		return ToolResult{}, errors.New("quote: symbol is required")
	}
	if len(rawList) > 5 {
		rawList = rawList[:5]
	}

	days := args.Days
	if days <= 0 {
		days = 20
	}
	if days > 250 {
		days = 250
	}
	period := strings.ToLower(strings.TrimSpace(args.Period))
	if period != "week" && period != "month" {
		period = "day"
	}
	chartMode := strings.ToLower(strings.TrimSpace(args.ChartMode))
	if chartMode == "" {
		chartMode = "none"
	}
	// Ensure enough bars when charting or comparing so MAs look meaningful.
	if chartMode == "kline" && days < 30 {
		days = 30
	}
	if chartMode == "compare" && days < 30 {
		days = 30
	}

	snaps := make([]*quoteSnapshot, len(rawList))
	errs := make([]error, len(rawList))
	g, gctx := errgroup.WithContext(ctx)
	for i, raw := range rawList {
		idx, item := i, raw
		g.Go(func() error {
			sn, err := t.fetchOneFull(gctx, item, period, days)
			snaps[idx] = sn
			errs[idx] = err
			return nil
		})
	}
	_ = g.Wait()

	var okSnaps []*quoteSnapshot
	var b strings.Builder
	for i, sn := range snaps {
		if errs[i] != nil || sn == nil {
			fmt.Fprintf(&b, "[%s] Error: %v\n\n", rawList[i], errs[i])
			continue
		}
		okSnaps = append(okSnaps, sn)
		b.WriteString(formatSnapshotReport(sn, period, len(rawList) == 1))
		b.WriteString("\n")
	}
	if len(okSnaps) == 0 {
		return ToolResult{}, fmt.Errorf("quote: failed for all symbols (%v)", errs[0])
	}

	// Render direct PNG charts when requested.
	var media []model.DirectMedia
	if t.renderer != nil && chartMode != "none" {
		switch chartMode {
		case "kline":
			for _, sn := range okSnaps {
				if len(sn.Candles) < 2 || len(media) >= 4 {
					continue
				}
				title := fmt.Sprintf("%s (%s) %s K线", sn.Name, sn.Symbol, periodLabelCN(period))
				sub := fmt.Sprintf("现价 %s (%+.2f%%)  %s", formatNum(sn.Price), sn.ChangePct, sn.Timestamp)
				if pngBytes, err := t.renderer.RenderKlineWithSubtitle(title, strings.TrimSpace(sub), sn.Candles); err == nil {
					media = append(media, model.DirectMedia{
						Data:     pngBytes,
						MimeType: "image/png",
						Filename: "kline_" + sanitizeFilename(sn.Symbol) + ".png",
						Caption:  title,
					})
				}
			}
		case "compare":
			if len(okSnaps) >= 2 {
				labels, series := alignCompareSeries(okSnaps)
				if len(series) >= 2 && len(labels) >= 2 {
					names := make([]string, len(series))
					for i, s := range series {
						names[i] = s.Name
					}
					title := fmt.Sprintf("相对收益对比（%s）", strings.Join(names, " vs "))
					if pngBytes, err := t.renderer.RenderCompare(title, labels, series); err == nil {
						media = append(media, model.DirectMedia{
							Data:     pngBytes,
							MimeType: "image/png",
							Filename: "compare.png",
							Caption:  title,
						})
					}
				}
			}
		}
	}

	if chartMode != "none" {
		if len(media) > 0 {
			names := make([]string, len(media))
			for i, m := range media {
				if m.Caption != "" {
					names[i] = m.Caption
				} else {
					names[i] = m.Filename
				}
			}
			fmt.Fprintf(&b, "\n[图表已生成: 共 %d 张 (%s)，将在你回复后自动发送到群里]\n", len(media), strings.Join(names, "；"))
		} else {
			fmt.Fprintf(&b, "\n[图表未生成: chart_mode=%s 需要足够的有效 K 线数据（compare 模式至少需要 2 个标的）]\n", chartMode)
		}
	}

	return ToolResult{
		TextForLLM:  netutil.WrapExternalData("quote:"+strings.Join(rawList, ","), strings.TrimSpace(b.String())),
		DirectMedia: media,
	}, nil
}

// --- cron.PriceFetcher implementation (Zero-LLM Alert Watcher) ---

// ResolveAlertSymbol resolves a user-supplied ticker or Chinese name into a canonical symbol + market.
func (t *QuoteTool) ResolveAlertSymbol(ctx context.Context, raw string) (symbol, displayName, market string, err error) {
	sn, err := t.fetchOneQuoteOnly(ctx, raw)
	if err != nil {
		return "", "", "", err
	}
	return sn.Symbol, sn.Name, sn.Market, nil
}

// FetchLivePrices batch-fetches current prices for alert evaluation with minimal network calls.
func (t *QuoteTool) FetchLivePrices(ctx context.Context, symbols []string) map[string]cron.PriceQuote {
	out := make(map[string]cron.PriceQuote, len(symbols))
	if len(symbols) == 0 {
		return out
	}

	var tencentSyms []string
	var otherSyms []string
	for _, s := range symbols {
		if strings.HasPrefix(s, "crypto:") || strings.HasPrefix(s, "em:") || strings.HasPrefix(s, "fund:") {
			otherSyms = append(otherSyms, s)
		} else {
			tencentSyms = append(tencentSyms, s)
		}
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)

	if len(tencentSyms) > 0 {
		g.Go(func() error {
			batch, err := t.fetchTencentBatch(gctx, tencentSyms)
			if err != nil {
				return nil
			}
			mu.Lock()
			for sym, sn := range batch {
				out[sym] = cron.PriceQuote{
					Symbol:    sym,
					Name:      sn.Name,
					Price:     sn.Price,
					ChangePct: sn.ChangePct,
					Market:    sn.Market,
					Status:    sn.Status,
				}
			}
			mu.Unlock()
			return nil
		})
	}

	for _, sym := range otherSyms {
		s := sym
		g.Go(func() error {
			if sn, err := t.fetchOneQuoteOnly(gctx, s); err == nil && sn != nil && sn.Price > 0 {
				mu.Lock()
				out[s] = cron.PriceQuote{
					Symbol:    s,
					Name:      sn.Name,
					Price:     sn.Price,
					ChangePct: sn.ChangePct,
					Market:    sn.Market,
					Status:    sn.Status,
				}
				mu.Unlock()
			}
			return nil
		})
	}
	_ = g.Wait()
	return out
}

// --- Normalization, Classification & Fuzzy Search ---

type symbolHit struct {
	Symbol string
	Name   string
	Market string
}

func splitSymbols(s string) []string {
	s = strings.NewReplacer("，", ",", "、", ",", ";", ",", "；", ",").Replace(s)
	parts := strings.Split(s, ",")
	var out []string
	seen := make(map[string]struct{})
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// normalizeTicker normalizes common tickers and aliases across CN/HK/US/Crypto/FX/Commodities/Funds.
// Returns canonical symbol or "" when empty. For unrecognized Chinese company names, returns "search:<raw>".
func normalizeTicker(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	low := strings.ToLower(s)

	// Already explicitly prefixed custom namespaces.
	if strings.HasPrefix(low, "crypto:") {
		base := strings.ToUpper(strings.TrimSuffix(strings.TrimSuffix(s[7:], "USDT"), "-USDT"))
		return "crypto:" + base
	}
	if strings.HasPrefix(low, "em:") {
		return "em:" + s[3:]
	}
	if strings.HasPrefix(low, "fund:") {
		return "fund:" + strings.TrimSpace(s[5:])
	}

	// Common index, crypto, FX, and commodity aliases.
	switch low {
	// HK indices
	case "hstech", "恒生科技", "恒生科技指数", "恒科":
		return "hkHSTECH"
	case "hsi", "恒生指数", "恒指":
		return "hkHSI"
	case "hscei", "国企指数", "恒生国企":
		return "hkHSCEI"
	// CN indices
	case "上证", "上证指数", "大盘", "sse", "sh000001":
		return "sh000001"
	case "深证", "深证成指", "深成指":
		return "sz399001"
	case "创业板", "创业板指":
		return "sz399006"
	case "科创50", "科创板":
		return "sh000688"
	case "沪深300", "csi300":
		return "sh000300"
	case "中证500", "csi500":
		return "sh000905"
	case "中证1000", "csi1000":
		return "sh000852"
	case "北证50", "bj899050":
		return "bj899050"
	// US & Global indices
	case "spx", "gspc", "标普", "标普500", "us.inx":
		return "us.INX"
	case "ndx", "纳指100", "纳斯达克100", "us.ndx":
		return "us.NDX"
	case "ixic", "纳指", "纳斯达克", "us.ixic":
		return "us.IXIC"
	case "dji", "djia", "道指", "道琼斯", "us.dji":
		return "us.DJI"
	case "n225", "日经", "日经225":
		return "em:100.N225"
	// Crypto
	case "btc", "btcusdt", "btc-usdt", "比特币", "大饼":
		return "crypto:BTC"
	case "eth", "ethusdt", "eth-usdt", "以太坊", "二饼":
		return "crypto:ETH"
	case "sol", "solusdt", "sol-usdt", "solana":
		return "crypto:SOL"
	case "doge", "dogeusdt", "狗狗币":
		return "crypto:DOGE"
	case "bnb", "bnbusdt":
		return "crypto:BNB"
	case "xrp", "xrpusdt", "瑞波币":
		return "crypto:XRP"
	case "ada", "adausdt":
		return "crypto:ADA"
	case "avax", "avaxusdt":
		return "crypto:AVAX"
	case "ton", "tonusdt":
		return "crypto:TON"
	case "sui", "suiusdt":
		return "crypto:SUI"
	// FX & Commodities (Eastmoney push2 secids)
	case "黄金", "现货黄金", "伦敦金", "comex黄金", "gold", "xau", "xauusd":
		return "em:101.GC00Y"
	case "国内金", "沪金", "au9999", "黄金9999":
		return "em:118.AU9999"
	case "白银", "现货白银", "silver", "xag", "xagusd":
		return "em:101.SI00Y"
	case "原油", "美油", "wti", "cl", "crude":
		return "em:102.CL00Y"
	case "布伦特", "布伦特原油", "brent":
		return "em:102.B00Y"
	case "美元人民币", "在岸人民币", "人民币汇率", "汇率", "usdcny", "usd/cny":
		return "em:120.USDCNY"
	case "离岸人民币", "usdcnh", "usd/cnh", "cnh":
		return "em:133.USDCNH"
	case "美元日元", "usdjpy", "usd/jpy":
		return "em:119.USDJPY"
	case "欧元美元", "eurusd", "eur/usd":
		return "em:119.EURUSD"
	case "美元指数", "dxy", "udi":
		return "em:100.UDI"
	}

	// Crypto pair suffix pattern e.g. PEPEUSDT / LINK-USDT.
	up := strings.ToUpper(s)
	if strings.HasSuffix(up, "USDT") && len(up) > 4 && isASCIIAlphaNum(strings.TrimSuffix(up, "USDT")) {
		return "crypto:" + strings.TrimSuffix(up, "USDT")
	}
	if strings.HasSuffix(up, "-USDT") && len(up) > 5 && isASCIIAlphaNum(strings.TrimSuffix(up, "-USDT")) {
		return "crypto:" + strings.TrimSuffix(up, "-USDT")
	}

	// Standard market prefixes.
	if strings.HasPrefix(low, "us") && len(s) > 2 && isASCIIStockRest(s[2:]) {
		return "us" + strings.ToUpper(s[2:])
	}
	if strings.HasPrefix(low, "hk") && len(s) > 2 {
		rest := s[2:]
		if isAllDigits(rest) {
			return "hk" + rest
		}
		return "hk" + strings.ToUpper(rest)
	}
	if (strings.HasPrefix(low, "sh") || strings.HasPrefix(low, "sz") || strings.HasPrefix(low, "bj")) && len(s) == 8 && isAllDigits(s[2:]) {
		return low
	}

	// Bare 6-digit A-share / ETF / BSE heuristic:
	// 6xxxxx (SH stocks), 5xxxxx (SH ETF/LOF/funds), 900xxx (SH B) -> sh
	// 4xxxxx, 8xxxxx, 92xxxx (Beijing Stock Exchange) -> bj
	// 0xxxxx, 1xxxxx (SZ ETF/LOF), 2xxxxx, 3xxxxx (ChiNext) -> sz
	if len(s) == 6 && isAllDigits(s) {
		switch {
		case strings.HasPrefix(s, "6") || strings.HasPrefix(s, "5") || strings.HasPrefix(s, "900"):
			return "sh" + s
		case strings.HasPrefix(s, "4") || strings.HasPrefix(s, "8") || strings.HasPrefix(s, "92"):
			return "bj" + s
		default:
			return "sz" + s
		}
	}
	// Bare 5-digit HK stock.
	if len(s) == 5 && isAllDigits(s) {
		return "hk" + s
	}
	// Contains CJK characters -> needs fuzzy symbol search!
	if containsCJK(s) {
		return "search:" + s
	}
	// Bare ASCII ticker (e.g. NVDA, AAPL, BRK.B).
	return "us" + strings.ToUpper(s)
}

func inferMarket(sym string) string {
	switch {
	case strings.HasPrefix(sym, "crypto:"):
		return "crypto"
	case strings.HasPrefix(sym, "fund:"):
		return "fund"
	case strings.HasPrefix(sym, "em:"):
		if strings.Contains(sym, "USD") || strings.Contains(sym, "EUR") || strings.Contains(sym, "UDI") ||
			strings.HasPrefix(sym, "em:101.") || strings.HasPrefix(sym, "em:102.") || strings.HasPrefix(sym, "em:118.") {
			return "fx"
		}
		return "us"
	case strings.HasPrefix(sym, "sh") || strings.HasPrefix(sym, "sz") || strings.HasPrefix(sym, "bj"):
		return "cn"
	case strings.HasPrefix(sym, "hk"):
		return "hk"
	case strings.HasPrefix(sym, "us"):
		return "us"
	default:
		return "us"
	}
}

func (t *QuoteTool) resolveSymbol(ctx context.Context, raw string) (string, error) {
	norm := normalizeTicker(raw)
	if norm == "" {
		return "", errors.New("empty symbol")
	}
	if strings.HasPrefix(norm, "search:") {
		q := strings.TrimPrefix(norm, "search:")
		hits, err := t.searchSymbols(ctx, q, 3)
		if err != nil || len(hits) == 0 {
			return "", fmt.Errorf("could not resolve symbol %q", q)
		}
		return hits[0].Symbol, nil
	}
	return norm, nil
}

// searchSymbols searches stocks/ETFs/indices via Tencent Smartbox with Eastmoney Suggest fallback.
func (t *QuoteTool) searchSymbols(ctx context.Context, query string, limit int) ([]symbolHit, error) {
	if limit <= 0 {
		limit = 6
	}
	// 1. Tencent Smartbox
	sbURL := "https://smartbox.gtimg.cn/s3/?v=2&t=all&c=1&q=" + url.QueryEscape(query)
	if raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), sbURL, 256<<10, nil); err == nil && code == 200 {
		if hits := parseTencentSmartbox(string(raw), limit); len(hits) > 0 {
			return hits, nil
		}
	}
	// 2. Eastmoney Suggest fallback
	emURL := "https://searchapi.eastmoney.com/api/suggest/get?input=" + url.QueryEscape(query) +
		"&type=14&token=D43BF722C8E33BDC906FB84D85E326E8&count=" + strconv.Itoa(limit)
	if raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), emURL, 256<<10, nil); err == nil && code == 200 {
		if hits := parseEastmoneySuggest(raw, limit); len(hits) > 0 {
			return hits, nil
		}
	}
	return nil, fmt.Errorf("no search matches for %q", query)
}

func parseTencentSmartbox(raw string, limit int) []symbolHit {
	start := strings.IndexByte(raw, '"')
	end := strings.LastIndexByte(raw, '"')
	if start < 0 || end <= start {
		return nil
	}
	body := raw[start+1 : end]
	if body == "" || body == "N" {
		return nil
	}
	rows := strings.Split(body, "^")
	var out []symbolHit
	for _, row := range rows {
		parts := strings.Split(row, "~")
		if len(parts) < 5 {
			continue
		}
		mkt, code, name := strings.ToLower(parts[0]), parts[1], decodeUnicodeEscapes(parts[2])
		var sym, label string
		switch mkt {
		case "sh", "sz", "bj":
			sym = mkt + code
			label = "A股"
		case "hk":
			sym = "hk" + code
			label = "港股"
		case "us":
			// smartbox gives e.g. "nvda.oq" -> "usNVDA"
			base := code
			if dot := strings.IndexByte(base, '.'); dot > 0 {
				base = base[:dot]
			}
			sym = "us" + strings.ToUpper(base)
			label = "美股"
		case "jj":
			sym = "fund:" + code
			label = "基金"
		default:
			continue
		}
		out = append(out, symbolHit{Symbol: sym, Name: name, Market: label})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func parseEastmoneySuggest(raw []byte, limit int) []symbolHit {
	var parsed struct {
		QuotationCodeTable struct {
			Data []struct {
				Code       string `json:"Code"`
				Name       string `json:"Name"`
				MktNum     string `json:"MktNum"`
				SecurityID string `json:"QuoteID"`
				Classify   string `json:"Classify"`
			} `json:"Data"`
		} `json:"QuotationCodeTable"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}
	var out []symbolHit
	for _, it := range parsed.QuotationCodeTable.Data {
		var sym, label string
		switch it.MktNum {
		case "1":
			sym = "sh" + it.Code
			label = "A股"
		case "0":
			if strings.HasPrefix(it.Code, "4") || strings.HasPrefix(it.Code, "8") || strings.HasPrefix(it.Code, "92") {
				sym = "bj" + it.Code
			} else {
				sym = "sz" + it.Code
			}
			label = "A股"
		case "116":
			sym = "hk" + it.Code
			label = "港股"
		case "105", "106", "107":
			sym = "us" + strings.ToUpper(it.Code)
			label = "美股"
		default:
			if it.SecurityID != "" {
				sym = "em:" + it.SecurityID
				label = it.Classify
			} else {
				continue
			}
		}
		out = append(out, symbolHit{Symbol: sym, Name: it.Name, Market: label})
		if len(out) >= limit {
			break
		}
	}
	return out
}

// --- Core Fetchers ---

func (t *QuoteTool) fetchOneQuoteOnly(ctx context.Context, raw string) (*quoteSnapshot, error) {
	sym, err := t.resolveSymbol(ctx, raw)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasPrefix(sym, "crypto:"):
		return t.fetchCryptoQuote(ctx, strings.TrimPrefix(sym, "crypto:"))
	case strings.HasPrefix(sym, "em:"):
		return t.fetchEastmoneyQuote(ctx, strings.TrimPrefix(sym, "em:"), sym)
	case strings.HasPrefix(sym, "fund:"):
		return t.fetchFundQuote(ctx, strings.TrimPrefix(sym, "fund:"))
	default:
		sn, err := t.fetchTencentSingle(ctx, sym)
		if err == nil && sn != nil {
			return sn, nil
		}
		// Fallback to Eastmoney push2 for A/HK/US.
		if secid := toEastmoneySecID(sym); secid != "" {
			if sn2, err2 := t.fetchEastmoneyQuote(ctx, secid, sym); err2 == nil && sn2 != nil {
				return sn2, nil
			}
		}
		// Fallback to Tiantian Fund if raw was a 6-digit code.
		trimmed := strings.TrimSpace(raw)
		if len(trimmed) == 6 && isAllDigits(trimmed) {
			if sn3, err3 := t.fetchFundQuote(ctx, trimmed); err3 == nil && sn3 != nil {
				return sn3, nil
			}
		}
		return nil, err
	}
}

func (t *QuoteTool) fetchOneFull(ctx context.Context, raw, period string, days int) (*quoteSnapshot, error) {
	sym, err := t.resolveSymbol(ctx, raw)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasPrefix(sym, "crypto:"):
		base := strings.TrimPrefix(sym, "crypto:")
		sn, err := t.fetchCryptoQuote(ctx, base)
		if err != nil {
			return nil, err
		}
		sn.Candles, _ = t.fetchCryptoKlines(ctx, base, period, days)
		return sn, nil

	case strings.HasPrefix(sym, "em:"):
		secid := strings.TrimPrefix(sym, "em:")
		sn, err := t.fetchEastmoneyQuote(ctx, secid, sym)
		if err != nil {
			return nil, err
		}
		sn.Candles, _ = t.fetchEastmoneyKlines(ctx, secid, period, days)
		return sn, nil

	case strings.HasPrefix(sym, "fund:"):
		code := strings.TrimPrefix(sym, "fund:")
		sn, err := t.fetchFundQuote(ctx, code)
		if err != nil {
			return nil, err
		}
		sn.Candles, _ = t.fetchFundHistory(ctx, code, days)
		return sn, nil

	default:
		sn, err := t.fetchTencentSingle(ctx, sym)
		if err != nil || sn == nil {
			// Fallback 1: Eastmoney push2
			if secid := toEastmoneySecID(sym); secid != "" {
				if sn2, err2 := t.fetchEastmoneyQuote(ctx, secid, sym); err2 == nil && sn2 != nil {
					sn2.Candles, _ = t.fetchEastmoneyKlines(ctx, secid, period, days)
					return sn2, nil
				}
			}
			// Fallback 2: 6-digit bare mutual fund code
			trimmed := strings.TrimSpace(raw)
			if len(trimmed) == 6 && isAllDigits(trimmed) {
				if sn3, err3 := t.fetchFundQuote(ctx, trimmed); err3 == nil && sn3 != nil {
					sn3.Candles, _ = t.fetchFundHistory(ctx, trimmed, days)
					return sn3, nil
				}
			}
			return nil, err
		}
		// Fetch K-line from Tencent (with Eastmoney push2his fallback).
		klSym := sym
		if strings.HasPrefix(sym, "us") && strings.Contains(sn.Code, ".") && !strings.HasPrefix(sym, "us.") {
			klSym = "us" + sn.Code
		}
		candles := t.fetchTencentKlines(ctx, klSym, period, days)
		if len(candles) == 0 {
			if secid := toEastmoneySecID(sym); secid != "" {
				candles, _ = t.fetchEastmoneyKlines(ctx, secid, period, days)
			}
		}
		sn.Candles = candles
		return sn, nil
	}
}

func (t *QuoteTool) fetchTencentSingle(ctx context.Context, sym string) (*quoteSnapshot, error) {
	batch, err := t.fetchTencentBatch(ctx, []string{sym})
	if err != nil {
		return nil, err
	}
	sn := batch[sym]
	if sn == nil {
		return nil, fmt.Errorf("quote: no market data returned for %q", sym)
	}
	return sn, nil
}

func (t *QuoteTool) fetchTencentBatch(ctx context.Context, syms []string) (map[string]*quoteSnapshot, error) {
	if len(syms) == 0 {
		return nil, nil
	}
	qtURL := "https://qt.gtimg.cn/q=" + url.QueryEscape(strings.Join(syms, ","))
	raw, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), qtURL, 1<<20, nil)
	if err != nil {
		return nil, err
	}
	utf8Text, err := decodeGBK(raw)
	if err != nil {
		utf8Text = string(raw)
	}
	res := make(map[string]*quoteSnapshot, len(syms))
	for _, line := range strings.Split(utf8Text, ";") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 2 || !strings.HasPrefix(line, "v_") {
			continue
		}
		key := strings.TrimPrefix(line[:eq], "v_")
		if sn := parseTencentQuoteDetail(line, key); sn != nil {
			res[key] = sn
		}
	}
	return res, nil
}

// parseTencentQuoteDetail extracts rich valuation, volume, turnover, 52W range, timestamp, and status.
func parseTencentQuoteDetail(raw, sym string) *quoteSnapshot {
	start := strings.IndexByte(raw, '"')
	end := strings.LastIndexByte(raw, '"')
	if start < 0 || end <= start {
		return nil
	}
	fields := strings.Split(raw[start+1:end], "~")
	if len(fields) < 35 {
		return nil
	}
	price := parseFloatSafe(fields[3])
	if price == 0 && parseFloatSafe(fields[4]) == 0 {
		return nil
	}
	mktCode := fields[0]
	sn := &quoteSnapshot{
		Symbol:    sym,
		Name:      strings.TrimSpace(fields[1]),
		Code:      strings.TrimSpace(fields[2]),
		Market:    inferMarket(sym),
		Price:     price,
		PrevClose: parseFloatSafe(fields[4]),
		Open:      parseFloatSafe(fields[5]),
		Change:    parseFloatSafe(fields[31]),
		ChangePct: parseFloatSafe(fields[32]),
		High:      parseFloatSafe(fields[33]),
		Low:       parseFloatSafe(fields[34]),
		Timestamp: formatTencentTimestamp(getField(fields, 30)),
	}
	if sn.Price == 0 {
		sn.Price = sn.PrevClose
	}

	switch mktCode {
	case "1", "51": // A-share (SH / SZ / BJ)
		sn.Market = "cn"
		if v := parseFloatSafe(getField(fields, 6)); v > 0 {
			sn.Volume = formatCompactVol(v) + "手"
		}
		if amt := parseFloatSafe(getField(fields, 37)); amt > 0 {
			// Tencent A-share fields[37] is in 万元
			if amt >= 1e4 {
				sn.Turnover = fmt.Sprintf("%.2f亿元", amt/1e4)
			} else {
				sn.Turnover = fmt.Sprintf("%.0f万元", amt)
			}
		}
		if tr := cleanNonZero(getField(fields, 38)); tr != "" {
			sn.TurnoverRate = tr + "%"
		}
		sn.PE = cleanNonZero(getField(fields, 39))
		sn.PB = cleanNonZero(getField(fields, 46))
		if fmc := cleanNonZero(getField(fields, 44)); fmc != "" {
			sn.FloatMktCap = fmc + "亿"
		}
		if tmc := cleanNonZero(getField(fields, 45)); tmc != "" {
			sn.TotalMktCap = tmc + "亿"
		}
		if len(fields) > 49 {
			if vr := cleanNonZero(fields[49]); vr != "" {
				sn.ExtraNote = "量比=" + vr
			}
		}
	case "100": // HK
		sn.Market = "hk"
		if v := parseFloatSafe(getField(fields, 6)); v > 0 {
			sn.Volume = formatCompactVol(v) + "股"
		}
		if amt := parseFloatSafe(getField(fields, 37)); amt > 0 {
			if amt >= 1e8 {
				sn.Turnover = fmt.Sprintf("%.2f亿港元", amt/1e8)
			} else if amt >= 1e4 {
				sn.Turnover = fmt.Sprintf("%.1f万港元", amt/1e4)
			}
		}
		sn.PE = cleanNonZero(getField(fields, 39))
		if fmc := cleanNonZero(getField(fields, 44)); fmc != "" {
			sn.FloatMktCap = fmc + "亿HKD"
		}
		if tmc := cleanNonZero(getField(fields, 45)); tmc != "" {
			sn.TotalMktCap = tmc + "亿HKD"
		}
		sn.High52W = cleanNonZero(getField(fields, 48))
		sn.Low52W = cleanNonZero(getField(fields, 49))
	case "200": // US
		sn.Market = "us"
		isUSIndex := strings.HasPrefix(sym, "us.") || strings.HasPrefix(sn.Code, ".")
		if v := parseFloatSafe(getField(fields, 6)); v > 0 {
			unit := "股"
			if isUSIndex {
				unit = ""
			}
			sn.Volume = formatCompactVol(v) + unit
		}
		// Tencent fields[37] for US indices (e.g. .IXIC/.INX/.DJI) is volume * index_points, not dollar turnover.
		if !isUSIndex {
			if amt := parseFloatSafe(getField(fields, 37)); amt > 0 {
				if amt >= 1e8 {
					sn.Turnover = fmt.Sprintf("$%.2f亿", amt/1e8)
				} else if amt >= 1e4 {
					sn.Turnover = fmt.Sprintf("$%.1f万", amt/1e4)
				}
			}
		}
		if tr := cleanNonZero(getField(fields, 38)); tr != "" {
			sn.TurnoverRate = tr + "%"
		}
		sn.PE = cleanNonZero(getField(fields, 39))
		if tmc := cleanNonZero(getField(fields, 45)); tmc != "" {
			sn.TotalMktCap = "$" + tmc + "亿"
		}
		sn.High52W = cleanNonZero(getField(fields, 48))
		sn.Low52W = cleanNonZero(getField(fields, 49))
	}
	sn.Status = inferMarketStatusLabel(sn.Market, time.Now())
	return sn
}

// Keep parseTencentQuote for backwards compatibility with existing tests.
func parseTencentQuote(raw string) (summary string, code string) {
	sn := parseTencentQuoteDetail(raw, "")
	if sn == nil {
		return "", ""
	}
	return fmt.Sprintf("%s (%s): Price=%s | Change=%+.2f%% | Open=%s | PrevClose=%s | High=%s | Low=%s",
		sn.Name, sn.Code, formatNum(sn.Price), sn.ChangePct, formatNum(sn.Open), formatNum(sn.PrevClose), formatNum(sn.High), formatNum(sn.Low)), sn.Code
}

func (t *QuoteTool) fetchTencentKlines(ctx context.Context, klSym, period string, days int) []Candle {
	klURL := fmt.Sprintf("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,,,%d,qfq", url.QueryEscape(klSym), period, days)
	klRaw, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), klURL, 4<<20, nil)
	if err != nil {
		return nil
	}
	return parseTencentKlineCandles(klRaw, klSym, period)
}

func parseTencentKlineCandles(raw []byte, sym, period string) []Candle {
	var parsed struct {
		Data map[string]map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}
	stock := parsed.Data[sym]
	if stock == nil {
		for _, v := range parsed.Data {
			stock = v
			break
		}
	}
	if stock == nil {
		return nil
	}
	keys := []string{"qfq" + period, period, "qfqday", "day"}
	var bars [][]any
	for _, k := range keys {
		if r, ok := stock[k]; ok && json.Unmarshal(r, &bars) == nil && len(bars) > 0 {
			break
		}
	}
	candles := make([]Candle, 0, len(bars))
	for _, bar := range bars {
		if len(bar) < 6 {
			continue
		}
		lbl := fmt.Sprintf("%v", bar[0])
		o := anyToFloat(bar[1])
		c := anyToFloat(bar[2])
		h := anyToFloat(bar[3])
		l := anyToFloat(bar[4])
		v := anyToFloat(bar[5])
		if o <= 0 && c <= 0 {
			continue
		}
		candles = append(candles, Candle{Label: lbl, Open: o, Close: c, High: h, Low: l, Volume: v})
	}
	return candles
}

// Keep parseTencentKline for backwards compatibility with existing tests.
func parseTencentKline(raw []byte, sym string) []string {
	candles := parseTencentKlineCandles(raw, sym, "day")
	lines := make([]string, 0, len(candles))
	for _, cd := range candles {
		lines = append(lines, fmt.Sprintf("%s | O:%.3f C:%.3f H:%.3f L:%.3f V:%.0f", cd.Label, cd.Open, cd.Close, cd.High, cd.Low, cd.Volume))
	}
	return lines
}

// --- Eastmoney Push2 Quote & K-line (Fallback + Global Indices / FX / Commodities) ---

func toEastmoneySecID(sym string) string {
	switch {
	case strings.HasPrefix(sym, "sh") && len(sym) == 8:
		return "1." + sym[2:]
	case strings.HasPrefix(sym, "sz") && len(sym) == 8:
		return "0." + sym[2:]
	case strings.HasPrefix(sym, "bj") && len(sym) == 8:
		return "0." + sym[2:]
	case strings.HasPrefix(sym, "hk") && len(sym) == 7 && isAllDigits(sym[2:]):
		return "116." + sym[2:]
	case sym == "hkHSI":
		return "100.HSI"
	case sym == "hkHSTECH":
		return "124.HSTECH"
	case sym == "hkHSCEI":
		return "100.HSCEI"
	case sym == "us.INX":
		return "100.SPX"
	case sym == "us.NDX":
		return "100.NDX"
	case sym == "us.IXIC":
		return "100.IXIC"
	case sym == "us.DJI":
		return "100.DJIA"
	case strings.HasPrefix(sym, "us") && len(sym) > 2:
		return "105." + strings.ToUpper(sym[2:])
	default:
		return ""
	}
}

func (t *QuoteTool) fetchEastmoneyQuote(ctx context.Context, secid, displaySym string) (*quoteSnapshot, error) {
	u := "https://push2.eastmoney.com/api/qt/stock/get?fltt=2&invt=2&secid=" + url.QueryEscape(secid) +
		"&fields=f43,f44,f45,f46,f47,f48,f57,f58,f60,f116,f117,f162,f167,f168,f169,f170"
	raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), u, 512<<10, map[string]string{
		"Referer": "https://quote.eastmoney.com/",
	})
	if err != nil {
		return nil, err
	}
	if code >= 400 {
		return nil, fmt.Errorf("eastmoney HTTP %d", code)
	}
	return parseEastmoneyQuote(raw, displaySym)
}

func parseEastmoneyQuote(raw []byte, displaySym string) (*quoteSnapshot, error) {
	var parsed struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed.Data == nil {
		return nil, fmt.Errorf("eastmoney: empty quote for %s", displaySym)
	}
	d := parsed.Data
	price := anyToFloat(d["f43"])
	prev := anyToFloat(d["f60"])
	if price == 0 {
		price = prev
	}
	if price == 0 {
		return nil, fmt.Errorf("eastmoney: no price for %s", displaySym)
	}
	sn := &quoteSnapshot{
		Symbol:    displaySym,
		Name:      fmt.Sprintf("%v", d["f58"]),
		Code:      fmt.Sprintf("%v", d["f57"]),
		Market:    inferMarket(displaySym),
		Price:     price,
		High:      anyToFloat(d["f44"]),
		Low:       anyToFloat(d["f45"]),
		Open:      anyToFloat(d["f46"]),
		PrevClose: prev,
		Change:    anyToFloat(d["f169"]),
		ChangePct: anyToFloat(d["f170"]),
		Timestamp: time.Now().Format("2006-01-02 15:04"),
		Status:    inferMarketStatusLabel(inferMarket(displaySym), time.Now()),
	}
	if vol := anyToFloat(d["f47"]); vol > 0 {
		sn.Volume = formatCompactVol(vol)
	}
	if amt := anyToFloat(d["f48"]); amt > 0 {
		if amt >= 1e8 {
			sn.Turnover = fmt.Sprintf("%.2f亿", amt/1e8)
		} else if amt >= 1e4 {
			sn.Turnover = fmt.Sprintf("%.1f万", amt/1e4)
		}
	}
	if pe := anyToFloat(d["f162"]); pe != 0 {
		sn.PE = fmt.Sprintf("%.2f", pe)
	}
	if pb := anyToFloat(d["f167"]); pb != 0 {
		sn.PB = fmt.Sprintf("%.2f", pb)
	}
	if tr := anyToFloat(d["f168"]); tr > 0 {
		sn.TurnoverRate = fmt.Sprintf("%.2f%%", tr)
	}
	if mc := anyToFloat(d["f116"]); mc > 0 {
		sn.TotalMktCap = fmt.Sprintf("%.1f亿", mc/1e8)
	}
	return sn, nil
}

func (t *QuoteTool) fetchEastmoneyKlines(ctx context.Context, secid, period string, days int) ([]Candle, error) {
	klt := "101" // daily
	switch period {
	case "week":
		klt = "102"
	case "month":
		klt = "103"
	}
	u := fmt.Sprintf("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56&klt=%s&fqt=1&lmt=%d&end=20500101",
		url.QueryEscape(secid), klt, days)
	raw, _, _, err := netutil.FetchLimited(ctx, t.httpClient(), u, 2<<20, map[string]string{
		"Referer": "https://quote.eastmoney.com/",
	})
	if err != nil {
		return nil, err
	}
	return parseEastmoneyKlines(raw), nil
}

func parseEastmoneyKlines(raw []byte) []Candle {
	var parsed struct {
		Data struct {
			Klines []string `json:"klines"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}
	candles := make([]Candle, 0, len(parsed.Data.Klines))
	for _, row := range parsed.Data.Klines {
		parts := strings.Split(row, ",")
		if len(parts) < 6 {
			continue
		}
		o := parseFloatSafe(parts[1])
		c := parseFloatSafe(parts[2])
		h := parseFloatSafe(parts[3])
		l := parseFloatSafe(parts[4])
		v := parseFloatSafe(parts[5])
		if o <= 0 && c <= 0 {
			continue
		}
		candles = append(candles, Candle{Label: parts[0], Open: o, Close: c, High: h, Low: l, Volume: v})
	}
	return candles
}

// --- Crypto Fetcher (OKX primary + Binance fallback, with Funding Rate) ---

func (t *QuoteTool) fetchCryptoQuote(ctx context.Context, base string) (*quoteSnapshot, error) {
	base = strings.ToUpper(strings.TrimSpace(base))
	// 1. OKX public ticker
	inst := base + "-USDT"
	okxURL := "https://www.okx.com/api/v5/market/ticker?instId=" + url.QueryEscape(inst)
	if raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), okxURL, 256<<10, nil); err == nil && code == 200 {
		if sn := parseOKXTicker(raw, base); sn != nil {
			// Best-effort perpetual funding rate
			frURL := "https://www.okx.com/api/v5/public/funding-rate?instId=" + url.QueryEscape(inst+"-SWAP")
			if frRaw, _, fcode, ferr := netutil.FetchLimited(ctx, t.httpClient(), frURL, 64<<10, nil); ferr == nil && fcode == 200 {
				var fp struct {
					Data []struct {
						FundingRate string `json:"fundingRate"`
					} `json:"data"`
				}
				if json.Unmarshal(frRaw, &fp) == nil && len(fp.Data) > 0 {
					fr := parseFloatSafe(fp.Data[0].FundingRate) * 100
					sn.FundingRate = fmt.Sprintf("%+.4f%%/8h", fr)
				}
			}
			return sn, nil
		}
	}

	// 2. Binance fallback
	pair := base + "USDT"
	bnURL := "https://api.binance.com/api/v3/ticker/24hr?symbol=" + url.QueryEscape(pair)
	raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), bnURL, 256<<10, nil)
	if err != nil {
		return nil, fmt.Errorf("crypto quote %s failed: %w", base, err)
	}
	if code >= 400 {
		return nil, fmt.Errorf("crypto quote %s HTTP %d", base, code)
	}
	sn := parseBinanceTicker(raw, base)
	if sn == nil {
		return nil, fmt.Errorf("crypto quote %s: empty response", base)
	}
	return sn, nil
}

func parseOKXTicker(raw []byte, base string) *quoteSnapshot {
	var parsed struct {
		Code string `json:"code"`
		Data []struct {
			Last      string `json:"last"`
			Open24h   string `json:"open24h"`
			High24h   string `json:"high24h"`
			Low24h    string `json:"low24h"`
			Vol24h    string `json:"vol24h"`
			VolCcy24h string `json:"volCcy24h"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed.Code != "0" || len(parsed.Data) == 0 {
		return nil
	}
	d := parsed.Data[0]
	last := parseFloatSafe(d.Last)
	open := parseFloatSafe(d.Open24h)
	if last <= 0 {
		return nil
	}
	pct := 0.0
	if open > 0 {
		pct = (last - open) / open * 100.0
	}
	volUSD := parseFloatSafe(d.VolCcy24h)
	turnover := ""
	if volUSD >= 1e8 {
		turnover = fmt.Sprintf("$%.2f亿(24h)", volUSD/1e8)
	} else if volUSD >= 1e4 {
		turnover = fmt.Sprintf("$%.1f万(24h)", volUSD/1e4)
	}
	return &quoteSnapshot{
		Symbol:    "crypto:" + base,
		Name:      base + "/USDT",
		Code:      base + "-USDT",
		Market:    "crypto",
		Price:     last,
		PrevClose: open,
		Open:      open,
		High:      parseFloatSafe(d.High24h),
		Low:       parseFloatSafe(d.Low24h),
		Change:    last - open,
		ChangePct: pct,
		Volume:    formatCompactVol(parseFloatSafe(d.Vol24h)) + " " + base,
		Turnover:  turnover,
		Timestamp: time.Now().Format("2006-01-02 15:04"),
		Status:    "24/7交易中",
	}
}

func parseBinanceTicker(raw []byte, base string) *quoteSnapshot {
	var d struct {
		LastPrice          string `json:"lastPrice"`
		OpenPrice          string `json:"openPrice"`
		HighPrice          string `json:"highPrice"`
		LowPrice           string `json:"lowPrice"`
		PriceChange        string `json:"priceChange"`
		PriceChangePercent string `json:"priceChangePercent"`
		Volume             string `json:"volume"`
		QuoteVolume        string `json:"quoteVolume"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil
	}
	last := parseFloatSafe(d.LastPrice)
	if last <= 0 {
		return nil
	}
	qVol := parseFloatSafe(d.QuoteVolume)
	turnover := ""
	if qVol >= 1e8 {
		turnover = fmt.Sprintf("$%.2f亿(24h)", qVol/1e8)
	} else if qVol >= 1e4 {
		turnover = fmt.Sprintf("$%.1f万(24h)", qVol/1e4)
	}
	return &quoteSnapshot{
		Symbol:    "crypto:" + base,
		Name:      base + "/USDT",
		Code:      base + "USDT",
		Market:    "crypto",
		Price:     last,
		PrevClose: parseFloatSafe(d.OpenPrice),
		Open:      parseFloatSafe(d.OpenPrice),
		High:      parseFloatSafe(d.HighPrice),
		Low:       parseFloatSafe(d.LowPrice),
		Change:    parseFloatSafe(d.PriceChange),
		ChangePct: parseFloatSafe(d.PriceChangePercent),
		Volume:    formatCompactVol(parseFloatSafe(d.Volume)) + " " + base,
		Turnover:  turnover,
		Timestamp: time.Now().Format("2006-01-02 15:04"),
		Status:    "24/7交易中",
	}
}

func (t *QuoteTool) fetchCryptoKlines(ctx context.Context, base, period string, days int) ([]Candle, error) {
	okxBar, bnInt := "1D", "1d"
	switch period {
	case "week":
		okxBar, bnInt = "1W", "1w"
	case "month":
		okxBar, bnInt = "1M", "1M"
	}
	limit := days
	if limit > 100 {
		limit = 100 // OKX single-page limit
	}
	okxURL := fmt.Sprintf("https://www.okx.com/api/v5/market/candles?instId=%s-USDT&bar=%s&limit=%d",
		url.QueryEscape(base), okxBar, limit)
	if raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), okxURL, 1<<20, nil); err == nil && code == 200 {
		var parsed struct {
			Data [][]string `json:"data"`
		}
		if json.Unmarshal(raw, &parsed) == nil && len(parsed.Data) > 0 {
			candles := make([]Candle, 0, len(parsed.Data))
			// OKX returns newest-first; reverse to chronological order.
			for i := len(parsed.Data) - 1; i >= 0; i-- {
				row := parsed.Data[i]
				if len(row) < 6 {
					continue
				}
				tsMs, _ := strconv.ParseInt(row[0], 10, 64)
				lbl := time.UnixMilli(tsMs).Format("2006-01-02")
				candles = append(candles, Candle{
					Label:  lbl,
					Open:   parseFloatSafe(row[1]),
					High:   parseFloatSafe(row[2]),
					Low:    parseFloatSafe(row[3]),
					Close:  parseFloatSafe(row[4]),
					Volume: parseFloatSafe(row[5]),
				})
			}
			return candles, nil
		}
	}

	bnURL := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%sUSDT&interval=%s&limit=%d",
		url.QueryEscape(base), bnInt, days)
	raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), bnURL, 1<<20, nil)
	if err != nil || code >= 400 {
		return nil, err
	}
	var rows [][]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	candles := make([]Candle, 0, len(rows))
	for _, row := range rows {
		if len(row) < 6 {
			continue
		}
		tsMs := int64(anyToFloat(row[0]))
		candles = append(candles, Candle{
			Label:  time.UnixMilli(tsMs).Format("2006-01-02"),
			Open:   anyToFloat(row[1]),
			High:   anyToFloat(row[2]),
			Low:    anyToFloat(row[3]),
			Close:  anyToFloat(row[4]),
			Volume: anyToFloat(row[5]),
		})
	}
	return candles, nil
}

// --- CN Mutual Fund (Tiantian Fund) ---

func (t *QuoteTool) fetchFundQuote(ctx context.Context, code string) (*quoteSnapshot, error) {
	u := fmt.Sprintf("https://fundgz.1234567.com.cn/js/%s.js", url.PathEscape(code))
	raw, _, status, err := netutil.FetchLimited(ctx, t.httpClient(), u, 128<<10, nil)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("fund %s HTTP %d", code, status)
	}
	return parseTiantianFund(string(raw), code)
}

func parseTiantianFund(raw, code string) (*quoteSnapshot, error) {
	start := strings.IndexByte(raw, '{')
	end := strings.LastIndexByte(raw, '}')
	if start < 0 || end <= start {
		return nil, fmt.Errorf("fund %s: invalid response", code)
	}
	var d struct {
		FundCode string `json:"fundcode"`
		Name     string `json:"name"`
		JZRQ     string `json:"jzrq"`
		DWJZ     string `json:"dwjz"`
		GSZ      string `json:"gsz"`
		GSZZL    string `json:"gszzl"`
		GZTime   string `json:"gztime"`
	}
	if err := json.Unmarshal([]byte(raw[start:end+1]), &d); err != nil {
		return nil, err
	}
	est := parseFloatSafe(d.GSZ)
	nav := parseFloatSafe(d.DWJZ)
	if est == 0 {
		est = nav
	}
	if est == 0 {
		return nil, fmt.Errorf("fund %s: no valuation data", code)
	}
	return &quoteSnapshot{
		Symbol:    "fund:" + code,
		Name:      d.Name,
		Code:      d.FundCode,
		Market:    "fund",
		Price:     est,
		PrevClose: nav,
		Change:    est - nav,
		ChangePct: parseFloatSafe(d.GSZZL),
		Timestamp: d.GZTime,
		Status:    "场外基金实时估值",
		ExtraNote: fmt.Sprintf("最新净值=%s (%s)", d.DWJZ, d.JZRQ),
	}, nil
}

func (t *QuoteTool) fetchFundHistory(ctx context.Context, code string, days int) ([]Candle, error) {
	if days > 100 {
		days = 100
	}
	u := fmt.Sprintf("https://api.fund.eastmoney.com/f10/lsjz?fundCode=%s&pageIndex=1&pageSize=%d", url.QueryEscape(code), days)
	raw, _, status, err := netutil.FetchLimited(ctx, t.httpClient(), u, 1<<20, map[string]string{
		"Referer": "https://fundf10.eastmoney.com/",
	})
	if err != nil || status >= 400 {
		return nil, err
	}
	var parsed struct {
		Data struct {
			List []struct {
				FSRQ string `json:"FSRQ"`
				DWJZ string `json:"DWJZ"`
			} `json:"LSJZList"`
		} `json:"Data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	candles := make([]Candle, 0, len(parsed.Data.List))
	for i := len(parsed.Data.List) - 1; i >= 0; i-- {
		it := parsed.Data.List[i]
		v := parseFloatSafe(it.DWJZ)
		if v <= 0 {
			continue
		}
		candles = append(candles, Candle{Label: it.FSRQ, Open: v, High: v, Low: v, Close: v})
	}
	return candles, nil
}

// --- Pure-Go Quantitative Technical Summary ---

func computeTechSummary(candles []Candle) string {
	n := len(candles)
	if n < 2 {
		return ""
	}
	closes := make([]float64, n)
	vols := make([]float64, n)
	for i, cd := range candles {
		closes[i] = cd.Close
		vols[i] = cd.Volume
	}
	last := closes[n-1]
	var parts []string

	// Period returns
	retAt := func(barsAgo int, label string) {
		if n > barsAgo && closes[n-1-barsAgo] > 0 {
			r := (last/closes[n-1-barsAgo] - 1.0) * 100.0
			parts = append(parts, fmt.Sprintf("%s:%+.2f%%", label, r))
		}
	}
	retAt(5, "5期")
	retAt(20, "20期")
	retAt(60, "60期")
	if n > 2 && closes[0] > 0 {
		parts = append(parts, fmt.Sprintf("区间(%d期):%+.2f%%", n, (last/closes[0]-1.0)*100.0))
	}

	// Moving averages
	maStr := func(w int) {
		if n >= w {
			m := meanSlice(closes[n-w:])
			parts = append(parts, fmt.Sprintf("MA%d=%s", w, formatNum(m)))
		}
	}
	maStr(5)
	maStr(20)
	maStr(60)
	if n >= 20 {
		ma20 := meanSlice(closes[n-20:])
		if ma20 > 0 {
			parts = append(parts, fmt.Sprintf("偏离MA20:%+.1f%%", (last/ma20-1.0)*100.0))
		}
	}

	// Wilder RSI(14)
	if n > 14 {
		rsi := calcWilderRSI(closes, 14)
		if !math.IsNaN(rsi) {
			tag := ""
			if rsi >= 70 {
				tag = "(超买)"
			} else if rsi <= 30 {
				tag = "(超卖)"
			}
			parts = append(parts, fmt.Sprintf("RSI14=%.1f%s", rsi, tag))
		}
	}

	// Max drawdown over fetched span
	peak := closes[0]
	maxDD := 0.0
	for _, c := range closes {
		if c > peak {
			peak = c
		}
		if peak > 0 {
			if dd := (peak - c) / peak; dd > maxDD {
				maxDD = dd
			}
		}
	}
	parts = append(parts, fmt.Sprintf("区间最大回撤:%.1f%%", maxDD*100.0))

	// 20-period annualized volatility
	if n >= 21 {
		tail := closes[n-21:]
		rets := make([]float64, 0, 20)
		for i := 1; i < len(tail); i++ {
			if tail[i-1] > 0 && tail[i] > 0 {
				rets = append(rets, math.Log(tail[i]/tail[i-1]))
			}
		}
		if len(rets) >= 5 {
			annVol := sampleStdev(rets) * math.Sqrt(252) * 100.0
			parts = append(parts, fmt.Sprintf("20日年化波动:%.1f%%", annVol))
		}
	}

	// Volume ratio (latest vs 5-day average)
	if n >= 6 && vols[n-1] > 0 {
		avg5 := meanSlice(vols[n-6 : n-1])
		if avg5 > 0 {
			parts = append(parts, fmt.Sprintf("量比(5日):%.2fx", vols[n-1]/avg5))
		}
	}

	return strings.Join(parts, " | ")
}

func formatSnapshotReport(sn *quoteSnapshot, period string, includeKlineTable bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%s) [%s %s]: 现价=%s | 涨跌=%+.2f (%+.2f%%) | 开=%s 昨收=%s 高=%s 低=%s",
		sn.Name, sn.Symbol, sn.Status, sn.Timestamp,
		formatNum(sn.Price), sn.Change, sn.ChangePct,
		formatNum(sn.Open), formatNum(sn.PrevClose), formatNum(sn.High), formatNum(sn.Low))

	var meta []string
	if sn.Volume != "" {
		meta = append(meta, "量="+sn.Volume)
	}
	if sn.Turnover != "" {
		meta = append(meta, "额="+sn.Turnover)
	}
	if sn.TurnoverRate != "" {
		meta = append(meta, "换手="+sn.TurnoverRate)
	}
	if sn.PE != "" {
		meta = append(meta, "PE="+sn.PE)
	}
	if sn.PB != "" {
		meta = append(meta, "PB="+sn.PB)
	}
	if sn.TotalMktCap != "" {
		meta = append(meta, "总市值="+sn.TotalMktCap)
	}
	if sn.High52W != "" && sn.Low52W != "" {
		meta = append(meta, fmt.Sprintf("52周=%s~%s", sn.Low52W, sn.High52W))
	}
	if sn.FundingRate != "" {
		meta = append(meta, "资金费率="+sn.FundingRate)
	}
	if sn.ExtraNote != "" {
		meta = append(meta, sn.ExtraNote)
	}
	if len(meta) > 0 {
		b.WriteString("\n估值与盘口: " + strings.Join(meta, " | "))
	}

	if tech := computeTechSummary(sn.Candles); tech != "" {
		b.WriteString("\n量化摘要(" + periodLabelCN(period) + "): " + tech)
	}

	if includeKlineTable && len(sn.Candles) > 0 {
		// Show up to the latest 15 bars compactly so prompt stays lean while full series powered indicators/charts.
		start := 0
		if len(sn.Candles) > 15 {
			start = len(sn.Candles) - 15
		}
		fmt.Fprintf(&b, "\n近期%sK线(共拉取%d根，展示最近%d根):\n", periodLabelCN(period), len(sn.Candles), len(sn.Candles)-start)
		for _, cd := range sn.Candles[start:] {
			fmt.Fprintf(&b, "%s | O:%s C:%s H:%s L:%s V:%s\n",
				cd.Label, formatNum(cd.Open), formatNum(cd.Close), formatNum(cd.High), formatNum(cd.Low), formatCompactVol(cd.Volume))
		}
	}
	return strings.TrimSpace(b.String())
}

func alignCompareSeries(snaps []*quoteSnapshot) ([]string, []Series) {
	if len(snaps) < 2 {
		return nil, nil
	}
	// Intersect date labels across all snapshots so comparison is strictly date-aligned.
	dateCounts := make(map[string]int)
	snapMaps := make([]map[string]float64, len(snaps))
	for i, sn := range snaps {
		m := make(map[string]float64, len(sn.Candles))
		for _, cd := range sn.Candles {
			if cd.Close > 0 && cd.Label != "" {
				if _, exists := m[cd.Label]; !exists {
					dateCounts[cd.Label]++
				}
				m[cd.Label] = cd.Close
			}
		}
		snapMaps[i] = m
	}
	var common []string
	for d, c := range dateCounts {
		if c == len(snaps) {
			common = append(common, d)
		}
	}
	sort.Strings(common)
	if len(common) < 2 {
		// Fallback: use shortest length index alignment when markets have different holidays.
		minN := len(snaps[0].Candles)
		for _, sn := range snaps[1:] {
			if len(sn.Candles) < minN {
				minN = len(sn.Candles)
			}
		}
		if minN < 2 {
			return nil, nil
		}
		labels := make([]string, minN)
		baseTail := snaps[0].Candles[len(snaps[0].Candles)-minN:]
		for i, cd := range baseTail {
			labels[i] = cd.Label
		}
		series := make([]Series, len(snaps))
		for si, sn := range snaps {
			tail := sn.Candles[len(sn.Candles)-minN:]
			vals := make([]float64, minN)
			for i, cd := range tail {
				vals[i] = cd.Close
			}
			series[si] = Series{Name: sn.Name, Values: vals}
		}
		return labels, series
	}

	series := make([]Series, len(snaps))
	for si, sn := range snaps {
		vals := make([]float64, len(common))
		for i, d := range common {
			vals[i] = snapMaps[si][d]
		}
		series[si] = Series{Name: sn.Name, Values: vals}
	}
	return common, series
}

// --- Pure-Go Market Pulse (Indices + Breadth + Sectors + Southbound Flow) ---

func (t *QuoteTool) execMarketPulse(ctx context.Context) (string, error) {
	var (
		idxSection     string
		breadthSection string
		sectorSection  string
		flowSection    string
	)
	g, gctx := errgroup.WithContext(ctx)

	// 1. Major CN/HK/US indices
	g.Go(func() error {
		syms := []string{"sh000001", "sz399001", "sz399006", "sh000688", "sh000300", "hkHSI", "hkHSTECH", "us.INX", "us.NDX", "us.DJI"}
		batch, err := t.fetchTencentBatch(gctx, syms)
		if err != nil {
			return nil
		}
		var lines []string
		for _, s := range syms {
			if sn := batch[s]; sn != nil {
				lines = append(lines, fmt.Sprintf("- %s: %s (%+.2f%%)", sn.Name, formatNum(sn.Price), sn.ChangePct))
			}
		}
		if len(lines) > 0 {
			idxSection = "【主要指数】\n" + strings.Join(lines, "\n")
		}
		return nil
	})

	// 2. A-share breadth (advancers/decliners/flat + combined turnover)
	g.Go(func() error {
		u := "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&fields=f12,f14,f2,f3,f6,f104,f105,f106&secids=1.000001,0.399001"
		raw, _, code, err := netutil.FetchLimited(gctx, t.httpClient(), u, 256<<10, map[string]string{"Referer": "https://quote.eastmoney.com/"})
		if err != nil || code >= 400 {
			return nil
		}
		var parsed struct {
			Data struct {
				Diff []map[string]any `json:"diff"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &parsed) != nil || len(parsed.Data.Diff) == 0 {
			return nil
		}
		var up, down, flat int
		var totalAmt float64
		for _, item := range parsed.Data.Diff {
			up += int(anyToFloat(item["f104"]))
			down += int(anyToFloat(item["f105"]))
			flat += int(anyToFloat(item["f106"]))
			totalAmt += anyToFloat(item["f6"])
		}
		if up+down > 0 || totalAmt > 0 {
			breadthSection = fmt.Sprintf("【A股盘面广度】上涨 %d 家 | 下跌 %d 家 | 平盘 %d 家 | 沪深两市成交额约 %.0f 亿元",
				up, down, flat, totalAmt/1e8)
		}
		return nil
	})

	// 3. A-share sector leaders (Top 5 gainers + Top 5 losers)
	g.Go(func() error {
		fetchSectors := func(desc int) []string {
			u := fmt.Sprintf("https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&po=%d&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f14,f3,f128", desc)
			raw, _, code, err := netutil.FetchLimited(gctx, t.httpClient(), u, 256<<10, map[string]string{"Referer": "https://quote.eastmoney.com/"})
			if err != nil || code >= 400 {
				return nil
			}
			var parsed struct {
				Data struct {
					Diff []map[string]any `json:"diff"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &parsed) != nil {
				return nil
			}
			var out []string
			for _, d := range parsed.Data.Diff {
				name := fmt.Sprintf("%v", d["f14"])
				pct := anyToFloat(d["f3"])
				lead := fmt.Sprintf("%v", d["f128"])
				if lead != "" && lead != "<nil>" && lead != "-" {
					out = append(out, fmt.Sprintf("%s(%+.2f%%, 领涨:%s)", name, pct, lead))
				} else {
					out = append(out, fmt.Sprintf("%s(%+.2f%%)", name, pct))
				}
			}
			return out
		}
		topUp := fetchSectors(1)
		topDown := fetchSectors(0)
		var lines []string
		if len(topUp) > 0 {
			lines = append(lines, "领涨行业: "+strings.Join(topUp, "、"))
		}
		if len(topDown) > 0 {
			lines = append(lines, "领跌行业: "+strings.Join(topDown, "、"))
		}
		if len(lines) > 0 {
			sectorSection = "【A股行业板块】\n" + strings.Join(lines, "\n")
		}
		return nil
	})

	// 4. Southbound / Northbound capital flow
	g.Go(func() error {
		u := "https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f54,f56,f62,f63,f65,f67"
		raw, _, code, err := netutil.FetchLimited(gctx, t.httpClient(), u, 256<<10, map[string]string{"Referer": "https://quote.eastmoney.com/"})
		if err != nil || code >= 400 {
			return nil
		}
		var parsed struct {
			Data struct {
				S2N struct {
					NetBuyAmt float64 `json:"netBuyAmt"` // 万元
				} `json:"s2n"`
				HK2SH struct {
					NetBuyAmt float64 `json:"dayNetAmtIn"`
				} `json:"hk2sh"`
				SH2HK struct {
					DayNetAmtIn float64 `json:"dayNetAmtIn"`
				} `json:"sh2hk"`
				SZ2HK struct {
					DayNetAmtIn float64 `json:"dayNetAmtIn"`
				} `json:"sz2hk"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &parsed) == nil {
			south := (parsed.Data.SH2HK.DayNetAmtIn + parsed.Data.SZ2HK.DayNetAmtIn) / 1e4
			if south != 0 {
				flowSection = fmt.Sprintf("【港股通南向资金】当日净流入约 %+.2f 亿元", south)
			}
		}
		return nil
	})

	_ = g.Wait()

	var sections []string
	for _, s := range []string{idxSection, breadthSection, sectorSection, flowSection} {
		if s != "" {
			sections = append(sections, s)
		}
	}
	if len(sections) == 0 {
		return "", errors.New("quote(mode=market): market pulse data temporarily unavailable")
	}
	return fmt.Sprintf("市场全景快照 (%s)\n\n%s", time.Now().Format("2006-01-02 15:04"), strings.Join(sections, "\n\n")), nil
}

// --- Pure-Go Finance Telegraph News (CLS primary + Eastmoney 7x24 fallback) ---

func (t *QuoteTool) execFinanceNews(ctx context.Context, keyword string) (string, error) {
	type newsItem struct {
		Time      string
		Title     string
		Content   string
		Important bool
	}

	var items []newsItem
	// 1. CLS Telegraph
	clsURL := "https://www.cls.cn/nodeapi/telegraphList?app=CailianpressWeb&os=web&sv=7.7.5&rn=20"
	if raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), clsURL, 1<<20, map[string]string{
		"Referer": "https://www.cls.cn/telegraph",
	}); err == nil && code == 200 {
		var parsed struct {
			Data struct {
				RollData []struct {
					CTime   int64  `json:"ctime"`
					Title   string `json:"title"`
					Content string `json:"content"`
					Level   string `json:"level"`
				} `json:"roll_data"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &parsed) == nil {
			for _, r := range parsed.Data.RollData {
				ts := time.Unix(r.CTime, 0).Format("01-02 15:04")
				items = append(items, newsItem{
					Time:      ts,
					Title:     strings.TrimSpace(r.Title),
					Content:   strings.TrimSpace(r.Content),
					Important: r.Level == "A" || r.Level == "B",
				})
			}
		}
	}

	// 2. Eastmoney 7x24 Fast News fallback
	if len(items) == 0 {
		emURL := "https://np-listapi.eastmoney.com/comm/web/getFastNewsList?client=web&biz=web_724&fastColumn=102&sortEnd=&pageSize=20&req_trace=1"
		if raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), emURL, 1<<20, map[string]string{
			"Referer": "https://kuaixun.eastmoney.com/",
		}); err == nil && code == 200 {
			var parsed struct {
				Data struct {
					FastNewsList []struct {
						ShowTime string `json:"showTime"`
						Title    string `json:"title"`
						Summary  string `json:"summary"`
					} `json:"fastNewsList"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &parsed) == nil {
				for _, r := range parsed.Data.FastNewsList {
					items = append(items, newsItem{
						Time:    r.ShowTime,
						Title:   strings.TrimSpace(r.Title),
						Content: strings.TrimSpace(r.Summary),
					})
				}
			}
		}
	}

	if len(items) == 0 {
		return "", errors.New("quote(mode=news): realtime finance telegraph temporarily unavailable")
	}

	kwLow := strings.ToLower(strings.TrimSpace(keyword))
	var lines []string
	for _, it := range items {
		text := it.Content
		if text == "" {
			text = it.Title
		}
		if kwLow != "" && !strings.Contains(strings.ToLower(it.Title+" "+text), kwLow) {
			continue
		}
		tag := ""
		if it.Important {
			tag = "[重点] "
		}
		lines = append(lines, fmt.Sprintf("- [%s] %s%s", it.Time, tag, clipRunes(text, 180)))
		if len(lines) >= 12 {
			break
		}
	}
	if len(lines) == 0 {
		return fmt.Sprintf("最近 20 条实时财经电报中未命中关键词 %q。", keyword), nil
	}
	header := "实时财经快讯:"
	if kwLow != "" {
		header = fmt.Sprintf("实时财经快讯 (筛选: %s):", keyword)
	}
	return header + "\n" + strings.Join(lines, "\n"), nil
}

// --- Pure-Go Company Fundamentals / F10 (Eastmoney Datacenter) ---

func (t *QuoteTool) execFundamentals(ctx context.Context, rawSym string) (string, error) {
	sym, err := t.resolveSymbol(ctx, rawSym)
	if err != nil {
		return "", err
	}
	var b strings.Builder

	// Live valuation snapshot first.
	if sn, qerr := t.fetchOneQuoteOnly(ctx, sym); qerr == nil && sn != nil {
		fmt.Fprintf(&b, "%s (%s) 实时估值: 现价=%s (%+.2f%%)", sn.Name, sn.Symbol, formatNum(sn.Price), sn.ChangePct)
		if sn.PE != "" {
			b.WriteString(" | PE=" + sn.PE)
		}
		if sn.PB != "" {
			b.WriteString(" | PB=" + sn.PB)
		}
		if sn.TotalMktCap != "" {
			b.WriteString(" | 总市值=" + sn.TotalMktCap)
		}
		if sn.High52W != "" && sn.Low52W != "" {
			fmt.Fprintf(&b, " | 52周=%s~%s", sn.Low52W, sn.High52W)
		}
		b.WriteString("\n\n")
	}

	var reportName, cols, filterVal string
	var unitDiv float64 = 1e8
	var unitLbl = "亿"
	switch {
	case (strings.HasPrefix(sym, "sh") || strings.HasPrefix(sym, "sz") || strings.HasPrefix(sym, "bj")) && len(sym) == 8:
		reportName = "RPT_LICO_FN_CPD"
		cols = "SECURITY_CODE,SECURITY_NAME_ABBR,REPORTDATE,TOTAL_OPERATE_INCOME,YSTZ,PARENT_NETPROFIT,SJLTZ,WEIGHTAVG_ROE,XSMLL,BASIC_EPS"
		filterVal = sym[2:]
	case strings.HasPrefix(sym, "hk") && len(sym) == 7 && isAllDigits(sym[2:]):
		reportName = "RPT_HKF10_FN_MAININDICATOR"
		cols = "SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,OPERATE_INCOME,OPERATE_INCOME_YOY,HOLDER_PROFIT,HOLDER_PROFIT_YOY,ROE_AVG,GROSS_PROFIT_RATIO,BASIC_EPS"
		filterVal = sym[2:]
	case strings.HasPrefix(sym, "us") && len(sym) > 2 && !strings.HasPrefix(sym, "us."):
		reportName = "RPT_USF10_FN_MAININDICATOR"
		cols = "SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,OPERATE_INCOME,OPERATE_INCOME_YOY,PARENT_HOLDER_NETPROFIT,PARENT_HOLDER_NETPROFIT_YOY,ROE_AVG,GROSS_PROFIT_RATIO,BASIC_EPS"
		filterVal = strings.ToUpper(sym[2:])
		unitLbl = "亿美元"
	default:
		if b.Len() > 0 {
			return strings.TrimSpace(b.String()) + "\n(该标的为指数/商品/加密资产，无公司财报F10数据)", nil
		}
		return "", fmt.Errorf("quote(mode=fundamentals): unsupported symbol %q for F10 fundamentals", sym)
	}

	sortCol := "REPORT_DATE"
	if reportName == "RPT_LICO_FN_CPD" {
		sortCol = "REPORTDATE"
	}
	u := fmt.Sprintf("https://datacenter.eastmoney.com/securities/api/data/v1/get?reportName=%s&columns=%s&filter=(SECURITY_CODE%%3D%%22%s%%22)&pageSize=4&sortColumns=%s&sortTypes=-1",
		reportName, cols, url.QueryEscape(filterVal), sortCol)

	raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), u, 1<<20, map[string]string{
		"Referer": "https://emweb.securities.eastmoney.com/",
	})
	if err != nil || code >= 400 {
		if b.Len() > 0 {
			return strings.TrimSpace(b.String()) + "\n(财报明细暂不可用)", nil
		}
		return "", fmt.Errorf("fundamentals fetch failed: %v", err)
	}

	var parsed struct {
		Result struct {
			Data []map[string]any `json:"data"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed.Result.Data) == 0 {
		if b.Len() > 0 {
			return strings.TrimSpace(b.String()) + "\n(未查到近期财报明细)", nil
		}
		return "", fmt.Errorf("no fundamentals returned for %s", sym)
	}

	b.WriteString("最近 4 期财报核心指标:\n")
	for _, row := range parsed.Result.Data {
		dt := firstStr(row, "REPORTDATE", "REPORT_DATE")
		if len(dt) > 10 {
			dt = dt[:10]
		}
		rev := firstFloat(row, "TOTAL_OPERATE_INCOME", "OPERATE_INCOME")
		revYoY := firstFloat(row, "YSTZ", "OPERATE_INCOME_YOY")
		np := firstFloat(row, "PARENT_NETPROFIT", "HOLDER_PROFIT", "PARENT_HOLDER_NETPROFIT")
		npYoY := firstFloat(row, "SJLTZ", "HOLDER_PROFIT_YOY", "PARENT_HOLDER_NETPROFIT_YOY")
		roe := firstFloat(row, "WEIGHTAVG_ROE", "ROE_AVG")
		gm := firstFloat(row, "XSMLL", "GROSS_PROFIT_RATIO")
		eps := firstFloat(row, "BASIC_EPS")

		fmt.Fprintf(&b, "- %s | 营收=%.2f%s(同比%+.1f%%) | 归母净利=%.2f%s(同比%+.1f%%) | 毛利率=%.1f%% | ROE=%.1f%% | EPS=%.2f\n",
			dt, rev/unitDiv, unitLbl, revYoY, np/unitDiv, unitLbl, npYoY, gm, roe, eps)
	}
	return strings.TrimSpace(b.String()), nil
}

// --- Helpers ---

func inferMarketStatusLabel(market string, now time.Time) string {
	switch market {
	case "crypto":
		return "24/7交易中"
	case "fund":
		return "实时估值"
	case "fx":
		if now.UTC().Weekday() == time.Saturday || (now.UTC().Weekday() == time.Sunday && now.UTC().Hour() < 22) {
			return "周末休市"
		}
		return "国际连续交易"
	}
	if cron.IsMarketOpen(market, now) {
		return "盘中"
	}
	return "休市/盘后"
}

func formatTencentTimestamp(ts string) string {
	ts = strings.TrimSpace(ts)
	if len(ts) == 14 && isAllDigits(ts) {
		return ts[0:4] + "-" + ts[4:6] + "-" + ts[6:8] + " " + ts[8:10] + ":" + ts[10:12]
	}
	return ts
}

func periodLabelCN(p string) string {
	switch p {
	case "week":
		return "周"
	case "month":
		return "月"
	default:
		return "日"
	}
}

func sanitizeFilename(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "chart"
	}
	return b.String()
}

func decodeGBK(b []byte) (string, error) {
	reader := transform.NewReader(bytes.NewReader(b), simplifiedchinese.GBK.NewDecoder())
	out, err := io.ReadAll(reader)
	return string(out), err
}

func decodeUnicodeEscapes(s string) string {
	if !strings.Contains(s, `\u`) {
		return s
	}
	var out string
	if err := json.Unmarshal([]byte(`"`+strings.ReplaceAll(s, `"`, `\"`)+`"`), &out); err == nil {
		return out
	}
	return s
}

func getField(fields []string, idx int) string {
	if idx < 0 || idx >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[idx])
}

func cleanNonZero(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || s == "0" || s == "0.00" || s == "0.0" {
		return ""
	}
	return s
}

func parseFloatSafe(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || s == "--" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func anyToFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0
		}
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		return parseFloatSafe(x)
	default:
		return 0
	}
}

func firstStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			s := strings.TrimSpace(fmt.Sprintf("%v", v))
			if s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}

func firstFloat(m map[string]any, keys ...string) float64 {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			if f := anyToFloat(v); f != 0 {
				return f
			}
		}
	}
	return 0
}

func containsCJK(s string) bool {
	for _, r := range s {
		if unicode.In(r, unicode.Han) {
			return true
		}
	}
	return false
}

func isASCIIAlphaNum(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func isASCIIStockRest(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
