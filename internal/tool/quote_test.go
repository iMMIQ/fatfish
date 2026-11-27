package tool

import (
	"bytes"
	"context"
	"image/png"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeTickerAndParseTencentQuote(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"hkHSTECH", "hkHSTECH"},
		{"hkhstech", "hkHSTECH"},
		{"hkHSI", "hkHSI"},
		{"hk00700", "hk00700"},
		{"HK00700", "hk00700"},
		{"恒生科技", "hkHSTECH"},
		{"恒指", "hkHSI"},
		{"大盘", "sh000001"},
		{"上证", "sh000001"},
		{"创业板", "sz399006"},
		{"中证500", "sh000905"},
		{"标普500", "us.INX"},
		{"纳指100", "us.NDX"},
		{"道指", "us.DJI"},
		{"usSMH", "usSMH"},
		{"nvda", "usNVDA"},
		{"600519", "sh600519"},
		{"510300", "sh510300"}, // Shanghai ETF bug fix
		{"588000", "sh588000"}, // STAR50 ETF bug fix
		{"832566", "bj832566"}, // Beijing Stock Exchange
		{"920001", "bj920001"}, // BSE 920xxx
		{"000001", "sz000001"},
		{"159915", "sz159915"}, // Shenzhen ETF
		{"00700", "hk00700"},
		{"BTC", "crypto:BTC"},
		{"比特币", "crypto:BTC"},
		{"SOLUSDT", "crypto:SOL"},
		{"PEPE-USDT", "crypto:PEPE"},
		{"黄金", "em:101.GC00Y"},
		{"离岸人民币", "em:133.USDCNH"},
		{"USDCNY", "em:120.USDCNY"},
		{"美元指数", "em:100.UDI"},
		{"fund:110011", "fund:110011"},
		{"中芯国际", "search:中芯国际"},
	}
	for _, tc := range cases {
		if got := normalizeTicker(tc.in); got != tc.want {
			t.Errorf("normalizeTicker(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	rawUS := `v_usSMH="200~半导体指数ETF-VanEck~SMH.OQ~422.46~426.75~402.67~1234567~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~2026-11-25 16:00:00~-4.29~-1.01~422.97~393.88~0~0~520000000~1.25~28.4~0~0~0~0~0~250.5~0~0~460.00~280.10";`
	summary, code := parseTencentQuote(rawUS)
	if code != "SMH.OQ" {
		t.Fatalf("expected quote code SMH.OQ, got %q", code)
	}
	if !strings.Contains(summary, "422.46") || !strings.Contains(summary, "-1.01%") {
		t.Fatalf("unexpected summary: %q", summary)
	}
	detail := parseTencentQuoteDetail(rawUS, "usSMH")
	if detail == nil || detail.PE != "28.4" || detail.High52W != "460.00" || detail.Low52W != "280.10" || detail.TotalMktCap != "$250.5亿" {
		t.Fatalf("unexpected rich US detail: %+v", detail)
	}

	// Rich A-share parsing (turnover, PE, PB, market cap, volume ratio)
	rawCN := `v_sh600519="1~贵州茅台~600519~1580.00~1560.00~1565.00~35000~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~20261127145900~20.00~1.28~1590.00~1558.00~0~0~553000~0.28~22.50~0~0~0~0~19850.0~19850.0~8.42~1716.00~1404.00~1.35";`
	cn := parseTencentQuoteDetail(rawCN, "sh600519")
	if cn == nil || cn.PE != "22.50" || cn.PB != "8.42" || cn.TurnoverRate != "0.28%" || !strings.Contains(cn.Turnover, "55.30亿元") || cn.Timestamp != "2026-11-27 14:59" {
		t.Fatalf("unexpected rich CN detail: %+v", cn)
	}

	klJSON := []byte(`{"code":0,"data":{"usSMH.OQ":{"day":[["2026-11-12","485.010","501.730","509.800","484.500","10210284.000"],["2026-11-13","499.930","486.750","500.270","473.260","12400180.000"]]}}}`)
	lines := parseTencentKline(klJSON, "usSMH.OQ")
	if len(lines) != 2 || !strings.Contains(lines[0], "509.800") {
		t.Fatalf("expected 2 US K-line bars with high 509.800, got %+v", lines)
	}
}

func TestMultiAssetParsersAndSearch(t *testing.T) {
	// Smartbox search
	sb := `v_hint="sh~688981~\u4e2d\u82af\u56fd\u9645~zxgj~GP-A^hk~00981~\u4e2d\u82af\u56fd\u9645~zxgj~GP"`
	hits := parseTencentSmartbox(sb, 5)
	if len(hits) != 2 || hits[0].Symbol != "sh688981" || hits[0].Name != "中芯国际" || hits[1].Symbol != "hk00981" {
		t.Fatalf("unexpected smartbox hits: %+v", hits)
	}

	// OKX crypto ticker
	okx := []byte(`{"code":"0","data":[{"last":"96500.5","open24h":"94000","high24h":"97200","low24h":"93500","vol24h":"18500","volCcy24h":"1760000000"}]}`)
	sn := parseOKXTicker(okx, "BTC")
	if sn == nil || sn.Symbol != "crypto:BTC" || math.Abs(sn.Price-96500.5) > 1e-6 || sn.ChangePct <= 2.6 {
		t.Fatalf("unexpected OKX ticker: %+v", sn)
	}

	// Binance crypto ticker
	bn := []byte(`{"lastPrice":"3450.25","openPrice":"3300.00","highPrice":"3500.00","lowPrice":"3280.00","priceChange":"150.25","priceChangePercent":"4.55","volume":"520000","quoteVolume":"1780000000"}`)
	snB := parseBinanceTicker(bn, "ETH")
	if snB == nil || snB.Symbol != "crypto:ETH" || math.Abs(snB.ChangePct-4.55) > 1e-6 {
		t.Fatalf("unexpected Binance ticker: %+v", snB)
	}

	// Tiantian mutual fund valuation
	fnRaw := `jsonpgz({"fundcode":"110011","name":"易方达优质精选混合(QDII)","jzrq":"2026-11-26","dwjz":"5.4321","gsz":"5.5100","gszzl":"1.43","gztime":"2026-11-27 14:30"});`
	fn, err := parseTiantianFund(fnRaw, "110011")
	if err != nil || fn.Symbol != "fund:110011" || math.Abs(fn.Price-5.51) > 1e-6 || math.Abs(fn.ChangePct-1.43) > 1e-6 {
		t.Fatalf("unexpected fund quote: %+v err=%v", fn, err)
	}

	// Eastmoney quote + kline
	emQ := []byte(`{"data":{"f43":7.2456,"f44":7.2600,"f45":7.2300,"f46":7.2400,"f57":"USDCNH","f58":"离岸人民币","f60":7.2380,"f169":0.0076,"f170":0.11}}`)
	emSn, err := parseEastmoneyQuote(emQ, "em:133.USDCNH")
	if err != nil || math.Abs(emSn.Price-7.2456) > 1e-6 || emSn.Market != "fx" {
		t.Fatalf("unexpected eastmoney quote: %+v err=%v", emSn, err)
	}
	emKL := []byte(`{"data":{"klines":["2026-11-25,7.230,7.238,7.245,7.220,1000","2026-11-26,7.238,7.245,7.255,7.232,1200"]}}`)
	cands := parseEastmoneyKlines(emKL)
	if len(cands) != 2 || math.Abs(cands[1].Close-7.245) > 1e-6 {
		t.Fatalf("unexpected eastmoney klines: %+v", cands)
	}
}

func TestTechSummaryAndFinancialCharts(t *testing.T) {
	// Build 30 realistic candles
	candles := make([]Candle, 30)
	price := 100.0
	for i := 0; i < 30; i++ {
		delta := 1.2
		if i%4 == 0 {
			delta = -2.0
		}
		open := price
		closeP := price + delta
		hi := math.Max(open, closeP) + 0.8
		lo := math.Min(open, closeP) - 0.8
		candles[i] = Candle{
			Label:  "2026-10-" + string(rune('0'+(i+1)/10)) + string(rune('0'+(i+1)%10)),
			Open:   open,
			High:   hi,
			Low:    lo,
			Close:  closeP,
			Volume: float64(10000 + i*500),
		}
		price = closeP
	}

	tech := computeTechSummary(candles)
	for _, want := range []string{"5期:", "20期:", "MA5=", "MA20=", "RSI14=", "区间最大回撤:", "20日年化波动:", "量比(5日):"} {
		if !strings.Contains(tech, want) {
			t.Fatalf("expected %q in tech summary, got: %s", want, tech)
		}
	}

	// Render enhanced K-line with MA overlays + volume subplot + subtitle
	fontPath := filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf")
	rend := NewChartRenderer(fontPath)
	pngBytes, err := rend.RenderKlineWithSubtitle("英伟达 (usNVDA) 日 K线", "现价 142.50 (+2.35%)", candles)
	if err != nil {
		t.Fatalf("RenderKlineWithSubtitle failed: %v", err)
	}
	img, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil || img.Bounds().Dy() != 660 {
		t.Fatalf("expected 660px tall K-line+volume chart, got bounds=%v err=%v", img.Bounds(), err)
	}

	// Align & render multi-symbol comparison chart
	sn1 := &quoteSnapshot{Name: "NVDA", Candles: candles}
	candles2 := make([]Candle, 30)
	for i, cd := range candles {
		candles2[i] = Candle{Label: cd.Label, Close: cd.Close*0.8 + float64(i)*0.5}
	}
	sn2 := &quoteSnapshot{Name: "AMD", Candles: candles2}
	labels, series := alignCompareSeries([]*quoteSnapshot{sn1, sn2})
	if len(labels) != 30 || len(series) != 2 {
		t.Fatalf("alignCompareSeries failed: labels=%d series=%d", len(labels), len(series))
	}
	cmpPNG, err := rend.RenderCompare("芯片龙头对比", labels, series)
	if err != nil {
		t.Fatalf("RenderCompare failed: %v", err)
	}
	if _, err := png.Decode(bytes.NewReader(cmpPNG)); err != nil {
		t.Fatalf("decode compare PNG failed: %v", err)
	}
}

func TestReadSourceSemanticChunking(t *testing.T) {
	if a, b := parsePageRange("12-18"); a != 12 || b != 18 {
		t.Fatalf("parsePageRange(12-18) = (%d,%d)", a, b)
	}
	if a, b := parsePageRange("5"); a != 5 || b != 5 {
		t.Fatalf("parsePageRange(5) = (%d,%d)", a, b)
	}

	// Simulate a long financial filing with filler pages and one specific revenue section.
	var doc strings.Builder
	doc.WriteString("2026年第三季度财报公告封面与目录\n")
	for i := 0; i < 15; i++ {
		doc.WriteString(strings.Repeat("常规公司治理及合规披露条款说明内容。", 40) + "\n")
	}
	doc.WriteString("第三季度数据中心营收同比增长112%达到308亿美元，毛利率维持在75.2%，自由现金流创新高。\n")
	for i := 0; i < 10; i++ {
		doc.WriteString(strings.Repeat("附注与历史会计政策变更说明内容。", 40) + "\n")
	}

	rt := NewReadSourceTool(nil, nil)
	excerpt := rt.selectRelevantChunks(context.Background(), doc.String(), "数据中心 营收 毛利率", 3, 4000)
	if !strings.Contains(excerpt, "308亿美元") || !strings.Contains(excerpt, "75.2%") {
		t.Fatalf("semantic chunk selection missed target financial metrics: %s", excerpt)
	}
}
