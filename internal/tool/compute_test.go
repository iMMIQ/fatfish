package tool

import (
	"bytes"
	"context"
	"errors"
	"image/png"
	"path/filepath"
	"strings"
	"testing"
)

// TestStarlarkOOMBlocked enforces SPEC 10.2 / Audit #8:
// single-step OOM patterns like [0]*100000000 must be rejected before VM execution.
func TestStarlarkOOMBlocked(t *testing.T) {
	ct := NewComputeTool(filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf"), 500000, 3)

	attacks := []string{
		`x = [0] * 100000000`,
		`x = "a" * 50000000`,
		`x = 100000000 * [1]`,
		`x = range(100000000)`,
		`x = [0] * (1000 * 1000)`,
		"n = 100000000\nx = [0] * n",
		"n = 100000000\nx = range(n)",
		"n = 100000000\nx = [0]\nx *= n",
		"x = [0]\nfor i in range(30):\n    x = x + x",
		"for i in [0] * 100000000:\n    pass",
		"while len([0] * 100000000):\n    break",
		"x = ([0] * 10000) * 10000",
		"a = [0] * 10000\nb = a * 10000",
	}
	for _, code := range attacks {
		_, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{Code: code})
		if err == nil {
			t.Fatalf("expected OOM attack %q to be blocked, got nil", code)
		}
		if !errors.Is(err, ErrStarlarkOOMBlocked) {
			t.Fatalf("expected ErrStarlarkOOMBlocked for %q, got %v", code, err)
		}
	}

	// Legitimate calculation with stats & finance must succeed cleanly.
	res, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `
m = stats.mean([10, 20, 30])
c = finance.cagr(100, 144, 2)
print("mean=", m, "cagr=", round(c, 4))
`,
	})
	if err != nil {
		t.Fatalf("legitimate compute failed: %v", err)
	}
	if !strings.Contains(res.TextForLLM, "mean= 20") || !strings.Contains(res.TextForLLM, "cagr= 0.2") {
		t.Fatalf("unexpected compute output: %q", res.TextForLLM)
	}
}

// TestRenderChineseChartPNG enforces SPEC Audit #6:
// bundled TrueType (.ttf) CJK font must render Chinese title/labels cleanly into non-blank PNG pixels.
func TestRenderChineseChartPNG(t *testing.T) {
	fontPath := filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf")
	ct := NewComputeTool(fontPath, 500000, 3)

	res, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `
chart.line(
    title="大肥鱼季度营收对比（亿元）",
    labels=["一季度", "二季度", "三季度", "四季度"],
    data={"营收": [12.5, 18.2, 24.0, 31.6], "利润": [3.1, 5.4, 7.8, 11.2]}
)
`,
	})
	if err != nil {
		t.Fatalf("Chinese chart render failed: %v", err)
	}
	if len(res.DirectMedia) != 1 || res.DirectMedia[0].MimeType != "image/png" {
		t.Fatalf("expected 1 PNG DirectMedia, got %+v", res.DirectMedia)
	}

	img, err := png.Decode(bytes.NewReader(res.DirectMedia[0].Data))
	if err != nil {
		t.Fatalf("decode rendered PNG: %v", err)
	}
	// Verify non-white pixels exist in the title band (y in [12..44]) proving CJK glyphs actually rendered!
	nonWhiteTitlePixels := 0
	b := img.Bounds()
	for y := 12; y < 44 && y < b.Max.Y; y++ {
		for x := 70; x < 450 && x < b.Max.X; x++ {
			r, g, bl, _ := img.At(x, y).RGBA()
			if r < 0xf000 || g < 0xf000 || bl < 0xf000 {
				nonWhiteTitlePixels++
			}
		}
	}
	if nonWhiteTitlePixels < 200 {
		t.Fatalf("Chinese title glyphs did not render properly (non-white title pixels=%d)", nonWhiteTitlePixels)
	}

	// Non-finite values (NaN/Inf) must fail cleanly rather than hanging Bresenham drawLine.
	if _, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `chart.line(title="bad", labels=["a","b"], data=[1.0, float("nan")])`,
	}); err == nil {
		t.Fatalf("expected error for NaN chart data")
	}
	// Chart count per compute call must be capped.
	if _, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `
for i in range(6):
    chart.line(title="t", labels=["a"], data=[1])
`,
	}); err == nil {
		t.Fatalf("expected error when exceeding max charts per compute call")
	}
}

func TestEnhancedFinanceAndCompareChart(t *testing.T) {
	fontPath := filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf")
	ct := NewComputeTool(fontPath, 500000, 3)

	res, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `
irr = finance.irr([-1000, 300, 400, 500])
xirr = finance.xirr([-1000, 1210], ["2024-01-01", "2026-01-01"])
dca = finance.dca([10.0, 8.0, 12.0, 10.0], 1000)
prices = [100, 102, 101, 105, 107, 106, 110, 112, 111, 115, 118, 117, 120, 122, 125, 123, 126, 128, 130, 132, 135]
vol = finance.volatility(prices)
rets = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
sh = finance.sharpe(rets, rf=0.02)
so = finance.sortino(rets, rf=0.02)
sma5 = finance.sma(prices, 5)
ema5 = finance.ema(prices, 5)
rsi = finance.rsi(prices, 14)
macd = finance.macd(prices)
bb = finance.bollinger(prices, 20)
call = finance.bs_option(S=100, K=100, T=1.0, r=0.05, sigma=0.2, kind="call")
put = finance.bs_option(S=100, K=100, T=1.0, r=0.05, sigma=0.2, kind="put")
parity_err = abs((call["price"] - put["price"]) - (100 - 100 * math.exp(-0.05)))

chart.compare(title="多标的对比", labels=["D1", "D2", "D3", "D4"], data={"A": [100, 105, 103, 112], "B": [50, 48, 52, 55]})
print("irr=", round(irr, 4), "xirr=", round(xirr, 4), "dca_ret=", round(dca["total_return"], 4))
print("vol=", round(vol, 4), "sharpe=", round(sh, 2), "sortino=", round(so, 2))
print("sma_len=", len(sma5), "ema_len=", len(ema5), "rsi=", round(rsi, 2), "macd_dif=", round(macd["dif"], 2), "bb_mid=", round(bb["mid"], 2))
print("call=", round(call["price"], 2), "delta=", round(call["delta"], 3), "parity_err=", round(parity_err, 6))
`,
	})
	if err != nil {
		t.Fatalf("enhanced finance compute failed: %v", err)
	}
	for _, want := range []string{"irr= 0.089", "xirr= 0.0999", "sma_len= 17", "ema_len= 21", "call= 10.45", "parity_err= 0.0"} {
		if !strings.Contains(res.TextForLLM, want) {
			t.Fatalf("expected %q in output, got:\n%s", want, res.TextForLLM)
		}
	}
	if len(res.DirectMedia) != 1 || res.DirectMedia[0].Filename != "compare.png" {
		t.Fatalf("expected compare.png direct media, got %+v", res.DirectMedia)
	}

	// Non-sign-changing cashflows must fail cleanly instead of returning garbage/NaN.
	if _, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `print(finance.irr([100, 200, 300]))`,
	}); err == nil {
		t.Fatal("expected finance.irr to reject all-positive cashflows")
	}
}

func TestRenderTableAndGuideCardPNG(t *testing.T) {
	fontPath := filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf")
	ct := NewComputeTool(fontPath, 500000, 3)

	res, err := ct.Execute(context.Background(), ToolContext{}, ComputeArgs{
		Code: `
chart.table(
    title="3000元档 27寸4K显示器横评",
    subtitle="写代码 + 偶尔FPS场景推荐对比",
    headers=["维度", "方案A (Fast IPS)", "方案B (MiniLED)", "方案C (办公60Hz)"],
    rows=[
        ["参考价位", "2200~2600元", "2900~3300元", "1800~2100元"],
        ["文字清晰度", "极佳，无光晕", "优秀，白底偶有光晕", "极佳"],
        ["FPS游戏", "160Hz 丝滑够用", "160Hz 丝滑+HDR", "60Hz 拖影严重(避坑)"],
    ],
    highlight_col=1
)
chart.card(
    title="关西4天不特种兵行程卡",
    subtitle="大阪住到底 + 抽2天往返京都",
    sections=[
        {"heading": "Day1 大阪落地", "items": ["心斋桥/道顿堀吃晚饭", "梅田蓝天大厦看夜景"]},
        {"heading": "Day2 京都东线", "items": ["早7:30前冲清水寺避开旅行团", "二年坂三年坂 -> 八坂神社 -> 鸭川"]},
    ],
    footer="避坑：岚山小火车旺季提前官网抢票，西瓜卡直接用手机钱包开。"
)
`,
	})
	if err != nil {
		t.Fatalf("table/card render failed: %v", err)
	}
	if len(res.DirectMedia) != 2 {
		t.Fatalf("expected 2 PNG media outputs (table + card), got %d", len(res.DirectMedia))
	}
	for i, dm := range res.DirectMedia {
		if dm.MimeType != "image/png" {
			t.Fatalf("media[%d] expected image/png, got %q", i, dm.MimeType)
		}
		img, err := png.Decode(bytes.NewReader(dm.Data))
		if err != nil {
			t.Fatalf("decode PNG[%d]: %v", i, err)
		}
		b := img.Bounds()
		if b.Dx() < 600 || b.Dy() < 200 {
			t.Fatalf("unexpected PNG[%d] dimensions: %v", i, b)
		}
	}
}
