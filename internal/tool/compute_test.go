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
}
