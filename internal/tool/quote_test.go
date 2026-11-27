package tool

import (
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
		{"usSMH", "usSMH"},
		{"nvda", "usNVDA"},
		{"600519", "sh600519"},
		{"000001", "sz000001"},
		{"00700", "hk00700"},
	}
	for _, tc := range cases {
		if got := normalizeTicker(tc.in); got != tc.want {
			t.Errorf("normalizeTicker(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	rawUS := `v_usSMH="200~半导体指数ETF-VanEck~SMH.OQ~422.46~426.75~402.67~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~2026-11-25~-4.29~-1.01~422.97~393.88";`
	summary, code := parseTencentQuote(rawUS)
	if code != "SMH.OQ" {
		t.Fatalf("expected quote code SMH.OQ, got %q", code)
	}
	if !strings.Contains(summary, "422.46") || !strings.Contains(summary, "-1.01%") {
		t.Fatalf("unexpected summary: %q", summary)
	}

	klJSON := []byte(`{"code":0,"data":{"usSMH.OQ":{"day":[["2026-11-12","485.010","501.730","509.800","484.500","10210284.000"],["2026-11-13","499.930","486.750","500.270","473.260","12400180.000"]]}}}`)
	lines := parseTencentKline(klJSON, "usSMH.OQ")
	if len(lines) != 2 || !strings.Contains(lines[0], "509.800") {
		t.Fatalf("expected 2 US K-line bars with high 509.800, got %+v", lines)
	}
}
