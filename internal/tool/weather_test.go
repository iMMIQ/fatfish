package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"image/png"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestWeatherToolBuiltinGeoAndForecast(t *testing.T) {
	// Built-in lookup must resolve major CN/Asian travel destinations without hitting external geocoding.
	for _, q := range []string{"东京", "厦门", "厦门市", "辽阳", "黄山", "从化", "潮汕", "大阪", "海参崴", "Reykjavik"} {
		if _, ok := lookupBuiltinGeo(q); !ok {
			t.Fatalf("lookupBuiltinGeo(%q) failed", q)
		}
	}

	forecastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"timezone": "Asia/Tokyo",
			"current": map[string]any{
				"time":                 "2026-11-28T14:00",
				"temperature_2m":       15.4,
				"apparent_temperature": 13.8,
				"relative_humidity_2m": 58.0,
				"weather_code":         1,
				"wind_speed_10m":       12.2,
			},
			"daily": map[string]any{
				"time":                          []string{"2026-11-28", "2026-11-29", "2026-11-30"},
				"weather_code":                  []int{1, 61, 0},
				"temperature_2m_max":            []float64{17.0, 14.2, 16.5},
				"temperature_2m_min":            []float64{8.1, 7.3, 6.9},
				"precipitation_probability_max": []float64{10, 75, 5},
				"sunrise":                       []string{"2026-11-28T06:29", "2026-11-29T06:30", "2026-11-30T06:31"},
				"sunset":                        []string{"2026-11-28T16:28", "2026-11-29T16:28", "2026-11-30T16:28"},
				"uv_index_max":                  []float64{3.2, 1.8, 3.6},
			},
		})
	}))
	defer forecastSrv.Close()

	wt := NewWeatherTool([]string{"127.0.0.1"})
	wt.forecastBaseURL = forecastSrv.URL

	res, err := wt.Execute(context.Background(), ToolContext{}, WeatherArgs{City: "东京", Days: 3})
	if err != nil {
		t.Fatalf("weather.Execute failed: %v", err)
	}
	if !strings.Contains(res.TextForLLM, "东京") ||
		!strings.Contains(res.TextForLLM, "降水概率 75%") ||
		!strings.Contains(res.TextForLLM, "日出 06:29 / 日落 16:28") ||
		!strings.Contains(res.TextForLLM, "<external_data") {
		t.Fatalf("unexpected weather output:\n%s", res.TextForLLM)
	}
}

func TestWeatherToolGeocodeRankingAndNominatimFallback(t *testing.T) {
	// Open-Meteo returns a tiny village before a major city -> must pick higher population city.
	geoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("name")
		if q == "FallbackTown" {
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{
				map[string]any{"name": "TestVille", "country": "X", "admin1": "Tiny", "latitude": 1.0, "longitude": 1.0, "population": 200, "feature_code": "PPL"},
				map[string]any{"name": "TestVille", "country": "Y", "admin1": "Metro", "latitude": 35.0, "longitude": 135.0, "population": 1500000, "feature_code": "PPLA"},
			},
		})
	}))
	defer geoSrv.Close()

	nomSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"display_name": "FallbackTown, ExampleProvince, ExampleCountry",
			"lat":          "28.5000",
			"lon":          "115.8000",
		}})
	}))
	defer nomSrv.Close()

	wt := NewWeatherTool([]string{"127.0.0.1"})
	wt.geoBaseURL = geoSrv.URL
	wt.nominatimBaseURL = nomSrv.URL

	p1, err := wt.resolvePlace(context.Background(), "TestVille")
	if err != nil || p1.Lat != 35.0 || p1.Region != "Y·Metro" {
		t.Fatalf("expected major city match, got %+v err=%v", p1, err)
	}
	p2, err := wt.resolvePlace(context.Background(), "FallbackTown")
	if err != nil || p2.Lat != 28.5 || p2.Region != "ExampleCountry" {
		t.Fatalf("expected nominatim fallback match, got %+v err=%v", p2, err)
	}
}

func TestTravelFXQuoteAliasesAndConversion(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"日元", "fx:JPYCNY"},
		{"JPYCNY", "fx:JPYCNY"},
		{"港币", "fx:HKDCNY"},
		{"泰铢", "fx:THBCNY"},
		{"欧元", "fx:EURCNY"},
		{"韩元", "fx:KRWCNY"},
		{"新币", "fx:SGDCNY"},
		{"台币", "fx:TWDCNY"},
		{"英镑", "fx:GBPCNY"},
		{"澳元", "fx:AUDCNY"},
		{"TRY/CNY", "fx:TRYCNY"},
	}
	for _, tc := range cases {
		if got := normalizeTicker(tc.in); got != tc.want {
			t.Errorf("normalizeTicker(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	fxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result":               "success",
			"time_last_update_utc": "Sat, 28 Nov 2026 00:00:01",
			"rates": map[string]float64{
				"CNY": 0.0418,
				"USD": 0.00626,
			},
		})
	}))
	defer fxSrv.Close()

	qt := NewQuoteTool([]string{"127.0.0.1"}, nil)
	qt.fxBaseURL = fxSrv.URL

	res, err := qt.Execute(context.Background(), ToolContext{}, QuoteArgs{Symbol: "日元"})
	if err != nil {
		t.Fatalf("quote FX failed: %v", err)
	}
	if !strings.Contains(res.TextForLLM, "日元兑人民币") || !strings.Contains(res.TextForLLM, "100 JPY ≈ 4.18 CNY") {
		t.Fatalf("unexpected FX quote output:\n%s", res.TextForLLM)
	}
}

func TestRenderCardTravelHeadingsAndLongFooter(t *testing.T) {
	if !hasLeadingOrdinalOrTag("Day 1 大阪") || !hasLeadingOrdinalOrTag("【避坑】") || !hasLeadingOrdinalOrTag("1. 方案A") || !hasLeadingOrdinalOrTag("D2 京都") {
		t.Fatalf("expected leading ordinal/tag detection to succeed")
	}
	if hasLeadingOrdinalOrTag("核心建议") {
		t.Fatalf("expected plain heading without ordinal to return false")
	}

	fontPath := filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf")
	cr := NewChartRenderer(fontPath)
	pngBytes, err := cr.RenderCard(
		"关西4日不特种兵攻略",
		"杭州出发 · 大阪住到底 · 2天往返京都",
		[]CardSection{
			{Heading: "Day 1 大阪市区", Items: []string{"下午落地难波办入住", "晚上心斋桥/道顿堀干饭+梅田夜景"}},
			{Heading: "Day 2 京都东线", Items: []string{"7:30前冲清水寺避旅行团", "二年坂→八坂神社→鸭川晚饭"}},
		},
		"避坑1：清水寺9点后全是团，务必早起\n避坑2：岚山小火车旺季提前官网抢票\n避坑3：日本新政退税挑唐吉诃德/BicCamera等0%手续费店，别踩第三方高抽成坑\n避坑4：西瓜卡手机钱包直接开",
	)
	if err != nil {
		t.Fatalf("RenderCard failed: %v", err)
	}
	if _, err := png.Decode(bytes.NewReader(pngBytes)); err != nil {
		t.Fatalf("RenderCard produced invalid PNG: %v", err)
	}
}
