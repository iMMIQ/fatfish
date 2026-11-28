package travel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
)

// WeatherInfo holds real-time weather fetched from Open-Meteo (zero API key required).
type WeatherInfo struct {
	Summary       string
	TempC         float64
	ApparentC     float64
	WindKmh       float64
	Code          int
	Extreme       bool
	ExtremeReason string
}

type openMeteoResp struct {
	Current struct {
		Temperature2m       float64 `json:"temperature_2m"`
		ApparentTemperature float64 `json:"apparent_temperature"`
		WeatherCode         int     `json:"weather_code"`
		WindSpeed10m        float64 `json:"wind_speed_10m"`
	} `json:"current"`
}

// FetchWeather queries Open-Meteo for current city conditions using the SSRF-safe HTTP client.
func FetchWeather(ctx context.Context, cli *http.Client, baseURL string, lat, lon float64) (*WeatherInfo, error) {
	if cli == nil {
		return nil, fmt.Errorf("travel: weather http client nil")
	}
	if baseURL == "" {
		baseURL = "https://api.open-meteo.com/v1/forecast"
	}
	u := fmt.Sprintf("%s?latitude=%.4f&longitude=%.4f&current=temperature_2m,apparent_temperature,weather_code,wind_speed_10m&timezone=auto", baseURL, lat, lon)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "fatfish-travel/1.0")
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("travel: weather http %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, err
	}
	var parsed openMeteoResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}

	cur := parsed.Current
	cond := wmoWeatherZH(cur.WeatherCode)
	windDesc := windDescZH(cur.WindSpeed10m)
	summary := fmt.Sprintf("%s %.0f°C(体感%.0f°C)", cond, math.Round(cur.Temperature2m), math.Round(cur.ApparentTemperature))
	if windDesc != "" {
		summary += " " + windDesc
	}

	extreme, reason := detectExtremeWeather(cur.WeatherCode, cur.Temperature2m, cur.WindSpeed10m)
	return &WeatherInfo{
		Summary:       summary,
		TempC:         cur.Temperature2m,
		ApparentC:     cur.ApparentTemperature,
		WindKmh:       cur.WindSpeed10m,
		Code:          cur.WeatherCode,
		Extreme:       extreme,
		ExtremeReason: reason,
	}, nil
}

func wmoWeatherZH(code int) string {
	switch code {
	case 0:
		return "晴"
	case 1:
		return "大部晴朗"
	case 2:
		return "多云"
	case 3:
		return "阴"
	case 45, 48:
		return "起雾"
	case 51, 53, 55:
		return "毛毛雨"
	case 56, 57:
		return "冻毛毛雨"
	case 61:
		return "小雨"
	case 63:
		return "中雨"
	case 65:
		return "大雨"
	case 66, 67:
		return "冻雨"
	case 71:
		return "小雪"
	case 73:
		return "中雪"
	case 75:
		return "大雪"
	case 77:
		return "雪粒"
	case 80, 81:
		return "阵雨"
	case 82:
		return "暴雨"
	case 85, 86:
		return "阵雪"
	case 95:
		return "雷暴"
	case 96, 99:
		return "冰雹雷暴"
	default:
		return "多云"
	}
}

func windDescZH(kmh float64) string {
	switch {
	case kmh >= 55:
		return "大风"
	case kmh >= 32:
		return "风较大"
	case kmh >= 16:
		return "微风"
	default:
		return ""
	}
}

func detectExtremeWeather(code int, tempC, windKmh float64) (bool, string) {
	switch {
	case code >= 95:
		return true, "雷暴天气"
	case code == 65 || code == 82:
		return true, "暴雨天气"
	case code == 75 || code == 86:
		return true, "暴雪天气"
	case tempC >= 38:
		return true, fmt.Sprintf("%.0f°C高温热浪", math.Round(tempC))
	case tempC <= -15:
		return true, fmt.Sprintf("%.0f°C极寒低温", math.Round(tempC))
	case windKmh >= 55:
		return true, fmt.Sprintf("%.0fkm/h妖风", math.Round(windKmh))
	default:
		return false, ""
	}
}
