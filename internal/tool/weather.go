package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iMMIQ/fatfish/internal/netutil"
	"github.com/iMMIQ/fatfish/internal/travel"
)

// WeatherArgs defines the flat, $ref-free schema for global destination weather & forecast lookup.
type WeatherArgs struct {
	City string `json:"city" jsonschema:"required,description=Destination city or scenic spot name in Chinese or English (e.g. 东京, 厦门, 辽阳, 黄山, Reykjavik, Kyoto)"`
	Days int    `json:"days,omitempty" jsonschema:"description=Number of daily forecast days including today (default 5, max 10)"`
}

// WeatherTool queries real-time weather, 1-10 day daily forecasts, precipitation probability,
// UV index, and sunrise/sunset times via Open-Meteo (zero API key, SSRF-safe, read-only).
type WeatherTool struct {
	mu               sync.RWMutex
	client           *http.Client
	geoBaseURL       string
	nominatimBaseURL string
	forecastBaseURL  string
}

func NewWeatherTool(allowHosts []string) *WeatherTool {
	return &WeatherTool{
		client: netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
			Timeout:    12 * time.Second,
			AllowHosts: allowHosts,
		}),
		geoBaseURL:       "https://geocoding-api.open-meteo.com/v1/search",
		nominatimBaseURL: "https://nominatim.openstreetmap.org/search",
		forecastBaseURL:  "https://api.open-meteo.com/v1/forecast",
	}
}

func (t *WeatherTool) UpdateAllowHosts(allowHosts []string) {
	cli := netutil.NewSafeHTTPClient(netutil.SafeClientOptions{
		Timeout:    12 * time.Second,
		AllowHosts: allowHosts,
	})
	t.mu.Lock()
	t.client = cli
	t.mu.Unlock()
}

func (t *WeatherTool) httpClient() *http.Client {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	return c
}

func (t *WeatherTool) Name() string   { return "weather" }
func (t *WeatherTool) ReadOnly() bool { return true }
func (t *WeatherTool) Description() string {
	return "Check real-time weather, local time, 1-10 day forecast (high/low temp, rain probability, UV index, sunrise/sunset) for any global travel destination or city."
}

type geoPlace struct {
	Name     string
	Region   string
	Lat      float64
	Lon      float64
	Timezone string
}

// extraGeoCities supplements travel.AllCities() with popular CN/Asia/global travel destinations
// whose Simplified Chinese names are sometimes missing or ambiguous in GeoNames.
var extraGeoCities = []struct {
	Names    []string
	Display  string
	Region   string
	Lat      float64
	Lon      float64
	Timezone string
}{
	{[]string{"广州", "guangzhou", "canton"}, "广州", "中国·广东", 23.1291, 113.2644, "Asia/Shanghai"},
	{[]string{"深圳", "shenzhen"}, "深圳", "中国·广东", 22.5431, 114.0579, "Asia/Shanghai"},
	{[]string{"佛山", "foshan"}, "佛山", "中国·广东", 23.0215, 113.1214, "Asia/Shanghai"},
	{[]string{"珠海", "zhuhai"}, "珠海", "中国·广东", 22.2710, 113.5767, "Asia/Shanghai"},
	{[]string{"东莞", "dongguan"}, "东莞", "中国·广东", 23.0207, 113.7518, "Asia/Shanghai"},
	{[]string{"惠州", "huizhou", "南昆山"}, "惠州", "中国·广东", 23.1118, 114.4162, "Asia/Shanghai"},
	{[]string{"从化", "conghua"}, "从化", "中国·广东", 23.5489, 113.5866, "Asia/Shanghai"},
	{[]string{"清远", "qingyuan"}, "清远", "中国·广东", 23.6820, 113.0560, "Asia/Shanghai"},
	{[]string{"潮汕", "汕头", "shantou", "chaoshan"}, "汕头(潮汕)", "中国·广东", 23.3541, 116.6819, "Asia/Shanghai"},
	{[]string{"潮州", "chaozhou"}, "潮州", "中国·广东", 23.6569, 116.6226, "Asia/Shanghai"},
	{[]string{"南京", "nanjing"}, "南京", "中国·江苏", 32.0603, 118.7969, "Asia/Shanghai"},
	{[]string{"苏州", "suzhou"}, "苏州", "中国·江苏", 31.2989, 120.5853, "Asia/Shanghai"},
	{[]string{"无锡", "wuxi"}, "无锡", "中国·江苏", 31.4912, 120.3119, "Asia/Shanghai"},
	{[]string{"扬州", "yangzhou"}, "扬州", "中国·江苏", 32.3942, 119.4129, "Asia/Shanghai"},
	{[]string{"南通", "nantong"}, "南通", "中国·江苏", 31.9802, 120.8943, "Asia/Shanghai"},
	{[]string{"宁波", "ningbo"}, "宁波", "中国·浙江", 29.8683, 121.5440, "Asia/Shanghai"},
	{[]string{"舟山", "zhoushan"}, "舟山", "中国·浙江", 29.9855, 122.2072, "Asia/Shanghai"},
	{[]string{"湖州", "安吉", "huzhou", "anji"}, "湖州/安吉", "中国·浙江", 30.8943, 120.0868, "Asia/Shanghai"},
	{[]string{"温州", "wenzhou"}, "温州", "中国·浙江", 27.9938, 120.6994, "Asia/Shanghai"},
	{[]string{"黄山", "huangshan"}, "黄山", "中国·安徽", 29.7147, 118.3375, "Asia/Shanghai"},
	{[]string{"合肥", "hefei"}, "合肥", "中国·安徽", 31.8206, 117.2272, "Asia/Shanghai"},
	{[]string{"福州", "fuzhou"}, "福州", "中国·福建", 26.0745, 119.2965, "Asia/Shanghai"},
	{[]string{"泉州", "quanzhou"}, "泉州", "中国·福建", 24.8741, 118.6757, "Asia/Shanghai"},
	{[]string{"武汉", "wuhan"}, "武汉", "中国·湖北", 30.5928, 114.3055, "Asia/Shanghai"},
	{[]string{"长沙", "changsha"}, "长沙", "中国·湖南", 28.2282, 112.9388, "Asia/Shanghai"},
	{[]string{"张家界", "zhangjiajie"}, "张家界", "中国·湖南", 29.1171, 110.4792, "Asia/Shanghai"},
	{[]string{"景德镇", "jingdezhen"}, "景德镇", "中国·江西", 29.2688, 117.1784, "Asia/Shanghai"},
	{[]string{"南昌", "nanchang"}, "南昌", "中国·江西", 28.6820, 115.8579, "Asia/Shanghai"},
	{[]string{"青岛", "qingdao"}, "青岛", "中国·山东", 36.0671, 120.3826, "Asia/Shanghai"},
	{[]string{"济南", "jinan"}, "济南", "中国·山东", 36.6512, 117.1201, "Asia/Shanghai"},
	{[]string{"威海", "weihai"}, "威海", "中国·山东", 37.5131, 122.1204, "Asia/Shanghai"},
	{[]string{"郑州", "zhengzhou"}, "郑州", "中国·河南", 34.7466, 113.6253, "Asia/Shanghai"},
	{[]string{"洛阳", "luoyang"}, "洛阳", "中国·河南", 34.6197, 112.4540, "Asia/Shanghai"},
	{[]string{"太原", "taiyuan"}, "太原", "中国·山西", 37.8706, 112.5489, "Asia/Shanghai"},
	{[]string{"大同", "datong"}, "大同", "中国·山西", 40.0768, 113.3001, "Asia/Shanghai"},
	{[]string{"天津", "tianjin"}, "天津", "中国·天津", 39.3434, 117.3616, "Asia/Shanghai"},
	{[]string{"石家庄", "shijiazhuang"}, "石家庄", "中国·河北", 38.0428, 114.5149, "Asia/Shanghai"},
	{[]string{"呼和浩特", "hohhot"}, "呼和浩特", "中国·内蒙古", 40.8426, 111.7492, "Asia/Shanghai"},
	{[]string{"沈阳", "shenyang"}, "沈阳", "中国·辽宁", 41.8057, 123.4315, "Asia/Shanghai"},
	{[]string{"大连", "dalian"}, "大连", "中国·辽宁", 38.9140, 121.6147, "Asia/Shanghai"},
	{[]string{"辽阳", "liaoyang"}, "辽阳", "中国·辽宁", 41.2673, 123.2370, "Asia/Shanghai"},
	{[]string{"本溪", "benxi"}, "本溪", "中国·辽宁", 41.2942, 123.7665, "Asia/Shanghai"},
	{[]string{"长春", "changchun"}, "长春", "中国·吉林", 43.8171, 125.3235, "Asia/Shanghai"},
	{[]string{"延吉", "延边", "yanji", "yanbian"}, "延吉(延边)", "中国·吉林", 42.8913, 129.5089, "Asia/Shanghai"},
	{[]string{"珲春", "hunchun"}, "珲春", "中国·吉林", 42.8626, 130.3659, "Asia/Shanghai"},
	{[]string{"黑河", "heihe"}, "黑河", "中国·黑龙江", 50.2452, 127.5285, "Asia/Shanghai"},
	{[]string{"漠河", "mohe"}, "漠河", "中国·黑龙江", 52.9720, 122.5362, "Asia/Shanghai"},
	{[]string{"三亚", "sanya"}, "三亚", "中国·海南", 18.2528, 109.5120, "Asia/Shanghai"},
	{[]string{"海口", "haikou"}, "海口", "中国·海南", 20.0440, 110.1999, "Asia/Shanghai"},
	{[]string{"桂林", "guilin"}, "桂林", "中国·广西", 25.2740, 110.2991, "Asia/Shanghai"},
	{[]string{"南宁", "nanning"}, "南宁", "中国·广西", 22.8170, 108.3665, "Asia/Shanghai"},
	{[]string{"大理", "dali"}, "大理", "中国·云南", 25.6065, 100.2676, "Asia/Shanghai"},
	{[]string{"丽江", "lijiang"}, "丽江", "中国·云南", 26.8550, 100.2278, "Asia/Shanghai"},
	{[]string{"西双版纳", "景洪", "xishuangbanna"}, "西双版纳", "中国·云南", 22.0089, 100.7974, "Asia/Shanghai"},
	{[]string{"香格里拉", "shangri-la"}, "香格里拉", "中国·云南", 27.8252, 99.7060, "Asia/Shanghai"},
	{[]string{"贵阳", "guiyang"}, "贵阳", "中国·贵州", 26.6470, 106.6302, "Asia/Shanghai"},
	{[]string{"川西", "康定", "kangding"}, "康定(川西)", "中国·四川", 30.0507, 101.9624, "Asia/Shanghai"},
	{[]string{"九寨沟", "jiuzhaigou"}, "九寨沟", "中国·四川", 33.2600, 103.9186, "Asia/Shanghai"},
	{[]string{"乐山", "leshan"}, "乐山", "中国·四川", 29.5521, 103.7656, "Asia/Shanghai"},
	{[]string{"拉萨", "lhasa"}, "拉萨", "中国·西藏", 29.6500, 91.1000, "Asia/Shanghai"},
	{[]string{"乌鲁木齐", "urumqi"}, "乌鲁木齐", "中国·新疆", 43.8256, 87.6168, "Asia/Shanghai"},
	{[]string{"阿勒泰", "喀纳斯", "altay"}, "阿勒泰", "中国·新疆", 47.8449, 88.1413, "Asia/Shanghai"},
	{[]string{"喀什", "kashgar"}, "喀什", "中国·新疆", 39.4677, 75.9898, "Asia/Shanghai"},
	{[]string{"兰州", "lanzhou"}, "兰州", "中国·甘肃", 36.0611, 103.8343, "Asia/Shanghai"},
	{[]string{"敦煌", "dunhuang"}, "敦煌", "中国·甘肃", 40.1421, 94.6620, "Asia/Shanghai"},
	{[]string{"西宁", "xining"}, "西宁", "中国·青海", 36.6171, 101.7782, "Asia/Shanghai"},
	{[]string{"银川", "yinchuan"}, "银川", "中国·宁夏", 38.4872, 106.2309, "Asia/Shanghai"},
	{[]string{"澳门", "macau", "macao"}, "澳门", "中国·澳门", 22.1987, 113.5439, "Asia/Macau"},
	{[]string{"名古屋", "nagoya"}, "名古屋", "日本", 35.1815, 136.9066, "Asia/Tokyo"},
	{[]string{"仙台", "sendai"}, "仙台", "日本", 38.2682, 140.8694, "Asia/Tokyo"},
	{[]string{"广岛", "hiroshima"}, "广岛", "日本", 34.3853, 132.4553, "Asia/Tokyo"},
	{[]string{"神户", "kobe"}, "神户", "日本", 34.6901, 135.1955, "Asia/Tokyo"},
	{[]string{"奈良", "nara"}, "奈良", "日本", 34.6851, 135.8048, "Asia/Tokyo"},
	{[]string{"甲府", "kofu"}, "甲府", "日本", 35.6621, 138.5683, "Asia/Tokyo"},
	{[]string{"盛冈", "morioka"}, "盛冈", "日本", 39.7020, 141.1545, "Asia/Tokyo"},
	{[]string{"岘港", "da nang", "danang"}, "岘港", "越南", 16.0544, 108.2022, "Asia/Ho_Chi_Minh"},
	{[]string{"芽庄", "nha trang"}, "芽庄", "越南", 12.2388, 109.1967, "Asia/Ho_Chi_Minh"},
	{[]string{"海参崴", "符拉迪沃斯托克", "vladivostok"}, "海参崴(符拉迪沃斯托克)", "俄罗斯", 43.1155, 131.8855, "Asia/Vladivostok"},
	{[]string{"夏威夷", "檀香山", "honolulu", "hawaii"}, "檀香山(夏威夷)", "美国", 21.3069, -157.8583, "Pacific/Honolulu"},
	{[]string{"皇后镇", "queenstown"}, "皇后镇", "新西兰", -45.0312, 168.6626, "Pacific/Auckland"},
}

func lookupBuiltinGeo(query string) (geoPlace, bool) {
	q := strings.TrimSpace(query)
	if q == "" {
		return geoPlace{}, false
	}
	// Strip common Chinese administrative suffixes for fuzzy lookup (e.g. "厦门市" -> "厦门").
	qStripped := strings.TrimRight(q, "市县区")
	if qStripped == "" {
		qStripped = q
	}
	low := strings.ToLower(q)
	lowStripped := strings.ToLower(qStripped)

	// 1. Check travel package's curated ~110-city atlas first.
	for _, cand := range []string{q, qStripped} {
		if c, ok := travel.FindCity(cand); ok {
			return geoPlace{
				Name:     c.NameZH,
				Region:   c.CountryZH,
				Lat:      c.Lat,
				Lon:      c.Lon,
				Timezone: c.Timezone,
			}, true
		}
	}

	// 2. Check extra CN/Asia/global travel destinations table.
	for _, item := range extraGeoCities {
		for _, alias := range item.Names {
			al := strings.ToLower(alias)
			if al == low || al == lowStripped {
				return geoPlace{
					Name:     item.Display,
					Region:   item.Region,
					Lat:      item.Lat,
					Lon:      item.Lon,
					Timezone: item.Timezone,
				}, true
			}
		}
	}
	return geoPlace{}, false
}

func (t *WeatherTool) resolvePlace(ctx context.Context, query string) (geoPlace, error) {
	if p, ok := lookupBuiltinGeo(query); ok {
		return p, nil
	}
	if p, ok := t.geocodeOpenMeteo(ctx, query); ok {
		return p, nil
	}
	if p, ok := t.geocodeNominatim(ctx, query); ok {
		return p, nil
	}
	return geoPlace{}, fmt.Errorf("weather: could not locate %q (try standard city name or English/Pinyin)", query)
}

func (t *WeatherTool) geocodeOpenMeteo(ctx context.Context, query string) (geoPlace, bool) {
	u := t.geoBaseURL + "?name=" + url.QueryEscape(query) + "&count=10&language=zh&format=json"
	body, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), u, 256<<10, nil)
	if err != nil || code >= 400 {
		return geoPlace{}, false
	}
	var parsed struct {
		Results []struct {
			Name        string  `json:"name"`
			Country     string  `json:"country"`
			Admin1      string  `json:"admin1"`
			Latitude    float64 `json:"latitude"`
			Longitude   float64 `json:"longitude"`
			Timezone    string  `json:"timezone"`
			Population  int     `json:"population"`
			FeatureCode string  `json:"feature_code"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || len(parsed.Results) == 0 {
		return geoPlace{}, false
	}
	// Rank capital / provincial / major cities ahead of tiny homonymous villages.
	featureRank := func(fc string) int {
		switch fc {
		case "PPLC":
			return 5
		case "PPLA":
			return 4
		case "PPLA2":
			return 3
		case "PPLA3":
			return 2
		case "PPL":
			return 1
		default:
			return 0
		}
	}
	sort.SliceStable(parsed.Results, func(i, j int) bool {
		ri, rj := parsed.Results[i], parsed.Results[j]
		if ri.Population != rj.Population {
			return ri.Population > rj.Population
		}
		return featureRank(ri.FeatureCode) > featureRank(rj.FeatureCode)
	})
	best := parsed.Results[0]
	region := best.Country
	if best.Admin1 != "" && best.Admin1 != best.Name {
		if region != "" {
			region += "·" + best.Admin1
		} else {
			region = best.Admin1
		}
	}
	return geoPlace{
		Name:     best.Name,
		Region:   region,
		Lat:      best.Latitude,
		Lon:      best.Longitude,
		Timezone: best.Timezone,
	}, true
}

func (t *WeatherTool) geocodeNominatim(ctx context.Context, query string) (geoPlace, bool) {
	u := t.nominatimBaseURL + "?format=json&limit=3&accept-language=zh-CN,en&q=" + url.QueryEscape(query)
	body, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), u, 256<<10, map[string]string{
		"User-Agent": "fatfish-weather/1.0 (https://github.com/iMMIQ/fatfish)",
	})
	if err != nil || code >= 400 {
		return geoPlace{}, false
	}
	var items []struct {
		DisplayName string `json:"display_name"`
		Lat         string `json:"lat"`
		Lon         string `json:"lon"`
	}
	if err := json.Unmarshal(body, &items); err != nil || len(items) == 0 {
		return geoPlace{}, false
	}
	lat, err1 := strconv.ParseFloat(items[0].Lat, 64)
	lon, err2 := strconv.ParseFloat(items[0].Lon, 64)
	if err1 != nil || err2 != nil {
		return geoPlace{}, false
	}
	disp := strings.TrimSpace(items[0].DisplayName)
	parts := strings.Split(disp, ",")
	name := strings.TrimSpace(query)
	region := ""
	if len(parts) >= 2 {
		region = strings.TrimSpace(parts[len(parts)-1])
	}
	return geoPlace{
		Name:   name,
		Region: region,
		Lat:    lat,
		Lon:    lon,
	}, true
}

func (t *WeatherTool) Execute(ctx context.Context, _ ToolContext, args WeatherArgs) (ToolResult, error) {
	city := strings.TrimSpace(args.City)
	if city == "" {
		return ToolResult{}, errors.New("weather: city is required")
	}
	days := args.Days
	if days <= 0 {
		days = 5
	}
	if days > 10 {
		days = 10
	}

	place, err := t.resolvePlace(ctx, city)
	if err != nil {
		return ToolResult{}, err
	}

	u := fmt.Sprintf("%s?latitude=%.4f&longitude=%.4f&current=temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,wind_speed_10m&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max,sunrise,sunset,uv_index_max&timezone=auto&forecast_days=%d",
		t.forecastBaseURL, place.Lat, place.Lon, days)
	raw, _, code, err := netutil.FetchLimited(ctx, t.httpClient(), u, 256<<10, map[string]string{
		"User-Agent": "fatfish-weather/1.0",
	})
	if err != nil {
		return ToolResult{}, fmt.Errorf("weather: fetch failed: %w", err)
	}
	if code >= 400 {
		return ToolResult{}, fmt.Errorf("weather: forecast HTTP %d", code)
	}

	var resp struct {
		Timezone string `json:"timezone"`
		Current  struct {
			Time                string  `json:"time"`
			Temperature2m       float64 `json:"temperature_2m"`
			ApparentTemperature float64 `json:"apparent_temperature"`
			RelativeHumidity2m  float64 `json:"relative_humidity_2m"`
			WeatherCode         int     `json:"weather_code"`
			WindSpeed10m        float64 `json:"wind_speed_10m"`
		} `json:"current"`
		Daily struct {
			Time                     []string  `json:"time"`
			WeatherCode              []int     `json:"weather_code"`
			TempMax                  []float64 `json:"temperature_2m_max"`
			TempMin                  []float64 `json:"temperature_2m_min"`
			PrecipitationProbMax     []float64 `json:"precipitation_probability_max"`
			Sunrise                  []string  `json:"sunrise"`
			Sunset                   []string  `json:"sunset"`
			UVIndexMax               []float64 `json:"uv_index_max"`
		} `json:"daily"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return ToolResult{}, fmt.Errorf("weather: parse failed: %w", err)
	}

	tz := resp.Timezone
	if tz == "" {
		tz = place.Timezone
	}
	var b strings.Builder
	locLabel := place.Name
	if place.Region != "" {
		locLabel += " (" + place.Region + ")"
	}
	fmt.Fprintf(&b, "目的地: %s | 坐标: %.2f, %.2f", locLabel, place.Lat, place.Lon)
	if tz != "" {
		if loc, lerr := time.LoadLocation(tz); lerr == nil {
			fmt.Fprintf(&b, " | 当地时间: %s (%s)", time.Now().In(loc).Format("2006-01-02 15:04"), tz)
		} else {
			fmt.Fprintf(&b, " | 时区: %s", tz)
		}
	}
	b.WriteByte('\n')

	fmt.Fprintf(&b, "当前实况: %s %.0f°C (体感%.0f°C) | 湿度 %.0f%% | 风速 %.0fkm/h\n",
		weatherCodeZH(resp.Current.WeatherCode),
		math.Round(resp.Current.Temperature2m),
		math.Round(resp.Current.ApparentTemperature),
		math.Round(resp.Current.RelativeHumidity2m),
		math.Round(resp.Current.WindSpeed10m),
	)

	if len(resp.Daily.Time) > 0 {
		b.WriteString("\n未来每日预报:\n")
		weekdays := [...]string{"周日", "周一", "周二", "周三", "周四", "周五", "周六"}
		for i, dayStr := range resp.Daily.Time {
			wd := ""
			if dt, perr := time.Parse("2006-01-02", dayStr); perr == nil {
				wd = "(" + weekdays[dt.Weekday()] + ")"
			}
			cond := "多云"
			if i < len(resp.Daily.WeatherCode) {
				cond = weatherCodeZH(resp.Daily.WeatherCode[i])
			}
			tMin, tMax := 0.0, 0.0
			if i < len(resp.Daily.TempMin) {
				tMin = resp.Daily.TempMin[i]
			}
			if i < len(resp.Daily.TempMax) {
				tMax = resp.Daily.TempMax[i]
			}
			line := fmt.Sprintf("- %s%s: %s %.0f~%.0f°C", dayStr, wd, cond, math.Round(tMin), math.Round(tMax))
			if i < len(resp.Daily.PrecipitationProbMax) {
				line += fmt.Sprintf(" | 降水概率 %.0f%%", math.Round(resp.Daily.PrecipitationProbMax[i]))
			}
			if i < len(resp.Daily.Sunrise) && i < len(resp.Daily.Sunset) {
				sr := shortClockTime(resp.Daily.Sunrise[i])
				ss := shortClockTime(resp.Daily.Sunset[i])
				if sr != "" && ss != "" {
					line += fmt.Sprintf(" | 日出 %s / 日落 %s", sr, ss)
				}
			}
			if i < len(resp.Daily.UVIndexMax) && resp.Daily.UVIndexMax[i] > 0 {
				line += fmt.Sprintf(" | UV %.0f", math.Round(resp.Daily.UVIndexMax[i]))
			}
			b.WriteString(line + "\n")
		}
	}

	return ToolResult{TextForLLM: netutil.WrapExternalData("weather:"+place.Name, strings.TrimSpace(b.String()))}, nil
}

func shortClockTime(iso string) string {
	if idx := strings.IndexByte(iso, 'T'); idx >= 0 && len(iso) >= idx+6 {
		return iso[idx+1 : idx+6]
	}
	return ""
}

func weatherCodeZH(code int) string {
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
