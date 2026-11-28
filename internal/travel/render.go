package travel

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"strings"
	"time"

	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"

	"github.com/iMMIQ/fatfish/internal/fontcache"
	"github.com/iMMIQ/fatfish/internal/model"
)

func loadTTF(path string) (*opentype.Font, error) {
	f, err := fontcache.Load(path)
	if err != nil {
		return nil, fmt.Errorf("travel: %w", err)
	}
	return f, nil
}

func makeFace(f *opentype.Font, size float64) (font.Face, error) {
	return opentype.NewFace(f, &opentype.FaceOptions{
		Size:    size,
		DPI:     72,
		Hinting: font.HintingFull,
	})
}

// Simplified equirectangular continent/island polygons (lon, lat) for zero-asset pure-Go world map rendering.
var worldPolygons = [][][2]float64{
	// North America
	{{-168, 66}, {-162, 70}, {-140, 70}, {-125, 70}, {-110, 68}, {-94, 62}, {-82, 62}, {-78, 52}, {-64, 60}, {-55, 52}, {-60, 46}, {-70, 42}, {-76, 35}, {-81, 25}, {-83, 30}, {-90, 29}, {-97, 26}, {-97, 19}, {-87, 21}, {-84, 10}, {-79, 8}, {-85, 11}, {-92, 15}, {-105, 20}, {-110, 25}, {-117, 32}, {-124, 41}, {-124, 49}, {-135, 57}, {-150, 60}, {-162, 55}},
	// Greenland
	{{-52, 60}, {-42, 60}, {-32, 68}, {-18, 76}, {-24, 82}, {-48, 82}, {-62, 76}, {-54, 68}},
	// South America
	{{-79, 9}, {-72, 12}, {-62, 10}, {-50, 1}, {-35, -6}, {-38, -14}, {-41, -23}, {-52, -33}, {-58, -38}, {-65, -43}, {-68, -55}, {-74, -52}, {-73, -40}, {-71, -30}, {-70, -18}, {-81, -5}, {-79, 2}},
	// Europe (mainland + Scandinavia)
	{{-9, 37}, {-9, 43}, {-2, 43}, {-5, 48}, {2, 51}, {8, 54}, {10, 57}, {5, 60}, {10, 64}, {20, 70}, {31, 70}, {42, 66}, {50, 66}, {58, 55}, {44, 46}, {38, 45}, {30, 46}, {26, 41}, {23, 37}, {19, 41}, {13, 45}, {16, 40}, {12, 42}, {8, 44}, {3, 43}, {-5, 36}},
	// UK & Ireland
	{{-6, 50}, {1, 51}, {-1, 55}, {-4, 58}, {-6, 56}, {-5, 52}},
	{{-10, 52}, {-6, 52}, {-6, 55}, {-9, 55}},
	// Iceland
	{{-24, 64}, {-14, 64}, {-15, 66}, {-23, 66}},
	// Africa
	{{-17, 15}, {-13, 28}, {-6, 36}, {10, 37}, {12, 33}, {25, 32}, {33, 31}, {39, 21}, {43, 12}, {51, 12}, {42, 0}, {40, -11}, {35, -24}, {28, -33}, {18, -34}, {14, -22}, {12, -6}, {9, 4}, {2, 6}, {-8, 5}, {-16, 11}},
	// Madagascar
	{{44, -25}, {47, -25}, {50, -13}, {47, -14}, {44, -20}},
	// Asia
	{{27, 41}, {36, 37}, {35, 31}, {44, 13}, {59, 22}, {51, 26}, {56, 26}, {66, 25}, {73, 20}, {77, 8}, {81, 16}, {88, 22}, {93, 20}, {98, 16}, {100, 7}, {104, 1}, {104, 10}, {109, 12}, {106, 20}, {114, 22}, {121, 25}, {122, 31}, {120, 36}, {126, 38}, {129, 35}, {131, 42}, {140, 52}, {142, 60}, {160, 60}, {175, 66}, {160, 70}, {135, 72}, {105, 76}, {80, 73}, {62, 69}, {50, 66}, {58, 55}, {48, 46}, {41, 41}},
	// Japan
	{{130, 31}, {132, 33}, {136, 34}, {140, 36}, {141, 41}, {145, 44}, {141, 45}, {139, 39}, {135, 36}, {130, 34}},
	// Taiwan & Philippines
	{{120, 22}, {122, 25}, {121, 25}, {120, 23}},
	{{120, 14}, {122, 18}, {124, 13}, {126, 7}, {122, 7}},
	// Indonesia / Borneo / Java / Sumatra
	{{95, 5}, {104, -1}, {106, -6}, {101, -4}},
	{{106, -6}, {115, -8}, {114, -9}, {106, -7}},
	{{109, 1}, {117, 6}, {119, 1}, {116, -3}, {111, -3}},
	// Australia
	{{114, -22}, {122, -18}, {130, -12}, {136, -12}, {142, -11}, {146, -18}, {153, -27}, {150, -37}, {141, -38}, {135, -35}, {129, -32}, {115, -34}, {113, -26}},
	// New Zealand
	{{167, -46}, {174, -41}, {178, -37}, {173, -35}, {172, -41}, {166, -45}},
}

// OverviewData bundles current travel state, city, visits, journal, and suggestions for Console & rendering.
type OverviewData struct {
	Enabled         bool
	State           *model.TravelState
	CurrentCity     model.TravelCity
	NextCity        *model.TravelCity
	LocalTimeStr    string
	TimezoneDelta   string
	CitiesVisited   int
	CountriesCount  int
	DaysOnRoad      int
	SnapshotPreview string
	Visits          []model.TravelVisit
	Journal         []model.TravelJournalEntry
	Suggestions     []model.TravelSuggestion
}

// Overview collects full read-only diagnostics for the Admin Console /travel page.
func (s *Service) Overview(ctx context.Context) (*OverviewData, error) {
	now := time.Now()
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return nil, err
	}
	_, groupLoc, _, _, _, _ := s.snapshotConfig()
	cur, _ := CityByID(st.CurrentCityID)
	curLoc := loadCityLocation(cur.Timezone)

	var nextPtr *model.TravelCity
	if st.NextCityID != "" {
		if nc, ok := CityByID(st.NextCityID); ok {
			nextPtr = &nc
		}
	}
	citiesCnt, countriesCnt, _ := s.store.CountTravelStats(ctx)
	days := int(now.Sub(st.StartedAt).Hours()/24) + 1
	if days < 1 {
		days = 1
	}
	visits, _ := s.store.ListTravelVisits(ctx, 40, false)
	journal, _ := s.store.ListTravelJournal(ctx, 20)
	sugs, _ := s.store.ListTravelSuggestions(ctx, "", 20)

	return &OverviewData{
		Enabled:         s.Enabled(),
		State:           st,
		CurrentCity:     cur,
		NextCity:        nextPtr,
		LocalTimeStr:    now.In(curLoc).Format("2006-01-02 15:04") + " " + periodZH(now.In(curLoc).Hour()),
		TimezoneDelta:   formatTimezoneDelta(now, groupLoc, curLoc),
		CitiesVisited:   citiesCnt,
		CountriesCount:  countriesCnt,
		DaysOnRoad:      days,
		SnapshotPreview: s.SnapshotForChannel(ctx, "", now, groupLoc),
		Visits:          visits,
		Journal:         journal,
		Suggestions:     sugs,
	}, nil
}

// RenderMapPNG renders a sleek dark-theme world footprint map PNG with curved great-circle arcs,
// visited city markers, current live position (including in-transit progress), and summary stats.
func (s *Service) RenderMapPNG(ctx context.Context) ([]byte, error) {
	now := time.Now()
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return nil, err
	}
	_, _, _, fontPath, _, _ := s.snapshotConfig()
	f, err := loadTTF(fontPath)
	if err != nil {
		return nil, err
	}
	titleFace, err := makeFace(f, 20)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	subFace, err := makeFace(f, 13)
	if err != nil {
		return nil, err
	}
	defer subFace.Close()
	lblFace, err := makeFace(f, 11)
	if err != nil {
		return nil, err
	}
	defer lblFace.Close()

	visits, _ := s.store.ListTravelVisits(ctx, 200, true) // chronological
	citiesCnt, countriesCnt, _ := s.store.CountTravelStats(ctx)
	curCity, _ := CityByID(st.CurrentCityID)

	const W, H = 1040, 580
	const mapX0, mapY0, mapW, mapH = 24, 62, 992, 444

	img := image.NewRGBA(image.Rect(0, 0, W, H))
	bgCol := color.RGBA{11, 18, 32, 255}
	mapBgCol := color.RGBA{15, 23, 42, 255}
	gridCol := color.RGBA{26, 38, 62, 255}
	landCol := color.RGBA{34, 49, 74, 255}
	coastCol := color.RGBA{59, 78, 108, 255}
	arcCol := color.RGBA{56, 189, 248, 255}
	transitCol := color.RGBA{245, 158, 11, 255}
	curCol := color.RGBA{34, 197, 94, 255}
	textCol := color.RGBA{226, 232, 240, 255}
	mutedCol := color.RGBA{148, 163, 184, 255}

	draw.Draw(img, img.Bounds(), &image.Uniform{bgCol}, image.Point{}, draw.Src)
	fillRectImg(img, mapX0, mapY0, mapX0+mapW, mapY0+mapH, mapBgCol)

	proj := func(lat, lon float64) (int, int) {
		x := mapX0 + int(math.Round((lon+180.0)/360.0*float64(mapW)))
		// Clip polar extremes nicely to [-62, 82] so map fills frame cleanly
		latClamped := math.Max(-62, math.Min(82, lat))
		y := mapY0 + int(math.Round((82.0-latClamped)/144.0*float64(mapH)))
		return x, y
	}

	// Graticule grid
	for lon := -150.0; lon <= 150.0; lon += 30.0 {
		x, _ := proj(0, lon)
		drawLineImg(img, x, mapY0, x, mapY0+mapH, gridCol)
	}
	for lat := -45.0; lat <= 60.0; lat += 25.0 {
		_, y := proj(lat, 0)
		drawLineImg(img, mapX0, y, mapX0+mapW, y, gridCol)
	}
	// Equator slightly brighter
	_, eqY := proj(0, 0)
	drawLineImg(img, mapX0, eqY, mapX0+mapW, eqY, color.RGBA{36, 52, 82, 255})

	// Continents (filled scanline polygons + outline)
	for _, poly := range worldPolygons {
		pts := make([]image.Point, len(poly))
		for i, p := range poly {
			px, py := proj(p[1], p[0])
			pts[i] = image.Pt(px, py)
		}
		fillPolygon(img, pts, landCol)
		for i := 0; i < len(pts); i++ {
			next := pts[(i+1)%len(pts)]
			drawLineImg(img, pts[i].X, pts[i].Y, next.X, next.Y, coastCol)
		}
	}

	// Route arcs between consecutive visits
	for i := 1; i < len(visits); i++ {
		a, b := visits[i-1], visits[i]
		drawGreatCircleArc(img, a.Lat, a.Lon, b.Lat, b.Lon, proj, arcCol, false)
	}

	// In-transit live leg arc + interpolated plane marker
	var liveLat, liveLon float64
	hasLiveTransit := false
	if st.Status == "in_transit" && st.NextCityID != "" {
		if nc, ok := CityByID(st.NextCityID); ok {
			drawGreatCircleArc(img, curCity.Lat, curCity.Lon, nc.Lat, nc.Lon, proj, transitCol, true)
			totalDur := st.TransitETA.Sub(st.DepartAt).Seconds()
			prog := 0.5
			if totalDur > 0 {
				prog = now.Sub(st.DepartAt).Seconds() / totalDur
				prog = math.Max(0.05, math.Min(0.95, prog))
			}
			liveLat, liveLon = slerpLatLon(curCity.Lat, curCity.Lon, nc.Lat, nc.Lon, prog)
			hasLiveTransit = true

			nx, ny := proj(nc.Lat, nc.Lon)
			fillCircleImg(img, nx, ny, 5, transitCol)
			drawTextImg(img, lblFace, nc.NameZH, nx+8, ny+4, transitCol)
		}
	}

	// Visited city dots & non-overlapping labels (prioritize recent stops)
	type labelBox struct{ x0, y0, x1, y1 int }
	var placed []labelBox
	tryLabel := func(text string, x, y int, col color.RGBA) {
		tw := measureText(lblFace, text)
		box := labelBox{x, y - 12, x + tw + 4, y + 3}
		if box.x1 > mapX0+mapW-4 {
			x -= tw + 16
			box = labelBox{x, y - 12, x + tw + 4, y + 3}
		}
		for _, p := range placed {
			if box.x0 < p.x1 && box.x1 > p.x0 && box.y0 < p.y1 && box.y1 > p.y0 {
				return
			}
		}
		placed = append(placed, box)
		drawTextImg(img, lblFace, text, x, y, col)
	}

	// Current city glowing marker
	cx, cy := proj(curCity.Lat, curCity.Lon)
	if hasLiveTransit {
		lx, ly := proj(liveLat, liveLon)
		fillCircleImg(img, lx, ly, 9, color.RGBA{245, 158, 11, 90})
		fillCircleImg(img, lx, ly, 5, transitCol)
		fillCircleImg(img, cx, cy, 4, arcCol)
	} else {
		fillCircleImg(img, cx, cy, 11, color.RGBA{34, 197, 94, 75})
		fillCircleImg(img, cx, cy, 6, curCol)
		fillCircleImg(img, cx, cy, 2, color.RGBA{255, 255, 255, 255})
		tryLabel(curCity.NameZH, cx+10, cy+4, curCol)
	}

	// Visited dots (newest labels first)
	for i := len(visits) - 1; i >= 0; i-- {
		v := visits[i]
		vx, vy := proj(v.Lat, v.Lon)
		if v.CityID != curCity.ID || hasLiveTransit {
			fillCircleImg(img, vx, vy, 4, arcCol)
			fillCircleImg(img, vx, vy, 2, color.RGBA{224, 242, 254, 255})
			tryLabel(v.CityName, vx+7, vy+4, textCol)
		}
	}

	// Map border
	drawRectBorder(img, mapX0, mapY0, mapX0+mapW, mapY0+mapH, color.RGBA{51, 65, 85, 255})

	// Header
	drawTextImg(img, titleFace, "环球旅行足迹图 · WORLD JOURNEY", mapX0, 38, color.RGBA{56, 189, 248, 255})
	curLoc := loadCityLocation(curCity.Timezone)
	headerRight := fmt.Sprintf("当前: %s·%s  当地时间 %s", curCity.CountryZH, curCity.NameZH, now.In(curLoc).Format("15:04"))
	if st.Status == "in_transit" {
		if nc, ok := CityByID(st.NextCityID); ok {
			headerRight = fmt.Sprintf("移动中: %s → %s (%s)", curCity.NameZH, nc.NameZH, transportModeZH(st.TransportMode))
		}
	}
	rw := measureText(subFace, headerRight)
	drawTextImg(img, subFace, headerRight, mapX0+mapW-rw, 38, textCol)

	// Footer stats bar
	days := int(now.Sub(st.StartedAt).Hours()/24) + 1
	if days < 1 {
		days = 1
	}
	statsText := fmt.Sprintf("已旅行 %d 天  |  足迹 %d 城 / %d 国  |  累计里程 %.0f km", days, citiesCnt, countriesCnt, st.TotalDistanceKm)
	drawTextImg(img, subFace, statsText, mapX0, H-32, textCol)
	if st.WeatherSummary != "" && st.Status == "staying" {
		wTxt := "当地天气: " + st.WeatherSummary
		ww := measureText(subFace, wTxt)
		drawTextImg(img, subFace, wTxt, mapX0+mapW-ww, H-32, mutedCol)
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// RenderBoardingPassPNG renders a modern boarding pass / travel postcard PNG for the current leg.
func (s *Service) RenderBoardingPassPNG(ctx context.Context) ([]byte, error) {
	now := time.Now()
	st, err := s.EnsureState(ctx, now)
	if err != nil {
		return nil, err
	}
	_, _, _, fontPath, _, _ := s.snapshotConfig()
	f, err := loadTTF(fontPath)
	if err != nil {
		return nil, err
	}
	bigFace, err := makeFace(f, 44)
	if err != nil {
		return nil, err
	}
	defer bigFace.Close()
	titleFace, err := makeFace(f, 18)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	bodyFace, err := makeFace(f, 14)
	if err != nil {
		return nil, err
	}
	defer bodyFace.Close()
	smallFace, err := makeFace(f, 12)
	if err != nil {
		return nil, err
	}
	defer smallFace.Close()

	curCity, _ := CityByID(st.CurrentCityID)
	visits, _ := s.store.ListTravelVisits(ctx, 2, false)
	journal, _ := s.store.ListTravelJournal(ctx, 1)

	fromCity := curCity
	toCity := curCity
	headerLabel := "TRAVEL POSTCARD · 在途明信片"
	statusTag := "STAYING"
	modeLabel := "EXPLORING"
	if st.Status == "in_transit" && st.NextCityID != "" {
		if nc, ok := CityByID(st.NextCityID); ok {
			fromCity = curCity
			toCity = nc
			headerLabel = "BOARDING PASS · 环球登机牌"
			statusTag = "IN TRANSIT"
			modeLabel = strings.ToUpper(emptyOr(st.TransportMode, "FLIGHT"))
		}
	} else if len(visits) >= 2 {
		if prev, ok := CityByID(visits[1].CityID); ok {
			fromCity = prev
			toCity = curCity
			headerLabel = "ARRIVAL PASS · 抵达纪念票"
			modeLabel = strings.ToUpper(emptyOr(st.TransportMode, "FLIGHT"))
		}
	}

	const W, H = 920, 400
	img := image.NewRGBA(image.Rect(0, 0, W, H))
	bg := color.RGBA{11, 18, 32, 255}
	cardBg := color.RGBA{22, 33, 56, 255}
	headerBg := color.RGBA{14, 116, 144, 255}
	accent := color.RGBA{56, 189, 248, 255}
	amber := color.RGBA{245, 158, 11, 255}
	green := color.RGBA{34, 197, 94, 255}
	white := color.RGBA{248, 250, 252, 255}
	muted := color.RGBA{148, 163, 184, 255}
	divider := color.RGBA{51, 65, 85, 255}

	draw.Draw(img, img.Bounds(), &image.Uniform{bg}, image.Point{}, draw.Src)
	fillRectImg(img, 24, 24, W-24, H-24, cardBg)
	fillRectImg(img, 24, 24, W-24, 82, headerBg)
	drawRectBorder(img, 24, 24, W-24, H-24, divider)

	// Stub separator dashed line
	stubX := 650
	for y := 88; y < H-32; y += 12 {
		drawLineImg(img, stubX, y, stubX, minInt(y+6, H-32), divider)
	}
	// Ticket punch notches
	fillCircleImg(img, stubX, 82, 10, bg)
	fillCircleImg(img, stubX, H-24, 10, bg)

	// Header text
	drawTextImg(img, titleFace, headerLabel, 48, 60, white)
	legTxt := fmt.Sprintf("LEG #%02d  ·  %s", st.LegCount, modeLabel)
	lw := measureText(bodyFace, legTxt)
	drawTextImg(img, bodyFace, legTxt, W-48-lw, 58, color.RGBA{224, 242, 254, 255})

	// Left main section: IATA route
	drawTextImg(img, bigFace, fromCity.AirportCode, 52, 160, white)
	drawTextImg(img, bodyFace, fmt.Sprintf("%s·%s", fromCity.CountryZH, fromCity.NameZH), 52, 188, muted)

	toX := 440
	drawTextImg(img, bigFace, toCity.AirportCode, toX, 160, accent)
	drawTextImg(img, bodyFace, fmt.Sprintf("%s·%s", toCity.CountryZH, toCity.NameZH), toX, 188, muted)

	// Stylized route arrow between IATA codes
	arrowY := 145
	drawLineImg(img, 185, arrowY, 415, arrowY, accent)
	drawLineImg(img, 185, arrowY+1, 415, arrowY+1, accent)
	fillCircleImg(img, 185, arrowY, 4, white)
	fillCircleImg(img, 415, arrowY, 4, accent)
	_, _, legDist := EstimateTransit(fromCity, toCity)
	distStr := fmt.Sprintf("%.0f km", legDist)
	if fromCity.ID == toCity.ID {
		distStr = "HOME BASE"
	}
	dw := measureText(smallFace, distStr)
	drawTextImg(img, smallFace, distStr, 300-dw/2, arrowY-10, muted)

	// Divider
	drawLineImg(img, 48, 214, stubX-24, 214, divider)

	// Latest journal note wrapped
	quote := "正在世界各地慢悠悠摸鱼打卡中。"
	if len(journal) > 0 && journal[0].Content != "" {
		quote = "“" + journal[0].Content + "”"
	}
	drawTextImg(img, smallFace, "RECENT NOTE · 随手记", 48, 242, muted)
	yCur := 270
	for _, line := range wrapRunes(quote, 32) {
		drawTextImg(img, bodyFace, line, 48, yCur, white)
		yCur += 24
		if yCur > 346 {
			break
		}
	}

	// Right stub section: status, clock, weather, total km
	rx := stubX + 26
	badgeCol := green
	if st.Status == "in_transit" {
		badgeCol = amber
	}
	fillRectImg(img, rx, 108, rx+110, 134, badgeCol)
	drawTextImg(img, smallFace, statusTag, rx+14, 126, color.RGBA{11, 18, 32, 255})

	curLoc := loadCityLocation(curCity.Timezone)
	drawTextImg(img, smallFace, "LOCAL TIME", rx, 168, muted)
	drawTextImg(img, titleFace, now.In(curLoc).Format("15:04"), rx, 194, white)
	drawTextImg(img, smallFace, now.In(curLoc).Format("2006-01-02")+" "+periodZH(now.In(curLoc).Hour()), rx, 214, muted)

	drawTextImg(img, smallFace, "WEATHER", rx, 248, muted)
	wLines := wrapRunes(emptyOr(st.WeatherSummary, "未同步"), 14)
	wy := 270
	for _, wl := range wLines {
		drawTextImg(img, bodyFace, wl, rx, wy, white)
		wy += 20
		if wy > 312 {
			break
		}
	}

	drawTextImg(img, smallFace, "TOTAL DISTANCE", rx, 336, muted)
	drawTextImg(img, bodyFace, fmt.Sprintf("%.0f km", st.TotalDistanceKm), rx, 358, accent)

	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// --- Pure-Go 2D drawing helpers ---

func slerpLatLon(lat1, lon1, lat2, lon2, t float64) (float64, float64) {
	rLat1, rLon1 := lat1*math.Pi/180, lon1*math.Pi/180
	rLat2, rLon2 := lat2*math.Pi/180, lon2*math.Pi/180
	x1, y1, z1 := math.Cos(rLat1)*math.Cos(rLon1), math.Cos(rLat1)*math.Sin(rLon1), math.Sin(rLat1)
	x2, y2, z2 := math.Cos(rLat2)*math.Cos(rLon2), math.Cos(rLat2)*math.Sin(rLon2), math.Sin(rLat2)
	dot := math.Max(-1, math.Min(1, x1*x2+y1*y2+z1*z2))
	omega := math.Acos(dot)
	if omega < 1e-6 {
		return lat1, lon1
	}
	sinO := math.Sin(omega)
	a := math.Sin((1-t)*omega) / sinO
	b := math.Sin(t*omega) / sinO
	x, y, z := a*x1+b*x2, a*y1+b*y2, a*z1+b*z2
	lat := math.Atan2(z, math.Hypot(x, y)) * 180 / math.Pi
	lon := math.Atan2(y, x) * 180 / math.Pi
	return lat, lon
}

func drawGreatCircleArc(img *image.RGBA, lat1, lon1, lat2, lon2 float64, proj func(float64, float64) (int, int), col color.RGBA, dashed bool) {
	const steps = 48
	prevX, prevY := proj(lat1, lon1)
	prevLon := lon1
	for i := 1; i <= steps; i++ {
		t := float64(i) / float64(steps)
		la, lo := slerpLatLon(lat1, lon1, lat2, lon2, t)
		px, py := proj(la, lo)
		// Avoid drawing horizontal wrap-around line across map edges at antimeridian
		if math.Abs(lo-prevLon) < 160 {
			if !dashed || i%2 == 1 {
				drawLineImg(img, prevX, prevY, px, py, col)
				drawLineImg(img, prevX, prevY+1, px, py+1, col)
			}
		}
		prevX, prevY = px, py
		prevLon = lo
	}
}

func fillPolygon(img *image.RGBA, pts []image.Point, col color.RGBA) {
	if len(pts) < 3 {
		return
	}
	minY, maxY := pts[0].Y, pts[0].Y
	for _, p := range pts[1:] {
		if p.Y < minY {
			minY = p.Y
		}
		if p.Y > maxY {
			maxY = p.Y
		}
	}
	b := img.Bounds()
	if minY < b.Min.Y {
		minY = b.Min.Y
	}
	if maxY >= b.Max.Y {
		maxY = b.Max.Y - 1
	}
	var xs []int
	for y := minY; y <= maxY; y++ {
		xs = xs[:0]
		j := len(pts) - 1
		for i := 0; i < len(pts); i++ {
			yi, yj := pts[i].Y, pts[j].Y
			if (yi < y && yj >= y) || (yj < y && yi >= y) {
				x := pts[i].X + int(float64(y-yi)/float64(yj-yi)*float64(pts[j].X-pts[i].X))
				xs = append(xs, x)
			}
			j = i
		}
		// Simple insertion sort for small vertex counts
		for a := 1; a < len(xs); a++ {
			for k := a; k > 0 && xs[k] < xs[k-1]; k-- {
				xs[k], xs[k-1] = xs[k-1], xs[k]
			}
		}
		for k := 0; k+1 < len(xs); k += 2 {
			x0, x1 := xs[k], xs[k+1]
			if x0 < b.Min.X {
				x0 = b.Min.X
			}
			if x1 >= b.Max.X {
				x1 = b.Max.X - 1
			}
			for x := x0; x <= x1; x++ {
				img.SetRGBA(x, y, col)
			}
		}
	}
}

func drawTextImg(img *image.RGBA, face font.Face, s string, x, y int, col color.RGBA) {
	d := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(col),
		Face: face,
		Dot:  fixed.P(x, y),
	}
	d.DrawString(s)
}

func measureText(face font.Face, s string) int {
	return font.MeasureString(face, s).Ceil()
}

func fillRectImg(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	if x0 > x1 {
		x0, x1 = x1, x0
	}
	if y0 > y1 {
		y0, y1 = y1, y0
	}
	draw.Draw(img, image.Rect(x0, y0, x1+1, y1+1), &image.Uniform{col}, image.Point{}, draw.Src)
}

func drawRectBorder(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	drawLineImg(img, x0, y0, x1, y0, col)
	drawLineImg(img, x0, y1, x1, y1, col)
	drawLineImg(img, x0, y0, x0, y1, col)
	drawLineImg(img, x1, y0, x1, y1, col)
}

func fillCircleImg(img *image.RGBA, cx, cy, r int, col color.RGBA) {
	b := img.Bounds()
	for dy := -r; dy <= r; dy++ {
		for dx := -r; dx <= r; dx++ {
			if dx*dx+dy*dy <= r*r {
				x, y := cx+dx, cy+dy
				if x >= b.Min.X && x < b.Max.X && y >= b.Min.Y && y < b.Max.Y {
					if col.A < 255 {
						blendPixel(img, x, y, col)
					} else {
						img.SetRGBA(x, y, col)
					}
				}
			}
		}
	}
}

func blendPixel(img *image.RGBA, x, y int, src color.RGBA) {
	dst := img.RGBAAt(x, y)
	a := float64(src.A) / 255.0
	inv := 1.0 - a
	img.SetRGBA(x, y, color.RGBA{
		R: uint8(float64(src.R)*a + float64(dst.R)*inv),
		G: uint8(float64(src.G)*a + float64(dst.G)*inv),
		B: uint8(float64(src.B)*a + float64(dst.B)*inv),
		A: 255,
	})
}

func drawLineImg(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	b := img.Bounds()
	maxSteps := (b.Dx()+b.Dy())*2 + 32
	dx := x1 - x0
	if dx < 0 {
		dx = -dx
	}
	dy := y1 - y0
	if dy > 0 {
		dy = -dy
	}
	sx := -1
	if x0 < x1 {
		sx = 1
	}
	sy := -1
	if y0 < y1 {
		sy = 1
	}
	err := dx + dy
	for steps := 0; steps < maxSteps; steps++ {
		if x0 >= b.Min.X && x0 < b.Max.X && y0 >= b.Min.Y && y0 < b.Max.Y {
			img.SetRGBA(x0, y0, col)
		}
		if x0 == x1 && y0 == y1 {
			break
		}
		e2 := 2 * err
		if e2 >= dy {
			err += dy
			x0 += sx
		}
		if e2 <= dx {
			err += dx
			y0 += sy
		}
	}
}

func wrapRunes(s string, maxPerLine int) []string {
	if maxPerLine <= 0 {
		return []string{s}
	}
	runes := []rune(strings.TrimSpace(s))
	if len(runes) == 0 {
		return nil
	}
	var out []string
	for len(runes) > maxPerLine {
		out = append(out, string(runes[:maxPerLine]))
		runes = runes[maxPerLine:]
	}
	if len(runes) > 0 {
		out = append(out, string(runes))
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
