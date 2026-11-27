package tool

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"os"
	"sync"

	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"
)

// ChartRenderer renders publication-grade PNG charts (line, bar, pie, scatter, kline)
// with TrueType (.ttf) CJK font support and zero CGO.
type ChartRenderer struct {
	fontPath string
	mu       sync.Mutex
	parsed   *opentype.Font
}

func NewChartRenderer(fontPath string) *ChartRenderer {
	return &ChartRenderer{fontPath: fontPath}
}

func (c *ChartRenderer) loadFont() (*opentype.Font, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.parsed != nil {
		return c.parsed, nil
	}
	raw, err := os.ReadFile(c.fontPath)
	if err != nil {
		return nil, fmt.Errorf("chart: read font %s: %w", c.fontPath, err)
	}
	f, err := opentype.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("chart: parse truetype font %s: %w", c.fontPath, err)
	}
	c.parsed = f
	return f, nil
}

// VerifyFont validates that the configured .ttf font exists and parses cleanly.
func (c *ChartRenderer) VerifyFont() error {
	_, err := c.loadFont()
	return err
}

func (c *ChartRenderer) newFace(size float64) (font.Face, error) {
	f, err := c.loadFont()
	if err != nil {
		return nil, err
	}
	return opentype.NewFace(f, &opentype.FaceOptions{
		Size:    size,
		DPI:     72,
		Hinting: font.HintingFull,
	})
}

var chartPalette = []color.RGBA{
	{R: 37, G: 99, B: 235, A: 255},  // blue
	{R: 220, G: 38, B: 38, A: 255},  // red
	{R: 5, G: 150, B: 105, A: 255},  // emerald
	{R: 217, G: 119, B: 6, A: 255},  // amber
	{R: 124, G: 58, B: 237, A: 255}, // violet
	{R: 14, G: 116, B: 144, A: 255}, // cyan
}

type Series struct {
	Name   string
	Values []float64
}

type Candle struct {
	Label string
	Open  float64
	High  float64
	Low   float64
	Close float64
}

// RenderLineOrBar renders a multi-series line or bar chart with Chinese labels as PNG bytes.
func (c *ChartRenderer) RenderLineOrBar(kind, title string, labels []string, series []Series) ([]byte, error) {
	if len(series) == 0 || len(series[0].Values) == 0 {
		return nil, errors.New("chart: empty data series")
	}
	const W, H = 900, 540
	const padL, padR, padT, padB = 78, 36, 72, 76

	img := image.NewRGBA(image.Rect(0, 0, W, H))
	draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{255, 255, 255, 255}}, image.Point{}, draw.Src)

	titleFace, err := c.newFace(20)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	axisFace, err := c.newFace(12)
	if err != nil {
		return nil, err
	}
	defer axisFace.Close()

	// Title
	drawText(img, titleFace, title, padL, 38, color.RGBA{17, 24, 39, 255})

	// Legend
	lx := padL
	for si, s := range series {
		col := chartPalette[si%len(chartPalette)]
		fillRect(img, lx, 48, lx+14, 58, col)
		name := s.Name
		if name == "" {
			name = fmt.Sprintf("Series %d", si+1)
		}
		drawText(img, axisFace, name, lx+20, 58, color.RGBA{55, 65, 81, 255})
		lx += 28 + textWidth(axisFace, name) + 14
	}

	// Determine min/max Y
	minY, maxY := math.Inf(1), math.Inf(-1)
	maxN := 0
	for _, s := range series {
		if len(s.Values) > maxN {
			maxN = len(s.Values)
		}
		for _, v := range s.Values {
			if v < minY {
				minY = v
			}
			if v > maxY {
				maxY = v
			}
		}
	}
	if kind == "bar" && minY > 0 {
		minY = 0
	}
	if maxY == minY {
		maxY = minY + 1
	}
	margin := (maxY - minY) * 0.08
	if kind != "bar" {
		minY -= margin
	}
	maxY += margin

	plotW := W - padL - padR
	plotH := H - padT - padB
	yToPx := func(v float64) int {
		ratio := (v - minY) / (maxY - minY)
		return padT + plotH - int(math.Round(ratio*float64(plotH)))
	}

	// Horizontal gridlines & Y ticks
	gridCol := color.RGBA{229, 231, 235, 255}
	axisCol := color.RGBA{107, 114, 128, 255}
	for i := 0; i <= 5; i++ {
		val := minY + (maxY-minY)*float64(i)/5.0
		py := yToPx(val)
		drawLine(img, padL, py, W-padR, py, gridCol)
		lbl := formatNum(val)
		drawText(img, axisFace, lbl, padL-textWidth(axisFace, lbl)-8, py+4, axisCol)
	}
	// Axes borders
	drawLine(img, padL, padT, padL, H-padB, axisCol)
	drawLine(img, padL, H-padB, W-padR, H-padB, axisCol)

	// Plot data
	if kind == "bar" {
		groupW := float64(plotW) / float64(maxN)
		barW := math.Max(4, (groupW*0.76)/float64(len(series)))
		zeroY := yToPx(0)
		for si, s := range series {
			col := chartPalette[si%len(chartPalette)]
			for i, v := range s.Values {
				x0 := padL + int(float64(i)*groupW+groupW*0.12+float64(si)*barW)
				x1 := x0 + int(barW) - 2
				if x1 <= x0 {
					x1 = x0 + 1
				}
				py := yToPx(v)
				y0, y1 := py, zeroY
				if y0 > y1 {
					y0, y1 = y1, y0
				}
				fillRect(img, x0, y0, x1, y1, col)
			}
		}
	} else {
		// Line or scatter
		for si, s := range series {
			col := chartPalette[si%len(chartPalette)]
			var prevX, prevY int
			for i, v := range s.Values {
				var px int
				if maxN <= 1 {
					px = padL + plotW/2
				} else {
					px = padL + int(math.Round(float64(i)*float64(plotW)/float64(maxN-1)))
				}
				py := yToPx(v)
				if kind != "scatter" && i > 0 {
					drawThickLine(img, prevX, prevY, px, py, 2, col)
				}
				fillCircle(img, px, py, 4, col)
				prevX, prevY = px, py
			}
		}
	}

	// X labels (skip evenly if crowded)
	step := 1
	if maxN > 12 {
		step = int(math.Ceil(float64(maxN) / 10.0))
	}
	for i := 0; i < maxN; i += step {
		lbl := fmt.Sprintf("%d", i+1)
		if i < len(labels) && labels[i] != "" {
			lbl = labels[i]
		}
		var px int
		if kind == "bar" {
			px = padL + int((float64(i)+0.5)*float64(plotW)/float64(maxN))
		} else if maxN <= 1 {
			px = padL + plotW/2
		} else {
			px = padL + int(math.Round(float64(i)*float64(plotW)/float64(maxN-1)))
		}
		tw := textWidth(axisFace, lbl)
		drawText(img, axisFace, lbl, px-tw/2, H-padB+22, axisCol)
	}

	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// RenderPie renders a pie chart with Chinese title and right-side percentage legend.
func (c *ChartRenderer) RenderPie(title string, labels []string, values []float64) ([]byte, error) {
	if len(values) == 0 {
		return nil, errors.New("chart: empty pie data")
	}
	var total float64
	for _, v := range values {
		if v > 0 {
			total += v
		}
	}
	if total <= 0 {
		return nil, errors.New("chart: pie values must sum to > 0")
	}
	const W, H = 840, 500
	img := image.NewRGBA(image.Rect(0, 0, W, H))
	draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{255, 255, 255, 255}}, image.Point{}, draw.Src)

	titleFace, err := c.newFace(20)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	lblFace, err := c.newFace(13)
	if err != nil {
		return nil, err
	}
	defer lblFace.Close()

	drawText(img, titleFace, title, 40, 42, color.RGBA{17, 24, 39, 255})

	cx, cy, r := 250, 275, 165
	// Precompute cumulative angles
	angles := make([]float64, len(values)+1)
	acc := 0.0
	for i, v := range values {
		if v > 0 {
			acc += v / total * 2 * math.Pi
		}
		angles[i+1] = acc
	}
	for y := cy - r; y <= cy+r; y++ {
		for x := cx - r; x <= cx+r; x++ {
			dx, dy := float64(x-cx), float64(y-cy)
			if dx*dx+dy*dy > float64(r*r) {
				continue
			}
			ang := math.Atan2(dy, dx)
			if ang < 0 {
				ang += 2 * math.Pi
			}
			for i := 0; i < len(values); i++ {
				if ang >= angles[i] && ang <= angles[i+1] {
					img.SetRGBA(x, y, chartPalette[i%len(chartPalette)])
					break
				}
			}
		}
	}

	// Legend on right side
	lx, ly := 470, 110
	for i, v := range values {
		if v <= 0 {
			continue
		}
		col := chartPalette[i%len(chartPalette)]
		fillRect(img, lx, ly-11, lx+14, ly+3, col)
		name := fmt.Sprintf("Item %d", i+1)
		if i < len(labels) && labels[i] != "" {
			name = labels[i]
		}
		txt := fmt.Sprintf("%s: %.1f%% (%s)", name, v/total*100, formatNum(v))
		drawText(img, lblFace, txt, lx+22, ly, color.RGBA{31, 41, 55, 255})
		ly += 28
	}

	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// RenderKline renders a candlestick K-line chart (red up, green down in CN convention).
func (c *ChartRenderer) RenderKline(title string, candles []Candle) ([]byte, error) {
	if len(candles) == 0 {
		return nil, errors.New("chart: empty kline candles")
	}
	const W, H = 920, 540
	const padL, padR, padT, padB = 78, 36, 68, 72

	img := image.NewRGBA(image.Rect(0, 0, W, H))
	draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{255, 255, 255, 255}}, image.Point{}, draw.Src)

	titleFace, err := c.newFace(20)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	axisFace, err := c.newFace(12)
	if err != nil {
		return nil, err
	}
	defer axisFace.Close()

	drawText(img, titleFace, title, padL, 38, color.RGBA{17, 24, 39, 255})

	minY, maxY := math.Inf(1), math.Inf(-1)
	for _, cd := range candles {
		if cd.Low < minY {
			minY = cd.Low
		}
		if cd.High > maxY {
			maxY = cd.High
		}
	}
	if maxY == minY {
		maxY = minY + 1
	}
	pad := (maxY - minY) * 0.08
	minY -= pad
	maxY += pad

	plotW := W - padL - padR
	plotH := H - padT - padB
	yToPx := func(v float64) int {
		return padT + plotH - int(math.Round((v-minY)/(maxY-minY)*float64(plotH)))
	}

	gridCol := color.RGBA{229, 231, 235, 255}
	axisCol := color.RGBA{107, 114, 128, 255}
	for i := 0; i <= 5; i++ {
		val := minY + (maxY-minY)*float64(i)/5.0
		py := yToPx(val)
		drawLine(img, padL, py, W-padR, py, gridCol)
		lbl := formatNum(val)
		drawText(img, axisFace, lbl, padL-textWidth(axisFace, lbl)-8, py+4, axisCol)
	}
	drawLine(img, padL, padT, padL, H-padB, axisCol)
	drawLine(img, padL, H-padB, W-padR, H-padB, axisCol)

	upCol := color.RGBA{220, 38, 38, 255}
	downCol := color.RGBA{5, 150, 105, 255}
	stepW := float64(plotW) / float64(len(candles))
	bodyHalf := int(math.Max(2, stepW*0.32))

	lblStep := 1
	if len(candles) > 10 {
		lblStep = int(math.Ceil(float64(len(candles)) / 8.0))
	}

	for i, cd := range candles {
		cx := padL + int((float64(i)+0.5)*stepW)
		col := upCol
		if cd.Close < cd.Open {
			col = downCol
		}
		drawLine(img, cx, yToPx(cd.High), cx, yToPx(cd.Low), col)
		yO, yC := yToPx(cd.Open), yToPx(cd.Close)
		if yO > yC {
			yO, yC = yC, yO
		}
		if yC == yO {
			yC = yO + 1
		}
		fillRect(img, cx-bodyHalf, yO, cx+bodyHalf, yC, col)

		if i%lblStep == 0 && cd.Label != "" {
			tw := textWidth(axisFace, cd.Label)
			drawText(img, axisFace, cd.Label, cx-tw/2, H-padB+22, axisCol)
		}
	}

	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func drawText(img *image.RGBA, face font.Face, s string, x, y int, col color.RGBA) {
	d := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(col),
		Face: face,
		Dot:  fixed.P(x, y),
	}
	d.DrawString(s)
}

func textWidth(face font.Face, s string) int {
	return font.MeasureString(face, s).Ceil()
}

func fillRect(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	if x0 > x1 {
		x0, x1 = x1, x0
	}
	if y0 > y1 {
		y0, y1 = y1, y0
	}
	draw.Draw(img, image.Rect(x0, y0, x1+1, y1+1), &image.Uniform{col}, image.Point{}, draw.Src)
}

func fillCircle(img *image.RGBA, cx, cy, r int, col color.RGBA) {
	for dy := -r; dy <= r; dy++ {
		for dx := -r; dx <= r; dx++ {
			if dx*dx+dy*dy <= r*r {
				img.SetRGBA(cx+dx, cy+dy, col)
			}
		}
	}
}

func drawLine(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	dx := absInt(x1 - x0)
	dy := -absInt(y1 - y0)
	sx := -1
	if x0 < x1 {
		sx = 1
	}
	sy := -1
	if y0 < y1 {
		sy = 1
	}
	err := dx + dy
	for {
		img.SetRGBA(x0, y0, col)
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

func drawThickLine(img *image.RGBA, x0, y0, x1, y1, radius int, col color.RGBA) {
	for ox := -radius / 2; ox <= radius/2; ox++ {
		for oy := -radius / 2; oy <= radius/2; oy++ {
			drawLine(img, x0+ox, y0+oy, x1+ox, y1+oy, col)
		}
	}
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func formatNum(v float64) string {
	av := math.Abs(v)
	switch {
	case av >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case av >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	case av == math.Trunc(av):
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}
