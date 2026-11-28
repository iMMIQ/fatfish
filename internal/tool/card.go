package tool

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"strings"

	"golang.org/x/image/font"
)

// CardSection defines one headed block inside a rendered guide/itinerary card.
type CardSection struct {
	Heading string
	Items   []string
}

// RenderTable renders a clean, CJK-wrapped side-by-side comparison table PNG (ideal for
// product/plan/spec comparisons in chat without wall-of-text spam).
// highlightCol >= 0 tints that column (0-based) as the recommended option.
func (c *ChartRenderer) RenderTable(title, subtitle string, headers []string, rows [][]string, highlightCol int) ([]byte, error) {
	if len(headers) == 0 {
		return nil, errors.New("chart.table: headers must be non-empty")
	}
	if len(headers) > 7 {
		headers = headers[:7]
	}
	if len(rows) > 20 {
		rows = rows[:20]
	}

	titleFace, err := c.newFace(22)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	subFace, err := c.newFace(13)
	if err != nil {
		return nil, err
	}
	defer subFace.Close()
	headFace, err := c.newFace(14)
	if err != nil {
		return nil, err
	}
	defer headFace.Close()
	bodyFace, err := c.newFace(13)
	if err != nil {
		return nil, err
	}
	defer bodyFace.Close()

	const (
		outerPad = 28
		cellPadX = 14
		cellPadY = 10
		lineH    = 20
		headH    = 40
	)
	nCols := len(headers)
	tableW := 860
	if nCols >= 5 {
		tableW = 960
	}
	W := tableW + outerPad*2

	// Allocate column widths: first label column slightly narrower when >= 3 columns.
	colW := make([]int, nCols)
	if nCols == 1 {
		colW[0] = tableW
	} else if nCols == 2 {
		colW[0] = tableW / 3
		colW[1] = tableW - colW[0]
	} else {
		firstW := int(float64(tableW) * 0.22)
		if firstW < 130 {
			firstW = 130
		}
		if firstW > 200 {
			firstW = 200
		}
		colW[0] = firstW
		rem := tableW - firstW
		each := rem / (nCols - 1)
		for i := 1; i < nCols; i++ {
			colW[i] = each
		}
		colW[nCols-1] += rem - each*(nCols-1)
	}

	// Pre-wrap header & body cells to calculate exact dynamic image height.
	wrappedHeaders := make([][]string, nCols)
	maxHeadLines := 1
	for ci, h := range headers {
		wrappedHeaders[ci] = wrapTextToWidth(headFace, strings.TrimSpace(h), colW[ci]-cellPadX*2, 2)
		if len(wrappedHeaders[ci]) > maxHeadLines {
			maxHeadLines = len(wrappedHeaders[ci])
		}
	}
	headerRowH := maxIntChart(headH, maxHeadLines*lineH+cellPadY*2)

	wrappedRows := make([][][]string, len(rows))
	rowHeights := make([]int, len(rows))
	totalRowsH := 0
	for ri, r := range rows {
		wrappedRows[ri] = make([][]string, nCols)
		maxLines := 1
		for ci := 0; ci < nCols; ci++ {
			cell := ""
			if ci < len(r) {
				cell = strings.TrimSpace(r[ci])
			}
			lines := wrapTextToWidth(bodyFace, cell, colW[ci]-cellPadX*2, 5)
			if len(lines) == 0 {
				lines = []string{""}
			}
			wrappedRows[ri][ci] = lines
			if len(lines) > maxLines {
				maxLines = len(lines)
			}
		}
		rh := maxLines*lineH + cellPadY*2
		if rh < 36 {
			rh = 36
		}
		rowHeights[ri] = rh
		totalRowsH += rh
	}

	bannerH := 64
	if strings.TrimSpace(subtitle) != "" {
		bannerH = 84
	}
	H := outerPad + bannerH + 14 + headerRowH + totalRowsH + outerPad
	if H > 2200 {
		H = 2200
	}

	bgCol := color.RGBA{248, 250, 252, 255}
	cardCol := color.RGBA{255, 255, 255, 255}
	accentCol := color.RGBA{37, 99, 235, 255}
	headBg := color.RGBA{30, 41, 59, 255}
	headHiBg := color.RGBA{29, 78, 216, 255}
	zebraBg := color.RGBA{248, 250, 252, 255}
	hiBg := color.RGBA{239, 246, 255, 255}
	hiZebraBg := color.RGBA{219, 234, 254, 255}
	borderCol := color.RGBA{226, 232, 240, 255}
	textDark := color.RGBA{15, 23, 42, 255}
	textMuted := color.RGBA{100, 116, 139, 255}
	textWhite := color.RGBA{255, 255, 255, 255}

	img := image.NewRGBA(image.Rect(0, 0, W, H))
	draw.Draw(img, img.Bounds(), &image.Uniform{bgCol}, image.Point{}, draw.Src)

	// Outer card container.
	fillRect(img, outerPad-8, outerPad-8, W-outerPad+8, H-outerPad+8, cardCol)
	drawRectBorder(img, outerPad-8, outerPad-8, W-outerPad+8, H-outerPad+8, borderCol)
	// Left accent strip on header banner.
	fillRect(img, outerPad, outerPad+6, outerPad+5, outerPad+bannerH-8, accentCol)

	titleLines := wrapTextToWidth(titleFace, strings.TrimSpace(title), tableW-24, 1)
	if len(titleLines) > 0 {
		drawText(img, titleFace, titleLines[0], outerPad+18, outerPad+32, textDark)
	}
	if sub := strings.TrimSpace(subtitle); sub != "" {
		subLines := wrapTextToWidth(subFace, sub, tableW-24, 1)
		if len(subLines) > 0 {
			drawText(img, subFace, subLines[0], outerPad+18, outerPad+58, textMuted)
		}
	}

	// Draw table header row.
	curY := outerPad + bannerH + 10
	curX := outerPad
	for ci := 0; ci < nCols; ci++ {
		bg := headBg
		if ci == highlightCol {
			bg = headHiBg
		}
		fillRect(img, curX, curY, curX+colW[ci]-1, curY+headerRowH-1, bg)
		for li, ln := range wrappedHeaders[ci] {
			drawText(img, headFace, ln, curX+cellPadX, curY+cellPadY+15+li*lineH, textWhite)
		}
		curX += colW[ci]
	}
	curY += headerRowH

	// Draw body rows.
	for ri := range rows {
		rh := rowHeights[ri]
		if curY+rh > H-outerPad+4 {
			break
		}
		curX = outerPad
		for ci := 0; ci < nCols; ci++ {
			bg := cardCol
			if ci == highlightCol {
				if ri%2 == 1 {
					bg = hiZebraBg
				} else {
					bg = hiBg
				}
			} else if ri%2 == 1 {
				bg = zebraBg
			}
			fillRect(img, curX, curY, curX+colW[ci]-1, curY+rh-1, bg)
			txtCol := textDark
			if ci == 0 {
				txtCol = color.RGBA{30, 41, 59, 255}
			}
			for li, ln := range wrappedRows[ri][ci] {
				drawText(img, bodyFace, ln, curX+cellPadX, curY+cellPadY+14+li*lineH, txtCol)
			}
			// Vertical column divider.
			if ci > 0 {
				drawLine(img, curX, curY, curX, curY+rh-1, borderCol)
			}
			curX += colW[ci]
		}
		// Horizontal row divider.
		drawLine(img, outerPad, curY+rh-1, outerPad+tableW-1, curY+rh-1, borderCol)
		curY += rh
	}
	drawRectBorder(img, outerPad, outerPad+bannerH+10, outerPad+tableW-1, curY-1, borderCol)

	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// RenderCard renders a structured multi-section guide / itinerary / checklist PNG card
// with CJK wrapping, section badges, and an optional tips/pitfalls footer.
func (c *ChartRenderer) RenderCard(title, subtitle string, sections []CardSection, footer string) ([]byte, error) {
	if len(sections) == 0 {
		return nil, errors.New("chart.card: sections must be non-empty")
	}
	if len(sections) > 10 {
		sections = sections[:10]
	}

	titleFace, err := c.newFace(22)
	if err != nil {
		return nil, err
	}
	defer titleFace.Close()
	subFace, err := c.newFace(13)
	if err != nil {
		return nil, err
	}
	defer subFace.Close()
	secFace, err := c.newFace(15)
	if err != nil {
		return nil, err
	}
	defer secFace.Close()
	bodyFace, err := c.newFace(13)
	if err != nil {
		return nil, err
	}
	defer bodyFace.Close()

	const (
		W        = 880
		outerPad = 28
		innerPad = 20
		lineH    = 21
	)
	contentW := W - outerPad*2 - innerPad*2

	type layoutSection struct {
		heading string
		items   [][]string
		height  int
	}
	laid := make([]layoutSection, 0, len(sections))
	totalSecH := 0
	for _, s := range sections {
		ls := layoutSection{heading: strings.TrimSpace(s.Heading)}
		h := 16 // top padding
		if ls.heading != "" {
			h += 28
		}
		for _, it := range s.Items {
			it = strings.TrimSpace(it)
			if it == "" {
				continue
			}
			wrapped := wrapTextToWidth(bodyFace, "• "+it, contentW-12, 4)
			ls.items = append(ls.items, wrapped)
			h += len(wrapped)*lineH + 6
		}
		h += 12 // bottom padding
		ls.height = h
		laid = append(laid, ls)
		totalSecH += h + 12
	}

	footerLines := wrapTextToWidth(subFace, strings.TrimSpace(footer), contentW-16, 3)
	footerH := 0
	if len(footerLines) > 0 {
		footerH = len(footerLines)*19 + 24
	}

	headerH := 76
	if strings.TrimSpace(subtitle) != "" {
		headerH = 98
	}
	H := outerPad + headerH + 14 + totalSecH + footerH + outerPad
	if H > 2400 {
		H = 2400
	}

	bgCol := color.RGBA{241, 245, 249, 255}
	headerBg := color.RGBA{15, 23, 42, 255}
	cardBg := color.RGBA{255, 255, 255, 255}
	borderCol := color.RGBA{226, 232, 240, 255}
	badgeCol := color.RGBA{37, 99, 235, 255}
	footerBg := color.RGBA{254, 249, 195, 255}
	footerBorder := color.RGBA{250, 204, 21, 255}
	textWhite := color.RGBA{255, 255, 255, 255}
	textSubWhite := color.RGBA{203, 213, 225, 255}
	textDark := color.RGBA{15, 23, 42, 255}
	textBody := color.RGBA{51, 65, 85, 255}
	textAmber := color.RGBA{133, 77, 14, 255}

	img := image.NewRGBA(image.Rect(0, 0, W, H))
	draw.Draw(img, img.Bounds(), &image.Uniform{bgCol}, image.Point{}, draw.Src)

	// Header banner.
	fillRect(img, outerPad, outerPad, W-outerPad, outerPad+headerH, headerBg)
	fillRect(img, outerPad, outerPad+headerH-4, W-outerPad, outerPad+headerH, badgeCol)
	tLines := wrapTextToWidth(titleFace, strings.TrimSpace(title), W-outerPad*2-innerPad*2, 1)
	if len(tLines) > 0 {
		drawText(img, titleFace, tLines[0], outerPad+innerPad, outerPad+40, textWhite)
	}
	if sub := strings.TrimSpace(subtitle); sub != "" {
		sLines := wrapTextToWidth(subFace, sub, W-outerPad*2-innerPad*2, 1)
		if len(sLines) > 0 {
			drawText(img, subFace, sLines[0], outerPad+innerPad, outerPad+68, textSubWhite)
		}
	}

	curY := outerPad + headerH + 14
	for si, ls := range laid {
		if curY+ls.height > H-outerPad {
			break
		}
		fillRect(img, outerPad, curY, W-outerPad, curY+ls.height, cardBg)
		drawRectBorder(img, outerPad, curY, W-outerPad, curY+ls.height, borderCol)
		// Section color accent bar on left edge.
		secCol := chartPalette[si%len(chartPalette)]
		fillRect(img, outerPad, curY, outerPad+5, curY+ls.height, secCol)

		y := curY + 26
		if ls.heading != "" {
			tag := fmt.Sprintf("%d. %s", si+1, ls.heading)
			hLines := wrapTextToWidth(secFace, tag, contentW, 1)
			if len(hLines) > 0 {
				drawText(img, secFace, hLines[0], outerPad+innerPad, y, textDark)
			}
			y += 26
		}
		for _, wrapped := range ls.items {
			for li, ln := range wrapped {
				x := outerPad + innerPad + 4
				if li > 0 {
					x += 12
				}
				drawText(img, bodyFace, ln, x, y, textBody)
				y += lineH
			}
			y += 6
		}
		curY += ls.height + 12
	}

	if len(footerLines) > 0 && curY+footerH <= H-outerPad+8 {
		fillRect(img, outerPad, curY, W-outerPad, curY+footerH, footerBg)
		drawRectBorder(img, outerPad, curY, W-outerPad, curY+footerH, footerBorder)
		fy := curY + 24
		for _, fl := range footerLines {
			drawText(img, subFace, fl, outerPad+innerPad, fy, textAmber)
			fy += 19
		}
	}

	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func drawRectBorder(img *image.RGBA, x0, y0, x1, y1 int, col color.RGBA) {
	drawLine(img, x0, y0, x1, y0, col)
	drawLine(img, x0, y1, x1, y1, col)
	drawLine(img, x0, y0, x0, y1, col)
	drawLine(img, x1, y0, x1, y1, col)
}

// wrapTextToWidth wraps mixed CJK/ASCII text by rendered pixel width up to maxLines.
func wrapTextToWidth(face font.Face, text string, maxWidth int, maxLines int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if maxWidth < 40 {
		maxWidth = 40
	}
	if maxLines <= 0 {
		maxLines = 1
	}

	var lines []string
	paragraphs := strings.Split(text, "\n")
	for _, para := range paragraphs {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		runes := []rune(para)
		start := 0
		for start < len(runes) {
			if len(lines) >= maxLines {
				break
			}
			end := start
			for end < len(runes) {
				cand := string(runes[start : end+1])
				if textWidth(face, cand) > maxWidth && end > start {
					break
				}
				end++
			}
			if end == start {
				end = start + 1
			}
			line := strings.TrimSpace(string(runes[start:end]))
			isLastAllowed := len(lines) == maxLines-1
			hasMore := end < len(runes)
			if isLastAllowed && hasMore {
				lr := []rune(line)
				for len(lr) > 1 && textWidth(face, string(lr)+"...") > maxWidth {
					lr = lr[:len(lr)-1]
				}
				line = string(lr) + "..."
				lines = append(lines, line)
				return lines
			}
			lines = append(lines, line)
			start = end
		}
	}
	return lines
}
