package room

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/iMMIQ/fatfish/internal/transport"
)

var (
	// Reject obvious short banter / trolling phrases that happen to contain advisory words.
	advisoryBanterRe = regexp.MustCompile(`(?i)^(来点推荐|给点推荐|推荐个屁|推荐个锤子|建议重开|建议直接寄|建议紫砂|建议躺平|建议睡觉|建议摸鱼|啥攻略|什么攻略)[？?！!。\s]*$`)

	// Concrete guide / recommendation / troubleshooting keywords.
	advisoryStrongKeywords = []string{
		"攻略", "路线", "行程", "怎么玩", "几天游", "自由行", "旅游规划", "玩几天",
		"怎么选", "选哪个", "买哪个", "推荐个", "推荐一款", "求推荐", "选购", "选购指南",
		"值不值得", "值得买", "性价比", "横评", "对比一下", "哪个好", "帮我选", "帮我挑",
		"避坑", "踩坑", "防坑", "注意事项",
		"配装", "配置单", "装机", "出装", "加点", "阵容", "怎么打", "过关", "打法",
		"教程", "步骤", "怎么搞", "怎么搭", "怎么配", "怎么装", "怎么解决", "怎么修",
		"排查", "报错", "故障", "优化方案", "架构", "方案", "建议", "规划", "清单", "预算",
		"recommend", "guide", "itinerary", "compare", "troubleshoot", "which one",
	}

	// Follow-up constraint refinements inside an already-active advisory thread.
	advisoryFollowupHints = []string{
		"预算", "加钱", "便宜", "换一个", "还有吗", "还有别的", "如果", "那", "带老人", "带娃",
		"几天", "顺路", "酒店", "机票", "显卡", "内存", "接口", "尺寸", "重量", "续航",
		"报错", "日志", "还是不行", "改完", "下一步", "具体", "详细", "展开", "对比", "表格",
	}

	// Comparative choice patterns like "A还是B" or "A和B选谁".
	advisoryChoiceRe = regexp.MustCompile(`(.{2,16})(还是|和|跟|vs|VS)(.{2,16})(哪个|谁|好|选|买|稳|值|强)`)

	// Structured list / section prefixes for cohesive bubble formatting and forward splitting.
	structuredLineRe = regexp.MustCompile(`^(\d+[\.、\)]|[一二三四五六七八九十]+、|[-•·*]\s+|【[^】]{1,20}】|\[[^\]]{1,20}\]|Day\s*\d+|D\d+\b|第\s*\d+\s*[天步点条])`)
)

// DetectAdvisoryIntent identifies explicit guide, recommendation, buying-advice, or troubleshooting turns
// without adding latency or mutating the KV cache prefix.
func DetectAdvisoryIntent(text, trigKind string, inAdvisoryThread bool) bool {
	// Only activate on explicit / engaged user interactions, never on random proactive banter or pokes.
	switch trigKind {
	case "mention", "reply", "dm", "thread":
	default:
		return false
	}

	clean := strings.TrimSpace(text)
	if clean == "" {
		return false
	}
	if advisoryBanterRe.MatchString(clean) {
		return false
	}

	runeLen := len([]rune(clean))
	low := strings.ToLower(clean)

	// Inside an active advisory thread, short constraint refinements stay in advisory mode.
	if inAdvisoryThread && trigKind == "thread" {
		if runeLen >= 4 {
			for _, h := range advisoryFollowupHints {
				if strings.Contains(low, h) {
					return true
				}
			}
			if strings.ContainsAny(clean, "?？") && runeLen >= 6 {
				return true
			}
		}
		return false
	}

	// Require enough substance so 2-4 char casual jokes don't trigger heavy advisory mode.
	if runeLen < 6 {
		return false
	}

	for _, kw := range advisoryStrongKeywords {
		if strings.Contains(low, kw) {
			return true
		}
	}
	if advisoryChoiceRe.MatchString(clean) {
		return true
	}

	// Long explicit technical/planning question with explicit ask markers.
	if runeLen >= 22 && strings.ContainsAny(clean, "?？") {
		for _, cue := range []string{"怎么", "如何", "怎样", "请问", "帮忙", "帮看", "有啥", "有什么", "求"} {
			if strings.Contains(low, cue) {
				return true
			}
		}
	}
	return false
}

// IsStructuredReply reports whether text is a structured multi-point guide/advice reply
// that should not be shattered line-by-line across fragmented short chat bubbles.
func IsStructuredReply(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	if strings.Contains(text, "```") || containsMarkdownTable(text) {
		return true
	}
	lines := nonEmptyLines(text)
	if len(lines) < 2 {
		return false
	}
	structuredHits := 0
	for _, line := range lines {
		if structuredLineRe.MatchString(line) {
			structuredHits++
		}
	}
	if structuredHits >= 2 {
		return true
	}
	return len([]rune(text)) > 120 && len(lines) >= 4
}

// BuildForwardGuide packages a long group guide/advice reply into a crisp in-channel verdict lead-in
// plus cleanly sectioned nodes for a OneBot merged-forward card (preventing group chat spam).
func BuildForwardGuide(text, botName, botID string) (leadIn string, nodes []transport.ForwardNode) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", nil
	}
	if strings.TrimSpace(botName) == "" {
		botName = "fatfish"
	}

	// Split into logical sections by blank lines first; if none, group by section headers or line chunks.
	sections := splitGuideSections(text)
	if len(sections) == 0 {
		return "", nil
	}

	// Extract a concise first-line verdict for the main group chat bubble.
	firstLines := nonEmptyLines(sections[0])
	verdict := ""
	if len(firstLines) > 0 {
		verdict = firstLines[0]
	}
	verdictRunes := []rune(verdict)
	if len(verdictRunes) > 85 {
		verdict = strings.TrimRightFunc(string(verdictRunes[:82]), unicode.IsSpace) + "..."
	}
	if verdict != "" {
		leadIn = verdict + "\n（详细攻略整理在上面转发卡片里了，点开看）"
	} else {
		leadIn = "详细攻略整理在上面转发卡片里了，点开看"
	}

	const maxForwardNodes = 10
	if len(sections) > maxForwardNodes {
		mergedTail := strings.Join(sections[maxForwardNodes-1:], "\n\n")
		sections = append(sections[:maxForwardNodes-1], mergedTail)
	}

	nodes = make([]transport.ForwardNode, 0, len(sections))
	for _, sec := range sections {
		sec = strings.TrimSpace(sec)
		if sec == "" {
			continue
		}
		nodes = append(nodes, transport.ForwardNode{
			SenderID:   botID,
			SenderName: botName,
			Text:       sec,
		})
	}
	return leadIn, nodes
}

func splitGuideSections(text string) []string {
	// 1. Prefer explicit paragraph breaks (\n\n).
	rawParas := regexp.MustCompile(`\n\s*\n+`).Split(text, -1)
	var paras []string
	for _, p := range rawParas {
		if t := strings.TrimSpace(p); t != "" {
			paras = append(paras, t)
		}
	}
	if len(paras) >= 2 {
		return paras
	}

	// 2. Group by structured headings (【...】 / Day N / 1. / 避坑：) when single-newline formatted.
	lines := nonEmptyLines(text)
	if len(lines) <= 3 {
		return []string{text}
	}
	var groups []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			groups = append(groups, strings.Join(cur, "\n"))
			cur = nil
		}
	}
	for i, ln := range lines {
		isHeading := strings.HasPrefix(ln, "【") ||
			strings.HasPrefix(ln, "避坑") ||
			strings.HasPrefix(ln, "总结") ||
			strings.HasPrefix(ln, "注意") ||
			structuredLineRe.MatchString(ln)
		if i > 0 && isHeading && len(cur) > 0 && len([]rune(strings.Join(cur, "\n"))) >= 45 {
			flush()
		}
		cur = append(cur, ln)
	}
	flush()
	if len(groups) == 0 {
		return []string{text}
	}
	return groups
}

func nonEmptyLines(s string) []string {
	raw := strings.Split(s, "\n")
	out := make([]string, 0, len(raw))
	for _, l := range raw {
		if t := strings.TrimSpace(l); t != "" {
			out = append(out, t)
		}
	}
	return out
}
