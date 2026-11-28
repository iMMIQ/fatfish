package room

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/iMMIQ/fatfish/internal/transport"
)

// AdvisoryDomain classifies explicit advice/guide requests so domain-specific SOP hints
// can be injected on the active turn without ever mutating the KV cache prefix ([msg 0..1]).
type AdvisoryDomain string

const (
	AdvisoryDomainNone    AdvisoryDomain = ""
	AdvisoryDomainGeneral AdvisoryDomain = "general"
	AdvisoryDomainTravel  AdvisoryDomain = "travel"
)

var (
	// Reject obvious short banter / trolling phrases that happen to contain advisory words.
	advisoryBanterRe = regexp.MustCompile(`(?i)^(来点推荐|给点推荐|推荐个屁|推荐个锤子|建议重开|建议直接寄|建议紫砂|建议躺平|建议睡觉|建议摸鱼|啥攻略|什么攻略|群友怎么天天旅游|怎么天天旅游)[？?！!。\s]*$`)

	// Reject casual self-referential or joke questions mentioning travel/food words.
	travelCasualBanterRe = regexp.MustCompile(`(好吃的群友|你在哪旅游|你最近在哪旅游|你也去旅游|天天旅游|你去过.*吗)`)

	// Unambiguous travel-guide keywords (trigger AdvisoryDomainTravel directly when >= 6 runes).
	travelStrongKeywords = []string{
		"旅游攻略", "旅行攻略", "打卡攻略", "旅游规划", "自由行", "自驾游", "懒人游", "跟团游",
		"怎么玩", "玩几天", "几天游", "一日游", "两日游", "二日游", "三日游", "四日游", "五日游",
		"去哪玩", "去哪里玩", "去哪儿玩", "去哪里合适", "去哪儿合适", "适合去哪", "适合去哪儿",
		"住哪", "住哪里", "住哪片区", "推荐去的地方", "有什么好玩", "有啥好玩", "有什么好吃", "有啥好吃",
		"刷段", "捡漏票", "甩尾", "连程航班", "联程航班", "里程票", "常旅客",
		"itinerary", "travel guide",
	}

	// Travel domain nouns/topics that activate AdvisoryDomainTravel when paired with an ask/eval cue.
	travelTopicKeywords = []string{
		"旅游", "旅行", "出游", "行程", "路线", "攻略", "景点", "景区", "打卡", "特种兵",
		"酒店", "民宿", "房晚", "青旅", "度假村", "温泉", "洗浴", "汤泉", "水疗",
		"美食", "好吃的", "餐厅", "馆子", "探店", "夜市", "小吃", "特产", "到店",
		"机票", "航班", "航司", "转机", "连程", "联程", "延误险", "高铁站", "火车站", "机场",
		"签证", "免签", "落地签", "过境签", "退税", "周游券", "通票", "西瓜卡", "租车", "自驾",
		"极光", "红叶", "滑雪", "海岛", "邮轮", "赶海", "露营", "古镇", "博物馆",
	}

	// Ask/evaluation cues paired with travelTopicKeywords to separate real requests from casual chatter.
	travelAskCues = []string{
		"推荐", "点评", "锐评", "评价", "测评", "避坑", "踩坑", "防坑", "注意事项",
		"哪家", "哪个", "哪里", "哪儿", "有什么", "有啥", "怎么", "怎样", "如何", "咋",
		"合适", "划算", "值得", "强", "好不好", "怎么样", "靠谱", "还是", "没想好",
		"多少钱", "开销", "预算", "费用", "花费", "价格", "成本",
		"规划", "安排", "计划", "排", "政策", "手续费", "规定", "规则",
		"可以吗", "行不行", "能不能", "能吗", "帮我", "帮看", "帮查", "请问", "求",
		"?", "？",
	}

	// Concrete general guide / recommendation / troubleshooting keywords.
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
		"预算", "加钱", "便宜", "换一个", "换一家", "还有吗", "还有别的", "如果", "那", "不要", "不去", "跳过",
		"带老人", "带爸妈", "带娃", "情侣", "一个人", "单人", "特种兵", "躺平", "不换酒店", "加一个", "再加",
		"几天", "顺路", "酒店", "机票", "高铁", "直飞", "红眼", "延误险", "转机", "最推荐", "片区",
		"显卡", "内存", "接口", "尺寸", "重量", "续航",
		"报错", "日志", "还是不行", "改完", "下一步", "具体", "详细", "展开", "对比", "表格",
	}

	// Comparative choice patterns like "A还是B" or "A和B选谁".
	advisoryChoiceRe = regexp.MustCompile(`(.{2,16})(还是|和|跟|vs|VS)(.{1,16})(哪个|谁|好|选|买|稳|值|强|两天|一天|三天)`)

	// Explicit non-travel technical/hardware/gaming keywords used to detect topic switch out of a travel thread.
	nonTravelDomainKeywords = []string{
		"显卡", "显示器", "内存", "主板", "固态", "硬盘", "cpu", "gpu", "docker", "nginx", "linux",
		"报错", "代码", "编译", "接口", "配装", "出装", "阵容", "副本", "耳机", "手机", "电脑", "路由器",
	}

	// Structured list / section prefixes for cohesive bubble formatting and forward splitting.
	structuredLineRe = regexp.MustCompile(`^(\d+[\.、\)]|[一二三四五六七八九十]+、|[-•·*]\s+|【[^】]{1,20}】|\[[^\]]{1,20}\]|Day\s*\d+|D\d+\b|第\s*\d+\s*[天步点条])`)

	mdHeadingLineRe = regexp.MustCompile(`^\s*#{1,6}\s+(.+?)\s*#*\s*$`)
	mdBoldRe        = regexp.MustCompile(`\*\*([^*\n]+?)\*\*`)
	mdUnderscoreRe  = regexp.MustCompile(`__([^_\n]+?)__`)
	mdQuoteLineRe   = regexp.MustCompile(`^\s*>\s?`)
)

// DetectAdvisoryIntent identifies explicit guide, recommendation, buying-advice, or troubleshooting turns
// without adding latency or mutating the KV cache prefix.
func DetectAdvisoryIntent(text, trigKind string, inAdvisoryThread bool) bool {
	return DetectAdvisoryDomainWithPrev(text, trigKind, inAdvisoryThread, AdvisoryDomainGeneral) != AdvisoryDomainNone
}

// DetectAdvisoryDomain identifies whether a turn is a general advisory request or a travel-specific guide request.
func DetectAdvisoryDomain(text, trigKind string, inAdvisoryThread bool) AdvisoryDomain {
	return DetectAdvisoryDomainWithPrev(text, trigKind, inAdvisoryThread, AdvisoryDomainGeneral)
}

// DetectAdvisoryDomainWithPrev preserves the active advisory sub-domain (e.g. travel) across short follow-up
// constraint refinements (including explicit @mention or reply follow-ups inside an active advisory thread).
func DetectAdvisoryDomainWithPrev(text, trigKind string, inAdvisoryThread bool, prevDomain AdvisoryDomain) AdvisoryDomain {
	// Only activate on explicit / engaged user interactions, never on random proactive banter or pokes.
	switch trigKind {
	case "mention", "reply", "dm", "thread":
	default:
		return AdvisoryDomainNone
	}

	clean := strings.TrimSpace(text)
	if clean == "" {
		return AdvisoryDomainNone
	}
	if advisoryBanterRe.MatchString(clean) {
		return AdvisoryDomainNone
	}

	runeLen := len([]rune(clean))
	low := strings.ToLower(clean)

	// 1. Inside an active advisory thread, short constraint refinements stay in advisory mode
	// even if the user @mentions or replies to the bot again (e.g. "@bot 不要江浙沪", "预算不限，最推荐哪个").
	if inAdvisoryThread && (trigKind == "thread" || trigKind == "mention" || trigKind == "reply") {
		if dom := detectFreshAdvisoryDomain(clean, low, runeLen); dom != AdvisoryDomainNone {
			if dom == AdvisoryDomainGeneral && prevDomain == AdvisoryDomainTravel && !hasAnySubstr(low, nonTravelDomainKeywords) {
				return AdvisoryDomainTravel
			}
			return dom
		}
		if runeLen >= 4 {
			matchedFollowup := false
			for _, h := range advisoryFollowupHints {
				if strings.Contains(low, h) {
					matchedFollowup = true
					break
				}
			}
			if !matchedFollowup && strings.ContainsAny(clean, "?？") && runeLen >= 6 {
				matchedFollowup = true
			}
			if matchedFollowup {
				if prevDomain == AdvisoryDomainTravel || hasAnySubstr(low, travelTopicKeywords) {
					return AdvisoryDomainTravel
				}
				return AdvisoryDomainGeneral
			}
		}
		if trigKind == "thread" {
			return AdvisoryDomainNone
		}
	}

	return detectFreshAdvisoryDomain(clean, low, runeLen)
}

func detectFreshAdvisoryDomain(clean, low string, runeLen int) AdvisoryDomain {
	// Require enough substance so 2-4 char casual jokes don't trigger heavy advisory mode.
	if runeLen < 6 {
		return AdvisoryDomainNone
	}

	isTravelBanter := travelCasualBanterRe.MatchString(clean)
	if !isTravelBanter {
		if hasAnySubstr(low, travelStrongKeywords) {
			return AdvisoryDomainTravel
		}
		if hasAnySubstr(low, travelTopicKeywords) && (hasAnySubstr(low, travelAskCues) || advisoryChoiceRe.MatchString(clean)) {
			return AdvisoryDomainTravel
		}
	}

	if hasAnySubstr(low, advisoryStrongKeywords) || advisoryChoiceRe.MatchString(clean) {
		if !isTravelBanter && hasAnySubstr(low, travelTopicKeywords) {
			return AdvisoryDomainTravel
		}
		return AdvisoryDomainGeneral
	}

	// Long explicit technical/planning question with explicit ask markers.
	if runeLen >= 22 && strings.ContainsAny(clean, "?？") {
		for _, cue := range []string{"怎么", "如何", "怎样", "请问", "帮忙", "帮看", "有啥", "有什么", "求"} {
			if strings.Contains(low, cue) {
				if !isTravelBanter && hasAnySubstr(low, travelTopicKeywords) {
					return AdvisoryDomainTravel
				}
				return AdvisoryDomainGeneral
			}
		}
	}
	return AdvisoryDomainNone
}

func hasAnySubstr(s string, list []string) bool {
	for _, kw := range list {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// SanitizeReplyMarkdown removes raw Markdown formatting (**bold**, ### headings, > quotes)
// outside fenced code blocks so QQ chat bubbles and forward cards stay clean plain text.
func SanitizeReplyMarkdown(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	lines := strings.Split(text, "\n")
	inCode := false
	for i, ln := range lines {
		trimmed := strings.TrimSpace(ln)
		if strings.HasPrefix(trimmed, "```") {
			inCode = !inCode
			continue
		}
		if inCode {
			continue
		}
		if m := mdHeadingLineRe.FindStringSubmatch(ln); len(m) == 2 {
			heading := strings.TrimSpace(mdBoldRe.ReplaceAllString(m[1], "$1"))
			if heading != "" {
				if !strings.HasPrefix(heading, "【") {
					heading = "【" + heading + "】"
				}
				lines[i] = heading
				continue
			}
		}
		clean := mdQuoteLineRe.ReplaceAllString(ln, "")
		clean = mdBoldRe.ReplaceAllString(clean, "$1")
		clean = mdUnderscoreRe.ReplaceAllString(clean, "$1")
		lines[i] = clean
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// ExtractGuideTitle derives a concise forward-card title from the guide text (e.g. "大肥鱼 · 关西4日攻略"),
// falling back to "<botName> 攻略卡片".
func ExtractGuideTitle(text, botName string) string {
	botName = strings.TrimSpace(botName)
	if botName == "" {
		botName = "fatfish"
	}
	clean := SanitizeReplyMarkdown(text)
	lines := nonEmptyLines(clean)
	for _, ln := range lines {
		// Prefer an explicit 【...】 heading if it looks like a guide title.
		if strings.HasPrefix(ln, "【") {
			if end := strings.Index(ln, "】"); end > len("【") {
				cand := strings.TrimSpace(ln[len("【"):end])
				r := []rune(cand)
				if len(r) >= 4 && len(r) <= 18 && !strings.HasPrefix(cand, "避坑") && !strings.HasPrefix(cand, "注意") {
					return botName + " · " + cand
				}
			}
		}
	}
	if len(lines) > 0 {
		first := strings.TrimSpace(lines[0])
		for _, sep := range []string{"：", ":", "。", "！", "!", "，", ","} {
			if idx := strings.Index(first, sep); idx > 0 {
				prefix := strings.TrimSpace(first[:idx])
				pr := []rune(prefix)
				if len(pr) >= 5 && len(pr) <= 18 {
					return botName + " · " + prefix
				}
			}
		}
	}
	return botName + " 攻略卡片"
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
	text = SanitizeReplyMarkdown(text)
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
