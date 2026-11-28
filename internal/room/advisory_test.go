package room

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/agent"
	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/store"
)

func TestDetectAdvisoryIntentRegression(t *testing.T) {
	cases := []struct {
		name             string
		text             string
		trigKind         string
		inAdvisoryThread bool
		want             bool
	}{
		// Positive guide / advice / buying / troubleshooting cases
		{name: "tech buying advice", text: "预算3000想买个27寸4K显示器，写代码偶尔打FPS，帮我选选", trigKind: "mention", want: true},
		{name: "travel itinerary", text: "十一想去关西玩4天，大阪京都怎么排不特种兵？", trigKind: "mention", want: true},
		{name: "docker troubleshoot", text: "Docker 配 Nginx 反代老是 502，宿主机端口通的，咋排查？", trigKind: "reply", want: true},
		{name: "game build", text: "这个版本黑神话大圣残躯怎么打，求个配装思路", trigKind: "mention", want: true},
		{name: "ab comparison", text: "索尼XM5和Bose QC Ultra哪个降噪更稳", trigKind: "dm", want: true},
		{name: "dm advice", text: "帮我做个减脂期的饮食规划，工作日点外卖", trigKind: "dm", want: true},
		{name: "followup budget refine", text: "预算加到5000呢", trigKind: "thread", inAdvisoryThread: true, want: true},
		{name: "followup elder constraint", text: "如果带老人去呢？", trigKind: "thread", inAdvisoryThread: true, want: true},
		{name: "long explicit question", text: "我在Linux服务器上部署了一个Go服务但是重启后环境变量全丢了，请问有什么靠谱的持久化方法吗？", trigKind: "mention", want: true},

		// Negative casual banter / trolling / non-explicit trigger guards
		{name: "vague banter recommend", text: "来点推荐", trigKind: "mention", want: false},
		{name: "trolling advice", text: "建议重开", trigKind: "mention", want: false},
		{name: "short casual greeting", text: "在干嘛呢", trigKind: "mention", want: false},
		{name: "short banter", text: "今天吃什么好啊", trigKind: "proactive", want: false},
		{name: "proactive never advisory", text: "求一个日本关西五天旅游详细攻略", trigKind: "proactive", want: false},
		{name: "poke never advisory", text: "求推荐显示器", trigKind: "poke", want: false},
		{name: "thread casual close", text: "哈哈行", trigKind: "thread", inAdvisoryThread: true, want: false},
		{name: "non-advisory thread short", text: "预算加到5000呢", trigKind: "thread", inAdvisoryThread: false, want: true}, // explicit budget keyword
		{name: "non-advisory thread ok", text: "真的假的", trigKind: "thread", inAdvisoryThread: false, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DetectAdvisoryIntent(tc.text, tc.trigKind, tc.inAdvisoryThread)
			if got != tc.want {
				t.Fatalf("DetectAdvisoryIntent(%q, %q, %v) = %v, want %v", tc.text, tc.trigKind, tc.inAdvisoryThread, got, tc.want)
			}
		})
	}
}

func TestFormatBubblesPreservesStructuredGuides(t *testing.T) {
	pol := config.PolicyConfig{
		SplitNewlineBubbles: true,
		MaxReplyBubbles:     3,
		StripTrailingPeriod: true,
	}

	// Casual short multi-line banter still splits into typing bubbles.
	casual := "草\n你认真的吗\n赶紧买吧"
	gotCasual := FormatBubbles(casual, pol)
	if len(gotCasual) != 3 {
		t.Fatalf("expected 3 casual bubbles, got %d: %#v", len(gotCasual), gotCasual)
	}

	// Structured numbered guide stays cohesive instead of shattering lines 1..2 and clumping 3..6.
	guide := strings.Join([]string{
		"3000块首选27寸4K 160Hz Fast IPS，写代码不发虚打FPS也够用。",
		"1. 稳妥性价比：选一线大厂27寸4K160Hz基础款，售后省心。",
		"2. 想看HDR电影：加预算看高分区MiniLED，但先确认是否介意白底光晕。",
		"3. 避坑：千万别买4K 60Hz办公屏硬打FPS，拖影会很难受。",
		"你现在什么显卡？",
	}, "\n")
	gotGuide := FormatBubbles(guide, pol)
	if len(gotGuide) != 1 {
		t.Fatalf("expected structured guide to stay 1 cohesive block, got %d: %#v", len(gotGuide), gotGuide)
	}

	// Two clean paragraphs separated by blank line stay 2 coherent blocks.
	twoParaGuide := "核心结论：大阪住到底抽2天去京都最省折腾。\n1. Day1 大阪心斋桥+梅田\n2. Day2 京都清水寺东线\n\n避坑：清水寺早8点前冲，西瓜卡手机直接开，别买实体卡。"
	gotTwo := FormatBubbles(twoParaGuide, pol)
	if len(gotTwo) != 2 {
		t.Fatalf("expected 2 coherent paragraph blocks, got %d: %#v", len(gotTwo), gotTwo)
	}
}

func TestBuildForwardGuide(t *testing.T) {
	longGuide := strings.Join([]string{
		"4天关西游直接大阪住到底+抽2天往返京都最省换酒店的折腾。",
		"",
		"【Day1 大阪】下午落地直接去心斋桥/道顿堀吃晚饭，晚上去梅田蓝天大厦看夜景。",
		"",
		"【Day2 京都东线】早7点半前冲清水寺避开旅行团，顺路二年坂三年坂、八坂神社、鸭川晚饭。",
		"",
		"【Day3 京都西线】岚山小火车加渡月桥，下午金阁寺和伏见稻荷二选一别硬塞。",
		"",
		"【避坑清单】岚山小火车旺季提前官网抢票，西瓜卡直接用手机钱包开，不用去窗口排队。",
	}, "\n")

	lead, nodes := BuildForwardGuide(longGuide, "fatfish", "10001")
	if !strings.Contains(lead, "大阪住到底") || !strings.Contains(lead, "转发卡片") {
		t.Fatalf("unexpected lead-in: %q", lead)
	}
	if len(nodes) != 5 {
		t.Fatalf("expected 5 forward nodes, got %d", len(nodes))
	}
	if nodes[0].SenderID != "10001" || nodes[0].SenderName != "fatfish" {
		t.Fatalf("unexpected node sender metadata: %+v", nodes[0])
	}
}

func TestTravelAdvisoryRegressionSuiteFromProdHistory(t *testing.T) {
	// Real-world anonymized travel/dining/hotel/aviation questions distilled from production chat patterns.
	travelCases := []struct {
		name             string
		text             string
		trigKind         string
		inAdvisoryThread bool
		prevDomain       AdvisoryDomain
		want             AdvisoryDomain
	}{
		// Positive travel queries (previously missed or generic)
		{name: "hot spring recommend", text: "广东温泉有推荐的嘛？", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "local food ask", text: "杭州下沙有什么好吃的店", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "bathhouse recommend", text: "太原有什么洗浴中心推荐", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "city food and fun", text: "辽阳有什么好玩的好吃的", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "one day trip", text: "辽阳一日游怎么玩比较好", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "destination recommend", text: "香港有推荐去的地方吗", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "hotel budget pick", text: "深圳500左右酒店哪家强", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "hotel review", text: "点评一下深圳东华假日酒店", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "hotel sharp review", text: "锐评一下深航国际酒店怎么样，我特别喜欢这家", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "iceland cost estimate", text: "北京出发，去冰岛看极光需要看多少钱，考虑往返路费和酒店", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "3-day departure ask", text: "11月杭州出发3天旅游去哪里合适", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "winter destination ask", text: "12月份如果被裁了适合去哪儿玩", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "border lazy trip cost", text: "北京出发去海参崴延边懒人游来回开销多少", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "mileage run flight", text: "推荐下近一个月北京出发的国航刷段性价比捡漏票", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "connecting flight rule", text: "连程航班，可以甩掉第一程吗？", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "tax refund policy", text: "日本现在对外国游客的退税政策是怎样的？", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "xiamen pace choice", text: "我没想好落地厦门是特种兵还是玩两天", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "hk 2-day plan", text: "我计划24~25去hk看电影，住一晚丽晶酒店，帮我规划下剩下的行程", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "spot photo guide", text: "帮我制定个打卡攻略和最佳出行时间计划吧", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "station food recommend", text: "高铁站附近有啥具体到店的推荐", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "must-eat restaurant ask", text: "青浦新城附近有没有什么必吃的饭店", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "metro line must-eat", text: "沿17号线，有什么必吃的", trigKind: "mention", want: AdvisoryDomainTravel},
		{name: "city must-eat ask", text: "无锡有啥必吃的", trigKind: "mention", want: AdvisoryDomainTravel},

		// Follow-ups with explicit @mention inside active travel thread must stay in Travel domain!
		{name: "mention followup exclude region", text: "不要江浙沪", trigKind: "mention", inAdvisoryThread: true, prevDomain: AdvisoryDomainTravel, want: AdvisoryDomainTravel},
		{name: "mention followup unlimited budget", text: "预算不限，最推荐哪个", trigKind: "mention", inAdvisoryThread: true, prevDomain: AdvisoryDomainTravel, want: AdvisoryDomainTravel},
		{name: "mention followup add stop", text: "能不能中午再加一个锡林浩特", trigKind: "mention", inAdvisoryThread: true, prevDomain: AdvisoryDomainTravel, want: AdvisoryDomainTravel},
		{name: "mention followup what to eat", text: "那青浦新城吃什么", trigKind: "mention", inAdvisoryThread: true, prevDomain: AdvisoryDomainTravel, want: AdvisoryDomainTravel},
		{name: "mention followup parking fee", text: "蟠龙天地停车多少钱", trigKind: "mention", inAdvisoryThread: true, prevDomain: AdvisoryDomainTravel, want: AdvisoryDomainTravel},

		// Non-travel general advisory stays General
		{name: "monitor buying stays general", text: "预算3000想买个27寸4K显示器，写代码偶尔打FPS，帮我选选", trigKind: "mention", want: AdvisoryDomainGeneral},
		{name: "docker debug stays general", text: "Docker 配 Nginx 反代老是 502，宿主机端口通的，咋排查？", trigKind: "reply", want: AdvisoryDomainGeneral},

		// Banter / trolling / proactive guards must NOT falsely trigger
		{name: "banter group travels", text: "群友怎么天天旅游", trigKind: "mention", want: AdvisoryDomainNone},
		{name: "banter tasty members", text: "那你记得好吃的群友吗", trigKind: "mention", want: AdvisoryDomainNone},
		{name: "banter ask bot location", text: "你最近在哪旅游呢", trigKind: "mention", want: AdvisoryDomainNone},
		{name: "banter ask bot visited", text: "你去过日本吗", trigKind: "mention", want: AdvisoryDomainNone},
		{name: "proactive travel never advisory", text: "深圳500左右酒店哪家强", trigKind: "proactive", want: AdvisoryDomainNone},
	}

	for _, tc := range travelCases {
		t.Run(tc.name, func(t *testing.T) {
			got := DetectAdvisoryDomainWithPrev(tc.text, tc.trigKind, tc.inAdvisoryThread, tc.prevDomain)
			if got != tc.want {
				t.Fatalf("DetectAdvisoryDomainWithPrev(%q, %q, %v, %q) = %q, want %q",
					tc.text, tc.trigKind, tc.inAdvisoryThread, tc.prevDomain, got, tc.want)
			}
		})
	}
}

func TestSanitizeReplyMarkdownAndGuideTitle(t *testing.T) {
	raw := strings.Join([]string{
		"### 关西4日攻略",
		"**核心结论**：大阪住到底，抽2天往返京都。",
		"> 避坑：清水寺早8点前冲。",
		"```python",
		"x = 2 ** 3  # keep code intact",
		"```",
	}, "\n")
	got := SanitizeReplyMarkdown(raw)
	if strings.Contains(got, "###") || strings.Contains(got, "**核心结论**") || strings.Contains(got, "> 避坑") {
		t.Fatalf("markdown markers leaked into plain text:\n%s", got)
	}
	if !strings.Contains(got, "【关西4日攻略】") || !strings.Contains(got, "x = 2 ** 3") {
		t.Fatalf("expected heading conversion and preserved code block, got:\n%s", got)
	}
	title := ExtractGuideTitle(got, "大肥鱼")
	if title != "大肥鱼 · 关西4日攻略" {
		t.Fatalf("ExtractGuideTitle() = %q, want %q", title, "大肥鱼 · 关西4日攻略")
	}

	// Strip leaked synthetic chat-log transcript prefixes and [@Name] pseudo-mentions.
	leakedPoke := "[12:23] Alice(123456789): [戳了 Bob]\n[12:23] Alice(123456789): [戳了你] \n戳什么戳 我在吃饭"
	if gotClean := SanitizeReplyMarkdown(leakedPoke); gotClean != "戳什么戳 我在吃饭" {
		t.Fatalf("expected leaked transcript lines stripped, got %q", gotClean)
	}
	leakedMention := "[@Alice] 你猜是为啥\n潜水不发表情 那叫装死"
	if gotClean := SanitizeReplyMarkdown(leakedMention); gotClean != "你猜是为啥\n潜水不发表情 那叫装死" {
		t.Fatalf("expected leaked [@Name] stripped, got %q", gotClean)
	}
	leakedOnly := "[12:23] Alice(123456789): [戳了你]"
	if gotClean := SanitizeReplyMarkdown(leakedOnly); gotClean != "" {
		t.Fatalf("expected pure leaked synthetic line to sanitize to empty, got %q", gotClean)
	}
}

func TestAdvisoryDynamicParamsAndForwardDelivery(t *testing.T) {
	var mu sync.Mutex
	var captured []infer.ChatRequest
	var longGuideReply = strings.Repeat("【方案细节】3000元档首选27寸4K 160Hz Fast IPS面板，兼顾代码字体锐利与FPS低延迟响应，避坑60Hz办公屏拖影问题。\n\n", 6)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req infer.ChatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		captured = append(captured, req)
		n := len(captured)
		mu.Unlock()

		reply := "哈哈行"
		if n == 2 {
			reply = strings.TrimSpace(longGuideReply)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message":       map[string]any{"content": reply},
			}},
			"usage": map[string]any{"prompt_tokens": 120, "completion_tokens": 40},
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlCfg := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "UTC"
models:
  primary:
    base_url: "` + srv.URL + `"
    model: "test-m"
    temperature: 0.9
    presence_penalty: 0.2
    reasoning_effort: "low"
    timeout_sec: 5
policy:
  explicit_debounce_sec: 0.02
  advisory:
    enabled: true
    reasoning_effort: "medium"
    temperature: 0.55
    presence_penalty: 0.0
    max_tool_iterations: 8
    forward_min_chars: 200
  rules:
    reply_gate: "mentioned"
`
	if err := os.WriteFile(cfgPath, []byte(yamlCfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgMgr, err := config.NewManager(cfgPath, policy.ValidateConfig)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	st, err := store.Open(dir, 512, 5)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	pol, _ := policy.NewEngine(cfgMgr.Get().Policy)
	hub := infer.NewHub(cfgMgr.Get().Models)
	fb := frame.NewBuilder(st, cfgMgr.Get().Frame)
	runner := agent.NewRunner(hub, st, fb, nil, nil)
	tr := &mockTransport{}

	mgr := NewManager(Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Hub:       hub,
		Frames:    fb,
		Runner:    runner,
		Transport: tr,
	})
	defer mgr.Drain(2 * time.Second)

	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "88888")

	// Turn 1: casual banter -> default low reasoning & 0.9 temp.
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m1",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts:         []model.Part{{Kind: model.PartText, Text: "在干嘛呢"}},
		CreatedAt:     time.Now(),
	})
	deadline := time.Now().Add(3 * time.Second)
	for tr.sentTexts.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if tr.sentTexts.Load() < 1 {
		t.Fatalf("turn 1 did not deliver")
	}

	// Turn 2: explicit buying advice -> dynamic medium reasoning, 0.55 temp, 0.0 presence_penalty, and forward card delivery!
	mgr.Dispatch(&model.Message{
		ChannelID:     ch,
		PlatformMsgID: "m2",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts:         []model.Part{{Kind: model.PartText, Text: "预算3000买个27寸4K显示器写代码打FPS怎么选"}},
		CreatedAt:     time.Now(),
	})
	deadline = time.Now().Add(3 * time.Second)
	for tr.sentForwards.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if tr.sentForwards.Load() != 1 {
		t.Fatalf("expected 1 merged forward card for long group guide, got %d", tr.sentForwards.Load())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 2 {
		t.Fatalf("expected 2 LLM requests, got %d", len(captured))
	}
	r1, r2 := captured[0], captured[1]
	if r1.ReasoningEffort != "low" || r1.Temperature == nil || *r1.Temperature != 0.9 {
		t.Fatalf("turn 1 should use default casual params, got effort=%q temp=%v", r1.ReasoningEffort, r1.Temperature)
	}
	if r2.ReasoningEffort != "medium" || r2.Temperature == nil || *r2.Temperature != 0.55 || r2.PresencePenalty == nil || *r2.PresencePenalty != 0.0 {
		t.Fatalf("turn 2 should use advisory params, got effort=%q temp=%v pp=%v", r2.ReasoningEffort, r2.Temperature, r2.PresencePenalty)
	}
	// KV Cache invariant: [msg 0] (system) and [msg 1] (pinned snapshot) must be byte-identical across turns!
	b1, _ := json.Marshal(r1.Messages[:2])
	b2, _ := json.Marshal(r2.Messages[:2])
	if string(b1) != string(b2) {
		t.Fatalf("KV prefix [msg 0..1] mutated between casual and advisory turns!\n1: %s\n2: %s", b1, b2)
	}
}
