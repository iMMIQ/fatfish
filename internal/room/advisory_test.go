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
