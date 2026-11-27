package room

import (
	"testing"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
)

func TestFormatBubblesAndStripPeriod(t *testing.T) {
	pol := config.PolicyConfig{
		SplitNewlineBubbles: true,
		MaxReplyBubbles:     4,
		StripTrailingPeriod: true,
	}

	// Short casual lines split and drop isolated trailing Chinese period.
	b := FormatBubbles("哈哈哈哈。\n确实如此。\n走吧。", pol)
	if len(b) != 3 || b[0] != "哈哈哈哈" || b[1] != "确实如此" || b[2] != "走吧" {
		t.Fatalf("unexpected bubbles: %#v", b)
	}

	// Code fences must never be split into multiple bubbles.
	code := "```go\nfmt.Println(1)\nfmt.Println(2)\n```"
	b2 := FormatBubbles(code, pol)
	if len(b2) != 1 || b2[0] != code {
		t.Fatalf("code fence should not be split: %#v", b2)
	}

	// Smart reply decoration: direct reply omits quote, displaced adds quote, poke never quotes.
	pol.SmartReplyDecoration = true
	trig := triggerState{kind: "mention", msg: model.Message{PlatformMsgID: "m100"}}
	if r, _ := SmartReplyDecoration(trig, 0, pol); r != "" {
		t.Fatalf("direct reply should omit quote, got %q", r)
	}
	if r, _ := SmartReplyDecoration(trig, 2, pol); r != "m100" {
		t.Fatalf("displaced reply should quote m100, got %q", r)
	}
	trigPoke := triggerState{kind: "poke", msg: model.Message{PlatformMsgID: "m101"}}
	if r, _ := SmartReplyDecoration(trigPoke, 3, pol); r != "" {
		t.Fatalf("poke must never attach quote, got %q", r)
	}
}
