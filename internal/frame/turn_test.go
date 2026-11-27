package frame

import (
	"strings"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

func TestFormatUserTurnPureMentionNotEmpty(t *testing.T) {
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "55555")
	p := FormatUserTurnWithLimits([]model.Message{{
		ChannelID:     ch,
		PlatformMsgID: "m1",
		SenderID:      "u1",
		SenderName:    "Alice",
		MentionedBot:  true,
		Parts:         nil,
		CreatedAt:     time.Unix(1760000000, 0),
	}}, time.UTC, nil, "m1", 2, 6)

	if !strings.Contains(p.Text, "Alice(u1): [@了你]") {
		t.Fatalf("expected pure @bot mention placeholder, got %q", p.Text)
	}
}
