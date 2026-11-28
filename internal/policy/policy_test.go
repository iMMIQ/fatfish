package policy

import (
	"testing"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
)

func TestPolicyEngineDeterministicAndOverrides(t *testing.T) {
	cdOverride := 15
	tmOverride := 5
	cfg := config.PolicyConfig{
		Nicknames:            []string{"fatfish"},
		ProactiveCooldownSec: 60,
		ThreadMaxTurns:       3,
		Rules: config.PolicyRulesConfig{
			ReplyGate:          "mentioned || replied_to_bot || poked || (!is_night && cooldown_ok && (hit_nickname ? rand < 0.70 : rand < 0.03))",
			DropStale:          "proactive && (new_msgs >= 4 || wait_sec >= 20)",
			AutoCollectSticker: "is_group && !from_bot && image_kb > 0 && image_kb < 2048",
		},
		Overrides: map[string]config.PolicyOverrideBlock{
			"group:12345": {
				ProactiveCooldownSec: &cdOverride,
				ThreadMaxTurns:       &tmOverride,
				Rules: config.PolicyRulesConfig{
					ReplyGate: "mentioned || (cooldown_ok && rand < 0.50)",
				},
			},
		},
	}

	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	chDefault := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "99999")
	chOverride := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "12345")

	// Mention always passes.
	ok, err := eng.EvalReplyGate(chDefault, PolicyEnv{Mentioned: true, IsNight: true, Rand: 0.99})
	if err != nil || !ok {
		t.Fatalf("expected mention to pass, ok=%v err=%v", ok, err)
	}

	// Deterministic random threshold on base rule.
	ok, _ = eng.EvalReplyGate(chDefault, PolicyEnv{CooldownOK: true, Rand: 0.25})
	if ok {
		t.Fatalf("expected rand=0.25 to fail base 0.03 threshold")
	}

	// Override rule allows rand=0.25 < 0.50 and overrides numeric fields.
	ok, _ = eng.EvalReplyGate(chOverride, PolicyEnv{CooldownOK: true, Rand: 0.25})
	if !ok {
		t.Fatalf("expected rand=0.25 to pass override 0.50 threshold")
	}
	if eng.ProactiveCooldownSec(chDefault) != 60 || eng.ProactiveCooldownSec(chOverride) != 15 {
		t.Fatalf("unexpected proactive cooldown overrides: default=%d override=%d", eng.ProactiveCooldownSec(chDefault), eng.ProactiveCooldownSec(chOverride))
	}
	if eng.ThreadMaxTurns(chDefault) != 3 || eng.ThreadMaxTurns(chOverride) != 5 {
		t.Fatalf("unexpected thread_max_turns overrides: default=%d override=%d", eng.ThreadMaxTurns(chDefault), eng.ThreadMaxTurns(chOverride))
	}

	// Stale drop checks.
	drop, _ := eng.EvalDropStale(chDefault, PolicyEnv{Proactive: true, NewMsgs: 5})
	if !drop {
		t.Fatalf("expected proactive stale drop when new_msgs=5")
	}
	drop, _ = eng.EvalDropStale(chDefault, PolicyEnv{Proactive: false, NewMsgs: 10})
	if drop {
		t.Fatalf("never drop explicit mentions on drop_stale")
	}
}

func TestPolicyInvalidExprRejected(t *testing.T) {
	cfg := config.PolicyConfig{
		Rules: config.PolicyRulesConfig{
			ReplyGate: "unknown_field > 10",
		},
	}
	if _, err := NewEngine(cfg); err == nil {
		t.Fatalf("expected compile error for unknown PolicyEnv field")
	}
}

func TestBotTravelClockPolicyEnv(t *testing.T) {
	cfg := config.PolicyConfig{
		Rules: config.PolicyRulesConfig{
			ReplyGate: "mentioned || replied_to_bot || poked || (in_thread && thread_turns < 3) || (cooldown_ok && !mentioned_other && !replied_to_other && (hit_nickname ? ((is_night || bot_sleeping || bot_in_transit) ? rand < 0.40 : rand < 0.72) : ((is_night || bot_sleeping || bot_in_transit) ? rand < 0.01 : (burst_users >= 3 ? rand < 0.12 : rand < 0.06))))",
		},
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("compile travel-aware reply_gate: %v", err)
	}
	ch := model.MakeChannelID("onebot", "100001", model.ChannelGroup, "999")
	// Explicit mention still replies while sleeping/in_transit
	ok, err := eng.EvalReplyGate(ch, PolicyEnv{Mentioned: true, BotSleeping: true, BotInTransit: true, BotLocalHour: 3, Rand: 0.99})
	if err != nil || !ok {
		t.Fatalf("expected mentioned reply while sleeping, ok=%v err=%v", ok, err)
	}
	// Random proactive suppressed when bot_sleeping with rand=0.05
	ok, err = eng.EvalReplyGate(ch, PolicyEnv{CooldownOK: true, BotSleeping: true, BotLocalHour: 3, Rand: 0.05})
	if err != nil || ok {
		t.Fatalf("expected suppressed proactive while bot_sleeping, ok=%v err=%v", ok, err)
	}
	// Random proactive allowed when awake with rand=0.05
	ok, err = eng.EvalReplyGate(ch, PolicyEnv{CooldownOK: true, BotSleeping: false, BotLocalHour: 15, Rand: 0.05})
	if err != nil || !ok {
		t.Fatalf("expected allowed proactive while awake, ok=%v err=%v", ok, err)
	}
}
