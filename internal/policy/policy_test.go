package policy

import (
	"testing"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
)

func TestPolicyEngineDeterministicAndOverrides(t *testing.T) {
	cfg := config.PolicyConfig{
		Nicknames: []string{"fatfish"},
		Rules: config.PolicyRulesConfig{
			ReplyGate:          "mentioned || replied_to_bot || poked || (!is_night && cooldown_ok && (hit_nickname ? rand < 0.70 : rand < 0.03))",
			DropStale:          "proactive && (new_msgs >= 4 || wait_sec >= 20)",
			AutoCollectSticker: "is_group && !from_bot && image_kb > 0 && image_kb < 2048",
		},
		Overrides: map[string]config.PolicyOverrideBlock{
			"group:12345": {
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

	// Override rule allows rand=0.25 < 0.50.
	ok, _ = eng.EvalReplyGate(chOverride, PolicyEnv{CooldownOK: true, Rand: 0.25})
	if !ok {
		t.Fatalf("expected rand=0.25 to pass override 0.50 threshold")
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
