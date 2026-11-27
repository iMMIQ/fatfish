package policy

import (
	"fmt"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
)

// PolicyEnv is the strongly-typed evaluation environment exposed to expr rules.
type PolicyEnv struct {
	IsGroup      bool    `expr:"is_group"`
	FromBot      bool    `expr:"from_bot"`
	IsAdmin      bool    `expr:"is_admin"`
	Mentioned    bool    `expr:"mentioned"`
	RepliedToBot bool    `expr:"replied_to_bot"`
	Poked        bool    `expr:"poked"`
	HitNickname  bool    `expr:"hit_nickname"`
	InThread     bool    `expr:"in_thread"`
	ThreadTurns  int     `expr:"thread_turns"`
	CooldownOK   bool    `expr:"cooldown_ok"`
	IsNight      bool    `expr:"is_night"`
	Hour         int     `expr:"hour"`
	BurstUsers   int     `expr:"burst_users"`
	NewMsgs      int     `expr:"new_msgs"`
	WaitSec      float64 `expr:"wait_sec"`
	Proactive    bool    `expr:"proactive"`
	TextLen      int     `expr:"text_len"`
	ImageKB      int     `expr:"image_kb"`
	Rand         float64 `expr:"rand"`
}

// CompiledRules holds precompiled expr programs for one policy scope.
type CompiledRules struct {
	ReplyGate          *vm.Program
	DropStale          *vm.Program
	AutoCollectSticker *vm.Program
}

// Engine holds compiled base rules and per-channel overrides with safe atomic swapping.
type Engine struct {
	mu        sync.RWMutex
	cfg       config.PolicyConfig
	base      *CompiledRules
	overrides map[string]*CompiledRules
}

// NewEngine compiles all base and per-channel policy expressions.
func NewEngine(cfg config.PolicyConfig) (*Engine, error) {
	base, ov, err := compileAll(cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:       cfg,
		base:      base,
		overrides: ov,
	}, nil
}

// ValidateConfig compiles all policy rules in cfg without mutating any live state.
func ValidateConfig(cfg *config.Config) error {
	_, _, err := compileAll(cfg.Policy)
	return err
}

// Update recompiles rules from cfg and swaps them atomically.
func (e *Engine) Update(cfg config.PolicyConfig) error {
	base, ov, err := compileAll(cfg)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.cfg = cfg
	e.base = base
	e.overrides = ov
	e.mu.Unlock()
	return nil
}

func compileAll(cfg config.PolicyConfig) (*CompiledRules, map[string]*CompiledRules, error) {
	base, err := compileRuleSet("base", cfg.Rules, nil)
	if err != nil {
		return nil, nil, err
	}
	overrides := make(map[string]*CompiledRules, len(cfg.Overrides))
	for chKey, block := range cfg.Overrides {
		cr, err := compileRuleSet("override["+chKey+"]", block.Rules, base)
		if err != nil {
			return nil, nil, err
		}
		overrides[chKey] = cr
	}
	return base, overrides, nil
}

func compileRuleSet(scope string, rules config.PolicyRulesConfig, fallback *CompiledRules) (*CompiledRules, error) {
	out := &CompiledRules{}
	var err error

	if strings.TrimSpace(rules.ReplyGate) != "" {
		out.ReplyGate, err = compileBool(scope+".reply_gate", rules.ReplyGate)
		if err != nil {
			return nil, err
		}
	} else if fallback != nil {
		out.ReplyGate = fallback.ReplyGate
	} else {
		out.ReplyGate, err = compileBool(scope+".reply_gate", "mentioned || replied_to_bot || poked")
		if err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(rules.DropStale) != "" {
		out.DropStale, err = compileBool(scope+".drop_stale", rules.DropStale)
		if err != nil {
			return nil, err
		}
	} else if fallback != nil {
		out.DropStale = fallback.DropStale
	} else {
		out.DropStale, err = compileBool(scope+".drop_stale", "proactive && (new_msgs >= 4 || wait_sec >= 20)")
		if err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(rules.AutoCollectSticker) != "" {
		out.AutoCollectSticker, err = compileBool(scope+".auto_collect_sticker", rules.AutoCollectSticker)
		if err != nil {
			return nil, err
		}
	} else if fallback != nil {
		out.AutoCollectSticker = fallback.AutoCollectSticker
	} else {
		out.AutoCollectSticker, err = compileBool(scope+".auto_collect_sticker", "false")
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func compileBool(name, src string) (*vm.Program, error) {
	prog, err := expr.Compile(src, expr.Env(PolicyEnv{}), expr.AsBool())
	if err != nil {
		return nil, fmt.Errorf("policy: compile %s (%q): %w", name, src, err)
	}
	return prog, nil
}

func (e *Engine) rulesFor(ch model.ChannelID) *CompiledRules {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if cr, ok := e.overrides[string(ch)]; ok {
		return cr
	}
	// Also allow shorthand override keys like "group:123456789" or "dm:123456789".
	if _, _, kind, nativeID, ok := model.ParseChannelID(ch); ok {
		shortKey := string(kind) + ":" + nativeID
		if cr, ok := e.overrides[shortKey]; ok {
			return cr
		}
	}
	return e.base
}

// ChannelAllowed checks enabled_channels / disabled_channels allow/deny lists.
func (e *Engine) ChannelAllowed(ch model.ChannelID) bool {
	e.mu.RLock()
	enabled := e.cfg.EnabledChannels
	disabled := e.cfg.DisabledChannels
	e.mu.RUnlock()

	idStr := string(ch)
	nativeID := ch.NativeID()
	matches := func(list []string) bool {
		for _, item := range list {
			if item == idStr || item == nativeID {
				return true
			}
			if _, _, kind, nid, ok := model.ParseChannelID(ch); ok && item == string(kind)+":"+nid {
				return true
			}
		}
		return false
	}
	if matches(disabled) {
		return false
	}
	if len(enabled) > 0 && !matches(enabled) {
		return false
	}
	return true
}

// IsIgnoredBot returns true if senderID is listed in policy.ignored_bots.
func (e *Engine) IsIgnoredBot(senderID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, id := range e.cfg.IgnoredBots {
		if id == senderID {
			return true
		}
	}
	return false
}

// HitNickname returns true if text contains any configured bot nickname (case-insensitive).
func (e *Engine) HitNickname(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	lower := strings.ToLower(text)
	e.mu.RLock()
	nicks := e.cfg.Nicknames
	e.mu.RUnlock()
	for _, n := range nicks {
		if n != "" && strings.Contains(lower, strings.ToLower(n)) {
			return true
		}
	}
	return false
}

// EvalReplyGate evaluates the reply_gate rule for a channel.
func (e *Engine) EvalReplyGate(ch model.ChannelID, env PolicyEnv) (bool, error) {
	return runBool(e.rulesFor(ch).ReplyGate, env)
}

// EvalDropStale evaluates the drop_stale rule for a channel.
func (e *Engine) EvalDropStale(ch model.ChannelID, env PolicyEnv) (bool, error) {
	return runBool(e.rulesFor(ch).DropStale, env)
}

// EvalAutoCollectSticker evaluates the auto_collect_sticker rule for a channel.
func (e *Engine) EvalAutoCollectSticker(ch model.ChannelID, env PolicyEnv) (bool, error) {
	return runBool(e.rulesFor(ch).AutoCollectSticker, env)
}

func runBool(prog *vm.Program, env PolicyEnv) (bool, error) {
	out, err := expr.Run(prog, env)
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("policy: expected bool result, got %T", out)
	}
	return b, nil
}
