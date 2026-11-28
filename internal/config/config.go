package config

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

// Config is the root configuration matching configs/fatfish.example.yaml.
type Config struct {
	DataDir   string          `yaml:"data_dir"`
	Timezone  string          `yaml:"timezone"`
	Transport TransportConfig `yaml:"transport"`
	Models    ModelsConfig    `yaml:"models"`
	Persona   PersonaConfig   `yaml:"persona"`
	Frame     FrameConfig     `yaml:"frame"`
	Policy     PolicyConfig     `yaml:"policy"`
	LinkParser LinkParserConfig `yaml:"link_parser"`
	Sticker    StickerConfig    `yaml:"sticker"`
	Travel     TravelConfig     `yaml:"travel"`
	KB         KBConfig         `yaml:"kb"`
	Tools      ToolsConfig      `yaml:"tools"`
	MCP       MCPConfig       `yaml:"mcp"`
	Console   ConsoleConfig   `yaml:"console"`

	// Runtime-populated metadata (not serialized to YAML).
	ConfigPath  string         `yaml:"-"`
	PersonaPath string         `yaml:"-"`
	PersonaText string         `yaml:"-"`
	PersonaHash string         `yaml:"-"`
	loc         *time.Location `yaml:"-"`
}

type TransportConfig struct {
	OneBot OneBotConfig `yaml:"onebot"`
}

type OneBotConfig struct {
	WSURL           string   `yaml:"ws_url"`
	WSListen        string   `yaml:"ws_listen"` // Optional Reverse WS listen address e.g. "0.0.0.0:6199"
	Token           string   `yaml:"token"`
	SelfID          string   `yaml:"self_id"`
	AdminIDs        []string `yaml:"admin_ids"`
	ReconnectMinSec int      `yaml:"reconnect_min_sec"`
	ReconnectMaxSec int      `yaml:"reconnect_max_sec"`
	APITimeoutSec   int      `yaml:"api_timeout_sec"`
}

type ModelsConfig struct {
	Primary         ModelEndpointConfig `yaml:"primary"`
	PrimaryFallback ModelEndpointConfig `yaml:"primary_fallback"`
	Fast            ModelEndpointConfig `yaml:"fast"`
	Distill         ModelEndpointConfig `yaml:"distill"`
	Embed           ModelEndpointConfig `yaml:"embed"`
	Rerank          ModelEndpointConfig `yaml:"rerank"`
	ASR             ModelEndpointConfig `yaml:"asr"`
	Translate       ModelEndpointConfig `yaml:"translate"`
}

type ModelEndpointConfig struct {
	Enabled          *bool    `yaml:"enabled,omitempty"`
	BaseURL          string   `yaml:"base_url"`
	APIKey           string   `yaml:"api_key"`
	Model            string   `yaml:"model"`
	Multimodal       bool     `yaml:"multimodal"`
	Temperature      *float64 `yaml:"temperature,omitempty"`
	TopP             *float64 `yaml:"top_p,omitempty"`
	PresencePenalty  *float64 `yaml:"presence_penalty,omitempty"`
	FrequencyPenalty *float64 `yaml:"frequency_penalty,omitempty"`
	ReasoningEffort  string   `yaml:"reasoning_effort,omitempty"` // Optional: "low" | "medium" | "high"
	ThinkingType     string   `yaml:"thinking_type,omitempty"`    // Optional: "enabled" | "disabled"
	Dim              int      `yaml:"dim"`
	MaxBatch         int      `yaml:"max_batch"`
	TimeoutSec       int      `yaml:"timeout_sec"`
	MaxConcurrent    int      `yaml:"max_concurrent"`
}

func (m ModelEndpointConfig) IsEnabled() bool {
	if m.Enabled != nil {
		return *m.Enabled
	}
	return strings.TrimSpace(m.BaseURL) != "" && strings.TrimSpace(m.Model) != ""
}

type PersonaConfig struct {
	File string `yaml:"file"`
}

type FrameConfig struct {
	InputTokenBudget      int                   `yaml:"input_token_budget"`
	TrimHighWater         float64               `yaml:"trim_high_water"`
	TrimLowWater          float64               `yaml:"trim_low_water"`
	SegmentIdleMinutes    int                   `yaml:"segment_idle_minutes"`
	SummaryTokenBudget    int                   `yaml:"summary_token_budget"`
	PinnedSnapshotBudgets PinnedSnapshotBudgets `yaml:"pinned_snapshot_budgets"`
	MaxImagesPerRequest   int                   `yaml:"max_images_per_request"`
	MaxBackgroundImages   int                   `yaml:"max_background_images"`
	ImageTokenReserve     int                   `yaml:"image_token_reserve"`
	ImageMaxDim           int                   `yaml:"image_max_dim"`
	MaxImageMiB           int                   `yaml:"max_image_mib"`
	MediaRetentionHours   int                   `yaml:"media_retention_hours"`
	RoomMediaMiB          int                   `yaml:"room_media_mib"`
	TotalMediaMiB         int                   `yaml:"total_media_mib"`
}

type PinnedSnapshotBudgets struct {
	Lore       int `yaml:"lore"`
	Notes      int `yaml:"notes"`
	Profiles   int `yaml:"profiles"`
	Highlights int `yaml:"highlights"`
	Travel     int `yaml:"travel"`
}

type PolicyConfig struct {
	EnabledChannels        []string                       `yaml:"enabled_channels"`
	DisabledChannels       []string                       `yaml:"disabled_channels"`
	IgnoredBots            []string                       `yaml:"ignored_bots"`
	Nicknames              []string                       `yaml:"nicknames"`
	MentionWaitSec         float64                        `yaml:"mention_wait_sec"`
	ExplicitDebounceSec    float64                        `yaml:"explicit_debounce_sec"`
	MediaWaitSec           float64                        `yaml:"media_wait_sec"`
	ASRWaitSec             float64                        `yaml:"asr_wait_sec"`
	InputDebounceSec       float64                        `yaml:"input_debounce_sec"`
	ProactiveCooldownSec   int                            `yaml:"proactive_cooldown_sec"`
	GateTokenBudget        int                            `yaml:"gate_token_budget"`
	GateIncludeImages      bool                           `yaml:"gate_include_images"`
	ThreadContinuationSec  int                            `yaml:"thread_continuation_sec"`
	ThreadMaxTurns         int                            `yaml:"thread_max_turns"`
	ThreadMinIntervalSec   float64                        `yaml:"thread_min_interval_sec"`
	MuteBackoffSec         int                            `yaml:"mute_backoff_sec"`
	RateLimitBackoffSec    int                            `yaml:"rate_limit_backoff_sec"`
	MaxMsgsPerMin          int                            `yaml:"max_msgs_per_min"`
	SmartReplyDecoration   bool                           `yaml:"smart_reply_decoration"`
	SplitNewlineBubbles    bool                           `yaml:"split_newline_bubbles"`
	MaxReplyBubbles        int                            `yaml:"max_reply_bubbles"`
	BubbleTypingDelayMax   float64                        `yaml:"bubble_typing_delay_max_sec"`
	StripTrailingPeriod     bool                           `yaml:"strip_trailing_period"`
	RecallGossipProb        float64                        `yaml:"recall_gossip_prob"`
	RecallGossipCooldownSec int                            `yaml:"recall_gossip_cooldown_sec"`
	PlusOne                 PlusOneConfig                  `yaml:"plus_one"`
	Poke                   PokeConfig                     `yaml:"poke"`
	Advisory               AdvisoryConfig                 `yaml:"advisory"`
	Rules                  PolicyRulesConfig              `yaml:"rules"`
	Overrides              map[string]PolicyOverrideBlock `yaml:"overrides"`
}

// AdvisoryConfig tunes dynamic reasoning, sampling, tool depth, thread continuation,
// and optional long-guide forward packaging when a user asks for guides/advice.
type AdvisoryConfig struct {
	Enabled               *bool    `yaml:"enabled,omitempty"`
	ReasoningEffort       string   `yaml:"reasoning_effort,omitempty"`
	Temperature           *float64 `yaml:"temperature,omitempty"`
	PresencePenalty       *float64 `yaml:"presence_penalty,omitempty"`
	MaxToolIterations     int      `yaml:"max_tool_iterations,omitempty"`
	ThreadContinuationSec int      `yaml:"thread_continuation_sec,omitempty"`
	ThreadMaxTurns        int      `yaml:"thread_max_turns,omitempty"`
	ForwardMinChars       int      `yaml:"forward_min_chars,omitempty"`
}

func (a AdvisoryConfig) IsEnabled() bool {
	if a.Enabled == nil {
		return true
	}
	return *a.Enabled
}

type PlusOneConfig struct {
	Enabled     bool    `yaml:"enabled"`
	MinUsers    int     `yaml:"min_users"`
	RepeatProb  float64 `yaml:"repeat_prob"`
	BreakProb   float64 `yaml:"break_prob"`
	CooldownSec int     `yaml:"cooldown_sec"`
}

type PokeConfig struct {
	Mode               string  `yaml:"mode"` // bot_only | all | ignore
	CooldownSec        int     `yaml:"cooldown_sec"`
	ReverseProb        float64 `yaml:"reverse_prob"`
	OtherPokeReplyProb float64 `yaml:"other_poke_reply_prob"`
	AfterReplyProb     float64 `yaml:"after_reply_prob"`
	AfterReplyMaxChars int     `yaml:"after_reply_max_chars"`
	AfterReplyDelaySec float64 `yaml:"after_reply_delay_sec"`
}

type PolicyRulesConfig struct {
	ReplyGate          string `yaml:"reply_gate"`
	DropStale          string `yaml:"drop_stale"`
	AutoCollectSticker string `yaml:"auto_collect_sticker"`
}

type PolicyOverrideBlock struct {
	ProactiveCooldownSec *int              `yaml:"proactive_cooldown_sec,omitempty"`
	ThreadMaxTurns       *int              `yaml:"thread_max_turns,omitempty"`
	Rules                PolicyRulesConfig `yaml:"rules"`
}

// LinkParserConfig controls proactive URL extraction, Tier-1 site parsing, caching, and fatfish-decided replies.
type LinkParserConfig struct {
	Enabled          bool     `yaml:"enabled"`
	ProactiveReply   bool     `yaml:"proactive_reply"`
	CooldownSec      int      `yaml:"cooldown_sec"`
	DedupTTLSec      int      `yaml:"dedup_ttl_sec"`
	MaxURLsPerMsg    int      `yaml:"max_urls_per_msg"`
	WaitSec          float64  `yaml:"wait_sec"`
	TimeoutSec       int      `yaml:"timeout_sec"`
	CacheTTLSec      int      `yaml:"cache_ttl_sec"`
	NightProactive   bool     `yaml:"night_proactive"`
	TwitterAPIBase   string   `yaml:"twitter_api_base"`
	GitHubToken      string   `yaml:"github_token"`
	BilibiliCookie   string   `yaml:"bilibili_cookie"`
	IgnoreDomains    []string `yaml:"ignore_domains"`
	DisabledChannels []string `yaml:"disabled_channels"`
}

type StickerConfig struct {
	Enabled              bool    `yaml:"enabled"`
	PackDir              string  `yaml:"pack_dir"`
	DefaultPack          string  `yaml:"default_pack"`
	DocCenteringAlpha    float64 `yaml:"doc_centering_alpha"`
	ReactionIntentWeight float64 `yaml:"reaction_intent_weight"`
	ExplicitIntentWeight float64 `yaml:"explicit_intent_weight"`
	RecentWindow         int     `yaml:"recent_window"`
	MMRLambda            float64 `yaml:"mmr_lambda"`
	AsyncSendTimeoutSec  int     `yaml:"async_send_timeout_sec"`
	MaxSendMiB           int     `yaml:"max_send_mib"`
	AutoCollect          bool    `yaml:"auto_collect"`
}

// TravelConfig configures the optional global continuous world-travel life-simulation subsystem.
type TravelConfig struct {
	Enabled                 bool     `yaml:"enabled"`
	HomeCity                string   `yaml:"home_city"`                  // Starting city ID or name (default "hangzhou")
	RouteStyle              string   `yaml:"route_style"`                // "realistic" | "wander"
	StayMinDays             int      `yaml:"stay_min_days"`              // Minimum stay per city (default 3)
	StayMaxDays             int      `yaml:"stay_max_days"`              // Maximum stay per city (default 6)
	JournalIntervalHours    float64  `yaml:"journal_interval_hours"`     // Hours between micro-diary entries (default 5.0)
	WeatherIntervalHours    float64  `yaml:"weather_interval_hours"`     // Hours between Open-Meteo weather refreshes (default 3.0)
	JournalModel            string   `yaml:"journal_model"`              // Inference hub role: "primary" | "fast" | "distill" (default "primary")
	AllowSuggestions        bool     `yaml:"allow_suggestions"`          // Allow group members to suggest next stop via travel tool
	ProactiveEnabled        bool     `yaml:"proactive_enabled"`          // Emit low-frequency arrival/extreme-weather check-ins
	ProactiveChannels       []string `yaml:"proactive_channels"`         // Allowed channel IDs / native IDs, or ["*"] for all allowed channels
	ProactiveQuietStartHour int      `yaml:"proactive_quiet_start_hour"` // Config TZ hour to begin quiet window (default 23)
	ProactiveQuietEndHour   int      `yaml:"proactive_quiet_end_hour"`   // Config TZ hour to end quiet window (default 9)
	ProactiveMinGapHours    float64  `yaml:"proactive_min_gap_hours"`    // Minimum hours between proactive travel posts (default 36)
}

// KBConfig configures an optional external read-only SQLite FTS5 reference knowledge base (kb.db).
type KBConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Path       string `yaml:"path"`        // Path to read-only SQLite kb.db (e.g. "/app/data/kb.db")
	MaxResults int    `yaml:"max_results"` // Default top-K reference chunks returned per search (default 4, max 6)
}

type ToolsConfig struct {
	DefaultTimeoutSec      int      `yaml:"default_timeout_sec"`
	AllowRelayChannels     []string `yaml:"allow_relay_channels"`
	MaxSchedulesPerChannel int      `yaml:"max_schedules_per_channel"`
	MinCronIntervalMinutes int      `yaml:"min_cron_interval_minutes"`
	ChartFontPath          string   `yaml:"chart_font_path"`
	StarlarkMaxSteps       int      `yaml:"starlark_max_steps"`
	StarlarkTimeoutSec     int      `yaml:"starlark_timeout_sec"`
	SSRFAllowHosts         []string `yaml:"ssrf_allow_hosts"`
}

type MCPConfig struct {
	Servers []MCPServerConfig `yaml:"servers"`
}

type MCPServerConfig struct {
	Name           string            `yaml:"name"`
	Transport      string            `yaml:"transport"` // "streamable_http" | "stdio"
	URL            string            `yaml:"url"`
	Command        string            `yaml:"command"`
	Args           []string          `yaml:"args"`
	Headers        map[string]string `yaml:"headers"`
	Env            map[string]string `yaml:"env"`
	TimeoutSec     int               `yaml:"timeout_sec"`
	ReadOnly       bool              `yaml:"read_only,omitempty"`
	MaxOutputRunes int               `yaml:"max_output_runes,omitempty"`
}

type ConsoleConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
	Token   string `yaml:"token"`
}

var envVarRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// ExpandEnv expands ${VAR} references using os.LookupEnv while leaving literal text intact.
func ExpandEnv(raw []byte) []byte {
	return envVarRe.ReplaceAllFunc(raw, func(match []byte) []byte {
		sub := envVarRe.FindSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		if val, ok := os.LookupEnv(string(sub[1])); ok {
			return []byte(val)
		}
		return nil
	})
}

// Load parses a YAML file from disk, applies defaults, expands ${ENV}, and loads persona markdown.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	cfg, err := ParseBytes(raw, filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(path)
	if err == nil {
		cfg.ConfigPath = absPath
	} else {
		cfg.ConfigPath = path
	}
	return cfg, nil
}

// ParseBytes parses YAML configuration bytes using baseDir to resolve relative paths.
func ParseBytes(raw []byte, baseDir string) (*Config, error) {
	expanded := ExpandEnv(raw)
	cfg := defaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: parse yaml: %w", err)
	}
	applyDefaults(cfg)
	if err := loadPersona(cfg, baseDir); err != nil {
		return nil, err
	}
	if err := validateBasic(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaultConfig() *Config {
	return &Config{
		DataDir:  "./data",
		Timezone: "Asia/Shanghai",
		Transport: TransportConfig{
			OneBot: OneBotConfig{
				ReconnectMinSec: 1,
				ReconnectMaxSec: 30,
				APITimeoutSec:   10,
			},
		},
		Frame: FrameConfig{
			InputTokenBudget:   32768,
			TrimHighWater:      0.80,
			TrimLowWater:       0.50,
			SegmentIdleMinutes: 30,
			SummaryTokenBudget: 512,
			PinnedSnapshotBudgets: PinnedSnapshotBudgets{
				Lore:       120,
				Notes:      140,
				Profiles:   220,
				Highlights: 220,
				Travel:     140,
			},
			MaxImagesPerRequest: 6,
			MaxBackgroundImages: 2,
			ImageTokenReserve:   1600,
			ImageMaxDim:         1280,
			MaxImageMiB:         10,
			MediaRetentionHours: 24,
			RoomMediaMiB:        256,
			TotalMediaMiB:       1024,
		},
		Policy: PolicyConfig{
			MentionWaitSec:        1.0,
			MediaWaitSec:          3.0,
			ASRWaitSec:            4.0,
			InputDebounceSec:      1.2,
			ProactiveCooldownSec:  60,
			GateTokenBudget:       4096,
			ThreadContinuationSec: 90,
			ThreadMaxTurns:        3,
			ThreadMinIntervalSec:  4.0,
			MuteBackoffSec:        1800,
			RateLimitBackoffSec:   75,
			MaxMsgsPerMin:         8,
			SmartReplyDecoration:  true,
			SplitNewlineBubbles:   true,
			MaxReplyBubbles:       4,
			BubbleTypingDelayMax:    1.0,
			StripTrailingPeriod:     true,
			RecallGossipProb:        0.15,
			RecallGossipCooldownSec: 300,
			PlusOne: PlusOneConfig{
				Enabled:     true,
				MinUsers:    3,
				RepeatProb:  0.32,
				BreakProb:   0.06,
				CooldownSec: 180,
			},
			Poke: PokeConfig{
				Mode:               "bot_only",
				CooldownSec:        15,
				OtherPokeReplyProb: 0.15,
				AfterReplyProb:     0.15,
				AfterReplyMaxChars: 35,
				AfterReplyDelaySec: 0.5,
			},
			Advisory: AdvisoryConfig{
				ReasoningEffort:       "medium",
				MaxToolIterations:     8,
				ThreadContinuationSec: 150,
				ThreadMaxTurns:        5,
				ForwardMinChars:       380,
			},
			Rules: PolicyRulesConfig{
				ReplyGate:          "mentioned || replied_to_bot || poked || in_thread || (!is_night && cooldown_ok && (hit_nickname ? rand < 0.70 : rand < 0.03))",
				DropStale:          "proactive && (new_msgs >= 4 || wait_sec >= 20)",
				AutoCollectSticker: "is_group && !from_bot && image_kb > 0 && image_kb < 2048",
			},
		},
		LinkParser: LinkParserConfig{
			Enabled:        true,
			ProactiveReply: true,
			CooldownSec:    15,
			DedupTTLSec:    1800,
			MaxURLsPerMsg:  2,
			WaitSec:        4.0,
			TimeoutSec:     12,
			CacheTTLSec:    1200,
			NightProactive: true,
			TwitterAPIBase: "https://api.fxtwitter.com",
		},
		Sticker: StickerConfig{
			Enabled:              true,
			PackDir:              "./data/stickers",
			DefaultPack:          "default",
			DocCenteringAlpha:    0.7,
			ReactionIntentWeight: 0.75,
			ExplicitIntentWeight: 0.25,
			RecentWindow:         30,
			MMRLambda:            0.7,
			AsyncSendTimeoutSec:  15,
			MaxSendMiB:           5,
		},
		Travel: TravelConfig{
			Enabled:                 false,
			HomeCity:                "hangzhou",
			RouteStyle:              "realistic",
			StayMinDays:             3,
			StayMaxDays:             6,
			JournalIntervalHours:    5.0,
			WeatherIntervalHours:    3.0,
			JournalModel:            "primary",
			AllowSuggestions:        true,
			ProactiveEnabled:        false,
			ProactiveQuietStartHour: 23,
			ProactiveQuietEndHour:   9,
			ProactiveMinGapHours:    36,
		},
		KB: KBConfig{
			Enabled:    false,
			Path:       "",
			MaxResults: 4,
		},
		Tools: ToolsConfig{
			DefaultTimeoutSec:      30,
			MaxSchedulesPerChannel: 10,
			MinCronIntervalMinutes: 5,
			ChartFontPath:          "./assets/fonts/NotoSansSC-Regular.ttf",
			StarlarkMaxSteps:       500000,
			StarlarkTimeoutSec:     3,
		},
		Console: ConsoleConfig{
			Enabled: true,
			Listen:  "127.0.0.1:6185",
		},
	}
}

func applyDefaults(cfg *Config) {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.Timezone == "" {
		cfg.Timezone = "UTC"
	}
	if cfg.Models.Primary.TimeoutSec <= 0 {
		cfg.Models.Primary.TimeoutSec = 90
	}
	if cfg.Models.PrimaryFallback.TimeoutSec <= 0 {
		cfg.Models.PrimaryFallback.TimeoutSec = cfg.Models.Primary.TimeoutSec
	}
	if cfg.Models.Fast.TimeoutSec <= 0 {
		cfg.Models.Fast.TimeoutSec = 20
	}
	if cfg.Models.Distill.TimeoutSec <= 0 {
		cfg.Models.Distill.TimeoutSec = 120
	}
	if cfg.Models.Distill.MaxConcurrent <= 0 {
		cfg.Models.Distill.MaxConcurrent = 1
	}
	if cfg.Models.Embed.Dim <= 0 {
		cfg.Models.Embed.Dim = 2048
	}
	if cfg.Models.Embed.MaxBatch <= 0 || cfg.Models.Embed.MaxBatch > 128 {
		cfg.Models.Embed.MaxBatch = 64
	}
	if cfg.Models.Embed.TimeoutSec <= 0 {
		cfg.Models.Embed.TimeoutSec = 15
	}
	if cfg.Models.Rerank.MaxBatch <= 0 || cfg.Models.Rerank.MaxBatch > 128 {
		cfg.Models.Rerank.MaxBatch = 64
	}
	if cfg.Models.Rerank.TimeoutSec <= 0 {
		cfg.Models.Rerank.TimeoutSec = 15
	}
	if cfg.Models.ASR.TimeoutSec <= 0 {
		cfg.Models.ASR.TimeoutSec = 15
	}
	if cfg.Models.Translate.TimeoutSec <= 0 {
		cfg.Models.Translate.TimeoutSec = 30
	}
	if cfg.Frame.InputTokenBudget <= 0 {
		cfg.Frame.InputTokenBudget = 32768
	}
	if cfg.Frame.TrimHighWater <= 0 {
		cfg.Frame.TrimHighWater = 0.80
	}
	if cfg.Frame.TrimLowWater <= 0 {
		cfg.Frame.TrimLowWater = 0.50
	}
	if cfg.Frame.ImageMaxDim <= 0 {
		cfg.Frame.ImageMaxDim = 1280
	}
	if cfg.Frame.MaxImageMiB <= 0 {
		cfg.Frame.MaxImageMiB = 10
	}
	if cfg.Frame.MediaRetentionHours <= 0 {
		cfg.Frame.MediaRetentionHours = 24
	}
	if cfg.Frame.RoomMediaMiB <= 0 {
		cfg.Frame.RoomMediaMiB = 256
	}
	if cfg.Frame.TotalMediaMiB <= 0 {
		cfg.Frame.TotalMediaMiB = 1024
	}
	if cfg.Policy.MaxReplyBubbles <= 0 {
		cfg.Policy.MaxReplyBubbles = 4
	}
	if cfg.Policy.RateLimitBackoffSec <= 0 {
		cfg.Policy.RateLimitBackoffSec = 75
	}
	if cfg.Policy.MaxMsgsPerMin < 0 {
		cfg.Policy.MaxMsgsPerMin = 8
	}
	if cfg.Policy.Poke.CooldownSec < 0 {
		cfg.Policy.Poke.CooldownSec = 15
	}
	if cfg.Policy.RecallGossipCooldownSec <= 0 {
		cfg.Policy.RecallGossipCooldownSec = 300
	}
	if strings.TrimSpace(cfg.Policy.Advisory.ReasoningEffort) == "" {
		cfg.Policy.Advisory.ReasoningEffort = "medium"
	}
	if cfg.Policy.Advisory.Temperature == nil {
		t := 0.55
		cfg.Policy.Advisory.Temperature = &t
	}
	if cfg.Policy.Advisory.PresencePenalty == nil {
		p := 0.0
		cfg.Policy.Advisory.PresencePenalty = &p
	}
	if cfg.Policy.Advisory.MaxToolIterations <= 0 {
		cfg.Policy.Advisory.MaxToolIterations = 8
	}
	if cfg.Policy.Advisory.ThreadContinuationSec <= 0 {
		cfg.Policy.Advisory.ThreadContinuationSec = 150
	}
	if cfg.Policy.Advisory.ThreadMaxTurns <= 0 {
		cfg.Policy.Advisory.ThreadMaxTurns = 5
	}
	if cfg.Policy.Advisory.ForwardMinChars < 0 {
		cfg.Policy.Advisory.ForwardMinChars = 380
	}
	if cfg.LinkParser.CooldownSec < 0 {
		cfg.LinkParser.CooldownSec = 15
	}
	if cfg.LinkParser.DedupTTLSec <= 0 {
		cfg.LinkParser.DedupTTLSec = 1800
	}
	if cfg.LinkParser.MaxURLsPerMsg <= 0 {
		cfg.LinkParser.MaxURLsPerMsg = 2
	}
	if cfg.LinkParser.WaitSec <= 0 {
		cfg.LinkParser.WaitSec = 4.0
	}
	if cfg.LinkParser.TimeoutSec <= 0 {
		cfg.LinkParser.TimeoutSec = 12
	}
	if cfg.LinkParser.CacheTTLSec <= 0 {
		cfg.LinkParser.CacheTTLSec = 1200
	}
	if strings.TrimSpace(cfg.LinkParser.TwitterAPIBase) == "" {
		cfg.LinkParser.TwitterAPIBase = "https://api.fxtwitter.com"
	}
	if cfg.Sticker.RecentWindow <= 0 {
		cfg.Sticker.RecentWindow = 30
	}
	if cfg.Sticker.AsyncSendTimeoutSec <= 0 {
		cfg.Sticker.AsyncSendTimeoutSec = 15
	}
	if cfg.Sticker.MaxSendMiB <= 0 {
		cfg.Sticker.MaxSendMiB = 5
	}
	if cfg.Frame.PinnedSnapshotBudgets.Travel <= 0 {
		cfg.Frame.PinnedSnapshotBudgets.Travel = 140
	}
	if strings.TrimSpace(cfg.Travel.HomeCity) == "" {
		cfg.Travel.HomeCity = "hangzhou"
	}
	if strings.TrimSpace(cfg.Travel.RouteStyle) == "" {
		cfg.Travel.RouteStyle = "realistic"
	}
	if cfg.Travel.StayMinDays <= 0 {
		cfg.Travel.StayMinDays = 3
	}
	if cfg.Travel.StayMaxDays < cfg.Travel.StayMinDays {
		cfg.Travel.StayMaxDays = cfg.Travel.StayMinDays + 2
	}
	if cfg.Travel.JournalIntervalHours <= 0 {
		cfg.Travel.JournalIntervalHours = 5.0
	}
	if cfg.Travel.WeatherIntervalHours <= 0 {
		cfg.Travel.WeatherIntervalHours = 3.0
	}
	if strings.TrimSpace(cfg.Travel.JournalModel) == "" {
		cfg.Travel.JournalModel = "primary"
	}
	if cfg.Travel.ProactiveMinGapHours <= 0 {
		cfg.Travel.ProactiveMinGapHours = 36
	}
	if cfg.Travel.ProactiveQuietStartHour < 0 || cfg.Travel.ProactiveQuietStartHour > 23 {
		cfg.Travel.ProactiveQuietStartHour = 23
	}
	if cfg.Travel.ProactiveQuietEndHour < 0 || cfg.Travel.ProactiveQuietEndHour > 23 {
		cfg.Travel.ProactiveQuietEndHour = 9
	}
	if cfg.Tools.DefaultTimeoutSec <= 0 {
		cfg.Tools.DefaultTimeoutSec = 30
	}
	if cfg.Tools.MaxSchedulesPerChannel <= 0 {
		cfg.Tools.MaxSchedulesPerChannel = 10
	}
	if cfg.Tools.MinCronIntervalMinutes <= 0 {
		cfg.Tools.MinCronIntervalMinutes = 5
	}
	if cfg.Tools.StarlarkMaxSteps <= 0 {
		cfg.Tools.StarlarkMaxSteps = 500000
	}
	if cfg.Tools.StarlarkTimeoutSec <= 0 {
		cfg.Tools.StarlarkTimeoutSec = 3
	}
	if cfg.Console.Listen == "" {
		cfg.Console.Listen = "127.0.0.1:6185"
	}
}

func loadPersona(cfg *Config, baseDir string) error {
	if strings.TrimSpace(cfg.Persona.File) == "" {
		cfg.PersonaText = "You are fatfish, a concise, helpful multimodal assistant."
	} else {
		p := cfg.Persona.File
		if !filepath.IsAbs(p) && baseDir != "" {
			// Try relative to cwd first, then relative to config file directory.
			if _, err := os.Stat(p); err != nil {
				cand := filepath.Join(baseDir, p)
				if _, err2 := os.Stat(cand); err2 == nil {
					p = cand
				}
			}
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("config: read persona %s: %w", cfg.Persona.File, err)
		}
		if absP, aerr := filepath.Abs(p); aerr == nil {
			cfg.PersonaPath = absP
		} else {
			cfg.PersonaPath = p
		}
		cfg.PersonaText = strings.TrimSpace(string(data))
	}
	sum := sha256.Sum256([]byte(cfg.PersonaText))
	cfg.PersonaHash = hex.EncodeToString(sum[:])
	return nil
}

var tzLocationCache sync.Map // map[string]*time.Location

func loadCachedLocation(tz string) (*time.Location, error) {
	if tz == "" || tz == "UTC" {
		return time.UTC, nil
	}
	if v, ok := tzLocationCache.Load(tz); ok {
		return v.(*time.Location), nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	tzLocationCache.Store(tz, loc)
	return loc, nil
}

func validateBasic(cfg *Config) error {
	loc, err := loadCachedLocation(cfg.Timezone)
	if err != nil {
		return fmt.Errorf("config: invalid timezone %q: %w", cfg.Timezone, err)
	}
	cfg.loc = loc
	if cfg.Frame.TrimHighWater <= 0 || cfg.Frame.TrimHighWater >= 1 {
		return errors.New("config: frame.trim_high_water must be in (0, 1)")
	}
	if cfg.Frame.TrimLowWater >= cfg.Frame.TrimHighWater {
		return errors.New("config: frame.trim_low_water must be less than trim_high_water")
	}
	if cfg.Sticker.DocCenteringAlpha < 0 || cfg.Sticker.DocCenteringAlpha > 1 {
		return errors.New("config: sticker.doc_centering_alpha must be in [0, 1]")
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Travel.RouteStyle)) {
	case "", "realistic", "wander":
	default:
		return fmt.Errorf("config: invalid travel.route_style %q (use realistic|wander)", cfg.Travel.RouteStyle)
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Travel.JournalModel)) {
	case "", "primary", "fast", "distill":
	default:
		return fmt.Errorf("config: invalid travel.journal_model %q (use primary|fast|distill)", cfg.Travel.JournalModel)
	}
	return nil
}

// EnsureConsoleToken guarantees a 32-byte random hex token persisted in data_dir if none was configured.
func EnsureConsoleToken(cfg *Config) (string, bool, error) {
	if tok := strings.TrimSpace(cfg.Console.Token); tok != "" {
		return tok, false, nil
	}
	tokenPath := filepath.Join(cfg.DataDir, ".console_token")
	if b, err := os.ReadFile(tokenPath); err == nil {
		if tok := strings.TrimSpace(string(b)); len(tok) >= 32 {
			cfg.Console.Token = tok
			return tok, false, nil
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return "", false, err
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", false, err
	}
	tok := hex.EncodeToString(buf)
	if err := os.WriteFile(tokenPath, []byte(tok+"\n"), 0o600); err != nil {
		return "", false, err
	}
	cfg.Console.Token = tok
	return tok, true, nil
}

// Location returns the cached parsed time.Location for cfg.Timezone (falling back to UTC).
func (c *Config) Location() *time.Location {
	if c == nil {
		return time.UTC
	}
	if c.loc != nil {
		return c.loc
	}
	loc, err := loadCachedLocation(c.Timezone)
	if err != nil {
		return time.UTC
	}
	return loc
}

// IsAdmin reports whether userID is in transport.onebot.admin_ids.
func (c *Config) IsAdmin(userID string) bool {
	for _, id := range c.Transport.OneBot.AdminIDs {
		if id == userID {
			return true
		}
	}
	return false
}

// Validator validates a candidate Config before an atomic hot-reload swap.
type Validator func(*Config) error

// Manager provides lock-free atomic reads and safe hot-reloading via fsnotify.
type Manager struct {
	path      string
	cur       atomic.Pointer[Config]
	validator Validator

	reloadMu  sync.Mutex // Serializes Reload / SaveAndReload / SavePersona
	mu        sync.Mutex
	listeners []func(*Config)
}

// NewManager loads the initial config, validates it, and prepares hot-reloading.
func NewManager(path string, validator Validator) (*Manager, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	if validator != nil {
		if err := validator(cfg); err != nil {
			return nil, err
		}
	}
	m := &Manager{
		path:      path,
		validator: validator,
	}
	m.cur.Store(cfg)
	return m, nil
}

// Get returns the active immutable Config snapshot.
func (m *Manager) Get() *Config {
	return m.cur.Load()
}

// OnReload registers a callback invoked after every successful atomic reload.
func (m *Manager) OnReload(fn func(*Config)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, fn)
}

// Reload parses, validates, and atomically swaps the live configuration.
func (m *Manager) Reload() (*Config, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	return m.reloadLocked()
}

func (m *Manager) reloadLocked() (*Config, error) {
	cfg, err := Load(m.path)
	if err != nil {
		return nil, err
	}
	// Preserve auto-generated console token if file omitted token.
	prev := m.cur.Load()
	if strings.TrimSpace(cfg.Console.Token) == "" && prev != nil && prev.Console.Token != "" {
		cfg.Console.Token = prev.Console.Token
	}
	if m.validator != nil {
		if err := m.validator(cfg); err != nil {
			return nil, err
		}
	}
	m.cur.Store(cfg)

	m.mu.Lock()
	ls := append([]func(*Config){}, m.listeners...)
	m.mu.Unlock()
	for _, fn := range ls {
		fn(cfg)
	}
	return cfg, nil
}

// SaveAndReload validates raw YAML bytes, atomically writes to disk, and swaps live config.
func (m *Manager) SaveAndReload(raw []byte) (*Config, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	cfg, err := ParseBytes(raw, filepath.Dir(m.path))
	if err != nil {
		return nil, err
	}
	prev := m.cur.Load()
	if strings.TrimSpace(cfg.Console.Token) == "" && prev != nil && prev.Console.Token != "" {
		cfg.Console.Token = prev.Console.Token
	}
	if m.validator != nil {
		if err := m.validator(cfg); err != nil {
			return nil, err
		}
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return nil, fmt.Errorf("config: write temp file: %w", err)
	}
	if err := os.Rename(tmp, m.path); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("config: atomic rename: %w", err)
	}
	cfg.ConfigPath = m.path
	m.cur.Store(cfg)

	m.mu.Lock()
	ls := append([]func(*Config){}, m.listeners...)
	m.mu.Unlock()
	for _, fn := range ls {
		fn(cfg)
	}
	return cfg, nil
}

// SavePersona atomically updates the active persona markdown file and hot-reloads config (rotating PersonaHash).
func (m *Manager) SavePersona(text string) (*Config, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	cur := m.cur.Load()
	if cur == nil || strings.TrimSpace(cur.PersonaPath) == "" {
		return nil, errors.New("config: persona.file is not configured")
	}
	tmp := cur.PersonaPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(text), 0o600); err != nil {
		return nil, fmt.Errorf("config: write persona temp file: %w", err)
	}
	if err := os.Rename(tmp, cur.PersonaPath); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("config: rename persona file: %w", err)
	}
	return m.reloadLocked()
}

// Watch monitors the config file's parent directory (and persona.file directory if separate) via fsnotify and reloads on edits.
func (m *Manager) Watch(ctx context.Context, onErr func(error)) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	absPath, err := filepath.Abs(m.path)
	if err != nil {
		absPath = m.path
	}
	dir := filepath.Dir(absPath)
	base := filepath.Base(absPath)
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return err
	}
	watchedNames := map[string]bool{base: true}
	if cur := m.cur.Load(); cur != nil && cur.PersonaPath != "" {
		pDir := filepath.Dir(cur.PersonaPath)
		if pDir != dir {
			_ = watcher.Add(pDir)
		}
		watchedNames[filepath.Base(cur.PersonaPath)] = true
	}
	go func() {
		defer watcher.Close()
		var debounce *time.Timer
		for {
			select {
			case <-ctx.Done():
				if debounce != nil {
					debounce.Stop()
				}
				return
			case ev, ok := <-watcher.Events:
				if !ok {
					return
				}
				if !watchedNames[filepath.Base(ev.Name)] {
					continue
				}
				if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
					continue
				}
				if debounce != nil {
					debounce.Stop()
				}
				debounce = time.AfterFunc(200*time.Millisecond, func() {
					if _, err := m.Reload(); err != nil && onErr != nil {
						onErr(err)
					}
				})
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				if onErr != nil {
					onErr(err)
				}
			}
		}
	}()
	return nil
}
