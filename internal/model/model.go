package model

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// ChannelKind distinguishes group chats from direct messages.
type ChannelKind string

const (
	ChannelGroup ChannelKind = "group"
	ChannelDM    ChannelKind = "dm"
)

// ChannelID is canonical: "<platform>:<bot_id>:<kind>:<native_id>"
// e.g. "onebot:100000001:group:123456789"
type ChannelID string

// ParseChannelID parses a canonical channel ID into its components.
func ParseChannelID(id ChannelID) (platform, botID string, kind ChannelKind, nativeID string, ok bool) {
	parts := strings.SplitN(string(id), ":", 4)
	if len(parts) != 4 {
		return "", "", "", "", false
	}
	return parts[0], parts[1], ChannelKind(parts[2]), parts[3], true
}

// MakeChannelID builds a canonical ChannelID.
func MakeChannelID(platform, botID string, kind ChannelKind, nativeID string) ChannelID {
	return ChannelID(platform + ":" + botID + ":" + string(kind) + ":" + nativeID)
}

// IsGroup returns true when the channel represents a group conversation.
func (c ChannelID) IsGroup() bool {
	_, _, kind, _, ok := ParseChannelID(c)
	return ok && kind == ChannelGroup
}

// NativeID returns the underlying platform group or user ID.
func (c ChannelID) NativeID() string {
	_, _, _, nativeID, _ := ParseChannelID(c)
	return nativeID
}

// Member represents a channel-scoped immutable participant identity.
type Member struct {
	ChannelID   ChannelID `json:"channel_id"`
	MemberID    string    `json:"member_id"`
	DisplayName string    `json:"display_name"`
	Role        string    `json:"role"` // "member" | "admin" | "owner" | "bot"
	UpdatedAt   time.Time `json:"updated_at"`
}

// PartKind identifies the normalized content part type.
type PartKind string

const (
	PartText    PartKind = "text"
	PartImage   PartKind = "image"
	PartAudio   PartKind = "audio"
	PartCard    PartKind = "card"
	PartFile    PartKind = "file"
	PartVideo   PartKind = "video"
	PartPoke    PartKind = "poke"
	PartQuote   PartKind = "quote"
	PartForward PartKind = "forward"
	PartMention PartKind = "mention"
	PartNotice  PartKind = "notice"
	PartLink    PartKind = "link"
)

// Part represents a single normalized content segment in a Message.
type Part struct {
	Kind       PartKind `json:"kind"`
	Text       string   `json:"text,omitempty"`
	MediaID    string   `json:"media_id,omitempty"`
	SHA256     string   `json:"sha256,omitempty"`
	MimeType   string   `json:"mime_type,omitempty"`
	Width      int      `json:"width,omitempty"`
	Height     int      `json:"height,omitempty"`
	DurationMs int      `json:"duration_ms,omitempty"`
	Transcript string   `json:"transcript,omitempty"`
	ASRStatus  string   `json:"asr_status,omitempty"` // "" | "pending" | "ok" | "failed"
	Title      string   `json:"title,omitempty"`
	Summary    string   `json:"summary,omitempty"`
	URL        string   `json:"url,omitempty"`
	FileID     string   `json:"file_id,omitempty"` // Platform file handle (e.g. OneBot record/file)
	FileSize   int64    `json:"file_size,omitempty"`
	RefMsgID   string   `json:"ref_msg_id,omitempty"`
	TargetID   string   `json:"target_id,omitempty"`
	Nodes      []Part   `json:"nodes,omitempty"`
}

// Message is the immutable normalized event persisted in L0 and passed to Rooms.
type Message struct {
	ID            int64     `json:"id,omitempty"`
	Seq           int64     `json:"seq"`
	ChannelID     ChannelID `json:"channel_id"`
	PlatformMsgID string    `json:"platform_msg_id,omitempty"`
	SenderID      string    `json:"sender_id"`
	SenderName    string    `json:"sender_name"`
	SenderRole    string    `json:"sender_role,omitempty"`
	FromBot       bool      `json:"from_bot"`
	MentionedBot  bool      `json:"mentioned_bot,omitempty"`
	RepliedToBot  bool      `json:"replied_to_bot,omitempty"`
	PokedBot      bool      `json:"poked_bot,omitempty"`
	SilentNotice  bool      `json:"silent_notice,omitempty"`
	ReplyToMsgID  string    `json:"reply_to_msg_id,omitempty"`
	Parts         []Part    `json:"parts"`
	CreatedAt     time.Time `json:"created_at"`
}

// PlainText concatenates searchable text across all parts (text, card titles, transcripts, files).
func (m Message) PlainText() string {
	var b strings.Builder
	appendPart := func(p Part) {
		switch p.Kind {
		case PartText:
			if t := strings.TrimSpace(p.Text); t != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(t)
			}
		case PartAudio:
			if t := strings.TrimSpace(p.Transcript); t != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(t)
			}
		case PartCard:
			if p.Title != "" || p.Summary != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(strings.TrimSpace(p.Title + " " + p.Summary))
			}
		case PartLink:
			txt := strings.TrimSpace(p.Text)
			if txt == "" {
				txt = strings.TrimSpace(p.Title + " " + p.Summary)
			}
			if txt != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(txt)
			}
		case PartFile:
			if p.Title != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(p.Title)
			}
		case PartPoke, PartMention, PartNotice:
			if p.Text != "" {
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString(p.Text)
			}
		}
	}
	for _, p := range m.Parts {
		appendPart(p)
		for _, n := range p.Nodes {
			appendPart(n)
		}
	}
	return b.String()
}

// TextOnly concatenates user-authored text (text, audio transcript, card title/summary),
// excluding synthetic mention/poke/notice labels so nickname matching never misfires on @OtherBot(id).
func (m Message) TextOnly() string {
	var b strings.Builder
	for _, p := range m.Parts {
		var s string
		switch p.Kind {
		case PartText:
			s = strings.TrimSpace(p.Text)
		case PartAudio:
			s = strings.TrimSpace(p.Transcript)
		case PartCard:
			s = strings.TrimSpace(p.Title + " " + p.Summary)
		}
		if s != "" {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(s)
		}
	}
	return b.String()
}

// MentionedOther reports whether the message explicitly @-mentions someone other than the bot
// without also mentioning the bot itself.
func (m Message) MentionedOther() bool {
	if m.MentionedBot {
		return false
	}
	for _, p := range m.Parts {
		if p.Kind == PartMention && p.TargetID != "" {
			return true
		}
	}
	return false
}

// RepliedToOther reports whether the message quotes/replies to a message not sent by the bot
// (without explicitly @-mentioning the bot).
func (m Message) RepliedToOther() bool {
	return m.ReplyToMsgID != "" && !m.RepliedToBot && !m.MentionedBot
}

// DedupHash computes a stable SHA-256 hex digest of plain text for idempotent ingest.
func DedupHash(plainText string) string {
	sum := sha256.Sum256([]byte(plainText))
	return hex.EncodeToString(sum[:])
}

// DeliveryStatus tracks the lifecycle of an outbound TurnPlan.
type DeliveryStatus string

const (
	DeliveryPlanned      DeliveryStatus = "planned"
	DeliverySending      DeliveryStatus = "sending"
	DeliverySent         DeliveryStatus = "sent"
	DeliveryPartial      DeliveryStatus = "partial"
	DeliveryAbortedEpoch DeliveryStatus = "aborted_epoch"
	DeliveryAbortedRisk  DeliveryStatus = "aborted_risk"
	DeliveryFailed       DeliveryStatus = "failed"
)

// DirectMedia is a binary asset generated by a tool (e.g. PNG chart) to send directly.
type DirectMedia struct {
	Data     []byte `json:"-"`
	MimeType string `json:"mime_type"`
	Filename string `json:"filename,omitempty"`
	Caption  string `json:"caption,omitempty"`
	MediaID  string `json:"media_id,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
}

// TurnPlan represents the model's planned output prior to delivery.
type TurnPlan struct {
	ChannelID      ChannelID     `json:"channel_id"`
	Epoch          int64         `json:"epoch"`
	SegmentID      int64         `json:"segment_id"`
	TriggerMsgID   string        `json:"trigger_msg_id,omitempty"`
	TriggerUserID  string        `json:"trigger_user_id,omitempty"`
	TriggerKind    string        `json:"trigger_kind,omitempty"` // "mention" | "reply" | "poke" | "thread" | "proactive" | "cron" | "dm"
	Advisory       bool          `json:"advisory,omitempty"`
	RawText        string        `json:"raw_text"`
	Bubbles        []string      `json:"bubbles"`
	ForwardLead    string        `json:"forward_lead,omitempty"`
	ForwardNodes   []string      `json:"forward_nodes,omitempty"`
	DirectMedia    []DirectMedia `json:"direct_media,omitempty"`
	StickerIntent  string        `json:"sticker_intent,omitempty"`
	ReplyToMsgID   string        `json:"reply_to_msg_id,omitempty"`
	MentionUserID  string        `json:"mention_user_id,omitempty"`
	PokeAfterReply bool          `json:"poke_after_reply,omitempty"`
}

// SentItem records an individual delivered bubble or media item in the ledger.
type SentItem struct {
	Kind          string `json:"kind"` // "text" | "image" | "sticker" | "poke" | "react"
	PlatformMsgID string `json:"platform_msg_id,omitempty"`
	Text          string `json:"text,omitempty"`
	MediaID       string `json:"media_id,omitempty"`
	StickerID     string `json:"sticker_id,omitempty"`
	SentAt        int64  `json:"sent_at"`
}

// SystemTriggerEvent is posted by internal/cron into a Room Actor's mailbox.
type SystemTriggerEvent struct {
	ChannelID  ChannelID `json:"channel_id"`
	ScheduleID int64     `json:"schedule_id,omitempty"`
	Kind       string    `json:"kind"` // "schedule" | "daily_digest" | "admin"
	Prompt     string    `json:"prompt"`
	CreatorID  string    `json:"creator_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// InboundEvent wraps messages, system triggers, or admin commands delivered to a Room Actor.
type InboundEvent struct {
	Message *Message
	System  *SystemTriggerEvent
	Command *AdminCommand
}

// AdminCommand represents an in-channel or console administrative directive.
type AdminCommand struct {
	ChannelID ChannelID
	SenderID  string
	Name      string // "reset" | "tr" | "status"
	Args      string
	Reply     chan string
}

// MediaAsset describes a canonicalized channel-scoped image stored in BlobStore.
type MediaAsset struct {
	ChannelID ChannelID `json:"channel_id"`
	MediaID   string    `json:"media_id"`
	SHA256    string    `json:"sha256"`
	MimeType  string    `json:"mime_type"`
	ByteSize  int64     `json:"byte_size"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	SenderID  string    `json:"sender_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Episode represents an L1 summarized conversational episode.
type Episode struct {
	ID           int64                `json:"id"`
	ChannelID    ChannelID            `json:"channel_id"`
	StartSeq     int64                `json:"start_seq"`
	EndSeq       int64                `json:"end_seq"`
	StartTS      time.Time            `json:"start_ts"`
	EndTS        time.Time            `json:"end_ts"`
	Topic        string               `json:"topic"`
	Summary      string               `json:"summary"`
	Participants []EpisodeParticipant `json:"participants"`
	Notable      []string             `json:"notable"`
	CreatedAt    time.Time            `json:"created_at"`
}

// EpisodeParticipant links an immutable member ID to their role in an episode.
type EpisodeParticipant struct {
	MemberID string `json:"id"`
	Role     string `json:"role_in_topic"`
}

// ProfileFact is an corroborated biographical or preference fact scoped to (channel_id, member_id).
type ProfileFact struct {
	Text             string  `json:"text"`
	Confidence       float64 `json:"confidence"`
	EvidenceEpisodes []int64 `json:"evidence_episodes"`
	Tentative        bool    `json:"tentative,omitempty"`
	UpdatedAt        int64   `json:"updated_at"`
}

// ProfileJoke keeps jokes/memes separated from factual bio attributes.
type ProfileJoke struct {
	Text      string `json:"text"`
	EpisodeID int64  `json:"episode_id,omitempty"`
	UpdatedAt int64  `json:"updated_at"`
}

// MemberProfile holds channel-scoped L2 facts, jokes, and interaction style for one member.
type MemberProfile struct {
	ChannelID ChannelID     `json:"channel_id"`
	MemberID  string        `json:"member_id"`
	Facts     []ProfileFact `json:"facts"`
	Jokes     []ProfileJoke `json:"jokes"`
	StyleNote string        `json:"style_note,omitempty"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// LoreEntry represents channel-specific slang, memes, or recurring references.
type LoreEntry struct {
	ID         int64     `json:"id"`
	ChannelID  ChannelID `json:"channel_id"`
	Term       string    `json:"term"`
	Meaning    string    `json:"meaning"`
	Confidence float64   `json:"confidence"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Note represents an explicit pinned or unpinned channel-scoped fact/note.
type Note struct {
	ID        int64     `json:"id"`
	ChannelID ChannelID `json:"channel_id"`
	Key       string    `json:"key"`
	Content   string    `json:"content"`
	AuthorID  string    `json:"author_id"`
	Pinned    bool      `json:"pinned"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Sticker represents a dual-vector indexed sticker asset.
type Sticker struct {
	StickerID   string    `json:"sticker_id"`
	PackID      string    `json:"pack_id"`
	SHA256      string    `json:"sha256"`
	RelPath     string    `json:"rel_path"`
	Status      string    `json:"status"` // ready | needs_review | disabled
	OCRText     string    `json:"ocr_text"`
	IntentText  string    `json:"intent_text"`
	ContentText string    `json:"content_text"`
	CreatedAt   time.Time `json:"created_at"`
}

// Schedule represents a persistent channel-scoped one-shot or cron task.
type Schedule struct {
	ID             int64     `json:"id"`
	ChannelID      ChannelID `json:"channel_id"`
	CreatorID      string    `json:"creator_id"`
	Kind           string    `json:"kind"` // "once" | "cron"
	CronExpr       string    `json:"cron_expr"`
	MarketDaysOnly string    `json:"market_days_only,omitempty"` // "" | "cn" | "hk" | "us"
	NextRunAt      time.Time `json:"next_run_at"`
	Prompt         string    `json:"prompt"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
}

// PriceAlert represents a zero-LLM channel-scoped market price or daily-change alert.
type PriceAlert struct {
	ID              int64     `json:"id"`
	ChannelID       ChannelID `json:"channel_id"`
	CreatorID       string    `json:"creator_id"`
	Symbol          string    `json:"symbol"`
	DisplayName     string    `json:"display_name"`
	Market          string    `json:"market"` // "cn" | "hk" | "us" | "crypto" | "fx" | "fund"
	Condition       string    `json:"condition"` // "above" | "below" | "pct_up" | "pct_down" | "pct_abs"
	Threshold       float64   `json:"threshold"`
	CooldownMin     int       `json:"cooldown_min"` // 0 = one-shot auto-disable after firing
	LastTriggeredAt time.Time `json:"last_triggered_at,omitempty"`
	Prompt          string    `json:"prompt"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
}

// DailyDigest stores a channel's daily activity summary.
type DailyDigest struct {
	ChannelID ChannelID `json:"channel_id"`
	DayKey    string    `json:"day_key"`
	Report    string    `json:"report"`
	StatsJSON string    `json:"stats_json"`
	CreatedAt time.Time `json:"created_at"`
}

// KVMetric records token usage and KV prefix cache telemetry for one LLM call.
type KVMetric struct {
	ID               int64     `json:"id"`
	ChannelID        ChannelID `json:"channel_id"`
	SegmentID        int64     `json:"segment_id"`
	ModelID          string    `json:"model_id"`
	Attempt          int       `json:"attempt"`
	PromptTokens     int       `json:"prompt_tokens"`
	CachedTokens     int       `json:"cached_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	PrefixHash       string    `json:"prefix_hash"`
	MissReason       string    `json:"miss_reason"`
	LatencyMs        int64     `json:"latency_ms"`
	CreatedAt        time.Time `json:"created_at"`
}

// SnapshotInput provides the channel-scoped memory components used ONCE when opening a new Frame segment.
// Lives in internal/model so internal/memory does not need to import internal/frame (SPEC §2 DAG).
type SnapshotInput struct {
	SessionTime      string // Local date/weekday/period anchor frozen at segment start
	TravelStatus     string // Optional global bot travel state + local clock + weather + recent micro-diary
	Lore             []LoreEntry
	PinnedNotes      []Note
	PreviousSummary  string
	RecentHighlights []string // Recent 48h high-signal episodes / digest snippets
	MemberCards      []string // Pre-formatted active member summaries
}

// TravelCity defines a real-world destination in the built-in global atlas.
type TravelCity struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	NameZH      string   `json:"name_zh"`
	Country     string   `json:"country"`
	CountryZH   string   `json:"country_zh"`
	Region      string   `json:"region"` // "east_asia" | "se_asia" | "south_asia" | "middle_east" | "europe" | "north_america" | "south_america" | "africa" | "oceania" | "polar"
	Lat         float64  `json:"lat"`
	Lon         float64  `json:"lon"`
	Timezone    string   `json:"timezone"` // IANA timezone e.g. "Asia/Tokyo"
	Currency    string   `json:"currency"` // ISO 4217 e.g. "JPY"
	AirportCode string   `json:"airport_code"`
	Tags        []string `json:"tags"`
	Vibe        string   `json:"vibe"`
}

// TravelState persists the bot's global, cross-channel consistent world-travel state machine.
type TravelState struct {
	Status           string    `json:"status"` // "staying" | "in_transit"
	CurrentCityID    string    `json:"current_city_id"`
	NextCityID       string    `json:"next_city_id,omitempty"`
	TransportMode    string    `json:"transport_mode,omitempty"` // "flight" | "train" | "ferry"
	ArrivedAt        time.Time `json:"arrived_at"`
	DepartAt         time.Time `json:"depart_at"`
	TransitETA       time.Time `json:"transit_eta,omitempty"`
	WeatherSummary   string    `json:"weather_summary,omitempty"`
	WeatherCode      int       `json:"weather_code"`
	WeatherTempC     float64   `json:"weather_temp_c"`
	WeatherUpdatedAt time.Time `json:"weather_updated_at,omitempty"`
	LastJournalAt    time.Time `json:"last_journal_at,omitempty"`
	LastProactiveAt  time.Time `json:"last_proactive_at,omitempty"`
	LegCount         int       `json:"leg_count"`
	TotalDistanceKm  float64   `json:"total_distance_km"`
	StartedAt        time.Time `json:"started_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// TravelJournalEntry records a short, persona-voiced micro-diary entry along the journey.
type TravelJournalEntry struct {
	ID        int64     `json:"id"`
	CityID    string    `json:"city_id"`
	CityName  string    `json:"city_name"`
	Country   string    `json:"country"`
	Status    string    `json:"status"` // "staying" | "in_transit"
	Weather   string    `json:"weather,omitempty"`
	LocalTime string    `json:"local_time,omitempty"`
	Kind      string    `json:"kind"` // "diary" | "arrival" | "departure" | "extreme_weather"
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// TravelVisit records a visited city leg in chronological order for footprint maps & history.
type TravelVisit struct {
	ID            int64     `json:"id"`
	CityID        string    `json:"city_id"`
	CityName      string    `json:"city_name"`
	Country       string    `json:"country"`
	Lat           float64   `json:"lat"`
	Lon           float64   `json:"lon"`
	TransportMode string    `json:"transport_mode,omitempty"`
	DistanceKm    float64   `json:"distance_km"`
	ArrivedAt     time.Time `json:"arrived_at"`
	DepartedAt    time.Time `json:"departed_at,omitempty"`
}

// TravelSuggestion records a channel-scoped member suggestion for the next travel destination.
// Attribution (MemberID/MemberName) is strictly scoped to ChannelID (never leaked across channels).
type TravelSuggestion struct {
	ID         int64     `json:"id"`
	ChannelID  ChannelID `json:"channel_id"`
	MemberID   string    `json:"member_id"`
	MemberName string    `json:"member_name"`
	CityID     string    `json:"city_id"`
	CityName   string    `json:"city_name"`
	Note       string    `json:"note,omitempty"`
	Status     string    `json:"status"` // "pending" | "selected" | "visited" | "expired"
	CreatedAt  time.Time `json:"created_at"`
	VisitedAt  time.Time `json:"visited_at,omitempty"`
}
