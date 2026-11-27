package tool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iMMIQ/fatfish/internal/cron"
	"github.com/iMMIQ/fatfish/internal/memory"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/sticker"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/transport"
)

// --- 1. recall ---

type RecallArgs struct {
	Mode   string `json:"mode" jsonschema:"required,enum=recent,enum=raw,enum=memory,description=Retrieval mode: recent (latest raw messages), raw (FTS5 keyword/sender search), memory (L1 episodes + L2 profiles/lore/notes/digests)"`
	Query  string `json:"query,omitempty" jsonschema:"description=Search keywords or semantic question"`
	Sender string `json:"sender,omitempty" jsonschema:"description=Optional member nickname, historical alias, or member ID"`
	Limit  int    `json:"limit,omitempty" jsonschema:"description=Max items to return (default 15, max 50)"`
}

type RecallTool struct {
	mem *memory.Service
}

func NewRecallTool(mem *memory.Service) *RecallTool { return &RecallTool{mem: mem} }
func (t *RecallTool) Name() string                  { return "recall" }
func (t *RecallTool) ReadOnly() bool                { return true }
func (t *RecallTool) Description() string {
	return "Search current channel history, member aliases, episodic memories, profiles, lore, notes, and daily digests."
}

func (t *RecallTool) Execute(ctx context.Context, tc ToolContext, args RecallArgs) (ToolResult, error) {
	res, err := t.mem.Recall(ctx, memory.RecallOptions{
		ChannelID: tc.ChannelID, // Strictly enforced from ToolContext, never LLM args
		Mode:      args.Mode,
		Query:     args.Query,
		Sender:    args.Sender,
		Limit:     args.Limit,
	})
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{TextForLLM: res.Summary}, nil
}

// --- 2. note ---

type NoteArgs struct {
	Action  string `json:"action" jsonschema:"required,enum=save,enum=list,enum=delete,description=Operation to perform"`
	Key     string `json:"key,omitempty" jsonschema:"description=Short unique key for save/delete"`
	Content string `json:"content,omitempty" jsonschema:"description=Note content for save"`
	Pinned  *bool  `json:"pinned,omitempty" jsonschema:"description=Pin into segment memory snapshot (default true)"`
}

type NoteTool struct {
	store *store.Store
}

func NewNoteTool(st *store.Store) *NoteTool { return &NoteTool{store: st} }
func (t *NoteTool) Name() string            { return "note" }
func (t *NoteTool) ReadOnly() bool          { return false }
func (t *NoteTool) Description() string {
	return "Save, list, or delete explicit channel-scoped facts and pinned notes (max 50 per channel)."
}

func (t *NoteTool) Execute(ctx context.Context, tc ToolContext, args NoteArgs) (ToolResult, error) {
	switch strings.ToLower(strings.TrimSpace(args.Action)) {
	case "save":
		if strings.TrimSpace(args.Key) == "" || strings.TrimSpace(args.Content) == "" {
			return ToolResult{}, errors.New("note.save requires key and content")
		}
		pinned := true
		if args.Pinned != nil {
			pinned = *args.Pinned
		}
		err := t.store.SaveNote(ctx, model.Note{
			ChannelID: tc.ChannelID,
			Key:       strings.TrimSpace(args.Key),
			Content:   strings.TrimSpace(args.Content),
			AuthorID:  tc.SenderID,
			Pinned:    pinned,
		}, 50)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: fmt.Sprintf("Saved note %q (pinned=%v).", args.Key, pinned)}, nil

	case "list":
		notes, err := t.store.ListNotes(ctx, tc.ChannelID, false)
		if err != nil {
			return ToolResult{}, err
		}
		if len(notes) == 0 {
			return ToolResult{TextForLLM: "(no notes saved in this channel)"}, nil
		}
		lines := make([]string, 0, len(notes))
		for _, n := range notes {
			tag := ""
			if n.Pinned {
				tag = "[pinned] "
			}
			lines = append(lines, fmt.Sprintf("- %s%s: %s", tag, n.Key, n.Content))
		}
		return ToolResult{TextForLLM: strings.Join(lines, "\n")}, nil

	case "delete":
		if strings.TrimSpace(args.Key) == "" {
			return ToolResult{}, errors.New("note.delete requires key")
		}
		deleted, err := t.store.DeleteNote(ctx, tc.ChannelID, strings.TrimSpace(args.Key))
		if err != nil {
			return ToolResult{}, err
		}
		if !deleted {
			return ToolResult{TextForLLM: fmt.Sprintf("Note %q not found.", args.Key)}, nil
		}
		return ToolResult{TextForLLM: fmt.Sprintf("Deleted note %q.", args.Key)}, nil

	default:
		return ToolResult{}, fmt.Errorf("unsupported note action %q", args.Action)
	}
}

// --- 7. schedule ---

type ScheduleArgs struct {
	Action   string `json:"action" jsonschema:"required,enum=create,enum=list,enum=cancel,description=Action to perform"`
	Kind     string `json:"kind,omitempty" jsonschema:"enum=once,enum=cron,description=Schedule kind for create"`
	RunAt    string `json:"run_at,omitempty" jsonschema:"description=Time for once schedule (RFC3339, YYYY-MM-DD HH:MM, or duration like 30m)"`
	CronExpr string `json:"cron_expr,omitempty" jsonschema:"description=Standard 5-field cron expression (minimum interval 5m)"`
	Prompt   string `json:"prompt,omitempty" jsonschema:"description=Reminder or task prompt to trigger in this channel"`
	ID       int64  `json:"id,omitempty" jsonschema:"description=Schedule ID for cancel"`
}

type ScheduleTool struct {
	sched *cron.Scheduler
	store *store.Store
}

func NewScheduleTool(sc *cron.Scheduler, st *store.Store) *ScheduleTool {
	return &ScheduleTool{sched: sc, store: st}
}

func (t *ScheduleTool) Name() string   { return "schedule" }
func (t *ScheduleTool) ReadOnly() bool { return false }
func (t *ScheduleTool) Description() string {
	return "Create, list, or cancel channel-scoped one-shot or recurring reminders/tasks."
}

func (t *ScheduleTool) Execute(ctx context.Context, tc ToolContext, args ScheduleArgs) (ToolResult, error) {
	switch strings.ToLower(strings.TrimSpace(args.Action)) {
	case "create":
		sc, err := t.sched.CreateSchedule(ctx, tc.ChannelID, tc.SenderID, args.Kind, args.CronExpr, args.RunAt, args.Prompt)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{
			TextForLLM: fmt.Sprintf("Created schedule #%d (%s), next run: %s", sc.ID, sc.Kind, sc.NextRunAt.Format(time.RFC3339)),
		}, nil

	case "list":
		list, err := t.store.ListSchedules(ctx, tc.ChannelID, true)
		if err != nil {
			return ToolResult{}, err
		}
		if len(list) == 0 {
			return ToolResult{TextForLLM: "(no active schedules in this channel)"}, nil
		}
		lines := make([]string, 0, len(list))
		for _, sc := range list {
			lines = append(lines, fmt.Sprintf("#%d [%s %s] next=%s creator=%s prompt=%q",
				sc.ID, sc.Kind, sc.CronExpr, sc.NextRunAt.Format("2006-01-02 15:04"), sc.CreatorID, sc.Prompt))
		}
		return ToolResult{TextForLLM: strings.Join(lines, "\n")}, nil

	case "cancel":
		if args.ID <= 0 {
			return ToolResult{}, errors.New("schedule.cancel requires positive id")
		}
		ok, err := t.store.CancelSchedule(ctx, tc.ChannelID, args.ID, tc.SenderID, tc.IsAdmin)
		if err != nil {
			return ToolResult{}, err
		}
		if !ok {
			return ToolResult{TextForLLM: fmt.Sprintf("Schedule #%d not found or permission denied.", args.ID)}, nil
		}
		return ToolResult{TextForLLM: fmt.Sprintf("Cancelled schedule #%d.", args.ID)}, nil

	default:
		return ToolResult{}, fmt.Errorf("unsupported schedule action %q", args.Action)
	}
}

// --- 8. send_sticker ---

type SendStickerArgs struct {
	Query  string `json:"query" jsonschema:"required,description=Desired sticker mood, caption, or visual description"`
	PackID string `json:"pack_id,omitempty" jsonschema:"description=Optional sticker pack filter"`
}

type SendStickerTool struct {
	eng   *sticker.Engine
	store *store.Store
}

func NewSendStickerTool(eng *sticker.Engine, st *store.Store) *SendStickerTool {
	return &SendStickerTool{eng: eng, store: st}
}

func (t *SendStickerTool) Name() string   { return "send_sticker" }
func (t *SendStickerTool) ReadOnly() bool { return false }
func (t *SendStickerTool) Description() string {
	return "Explicitly search and send a matching sticker image from the indexed sticker pack."
}

func (t *SendStickerTool) Execute(ctx context.Context, tc ToolContext, args SendStickerArgs) (ToolResult, error) {
	if t.eng == nil {
		return ToolResult{}, errors.New("sticker engine is disabled")
	}
	hit, err := t.eng.PickOne(ctx, sticker.SearchOptions{
		ChannelID:      tc.ChannelID,
		Query:          args.Query,
		PackID:         args.PackID,
		ExplicitLookup: true,
	})
	if err != nil {
		return ToolResult{}, err
	}
	if hit == nil {
		return ToolResult{TextForLLM: "No matching sticker found."}, nil
	}
	raw, err := t.eng.ReadStickerFile(hit.Sticker)
	if err != nil {
		return ToolResult{}, err
	}
	_ = t.store.RecordStickerUse(ctx, tc.ChannelID, hit.Sticker.StickerID)
	return ToolResult{
		TextForLLM: fmt.Sprintf("Selected sticker %s (%s).", hit.Sticker.StickerID, hit.Sticker.ContentText),
		DirectMedia: []model.DirectMedia{{
			Data:     raw,
			MimeType: "image/png",
			Filename: hit.Sticker.StickerID + ".png",
		}},
	}, nil
}

// --- 9. send_media ---

type SendMediaArgs struct {
	MediaID string `json:"media_id" jsonschema:"required,description=Short channel-scoped media_id (e.g. m_0042) from this channel's history"`
}

type SendMediaTool struct {
	store *store.Store
}

func NewSendMediaTool(st *store.Store) *SendMediaTool { return &SendMediaTool{store: st} }
func (t *SendMediaTool) Name() string                 { return "send_media" }
func (t *SendMediaTool) ReadOnly() bool               { return false }
func (t *SendMediaTool) Description() string {
	return "Re-send a previously visible image from the current channel using its short media_id."
}

func (t *SendMediaTool) Execute(ctx context.Context, tc ToolContext, args SendMediaArgs) (ToolResult, error) {
	// Strictly enforce (channel_id, media_id) ownership at the DB layer!
	asset, err := t.store.GetMediaAsset(ctx, tc.ChannelID, strings.TrimSpace(args.MediaID))
	if err != nil {
		return ToolResult{}, fmt.Errorf("media_id %q not found in current channel", args.MediaID)
	}
	raw, err := t.store.Blobs().ReadBytes(asset.SHA256, asset.MimeType)
	if err != nil {
		return ToolResult{}, err
	}
	return ToolResult{
		TextForLLM: fmt.Sprintf("Queued image %s for direct delivery.", asset.MediaID),
		DirectMedia: []model.DirectMedia{{
			Data:     raw,
			MimeType: asset.MimeType,
			MediaID:  asset.MediaID,
			SHA256:   asset.SHA256,
		}},
	}, nil
}

// --- 10. interact ---

type InteractArgs struct {
	Action        string `json:"action" jsonschema:"required,enum=poke,enum=react,enum=relay,description=Social interaction action"`
	EmojiID       string `json:"emoji_id,omitempty" jsonschema:"description=Platform emoji ID for react"`
	TargetChannel string `json:"target_channel,omitempty" jsonschema:"description=Target canonical channel_id for relay (admin + allowlist only)"`
	Text          string `json:"text,omitempty" jsonschema:"description=Message text for relay"`
}

type InteractTool struct {
	tr            transport.Transport
	allowRelays   map[string]struct{}
}

func NewInteractTool(tr transport.Transport, allowRelayChannels []string) *InteractTool {
	allow := make(map[string]struct{}, len(allowRelayChannels))
	for _, c := range allowRelayChannels {
		allow[strings.TrimSpace(c)] = struct{}{}
	}
	return &InteractTool{tr: tr, allowRelays: allow}
}

func (t *InteractTool) Name() string   { return "interact" }
func (t *InteractTool) ReadOnly() bool { return false }
func (t *InteractTool) Description() string {
	return "Perform social interactions: poke the triggering sender, react with an emoji on the trigger message, or relay (admin + allowlist only)."
}

func (t *InteractTool) Execute(ctx context.Context, tc ToolContext, args InteractArgs) (ToolResult, error) {
	if t.tr == nil {
		return ToolResult{}, errors.New("transport unavailable")
	}
	switch strings.ToLower(strings.TrimSpace(args.Action)) {
	case "poke":
		pokeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := t.tr.SendPoke(pokeCtx, tc.ChannelID, tc.SenderID); err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: "Poked user.", SideEffect: "poke"}, nil

	case "react":
		if tc.TriggerMsgID == "" || strings.TrimSpace(args.EmojiID) == "" {
			return ToolResult{}, errors.New("interact.react requires emoji_id and a trigger message")
		}
		if err := t.tr.SendReaction(ctx, tc.ChannelID, tc.TriggerMsgID, strings.TrimSpace(args.EmojiID)); err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: "Reacted with emoji " + args.EmojiID, SideEffect: "react"}, nil

	case "relay":
		// Server-side authorization: disabled by default for non-admins + explicit allowlist required.
		if !tc.IsAdmin {
			return ToolResult{}, errors.New("interact.relay is restricted to administrators")
		}
		target := strings.TrimSpace(args.TargetChannel)
		if _, ok := t.allowRelays[target]; !ok {
			return ToolResult{}, fmt.Errorf("interact.relay target %q is not in allow_relay_channels", target)
		}
		if strings.TrimSpace(args.Text) == "" {
			return ToolResult{}, errors.New("interact.relay requires text")
		}
		if _, err := t.tr.SendText(ctx, model.ChannelID(target), args.Text, transport.SendOptions{}); err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: "Relayed message to " + target, SideEffect: "relay"}, nil

	default:
		return ToolResult{}, fmt.Errorf("unsupported interact action %q", args.Action)
	}
}
