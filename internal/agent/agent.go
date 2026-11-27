package agent

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/tool"
)

const maxToolIterations = 6

var stickerMarkerRe = regexp.MustCompile(`\[\[\s*sticker\s*:\s*([^\]]+?)\s*\]\]`)

// Runner executes a single LLM turn (with optional multi-step tool calling, parallel read-only tool
// execution, KV cache metrics recording, and Text/Media delivery splitting).
type Runner struct {
	hub      *infer.Hub
	store    *store.Store
	frames   *frame.Builder
	registry *tool.Registry
	log      *slog.Logger
}

func NewRunner(hub *infer.Hub, st *store.Store, fb *frame.Builder, reg *tool.Registry, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		hub:      hub,
		store:    st,
		frames:   fb,
		registry: reg,
		log:      logger,
	}
}

// TurnInput describes the inputs for one agent turn inside a Room segment.
type TurnInput struct {
	ChannelID    model.ChannelID
	Epoch        int64
	Segment      *store.SegmentRecord
	PersonaText  string
	UserPayload  frame.StoredFramePayload
	SenderID     string
	SenderName   string
	IsAdmin      bool
	TriggerMsgID string
	TriggerKind  string
}

// TurnStats summarizes inference telemetry across all iterations of a turn.
type TurnStats struct {
	ModelUsed        string
	FallbackUsed     bool
	Iterations       int
	ToolNames        []string
	PromptTokens     int
	CachedTokens     int
	CompletionTokens int
	TotalLatencyMs   int64
}

// TurnOutput returns the text reply, direct media, optional async sticker intent,
// and the newly generated FrozenFrames ready to be persisted after delivery succeeds.
type TurnOutput struct {
	Text          string
	DirectMedia   []model.DirectMedia
	StickerIntent string
	NewFrames     []frame.StoredFramePayload
	Stats         TurnStats
}

// Run executes the agent loop and returns a TurnOutput without mutating the public timeline yet
// (caller persists NewFrames once delivery confirms what was actually sent).
func (r *Runner) Run(ctx context.Context, in TurnInput) (*TurnOutput, error) {
	modelKey := r.hub.ModelKey("primary")
	baseMsgs, prefixHash, estTokens, imgCount, err := r.frames.Assemble(ctx, in.Segment, in.PersonaText, &in.UserPayload, modelKey)
	if err != nil {
		return nil, err
	}

	msgs := append([]infer.ChatMessage(nil), baseMsgs...)
	var defs []infer.ToolDefinition
	if r.registry != nil {
		defs = r.registry.Definitions()
	}

	newFrames := []frame.StoredFramePayload{in.UserPayload}
	var directMedia []model.DirectMedia

	tc := tool.ToolContext{
		ChannelID:     in.ChannelID,
		SenderID:      in.SenderID,
		SenderName:    in.SenderName,
		IsAdmin:       in.IsAdmin,
		TriggerMsgID:  in.TriggerMsgID,
		Epoch:         in.Epoch,
		SegmentID:     in.Segment.ID,
		PokedThisTurn: &atomic.Bool{},
	}

	var finalText string
	var stats TurnStats
	for iter := 1; iter <= maxToolIterations; iter++ {
		req := infer.ChatRequest{
			Messages: msgs,
			Tools:    defs,
		}
		resp, err := r.hub.Chat(ctx, "primary", req)
		if err != nil {
			return nil, err
		}

		stats.Iterations = iter
		if resp.ModelUsed != "" {
			stats.ModelUsed = resp.ModelUsed
		}
		if resp.FallbackUsed {
			stats.FallbackUsed = true
		}
		stats.PromptTokens += resp.Usage.PromptTokens
		stats.CachedTokens += resp.Usage.CachedTokens
		stats.CompletionTokens += resp.Usage.CompletionTokens
		stats.TotalLatencyMs += resp.LatencyMs

		metricModelKey := modelKey
		if resp.FallbackUsed {
			metricModelKey = r.hub.ModelKey("primary_fallback")
		}

		// Record KV telemetry & first-attempt calibrator sample.
		if r.store != nil {
			_ = r.store.RecordKVMetric(ctx, model.KVMetric{
				ChannelID:        in.ChannelID,
				SegmentID:        in.Segment.ID,
				ModelID:          metricModelKey,
				Attempt:          iter,
				PromptTokens:     resp.Usage.PromptTokens,
				CachedTokens:     resp.Usage.CachedTokens,
				CompletionTokens: resp.Usage.CompletionTokens,
				PrefixHash:       prefixHash,
				LatencyMs:        resp.LatencyMs,
				CreatedAt:        time.Now(),
			})
		}
		if iter == 1 && !resp.FallbackUsed && resp.Usage.PromptTokens > 0 {
			r.frames.RecordCalibratorSample(ctx, modelKey, estTokens, resp.Usage.PromptTokens, imgCount)
		}

		// No tool calls -> final answer.
		if len(resp.ToolCalls) == 0 {
			finalText = strings.TrimSpace(resp.Content)
			break
		}

		// Append assistant tool-call frame to both live messages and newFrames.
		msgs = append(msgs, infer.ChatMessage{
			Role:      "assistant",
			Content:   resp.Content,
			ToolCalls: resp.ToolCalls,
		})
		newFrames = append(newFrames, frame.StoredFramePayload{
			Role:      "assistant",
			Text:      resp.Content,
			ToolCalls: resp.ToolCalls,
		})

		// Execute tool calls: parallel via errgroup when all calls in this batch are ReadOnly.
		results := r.executeToolBatch(ctx, tc, resp.ToolCalls)
		for i, call := range resp.ToolCalls {
			stats.ToolNames = append(stats.ToolNames, call.Function.Name)
			res := results[i]
			if len(res.DirectMedia) > 0 {
				directMedia = append(directMedia, res.DirectMedia...)
			}
			outText := res.TextForLLM
			if outText == "" {
				outText = "(ok)"
			}
			msgs = append(msgs, infer.ChatMessage{
				Role:       "tool",
				ToolCallID: call.ID,
				Name:       call.Function.Name,
				Content:    outText,
			})
			newFrames = append(newFrames, frame.StoredFramePayload{
				Role:       "tool",
				ToolCallID: call.ID,
				Name:       call.Function.Name,
				Text:       outText,
			})
		}
	}

	// Extract optional zero-latency async sticker marker [[sticker: ...]].
	cleanText, stickerIntent := ExtractStickerMarker(finalText)

	if cleanText != "" || len(directMedia) > 0 {
		newFrames = append(newFrames, frame.StoredFramePayload{
			Role: "assistant",
			Text: cleanText,
		})
	}

	return &TurnOutput{
		Text:          cleanText,
		DirectMedia:   directMedia,
		StickerIntent: stickerIntent,
		NewFrames:     newFrames,
		Stats:         stats,
	}, nil
}

func (r *Runner) executeToolBatch(ctx context.Context, tc tool.ToolContext, calls []infer.ToolCall) []tool.ToolResult {
	out := make([]tool.ToolResult, len(calls))
	if r.registry == nil {
		for i := range calls {
			out[i] = tool.ToolResult{TextForLLM: "error: tool registry unavailable"}
		}
		return out
	}

	allReadOnly := len(calls) > 1
	for _, c := range calls {
		e, ok := r.registry.Lookup(c.Function.Name)
		if !ok || !e.ReadOnly {
			allReadOnly = false
			break
		}
	}

	if allReadOnly {
		var mu sync.Mutex
		g, gctx := errgroup.WithContext(ctx)
		for idx, call := range calls {
			i, c := idx, call
			g.Go(func() error {
				res, err := r.registry.Execute(gctx, tc, c.Function.Name, c.Function.Arguments)
				if err != nil {
					res = tool.ToolResult{TextForLLM: fmt.Sprintf("tool %s error: %v", c.Function.Name, err)}
				}
				mu.Lock()
				out[i] = res
				mu.Unlock()
				return nil
			})
		}
		_ = g.Wait()
		return out
	}

	for i, c := range calls {
		res, err := r.registry.Execute(ctx, tc, c.Function.Name, c.Function.Arguments)
		if err != nil {
			res = tool.ToolResult{TextForLLM: fmt.Sprintf("tool %s error: %v", c.Function.Name, err)}
		}
		out[i] = res
	}
	return out
}

// ExtractStickerMarker strips optional trailing [[sticker: intent]] markers without extra LLM round-trips.
func ExtractStickerMarker(s string) (clean string, intent string) {
	matches := stickerMarkerRe.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return strings.TrimSpace(s), ""
	}
	intent = strings.TrimSpace(matches[len(matches)-1][1])
	clean = strings.TrimSpace(stickerMarkerRe.ReplaceAllString(s, ""))
	return clean, intent
}
