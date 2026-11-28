package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

var (
	stickerMarkerRe       = regexp.MustCompile(`(?i)\[\[\s*sticker\s*[:：]\s*([^\]]+?)\s*\]\]`)
	singleStickerMarkerRe = regexp.MustCompile(`(?i)\[\s*sticker\s*[:：]\s*([^\[\]\n]{1,40}?)\s*\]`)
	skipMarkerRe          = regexp.MustCompile(`(?i)\[\[\s*skip\s*\]\]`)
)

// Runner executes a single LLM turn (with optional multi-step tool calling, parallel read-only tool
// execution, KV cache metrics recording, and Text/Media delivery splitting).
type Runner struct {
	hub      *infer.Hub
	store    *store.Store
	frames   *frame.Builder
	registry *tool.Registry
	log      *slog.Logger

	toolTokMu      sync.RWMutex
	cachedToolHash string
	cachedToolTok  int

	teleMu     sync.Mutex
	teleCh     chan func()
	teleWG     sync.WaitGroup
	teleClosed bool
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

// EnableAsyncTelemetry moves non-critical KV metric & calibrator SQLite writes onto a bounded
// single-worker background queue so turn delivery and multi-step tool loops never stall on writeDB.
func (r *Runner) EnableAsyncTelemetry() {
	r.teleMu.Lock()
	defer r.teleMu.Unlock()
	if r.teleCh != nil || r.teleClosed {
		return
	}
	r.teleCh = make(chan func(), 128)
	r.teleWG.Add(1)
	go func(ch <-chan func()) {
		defer r.teleWG.Done()
		for fn := range ch {
			fn()
		}
	}(r.teleCh)
}

// Drain flushes queued async telemetry writes before closing SQLite pools.
func (r *Runner) Drain(timeout time.Duration) {
	r.teleMu.Lock()
	if r.teleClosed || r.teleCh == nil {
		r.teleClosed = true
		r.teleMu.Unlock()
		return
	}
	r.teleClosed = true
	close(r.teleCh)
	r.teleMu.Unlock()

	done := make(chan struct{})
	go func() {
		r.teleWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		r.log.Warn("agent telemetry drain timed out", "timeout", timeout)
	}
}

func (r *Runner) enqueueTelemetry(fn func()) {
	r.teleMu.Lock()
	ch := r.teleCh
	closed := r.teleClosed
	r.teleMu.Unlock()
	if ch == nil || closed {
		fn()
		return
	}
	select {
	case ch <- fn:
	default:
		// Drop telemetry rather than blocking user turn if queue is saturated.
	}
}

func (r *Runner) cachedToolTokens() int {
	if r.registry == nil {
		return 0
	}
	h := r.registry.Hash()
	r.toolTokMu.RLock()
	if r.cachedToolHash == h && h != "" {
		tok := r.cachedToolTok
		r.toolTokMu.RUnlock()
		return tok
	}
	r.toolTokMu.RUnlock()

	tok := frame.EstimateTextTokens(string(r.registry.FrozenJSON()))
	r.toolTokMu.Lock()
	r.cachedToolHash = h
	r.cachedToolTok = tok
	r.toolTokMu.Unlock()
	return tok
}

// TurnInput describes the inputs for one agent turn inside a Room segment.
type TurnInput struct {
	ChannelID         model.ChannelID
	Epoch             int64
	Segment           *store.SegmentRecord
	PersonaText       string
	UserPayload       frame.StoredFramePayload
	SenderID          string
	SenderName        string
	IsAdmin           bool
	TriggerMsgID      string
	TriggerKind       string
	AdvisoryMode      bool
	ReasoningEffort   string
	Temperature       *float64
	PresencePenalty   *float64
	MaxToolIterations int
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
// deferred frame trim index, and newly generated FrozenFrames ready to be persisted after delivery succeeds.
type TurnOutput struct {
	Text                   string
	DirectMedia            []model.DirectMedia
	StickerIntent          string
	Skipped                bool
	ContentFilterRecovered bool
	NewFrames              []frame.StoredFramePayload
	TrimUpToIndex          int // >= 0 if AssemblePlan deferred a whole-turn trim; -1 otherwise
	Stats                  TurnStats
}

// Run executes the agent loop and returns a TurnOutput without mutating the public timeline yet
// (caller persists NewFrames and commits deferred TrimUpToIndex once delivery confirms what was actually sent).
func (r *Runner) Run(ctx context.Context, in TurnInput) (*TurnOutput, error) {
	var defs []infer.ToolDefinition
	toolTokens := 0
	if r.registry != nil {
		defs = r.registry.Definitions()
		toolTokens = r.cachedToolTokens()
	}

	modelKey := r.hub.ModelKey("primary")
	assembled, err := r.frames.AssemblePlan(ctx, in.Segment, in.PersonaText, &in.UserPayload, modelKey, toolTokens)
	if err != nil {
		return nil, err
	}

	msgs := append([]infer.ChatMessage(nil), assembled.Messages...)
	userFrame := in.UserPayload
	if assembled.EffectiveUserPayload != nil {
		userFrame = *assembled.EffectiveUserPayload
	}
	newFrames := []frame.StoredFramePayload{userFrame}
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
	exhaustedTools := false
	contentFilterRecovered := false
	maxIter := maxToolIterations
	if in.MaxToolIterations > 0 {
		maxIter = in.MaxToolIterations
	}
	buildReq := func(m []infer.ChatMessage, tools []infer.ToolDefinition) infer.ChatRequest {
		req := infer.ChatRequest{
			Messages: m,
			Tools:    tools,
		}
		if strings.TrimSpace(in.ReasoningEffort) != "" {
			req.ReasoningEffort = strings.TrimSpace(in.ReasoningEffort)
		}
		if in.Temperature != nil {
			req.Temperature = in.Temperature
		}
		if in.PresencePenalty != nil {
			req.PresencePenalty = in.PresencePenalty
		}
		return req
	}
	for iter := 1; iter <= maxIter; iter++ {
		req := buildReq(msgs, defs)
		iterPrefixHash := assembled.PrefixHash
		if iter > 1 && len(newFrames) > 1 {
			h := sha256.New()
			_, _ = h.Write([]byte(assembled.PrefixHash))
			for _, nf := range newFrames[:len(newFrames)-1] {
				_, _ = h.Write([]byte{0})
				pb, _ := json.Marshal(nf)
				_, _ = h.Write(pb)
			}
			iterPrefixHash = hex.EncodeToString(h.Sum(nil))
		}
		resp, err := r.hub.Chat(ctx, "primary", req)
		if err != nil {
			// Self-heal poisoned segment history / pinned snapshot when upstream rejects with content filter (e.g. 1301):
			// retry once with only immutable system prompt + current turn so a single dirty history segment cannot brick a channel.
			if iter == 1 && infer.IsContentFiltered(err) && len(msgs) > 2 {
				r.log.Warn("content filter triggered on segment context, retrying without pinned history", "channel", in.ChannelID, "segment", in.Segment.ID)
				safeMsgs := []infer.ChatMessage{msgs[0], msgs[len(msgs)-1]}
				safeResp, safeErr := r.hub.Chat(ctx, "primary", buildReq(safeMsgs, defs))
				if safeErr == nil {
					msgs = safeMsgs
					resp = safeResp
					err = nil
					contentFilterRecovered = true
				}
			}
			if err != nil {
				return nil, err
			}
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
		missReason := ""
		if resp.Usage.PromptTokens > 0 && resp.Usage.CachedTokens == 0 {
			switch {
			case resp.FallbackUsed:
				missReason = "fallback"
			case !assembled.HadHistory && iter == 1:
				missReason = "new_segment"
			case assembled.Trimmed && iter == 1:
				missReason = "trimmed"
			default:
				missReason = "cold"
			}
		}

		// Record KV telemetry & first-attempt calibrator sample off the hot turn path when async enabled.
		metric := model.KVMetric{
			ChannelID:        in.ChannelID,
			SegmentID:        in.Segment.ID,
			ModelID:          metricModelKey,
			Attempt:          iter,
			PromptTokens:     resp.Usage.PromptTokens,
			CachedTokens:     resp.Usage.CachedTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			PrefixHash:       iterPrefixHash,
			MissReason:       missReason,
			LatencyMs:        resp.LatencyMs,
			CreatedAt:        time.Now(),
		}
		recordCal := iter == 1 && !resp.FallbackUsed && resp.Usage.PromptTokens > 0
		estTextTok := assembled.EstTextTokens
		actualPromptTok := resp.Usage.PromptTokens
		imgCnt := assembled.ImageCount
		if r.store != nil || recordCal {
			r.enqueueTelemetry(func() {
				tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if r.store != nil {
					_ = r.store.RecordKVMetric(tctx, metric)
				}
				if recordCal && r.frames != nil {
					r.frames.RecordCalibratorSample(tctx, modelKey, estTextTok, actualPromptTok, imgCnt)
				}
			})
		}

		// No tool calls -> final answer.
		if len(resp.ToolCalls) == 0 {
			finalText = strings.TrimSpace(resp.Content)
			break
		}

		// Append assistant tool-call frame (preserving reasoning_content in live messages for provider continuity).
		msgs = append(msgs, infer.ChatMessage{
			Role:             "assistant",
			Content:          resp.Content,
			ReasoningContent: resp.ReasoningContent,
			ToolCalls:        resp.ToolCalls,
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
			if res.SideEffect != "" {
				r.log.Info("tool side effect", "channel", in.ChannelID, "tool", call.Function.Name, "effect", res.SideEffect)
			}
			outText := res.TextForLLM
			if len(res.DirectMedia) > 0 {
				directMedia = append(directMedia, res.DirectMedia...)
				desc := make([]string, 0, len(res.DirectMedia))
				for _, dm := range res.DirectMedia {
					lbl := dm.Filename
					if dm.Caption != "" {
						if lbl != "" {
							lbl += " (" + dm.Caption + ")"
						} else {
							lbl = dm.Caption
						}
					}
					if lbl == "" {
						lbl = dm.MimeType
					}
					desc = append(desc, lbl)
				}
				mediaNotice := fmt.Sprintf("[System: %d image(s) generated and queued for direct delivery to this channel right after your reply: %s]", len(res.DirectMedia), strings.Join(desc, ", "))
				if outText == "" {
					outText = mediaNotice
				} else {
					outText += "\n" + mediaNotice
				}
			}
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
		if iter == maxIter {
			exhaustedTools = true
		}
	}

	// If all maxIter iterations returned tool_calls, run one final synthesis pass without tools
	// so the user receives an answer instead of silence.
	if exhaustedTools && finalText == "" && ctx.Err() == nil {
		if resp, err := r.hub.Chat(ctx, "primary", buildReq(msgs, nil)); err == nil {
			stats.Iterations++
			stats.PromptTokens += resp.Usage.PromptTokens
			stats.CachedTokens += resp.Usage.CachedTokens
			stats.CompletionTokens += resp.Usage.CompletionTokens
			stats.TotalLatencyMs += resp.LatencyMs
			finalText = strings.TrimSpace(resp.Content)
		}
	}

	// Extract optional zero-latency async sticker marker [[sticker: ...]] and [[skip]] graceful no-reply marker.
	cleanText, stickerIntent := ExtractStickerMarker(finalText)
	cleanText, skipped := ExtractSkipMarker(cleanText)

	if cleanText != "" || len(directMedia) > 0 || stickerIntent != "" {
		skipped = false
		assistantText := cleanText
		if assistantText == "" && stickerIntent != "" {
			assistantText = "[[sticker: " + stickerIntent + "]]"
		}
		newFrames = append(newFrames, frame.StoredFramePayload{
			Role: "assistant",
			Text: assistantText,
		})
	}

	return &TurnOutput{
		Text:                   cleanText,
		DirectMedia:            directMedia,
		StickerIntent:          stickerIntent,
		Skipped:                skipped,
		ContentFilterRecovered: contentFilterRecovered,
		NewFrames:              newFrames,
		TrimUpToIndex:          assembled.TrimUpToIndex,
		Stats:                  stats,
	}, nil
}

const maxParallelReadOnlyTools = 4

func (r *Runner) executeToolBatch(ctx context.Context, tc tool.ToolContext, calls []infer.ToolCall) []tool.ToolResult {
	out := make([]tool.ToolResult, len(calls))
	if r.registry == nil {
		for i := range calls {
			out[i] = tool.ToolResult{TextForLLM: "error: tool registry unavailable"}
		}
		return out
	}

	// Partition calls into maximal contiguous read-only segments separated by side-effect barriers.
	// Read-only segments run concurrently with a bounded concurrency cap (protecting 512 MiB container RAM),
	// while write/side-effect calls execute strictly in order.
	for i := 0; i < len(calls); {
		j := i
		for j < len(calls) && r.registry.IsReadOnlyCall(calls[j].Function.Name, calls[j].Function.Arguments) {
			j++
		}
		roCount := j - i
		if roCount >= 2 {
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(maxParallelReadOnlyTools)
			for idx := i; idx < j; idx++ {
				pos := idx
				c := calls[idx]
				g.Go(func() error {
					res, err := r.registry.Execute(gctx, tc, c.Function.Name, c.Function.Arguments)
					if err != nil {
						res = tool.ToolResult{TextForLLM: fmt.Sprintf("tool %s error: %v", c.Function.Name, err)}
					}
					out[pos] = res
					return nil
				})
			}
			_ = g.Wait()
		} else if roCount == 1 {
			c := calls[i]
			res, err := r.registry.Execute(ctx, tc, c.Function.Name, c.Function.Arguments)
			if err != nil {
				res = tool.ToolResult{TextForLLM: fmt.Sprintf("tool %s error: %v", c.Function.Name, err)}
			}
			out[i] = res
		}
		if j < len(calls) {
			c := calls[j]
			res, err := r.registry.Execute(ctx, tc, c.Function.Name, c.Function.Arguments)
			if err != nil {
				res = tool.ToolResult{TextForLLM: fmt.Sprintf("tool %s error: %v", c.Function.Name, err)}
			}
			out[j] = res
			i = j + 1
		} else {
			i = j
		}
	}
	return out
}

// ExtractStickerMarker strips optional [[sticker: intent]] (and malformed single-bracket [sticker: intent])
// markers without extra LLM round-trips.
func ExtractStickerMarker(s string) (clean string, intent string) {
	if matches := stickerMarkerRe.FindAllStringSubmatch(s, -1); len(matches) > 0 {
		intent = strings.TrimSpace(matches[len(matches)-1][1])
		s = stickerMarkerRe.ReplaceAllString(s, "")
	}
	if matches := singleStickerMarkerRe.FindAllStringSubmatch(s, -1); len(matches) > 0 {
		if intent == "" {
			intent = strings.TrimSpace(matches[len(matches)-1][1])
		}
		s = singleStickerMarkerRe.ReplaceAllString(s, "")
	}
	return strings.TrimSpace(s), intent
}

// ExtractSkipMarker detects and strips [[skip]] when the model chooses a natural silent close.
func ExtractSkipMarker(s string) (clean string, skipped bool) {
	if !skipMarkerRe.MatchString(s) {
		return strings.TrimSpace(s), false
	}
	clean = strings.TrimSpace(skipMarkerRe.ReplaceAllString(s, ""))
	return clean, clean == ""
}
