package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/tool"
)

type stubReadTool struct {
	name    string
	running *atomic.Int32
	maxConc *atomic.Int32
}

type stubArgs struct {
	X string `json:"x"`
}

func (s *stubReadTool) Name() string        { return s.name }
func (s *stubReadTool) Description() string { return "stub read-only tool" }
func (s *stubReadTool) ReadOnly() bool      { return true }
func (s *stubReadTool) Execute(_ context.Context, _ tool.ToolContext, args stubArgs) (tool.ToolResult, error) {
	cur := s.running.Add(1)
	for {
		prev := s.maxConc.Load()
		if cur <= prev || s.maxConc.CompareAndSwap(prev, cur) {
			break
		}
	}
	time.Sleep(30 * time.Millisecond)
	s.running.Add(-1)
	return tool.ToolResult{
		TextForLLM: "res:" + args.X,
		DirectMedia: []model.DirectMedia{{
			Data:     []byte("png"),
			MimeType: "image/png",
			Filename: s.name + ".png",
		}},
	}, nil
}

func TestRunnerParallelReadOnlyToolsAndStickerMarker(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		var req infer.ChatRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"finish_reason": "tool_calls",
					"message": map[string]any{
						"content":           "",
						"reasoning_content": "thinking step 1",
						"tool_calls": []map[string]any{
							{"id": "c1", "type": "function", "function": map[string]any{"name": "read_a", "arguments": `{"x":"1"}`}},
							{"id": "c2", "type": "function", "function": map[string]any{"name": "read_b", "arguments": `{"x":"2"}`}},
						},
					},
				}},
				"usage": map[string]any{"prompt_tokens": 200, "completion_tokens": 20},
			})
			return
		}
		// Verify reasoning_content was preserved in multi-step tool loop.
		foundReasoning := false
		for _, m := range req.Messages {
			if m.Role == "assistant" && m.ReasoningContent == "thinking step 1" {
				foundReasoning = true
			}
		}
		if !foundReasoning {
			t.Errorf("expected reasoning_content preserved on iteration 2")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message": map[string]any{
					"content": "all done! [[sticker: happy thumbs up]]",
				},
			}},
			"usage": map[string]any{"prompt_tokens": 260, "completion_tokens": 12, "prompt_tokens_details": map[string]any{"cached_tokens": 200}},
		})
	}))
	defer srv.Close()

	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()

	hub := infer.NewHub(config.ModelsConfig{
		Primary: config.ModelEndpointConfig{BaseURL: srv.URL, Model: "test-model", TimeoutSec: 5},
	})
	fb := frame.NewBuilder(st, config.FrameConfig{
		InputTokenBudget:    32768,
		TrimHighWater:       0.80,
		TrimLowWater:        0.50,
		SegmentIdleMinutes:  30,
		MaxImagesPerRequest: 6,
		ImageTokenReserve:   1600,
	})
	reg := tool.NewRegistry(5 * time.Second)
	var running, maxConc atomic.Int32
	_ = tool.Register[stubArgs](reg, &stubReadTool{name: "read_a", running: &running, maxConc: &maxConc})
	_ = tool.Register[stubArgs](reg, &stubReadTool{name: "read_b", running: &running, maxConc: &maxConc})
	reg.Freeze()

	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "2")
	seg, _, err := fb.EnsureActiveSegment(context.Background(), ch, 1, frame.ScopeHash("p", reg.Hash(), "test-model"), nil)
	if err != nil {
		t.Fatalf("EnsureActiveSegment: %v", err)
	}

	runner := NewRunner(hub, st, fb, reg, nil)
	out, err := runner.Run(context.Background(), TurnInput{
		ChannelID:   ch,
		Epoch:       1,
		Segment:     seg,
		PersonaText: "persona",
		UserPayload: frame.StoredFramePayload{Role: "user", Text: "hi"},
	})
	if err != nil {
		t.Fatalf("Runner.Run: %v", err)
	}
	if maxConc.Load() < 2 {
		t.Fatalf("expected parallel ReadOnly tool execution (maxConc >= 2), got %d", maxConc.Load())
	}
	if out.Text != "all done!" || out.StickerIntent != "happy thumbs up" {
		t.Fatalf("unexpected text/sticker: %q / %q", out.Text, out.StickerIntent)
	}
	if len(out.DirectMedia) != 2 {
		t.Fatalf("expected 2 DirectMedia items, got %d", len(out.DirectMedia))
	}
}

func TestRunnerExhaustedToolIterationsSynthesizesReply(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if n <= maxToolIterations {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"finish_reason": "tool_calls",
					"message": map[string]any{
						"tool_calls": []map[string]any{
							{"id": "c", "type": "function", "function": map[string]any{"name": "read_a", "arguments": `{"x":"loop"}`}},
						},
					},
				}},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message":       map[string]any{"content": "synthesized final reply"},
			}},
		})
	}))
	defer srv.Close()

	st, _ := store.Open(t.TempDir(), 512, 5)
	defer st.Close()
	hub := infer.NewHub(config.ModelsConfig{
		Primary: config.ModelEndpointConfig{BaseURL: srv.URL, Model: "test-model", TimeoutSec: 5},
	})
	fb := frame.NewBuilder(st, config.FrameConfig{InputTokenBudget: 32768, TrimHighWater: 0.8, TrimLowWater: 0.5, SegmentIdleMinutes: 30, MaxImagesPerRequest: 6, ImageTokenReserve: 1600})
	reg := tool.NewRegistry(5 * time.Second)
	var running, maxConc atomic.Int32
	_ = tool.Register[stubArgs](reg, &stubReadTool{name: "read_a", running: &running, maxConc: &maxConc})
	reg.Freeze()

	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "3")
	seg, _, _ := fb.EnsureActiveSegment(context.Background(), ch, 1, frame.ScopeHash("p", reg.Hash(), "test-model"), nil)

	out, err := NewRunner(hub, st, fb, reg, nil).Run(context.Background(), TurnInput{
		ChannelID:   ch,
		Epoch:       1,
		Segment:     seg,
		PersonaText: "persona",
		UserPayload: frame.StoredFramePayload{Role: "user", Text: "loop"},
	})
	if err != nil {
		t.Fatalf("Runner.Run: %v", err)
	}
	if out.Text != "synthesized final reply" {
		t.Fatalf("expected synthesized reply after tool exhaustion, got %q", out.Text)
	}
}

func TestExtractSkipMarker(t *testing.T) {
	clean, skip := ExtractSkipMarker("[[skip]]")
	if !skip || clean != "" {
		t.Fatalf("expected pure skip, got clean=%q skip=%v", clean, skip)
	}
	clean2, skip2 := ExtractSkipMarker("  [[ SKIP ]]  ")
	if !skip2 || clean2 != "" {
		t.Fatalf("expected case-insensitive skip, got clean=%q skip=%v", clean2, skip2)
	}
	clean3, skip3 := ExtractSkipMarker("行吧 [[skip]]")
	if skip3 || clean3 != "行吧" {
		t.Fatalf("expected text+skip to strip marker without skipping, got clean=%q skip=%v", clean3, skip3)
	}
}
