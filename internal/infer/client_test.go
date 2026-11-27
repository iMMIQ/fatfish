package infer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
)

func TestCircuitBreakerTransitions(t *testing.T) {
	cb := NewCircuitBreaker("test", 3, 50*time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := cb.Allow(); err != nil {
			t.Fatalf("call %d should be allowed: %v", i, err)
		}
		cb.Record(errors.New("boom"))
	}
	if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen after 3 failures, got %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected half-open probe allowed after open duration, got %v", err)
	}
	cb.Record(nil)
	if snap := cb.Snapshot(); snap.State != StateClosed {
		t.Fatalf("expected breaker closed after probe success, got %+v", snap)
	}
}

func TestEmbedBatchingASRAndChatCachedTokens(t *testing.T) {
	var embedCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/embeddings":
			embedCalls.Add(1)
			var req struct {
				Input []string `json:"input"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if len(req.Input) > 2 {
				http.Error(w, "batch too large", http.StatusBadRequest)
				return
			}
			data := make([]map[string]any, len(req.Input))
			for i := range req.Input {
				data[i] = map[string]any{"index": i, "embedding": []float32{1, 0}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": data})

		case "/audio/transcriptions":
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				http.Error(w, "multipart required", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"text": "你好世界"})

		case "/chat/completions":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"finish_reason": "stop",
					"message":       map[string]any{"content": "ok"},
				}},
				"usage": map[string]any{
					"prompt_tokens":        1000,
					"completion_tokens":    10,
					"prompt_tokens_details": map[string]any{"cached_tokens": 920},
				},
			})
		}
	}))
	defer srv.Close()

	hub := NewHub(config.ModelsConfig{
		Primary: config.ModelEndpointConfig{BaseURL: srv.URL, Model: "m1", TimeoutSec: 5},
		Embed:   config.ModelEndpointConfig{BaseURL: srv.URL, Model: "e1", Dim: 2, MaxBatch: 2, TimeoutSec: 5},
		ASR:     config.ModelEndpointConfig{BaseURL: srv.URL, Model: "a1", TimeoutSec: 5},
	})

	ctx := context.Background()
	embs, err := hub.Embed(ctx, []string{"a", "b", "c", "d", "e"})
	if err != nil || len(embs) != 5 || embedCalls.Load() != 3 {
		t.Fatalf("expected 5 embeddings across 3 chunks, got len=%d calls=%d err=%v", len(embs), embedCalls.Load(), err)
	}

	txt, err := hub.Transcribe(ctx, "voice.wav", []byte("RIFF...."))
	if err != nil || txt != "你好世界" {
		t.Fatalf("Transcribe: txt=%q err=%v", txt, err)
	}

	resp, err := hub.Chat(ctx, "primary", ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "hi"}}})
	if err != nil || resp.Usage.CachedTokens != 920 {
		t.Fatalf("Chat cached tokens: resp=%+v err=%v", resp, err)
	}
}
