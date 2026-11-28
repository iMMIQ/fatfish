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

func TestPrimaryAutomaticFallback(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model == "glm-5.4" {
			primaryHits.Add(1)
			http.Error(w, "upstream overloaded", http.StatusServiceUnavailable)
			return
		}
		if req.Model == "glm-5.3-flash" {
			fallbackHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{{
					"finish_reason": "stop",
					"message":       map[string]any{"content": "fallback-ok"},
				}},
			})
			return
		}
		http.Error(w, "unexpected model "+req.Model, http.StatusBadRequest)
	}))
	defer srv.Close()

	hub := NewHub(config.ModelsConfig{
		Primary:         config.ModelEndpointConfig{BaseURL: srv.URL, Model: "glm-5.4", TimeoutSec: 5},
		PrimaryFallback: config.ModelEndpointConfig{Model: "glm-5.3-flash"},
	})

	for i := 0; i < 4; i++ {
		resp, err := hub.Chat(context.Background(), "primary", ChatRequest{
			Messages: []ChatMessage{{Role: "user", Content: "hello"}},
		})
		if err != nil || resp.Content != "fallback-ok" {
			t.Fatalf("call %d expected fallback-ok, got resp=%+v err=%v", i, resp, err)
		}
	}
	if primaryHits.Load() != 3 || fallbackHits.Load() != 4 {
		t.Fatalf("expected 3 primary hits (then circuit open) and 4 fallback hits, got primary=%d fallback=%d",
			primaryHits.Load(), fallbackHits.Load())
	}
}

func TestReasoningEffortAndThinkingTypeInjection(t *testing.T) {
	var gotEffort string
	var gotThinkingType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model           string         `json:"model"`
			ReasoningEffort string         `json:"reasoning_effort"`
			Thinking        map[string]any `json:"thinking"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model == "m-primary" {
			gotEffort = req.ReasoningEffort
		}
		if req.Model == "m-fast" && req.Thinking != nil {
			gotThinkingType, _ = req.Thinking["type"].(string)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message":       map[string]any{"content": "ok"},
			}},
		})
	}))
	defer srv.Close()

	hub := NewHub(config.ModelsConfig{
		Primary: config.ModelEndpointConfig{BaseURL: srv.URL, Model: "m-primary", ReasoningEffort: "low", TimeoutSec: 5},
		Fast:    config.ModelEndpointConfig{BaseURL: srv.URL, Model: "m-fast", ThinkingType: "disabled", TimeoutSec: 5},
	})

	if _, err := hub.Chat(context.Background(), "primary", ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "hi"}}}); err != nil {
		t.Fatalf("primary Chat: %v", err)
	}
	if _, err := hub.Chat(context.Background(), "fast", ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "hi"}}}); err != nil {
		t.Fatalf("fast Chat: %v", err)
	}
	if gotEffort != "low" {
		t.Fatalf("expected reasoning_effort=low, got %q", gotEffort)
	}
	if gotThinkingType != "disabled" {
		t.Fatalf("expected thinking.type=disabled, got %q", gotThinkingType)
	}
}

func TestCircuitBreakerAbortProbeAndTranslateFallback(t *testing.T) {
	cb := NewCircuitBreaker("probe-test", 2, 30*time.Millisecond)
	_ = cb.Allow()
	cb.Record(errors.New("err1"))
	_ = cb.Allow()
	cb.Record(errors.New("err2"))
	if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open circuit, got %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected half-open probe allowed, got %v", err)
	}
	// Pre-call AbortProbe must NOT close the circuit or clear failures.
	cb.AbortProbe()
	if snap := cb.Snapshot(); snap.State != StateHalfOpen || snap.Failures != 2 {
		t.Fatalf("expected HalfOpen with failures=2 after AbortProbe, got %+v", snap)
	}
	// Another probe can now proceed and close the circuit on real success.
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected next probe allowed after AbortProbe, got %v", err)
	}
	cb.Record(nil)
	if snap := cb.Snapshot(); snap.State != StateClosed {
		t.Fatalf("expected Closed after probe success, got %+v", snap)
	}

	// Translate must fall back to primary when local translate endpoint fails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model == "tr-model" {
			http.Error(w, "translate down", http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"finish_reason": "stop",
				"message":       map[string]any{"content": "你好"},
			}},
		})
	}))
	defer srv.Close()

	trEnabled := true
	hub := NewHub(config.ModelsConfig{
		Primary:   config.ModelEndpointConfig{BaseURL: srv.URL, Model: "primary-model", TimeoutSec: 5},
		Translate: config.ModelEndpointConfig{Enabled: &trEnabled, BaseURL: srv.URL, Model: "tr-model", TimeoutSec: 5},
	})
	out, err := hub.Translate(context.Background(), "hello", "Simplified Chinese")
	if err != nil || out != "你好" {
		t.Fatalf("expected translate fallback to primary, got out=%q err=%v", out, err)
	}
}

func TestContentFilterSkipsSameVendorFallback(t *testing.T) {
	var primaryHits, fallbackHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model == "glm-5.4" {
			primaryHits.Add(1)
		} else {
			fallbackHits.Add(1)
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"contentFilter":[{"level":2,"role":"user"}],"error":{"code":"1301","message":"blocked"}}`))
	}))
	defer srv.Close()

	hub := NewHub(config.ModelsConfig{
		Primary:         config.ModelEndpointConfig{BaseURL: srv.URL, Model: "glm-5.4", TimeoutSec: 5},
		PrimaryFallback: config.ModelEndpointConfig{Model: "glm-5.3-flash"},
	})
	_, err := hub.Chat(context.Background(), "primary", ChatRequest{
		Messages: []ChatMessage{{Role: "user", Content: "test"}},
	})
	if !IsContentFiltered(err) {
		t.Fatalf("expected ErrContentFiltered, got %v", err)
	}
	if primaryHits.Load() != 1 || fallbackHits.Load() != 0 {
		t.Fatalf("expected content-filter to skip primary_fallback, got primary=%d fallback=%d",
			primaryHits.Load(), fallbackHits.Load())
	}
}
