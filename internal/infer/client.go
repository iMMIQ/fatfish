package infer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
)

// ChatMessage represents a wire-level OpenAI Chat Completions message.
// Content is either string or []ContentPart (marshaled canonically).
type ChatMessage struct {
	Role             string     `json:"role"`
	Content          any        `json:"content,omitempty"`
	ReasoningContent string     `json:"reasoning_content,omitempty"`
	ToolCalls        []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string     `json:"tool_call_id,omitempty"`
	Name             string     `json:"name,omitempty"`
}

// ContentPart represents a multimodal element inside ChatMessage.Content.
type ContentPart struct {
	Type     string    `json:"type"` // "text" | "image_url"
	Text     string    `json:"text,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
}

type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"` // "function"
	Function ToolCallFunction `json:"function"`
}

type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDefinition is a wire-level OpenAI function tool declaration.
type ToolDefinition struct {
	Type     string                 `json:"type"` // "function"
	Function ToolFunctionDefinition `json:"function"`
}

type ToolFunctionDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type ChatRequest struct {
	Model              string           `json:"model"`
	Messages           []ChatMessage    `json:"messages"`
	Tools              []ToolDefinition `json:"tools,omitempty"`
	ToolChoice         any              `json:"tool_choice,omitempty"`
	Temperature        *float64         `json:"temperature,omitempty"`
	MaxTokens          int              `json:"max_tokens,omitempty"`
	ResponseFormat     map[string]any   `json:"response_format,omitempty"`
	ChatTemplateKwargs map[string]any   `json:"chat_template_kwargs,omitempty"`
}

type ChatUsage struct {
	PromptTokens     int
	CachedTokens     int
	CompletionTokens int
	TotalTokens      int
}

type ChatResponse struct {
	Content          string
	ReasoningContent string
	ToolCalls        []ToolCall
	FinishReason     string
	ModelUsed        string
	FallbackUsed     bool
	Usage            ChatUsage
	LatencyMs        int64
}

type RerankResult struct {
	Index int     `json:"index"`
	Score float64 `json:"relevance_score"`
}

// Hub manages all OpenAI-compatible & local inference clients, circuit breakers, and GPU concurrency limits.
type Hub struct {
	mu       sync.RWMutex
	cfg      config.ModelsConfig
	client   *http.Client
	heavySem chan struct{}

	breakers map[string]*CircuitBreaker
}

// NewHub builds an inference Hub with dedicated circuit breakers per capability.
func NewHub(cfg config.ModelsConfig) *Hub {
	maxHeavy := cfg.Distill.MaxConcurrent
	if maxHeavy <= 0 {
		maxHeavy = 1
	}
	tr := &http.Transport{
		Proxy:               nil, // Direct LAN/API calls without dead local proxy interference
		MaxIdleConns:        32,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	h := &Hub{
		cfg:      cfg,
		client:   &http.Client{Transport: tr},
		heavySem: make(chan struct{}, maxHeavy),
		breakers: map[string]*CircuitBreaker{
			"primary":          NewCircuitBreaker("primary", 3, 30*time.Second),
			"primary_fallback": NewCircuitBreaker("primary_fallback", 3, 30*time.Second),
			"fast":             NewCircuitBreaker("fast", 3, 30*time.Second),
			"distill":          NewCircuitBreaker("distill", 3, 30*time.Second),
			"embed":            NewCircuitBreaker("embed", 3, 30*time.Second),
			"rerank":           NewCircuitBreaker("rerank", 3, 30*time.Second),
			"asr":              NewCircuitBreaker("asr", 3, 30*time.Second),
			"translate":        NewCircuitBreaker("translate", 3, 30*time.Second),
		},
	}
	return h
}

// UpdateConfig hot-swaps model endpoints without dropping live breaker telemetry.
func (h *Hub) UpdateConfig(cfg config.ModelsConfig) {
	h.mu.Lock()
	h.cfg = cfg
	h.mu.Unlock()
}

// BreakerSnapshots returns live status for all capability circuit breakers.
func (h *Hub) BreakerSnapshots() []BreakerSnapshot {
	names := []string{"primary", "primary_fallback", "fast", "distill", "embed", "rerank", "asr", "translate"}
	out := make([]BreakerSnapshot, 0, len(names))
	for _, n := range names {
		if cb := h.breakers[n]; cb != nil {
			out = append(out, cb.Snapshot())
		}
	}
	return out
}

func (h *Hub) endpoint(role string) (config.ModelEndpointConfig, *CircuitBreaker, error) {
	h.mu.RLock()
	cfg := h.cfg
	h.mu.RUnlock()

	var ep config.ModelEndpointConfig
	switch role {
	case "primary":
		ep = cfg.Primary
	case "primary_fallback":
		ep = cfg.PrimaryFallback
		if ep.Model != "" {
			if strings.TrimSpace(ep.BaseURL) == "" {
				ep.BaseURL = cfg.Primary.BaseURL
			}
			if strings.TrimSpace(ep.APIKey) == "" {
				ep.APIKey = cfg.Primary.APIKey
			}
			if ep.TimeoutSec <= 0 {
				ep.TimeoutSec = cfg.Primary.TimeoutSec
			}
		}
	case "fast":
		ep = cfg.Fast
		if !ep.IsEnabled() {
			ep = cfg.Primary
		}
	case "distill":
		ep = cfg.Distill
	case "embed":
		ep = cfg.Embed
	case "rerank":
		ep = cfg.Rerank
	case "asr":
		ep = cfg.ASR
	case "translate":
		ep = cfg.Translate
	default:
		return ep, nil, fmt.Errorf("infer: unknown role %q", role)
	}
	if !ep.IsEnabled() {
		return ep, h.breakers[role], fmt.Errorf("infer: %s endpoint is not configured", role)
	}
	return ep, h.breakers[role], nil
}

// ModelKey returns the canonical calibrator key "role|base_url|model".
func (h *Hub) ModelKey(role string) string {
	ep, _, _ := h.endpoint(role)
	return role + "|" + strings.TrimRight(ep.BaseURL, "/") + "|" + ep.Model
}

// Chat sends a Chat Completions request to role ("primary" | "primary_fallback" | "fast" | "distill").
// When role == "primary" fails (upstream error or open circuit) and primary_fallback is configured,
// it transparently falls back to primary_fallback. Never silently strips images on error.
func (h *Hub) Chat(ctx context.Context, role string, req ChatRequest) (*ChatResponse, error) {
	resp, err := h.chatOnce(ctx, role, req)
	if err != nil && role == "primary" && ctx.Err() == nil {
		if _, _, fbErr := h.endpoint("primary_fallback"); fbErr == nil {
			fbReq := req
			fbReq.Model = "" // Use primary_fallback's configured model ID
			if fbResp, err2 := h.chatOnce(ctx, "primary_fallback", fbReq); err2 == nil {
				fbResp.FallbackUsed = true
				return fbResp, nil
			}
		}
	}
	return resp, err
}

func (h *Hub) chatOnce(ctx context.Context, role string, req ChatRequest) (*ChatResponse, error) {
	ep, cb, err := h.endpoint(role)
	if err != nil {
		return nil, err
	}
	if err := cb.Allow(); err != nil {
		return nil, err
	}

	// Enforce heavy local GPU semaphore on distill role to prevent VRAM thrashing.
	if role == "distill" {
		select {
		case h.heavySem <- struct{}{}:
			defer func() { <-h.heavySem }()
		case <-ctx.Done():
			cb.Record(nil) // Context cancellation before call is not an upstream fault.
			return nil, ctx.Err()
		}
	}

	if req.Model == "" {
		req.Model = ep.Model
	}
	body, err := json.Marshal(req)
	if err != nil {
		cb.Record(nil)
		return nil, err
	}

	timeout := time.Duration(ep.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(ep.BaseURL, "/") + "/chat/completions"
	start := time.Now()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		cb.Record(err)
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if ep.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+ep.APIKey)
	}

	resp, err := h.client.Do(httpReq)
	if err != nil {
		cb.Record(err)
		return nil, fmt.Errorf("infer: chat %s: %w", role, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		cb.Record(err)
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := fmt.Errorf("infer: chat %s HTTP %d: %s", role, resp.StatusCode, truncateStr(string(respBytes), 400))
		cb.Record(apiErr)
		return nil, apiErr
	}

	var parsed struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content          string     `json:"content"`
				ReasoningContent string     `json:"reasoning_content"`
				ToolCalls        []ToolCall `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens        int `json:"prompt_tokens"`
			CompletionTokens    int `json:"completion_tokens"`
			TotalTokens         int `json:"total_tokens"`
			PromptCacheHitTok   int `json:"prompt_cache_hit_tokens"` // DeepSeek / some OpenAI proxies
			PromptTokensDetails struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		cb.Record(err)
		return nil, fmt.Errorf("infer: decode chat response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		err := errors.New("infer: empty choices in chat response")
		cb.Record(err)
		return nil, err
	}

	cached := parsed.Usage.PromptTokensDetails.CachedTokens
	if cached == 0 && parsed.Usage.PromptCacheHitTok > 0 {
		cached = parsed.Usage.PromptCacheHitTok
	}

	cb.Record(nil)
	ch := parsed.Choices[0]
	return &ChatResponse{
		Content:          ch.Message.Content,
		ReasoningContent: ch.Message.ReasoningContent,
		ToolCalls:        ch.Message.ToolCalls,
		FinishReason:     ch.FinishReason,
		ModelUsed:        req.Model,
		Usage: ChatUsage{
			PromptTokens:     parsed.Usage.PromptTokens,
			CachedTokens:     cached,
			CompletionTokens: parsed.Usage.CompletionTokens,
			TotalTokens:      parsed.Usage.TotalTokens,
		},
		LatencyMs: time.Since(start).Milliseconds(),
	}, nil
}

// Embed embeds texts using POST /v1/embeddings with strict max_batch chunking (1..128).
func (h *Hub) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	ep, cb, err := h.endpoint("embed")
	if err != nil {
		return nil, err
	}
	maxBatch := ep.MaxBatch
	if maxBatch <= 0 || maxBatch > 128 {
		maxBatch = 64
	}
	results := make([][]float32, len(texts))
	for start := 0; start < len(texts); start += maxBatch {
		end := start + maxBatch
		if end > len(texts) {
			end = len(texts)
		}
		chunk, err := h.embedChunk(ctx, ep, cb, texts[start:end])
		if err != nil {
			return nil, err
		}
		copy(results[start:end], chunk)
	}
	return results, nil
}

func (h *Hub) embedChunk(ctx context.Context, ep config.ModelEndpointConfig, cb *CircuitBreaker, texts []string) ([][]float32, error) {
	if err := cb.Allow(); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(map[string]any{
		"model": ep.Model,
		"input": texts,
	})
	timeout := time.Duration(ep.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(ep.BaseURL, "/") + "/embeddings"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		cb.Record(err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if ep.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ep.APIKey)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		cb.Record(err)
		return nil, fmt.Errorf("infer: embed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		cb.Record(err)
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := fmt.Errorf("infer: embed HTTP %d: %s", resp.StatusCode, truncateStr(string(body), 300))
		cb.Record(apiErr)
		return nil, apiErr
	}
	var parsed struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		cb.Record(err)
		return nil, err
	}
	out := make([][]float32, len(texts))
	for _, d := range parsed.Data {
		if d.Index >= 0 && d.Index < len(out) {
			out[d.Index] = d.Embedding
		}
	}
	cb.Record(nil)
	return out, nil
}

// Rerank scores documents against query via POST /v1/rerank, respecting max_batch <= 128.
func (h *Hub) Rerank(ctx context.Context, query string, docs []string) ([]RerankResult, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	ep, cb, err := h.endpoint("rerank")
	if err != nil {
		return nil, err
	}
	maxBatch := ep.MaxBatch
	if maxBatch <= 0 || maxBatch > 128 {
		maxBatch = 64
	}
	var all []RerankResult
	for start := 0; start < len(docs); start += maxBatch {
		end := start + maxBatch
		if end > len(docs) {
			end = len(docs)
		}
		part, err := h.rerankChunk(ctx, ep, cb, query, docs[start:end], start)
		if err != nil {
			return nil, err
		}
		all = append(all, part...)
	}
	return all, nil
}

func (h *Hub) rerankChunk(ctx context.Context, ep config.ModelEndpointConfig, cb *CircuitBreaker, query string, docs []string, offset int) ([]RerankResult, error) {
	if err := cb.Allow(); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(map[string]any{
		"model":     ep.Model,
		"query":     query,
		"documents": docs,
	})
	timeout := time.Duration(ep.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(ep.BaseURL, "/") + "/rerank"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		cb.Record(err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if ep.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ep.APIKey)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		cb.Record(err)
		return nil, fmt.Errorf("infer: rerank: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		cb.Record(err)
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := fmt.Errorf("infer: rerank HTTP %d: %s", resp.StatusCode, truncateStr(string(body), 300))
		cb.Record(apiErr)
		return nil, apiErr
	}
	var parsed struct {
		Results []struct {
			Index          int     `json:"index"`
			RelevanceScore float64 `json:"relevance_score"`
			Score          float64 `json:"score"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		cb.Record(err)
		return nil, err
	}
	out := make([]RerankResult, 0, len(parsed.Results))
	for _, r := range parsed.Results {
		sc := r.RelevanceScore
		if sc == 0 && r.Score != 0 {
			sc = r.Score
		}
		out = append(out, RerankResult{Index: offset + r.Index, Score: sc})
	}
	cb.Record(nil)
	return out, nil
}

// Transcribe calls OpenAI-compatible POST /v1/audio/transcriptions (multipart/form-data).
func (h *Hub) Transcribe(ctx context.Context, filename string, audioBytes []byte) (string, error) {
	if len(audioBytes) == 0 {
		return "", errors.New("infer: empty audio bytes")
	}
	ep, cb, err := h.endpoint("asr")
	if err != nil {
		return "", err
	}
	if err := cb.Allow(); err != nil {
		return "", err
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if filename == "" {
		filename = "voice.wav"
	}
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		cb.Record(nil)
		return "", err
	}
	if _, err := fw.Write(audioBytes); err != nil {
		cb.Record(nil)
		return "", err
	}
	_ = mw.WriteField("model", ep.Model)
	if err := mw.Close(); err != nil {
		cb.Record(nil)
		return "", err
	}

	timeout := time.Duration(ep.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(ep.BaseURL, "/") + "/audio/transcriptions"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, &buf)
	if err != nil {
		cb.Record(err)
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if ep.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ep.APIKey)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		cb.Record(err)
		return "", fmt.Errorf("infer: asr: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		cb.Record(err)
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := fmt.Errorf("infer: asr HTTP %d: %s", resp.StatusCode, truncateStr(string(body), 300))
		cb.Record(apiErr)
		return "", apiErr
	}
	var parsed struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		// Some ASR servers return plain text when response_format=text.
		cb.Record(nil)
		return strings.TrimSpace(string(body)), nil
	}
	cb.Record(nil)
	return strings.TrimSpace(parsed.Text), nil
}

// Translate translates text into targetLang using the optional local translate model (internal helper).
func (h *Hub) Translate(ctx context.Context, text, targetLang string) (string, error) {
	if strings.TrimSpace(text) == "" {
		return "", nil
	}
	if targetLang == "" {
		targetLang = "Simplified Chinese"
	}
	role := "translate"
	if _, _, err := h.endpoint("translate"); err != nil {
		role = "fast"
	}
	resp, err := h.Chat(ctx, role, ChatRequest{
		Messages: []ChatMessage{
			{Role: "system", Content: "Translate the following content accurately into " + targetLang + ". Output only the translation."},
			{Role: "user", Content: text},
		},
	})
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp.Content), nil
}

// ProbeEndpoint checks GET /models on a configured role endpoint for `fatfish check`.
func (h *Hub) ProbeEndpoint(ctx context.Context, role string) error {
	ep, _, err := h.endpoint(role)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := strings.TrimRight(ep.BaseURL, "/") + "/models"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if ep.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+ep.APIKey)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func truncateStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
