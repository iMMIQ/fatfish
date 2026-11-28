package infer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/iMMIQ/fatfish/internal/config"
)

func TestHubEmbedCacheAndDefensiveCopy(t *testing.T) {
	var reqCount atomic.Int32
	var totalTexts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		var body struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		totalTexts.Add(int32(len(body.Input)))
		data := make([]map[string]any, len(body.Input))
		for i, txt := range body.Input {
			data[i] = map[string]any{
				"index":     i,
				"embedding": []float32{float32(len(txt)), 1.0, 2.0, 3.0},
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer srv.Close()

	hub := NewHub(config.ModelsConfig{
		Embed: config.ModelEndpointConfig{BaseURL: srv.URL, Model: "emb-test", TimeoutSec: 5},
	})
	ctx := context.Background()

	// Batch with intra-batch duplicate: should only send 2 unique texts.
	v1, err := hub.Embed(ctx, []string{"alpha", "beta", "alpha"})
	if err != nil {
		t.Fatalf("Embed #1: %v", err)
	}
	if len(v1) != 3 || v1[0][0] != 5 || v1[1][0] != 4 || v1[2][0] != 5 {
		t.Fatalf("unexpected embed #1 result: %+v", v1)
	}
	if reqCount.Load() != 1 || totalTexts.Load() != 2 {
		t.Fatalf("expected 1 upstream call with 2 unique texts, got calls=%d texts=%d", reqCount.Load(), totalTexts.Load())
	}

	// Mutate returned slice in place — must NOT corrupt cached entry!
	v1[0][0] = 999
	v1[2][1] = 888

	// Repeat call with 1 hit + 1 new text: should only send "gamma".
	v2, err := hub.Embed(ctx, []string{"alpha", "gamma"})
	if err != nil {
		t.Fatalf("Embed #2: %v", err)
	}
	if v2[0][0] != 5 || v2[0][1] != 1.0 {
		t.Fatalf("cache was corrupted by caller mutation: got %+v", v2[0])
	}
	if reqCount.Load() != 2 || totalTexts.Load() != 3 {
		t.Fatalf("expected 2 calls / 3 total texts, got calls=%d texts=%d", reqCount.Load(), totalTexts.Load())
	}
}

func TestChatMessagePreencodedJSONMatchesStandard(t *testing.T) {
	orig := ChatMessage{
		Role: "user",
		Content: []ContentPart{
			{Type: "text", Text: "look at this"},
			{Type: "image_url", ImageURL: &ImageURL{URL: "data:image/jpeg;base64,AAAA"}},
		},
	}
	raw, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal orig: %v", err)
	}
	cached := orig
	cached.PreencodedJSON = raw
	raw2, err := json.Marshal(cached)
	if err != nil {
		t.Fatalf("Marshal cached: %v", err)
	}
	if !bytes.Equal(raw, raw2) {
		t.Fatalf("preencoded mismatch:\norig=%s\ncached=%s", raw, raw2)
	}
}
