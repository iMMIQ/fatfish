package sticker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/vec"
)

// SearchHit provides full scoring breakdown (also shown on the Console Sticker Manager).
type SearchHit struct {
	Sticker     model.Sticker `json:"sticker"`
	FinalScore  float64       `json:"final_score"`
	RRFScore    float64       `json:"rrf_score"`
	RerankScore float64       `json:"rerank_score"`
	DecayFactor float64       `json:"decay_factor"`
}

// Engine implements dual-vector (v_intent + v_content) sticker retrieval with doc-only centering,
// 3-way hybrid recall (FTS5 + v_intent + v_content), RRF, Rerank, exponential anti-repeat decay,
// MMR diversity, and bounded async post-ACK dispatch.
type Engine struct {
	store *store.Store
	hub   *infer.Hub
	cfg   config.StickerConfig
	dim   int
	log   *slog.Logger

	mu         sync.RWMutex
	stickers   map[string]model.Sticker
	intentIdx  *vec.FlatF16Index
	contentIdx *vec.FlatF16Index

	asyncSem chan struct{}
	asyncWG  sync.WaitGroup
}

func NewEngine(st *store.Store, hub *infer.Hub, cfg config.StickerConfig, dim int, logger *slog.Logger) *Engine {
	if dim <= 0 {
		dim = 2048
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		store:      st,
		hub:        hub,
		cfg:        cfg,
		dim:        dim,
		log:        logger,
		stickers:   make(map[string]model.Sticker),
		intentIdx:  vec.NewFlatF16Index(dim, float32(cfg.DocCenteringAlpha)),
		contentIdx: vec.NewFlatF16Index(dim, float32(cfg.DocCenteringAlpha)),
		asyncSem:   make(chan struct{}, 32), // Bounded background worker pool max=32
	}
}

// UpdateConfig hot-swaps StickerConfig safely.
func (e *Engine) UpdateConfig(cfg config.StickerConfig) {
	e.mu.Lock()
	e.cfg = cfg
	e.mu.Unlock()
}

func (e *Engine) getConfig() config.StickerConfig {
	e.mu.RLock()
	c := e.cfg
	e.mu.RUnlock()
	return c
}

// Drain waits up to timeout for all in-flight async sticker workers to finish (SPEC §11.3).
func (e *Engine) Drain(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		e.asyncWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		e.log.Warn("sticker async drain timed out", "timeout", timeout)
	}
}

// MimeTypeFor infers the MIME type of a sticker file from its RelPath extension.
func MimeTypeFor(st model.Sticker) string {
	switch strings.ToLower(filepath.Ext(st.RelPath)) {
	case ".gif":
		return "image/gif"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	default:
		return "image/png"
	}
}

// LoadIndex loads all ready stickers and their pre-centered float16 dual vectors into RAM.
func (e *Engine) LoadIndex(ctx context.Context) (int, error) {
	cfg := e.getConfig()
	rows, err := e.store.ListStickers(ctx, "ready")
	if err != nil {
		return 0, err
	}
	intentIdx := vec.NewFlatF16Index(e.dim, float32(cfg.DocCenteringAlpha))
	contentIdx := vec.NewFlatF16Index(e.dim, float32(cfg.DocCenteringAlpha))
	stMap := make(map[string]model.Sticker, len(rows))

	for _, r := range rows {
		stMap[r.Sticker.StickerID] = r.Sticker
		if len(r.IntentF16) == e.dim*2 {
			intentIdx.UpsertPreencoded(r.Sticker.StickerID, r.IntentF16)
		}
		if len(r.ContentF16) == e.dim*2 {
			contentIdx.UpsertPreencoded(r.Sticker.StickerID, r.ContentF16)
		}
	}

	e.mu.Lock()
	e.stickers = stMap
	e.intentIdx = intentIdx
	e.contentIdx = contentIdx
	e.mu.Unlock()
	return len(stMap), nil
}

// SearchOptions configures dual-vector sticker retrieval.
type SearchOptions struct {
	ChannelID      model.ChannelID
	Query          string // Intent/emotion or visual query
	ContextQuery   string // Optional "<last user msg> -> <bot reply>" for precision Rerank
	PackID         string
	ExplicitLookup bool // true -> 0.25 intent + 0.75 content; false -> 0.75 intent + 0.25 content
	TopK           int
}

// Search runs 3-way hybrid recall (FTS5 + v_intent + v_content) -> Weighted RRF -> Rerank ->
// Exponential recent-use decay -> MMR diversity.
func (e *Engine) Search(ctx context.Context, opts SearchOptions) ([]SearchHit, error) {
	cfg := e.getConfig()
	if opts.TopK <= 0 {
		opts.TopK = 5
	}
	intentW := float32(cfg.ReactionIntentWeight)
	if intentW <= 0 {
		intentW = 0.75
	}
	if opts.ExplicitLookup {
		intentW = float32(cfg.ExplicitIntentWeight)
		if intentW <= 0 {
			intentW = 0.25
		}
	}
	contentW := 1.0 - intentW

	// 1. FTS5 exact OCR/tag recall Top-20.
	ftsIDs, _ := e.store.SearchStickersFTS(ctx, opts.Query, opts.PackID, 20)
	ftsHits := make([]vec.Hit, 0, len(ftsIDs))
	for i, id := range ftsIDs {
		ftsHits = append(ftsHits, vec.Hit{ID: id, Score: float32(20 - i)})
	}

	// 2. Dual-vector recall Top-30 each (unshifted query q against doc-centered corpus).
	var intentHits, contentHits []vec.Hit
	e.mu.RLock()
	hasVecs := e.intentIdx.Len() > 0 || e.contentIdx.Len() > 0
	e.mu.RUnlock()

	if e.hub != nil && hasVecs && strings.TrimSpace(opts.Query) != "" {
		if embs, err := e.hub.Embed(ctx, []string{opts.Query}); err == nil && len(embs) == 1 && len(embs[0]) == e.dim {
			q := embs[0]
			filter := func(id string) bool {
				if opts.PackID == "" {
					return true
				}
				e.mu.RLock()
				st, ok := e.stickers[id]
				e.mu.RUnlock()
				return ok && st.PackID == opts.PackID
			}
			e.mu.RLock()
			intentHits = e.intentIdx.Search(q, 30, filter)
			contentHits = e.contentIdx.Search(q, 30, filter)
			e.mu.RUnlock()
		}
	}

	// 3. Weighted RRF Top-25.
	fused := vec.WeightedRRF(
		[][]vec.Hit{ftsHits, intentHits, contentHits},
		[]float32{0.8, intentW, contentW},
		60,
		25,
	)
	if len(fused) == 0 {
		return nil, nil
	}

	// Load metadata for fused candidates.
	e.mu.RLock()
	cands := make([]SearchHit, 0, len(fused))
	for _, h := range fused {
		if st, ok := e.stickers[h.ID]; ok {
			cands = append(cands, SearchHit{
				Sticker:     st,
				RRFScore:    float64(h.Score),
				FinalScore:  float64(h.Score),
				DecayFactor: 1.0,
			})
		}
	}
	e.mu.RUnlock()
	if len(cands) == 0 {
		return nil, nil
	}

	// 4. Precision Rerank (gracefully skipped when rerank circuit is open).
	rerankQuery := strings.TrimSpace(opts.ContextQuery)
	if rerankQuery == "" {
		rerankQuery = opts.Query
	}
	if e.hub != nil && len(cands) > 1 && rerankQuery != "" {
		docs := make([]string, len(cands))
		for i, c := range cands {
			docs[i] = strings.TrimSpace(c.Sticker.IntentText + " | " + c.Sticker.ContentText + " | " + c.Sticker.OCRText)
		}
		if rr, err := e.hub.Rerank(ctx, rerankQuery, docs); err == nil && len(rr) > 0 {
			for _, r := range rr {
				if r.Index >= 0 && r.Index < len(cands) {
					cands[r.Index].RerankScore = r.Score
					cands[r.Index].FinalScore = r.Score
				}
			}
		}
	}

	// 5. Exponential anti-repeat decay for stickers recently used in this channel.
	if opts.ChannelID != "" {
		recent, _ := e.store.RecentStickerUses(ctx, opts.ChannelID, cfg.RecentWindow)
		if len(recent) > 0 {
			lastPos := make(map[string]int, len(recent))
			for idx, id := range recent {
				if _, seen := lastPos[id]; !seen {
					lastPos[id] = idx // 0 = most recently used
				}
			}
			for i := range cands {
				if pos, used := lastPos[cands[i].Sticker.StickerID]; used {
					// Strongly penalize most recent sends (decay ~0.15 -> 0.85 over window).
					decay := 1.0 - 0.85*math.Exp(-float64(pos)/8.0)
					cands[i].DecayFactor = decay
					cands[i].FinalScore *= decay
				}
			}
		}
	}

	sort.Slice(cands, func(i, j int) bool {
		if cands[i].FinalScore == cands[j].FinalScore {
			return cands[i].Sticker.StickerID < cands[j].Sticker.StickerID
		}
		return cands[i].FinalScore > cands[j].FinalScore
	})

	// 6. MMR diversity across Top-K.
	mmrIn := make([]vec.Hit, len(cands))
	candByID := make(map[string]SearchHit, len(cands))
	for i, c := range cands {
		mmrIn[i] = vec.Hit{ID: c.Sticker.StickerID, Score: float32(c.FinalScore)}
		candByID[c.Sticker.StickerID] = c
	}
	vecLookup := func(id string) ([]float32, bool) {
		e.mu.RLock()
		defer e.mu.RUnlock()
		if v, ok := e.intentIdx.Vector(id); ok {
			return v, true
		}
		return e.contentIdx.Vector(id)
	}
	diverse := vec.MMRRerank(mmrIn, vecLookup, float32(cfg.MMRLambda), opts.TopK)
	out := make([]SearchHit, 0, len(diverse))
	for _, d := range diverse {
		out = append(out, candByID[d.ID])
	}
	return out, nil
}

// PickOne selects a sticker from Top-5 using low-temperature softmax sampling for natural variety.
func (e *Engine) PickOne(ctx context.Context, opts SearchOptions) (*SearchHit, error) {
	opts.TopK = 5
	hits, err := e.Search(ctx, opts)
	if err != nil || len(hits) == 0 {
		return nil, err
	}
	if len(hits) == 1 {
		return &hits[0], nil
	}
	const temp = 0.15
	maxS := hits[0].FinalScore
	weights := make([]float64, len(hits))
	sum := 0.0
	for i, h := range hits {
		w := math.Exp((h.FinalScore - maxS) / temp)
		weights[i] = w
		sum += w
	}
	r := rand.Float64() * sum
	acc := 0.0
	for i, w := range weights {
		acc += w
		if r <= acc {
			return &hits[i], nil
		}
	}
	return &hits[0], nil
}

// ReadStickerFile reads the raw image bytes for a sticker (preserving animated GIFs/WebP).
func (e *Engine) ReadStickerFile(st model.Sticker) ([]byte, error) {
	p := st.RelPath
	if !filepath.IsAbs(p) {
		p = filepath.Join(e.getConfig().PackDir, p)
	}
	return os.ReadFile(p)
}

// DispatchAsync sends a post-ACK reaction sticker on a bounded worker pool (max=32) with epoch check.
func (e *Engine) DispatchAsync(
	ctx context.Context,
	ch model.ChannelID,
	expectedEpoch int64,
	intentQuery string,
	contextQuery string,
	checkEpoch func() int64,
	sendFn func(ctx context.Context, st model.Sticker, imgBytes []byte) error,
) bool {
	cfg := e.getConfig()
	if !cfg.Enabled || strings.TrimSpace(intentQuery) == "" {
		return false
	}
	select {
	case e.asyncSem <- struct{}{}:
	default:
		return false // Pool saturated, gracefully skip reaction sticker
	}
	e.asyncWG.Add(1)
	go func() {
		defer e.asyncWG.Done()
		defer func() { <-e.asyncSem }()
		to := time.Duration(cfg.AsyncSendTimeoutSec) * time.Second
		if to <= 0 {
			to = 15 * time.Second
		}
		runCtx, cancel := context.WithTimeout(ctx, to)
		defer cancel()

		hit, err := e.PickOne(runCtx, SearchOptions{
			ChannelID:      ch,
			Query:          intentQuery,
			ContextQuery:   contextQuery,
			ExplicitLookup: false,
		})
		if err != nil || hit == nil {
			return
		}
		if checkEpoch != nil && checkEpoch() != expectedEpoch {
			return // Channel was /reset while selecting sticker -> abort cleanly
		}
		raw, err := e.ReadStickerFile(hit.Sticker)
		if err != nil {
			e.log.Warn("sticker read failed", "id", hit.Sticker.StickerID, "err", err)
			return
		}
		if checkEpoch != nil && checkEpoch() != expectedEpoch {
			return
		}
		if err := sendFn(runCtx, hit.Sticker, raw); err != nil {
			e.log.Warn("async sticker send failed", "id", hit.Sticker.StickerID, "err", err)
			return
		}
		_ = e.store.RecordStickerUse(runCtx, ch, hit.Sticker.StickerID)
	}()
	return true
}

// EnrichAndTag populates missing intent_text (for legacy single-caption packs via `reindex --enrich`)
// and optionally tags untagged `needs_review` stickers using the primary multimodal vision model.
func (e *Engine) EnrichAndTag(ctx context.Context, tagUntagged bool) (enriched int, tagged int, err error) {
	if e.hub == nil {
		return 0, 0, fmt.Errorf("sticker: inference hub required for enrich/tag")
	}
	rows, err := e.store.ListStickers(ctx, "")
	if err != nil {
		return 0, 0, err
	}
	for _, r := range rows {
		st := r.Sticker
		if st.Status == "ready" && strings.TrimSpace(st.IntentText) == "" && strings.TrimSpace(st.ContentText+st.OCRText) != "" {
			resp, cerr := e.hub.Chat(ctx, "fast", infer.ChatRequest{
				Messages: []infer.ChatMessage{
					{Role: "system", Content: "Given a sticker visual description/OCR, output a concise comma-separated list of emotions, tones, and chat reply scenes (intent_text) in Chinese. Output plain text only."},
					{Role: "user", Content: strings.TrimSpace(st.ContentText + " " + st.OCRText)},
				},
			})
			if cerr == nil && strings.TrimSpace(resp.Content) != "" {
				if uerr := e.store.UpdateStickerStatus(ctx, st.StickerID, st.Status, st.OCRText, strings.TrimSpace(resp.Content), st.ContentText); uerr == nil {
					enriched++
				}
			}
			continue
		}
		if tagUntagged && st.Status == "needs_review" && strings.TrimSpace(st.ContentText) == "" && strings.TrimSpace(st.IntentText) == "" {
			raw, rerr := e.ReadStickerFile(st)
			if rerr != nil || len(raw) == 0 {
				continue
			}
			dataURI := "data:" + MimeTypeFor(st) + ";base64," + base64.StdEncoding.EncodeToString(raw)
			resp, cerr := e.hub.Chat(ctx, "primary", infer.ChatRequest{
				Messages: []infer.ChatMessage{
					{Role: "system", Content: `Tag this chat sticker as JSON: {"ocr_text":"...","intent_text":"emotions/reply scenes in Chinese","content_text":"visual description in Chinese"}`},
					{Role: "user", Content: []infer.ContentPart{
						{Type: "text", Text: "Analyze this sticker:"},
						{Type: "image_url", ImageURL: &infer.ImageURL{URL: dataURI}},
					}},
				},
				ResponseFormat: map[string]any{"type": "json_object"},
			})
			if cerr != nil {
				continue
			}
			var parsed struct {
				OCRText     string `json:"ocr_text"`
				IntentText  string `json:"intent_text"`
				ContentText string `json:"content_text"`
			}
			if json.Unmarshal([]byte(resp.Content), &parsed) == nil && (parsed.IntentText != "" || parsed.ContentText != "") {
				if uerr := e.store.UpdateStickerStatus(ctx, st.StickerID, "needs_review", strings.TrimSpace(parsed.OCRText), strings.TrimSpace(parsed.IntentText), strings.TrimSpace(parsed.ContentText)); uerr == nil {
					tagged++
				}
			}
		}
	}
	return enriched, tagged, nil
}

// ReindexOne embeds a single sticker (used when approving a sticker in the Console) and refreshes RAM index.
func (e *Engine) ReindexOne(ctx context.Context, stickerID string) error {
	if e.hub != nil {
		rows, err := e.store.ListStickers(ctx, "ready")
		if err != nil {
			return err
		}
		for _, r := range rows {
			if r.Sticker.StickerID != stickerID {
				continue
			}
			it := strings.TrimSpace(r.Sticker.IntentText)
			ct := strings.TrimSpace(r.Sticker.ContentText + " " + r.Sticker.OCRText)
			if it == "" {
				it = ct
			}
			if ct == "" {
				ct = it
			}
			if it != "" {
				if embs, err := e.hub.Embed(ctx, []string{it, ct}); err == nil && len(embs) == 2 && len(embs[0]) == e.dim && len(embs[1]) == e.dim {
					vec.NormalizeInPlace(embs[0])
					vec.NormalizeInPlace(embs[1])
					_ = e.store.UpdateStickerVectors(ctx, stickerID, vec.EncodeFloat16(embs[0]), vec.EncodeFloat16(embs[1]))
				}
			}
			break
		}
	}
	_, err := e.LoadIndex(ctx)
	return err
}

// Reindex computes embeddings for all ready stickers, applies doc-only centering with alpha,
// persists float16 vectors to SQLite, and reloads the RAM indices.
func (e *Engine) Reindex(ctx context.Context, alpha float64) (int, error) {
	if e.hub == nil {
		return 0, fmt.Errorf("sticker: inference hub required for reindex")
	}
	rows, err := e.store.ListStickers(ctx, "ready")
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	intentTexts := make([]string, len(rows))
	contentTexts := make([]string, len(rows))
	for i, r := range rows {
		it := strings.TrimSpace(r.Sticker.IntentText)
		ct := strings.TrimSpace(r.Sticker.ContentText + " " + r.Sticker.OCRText)
		if it == "" {
			it = ct
		}
		if ct == "" {
			ct = it
		}
		intentTexts[i] = it
		contentTexts[i] = ct
	}

	intentEmbs, err := e.hub.Embed(ctx, intentTexts)
	if err != nil {
		return 0, fmt.Errorf("sticker: embed intent: %w", err)
	}
	contentEmbs, err := e.hub.Embed(ctx, contentTexts)
	if err != nil {
		return 0, fmt.Errorf("sticker: embed content: %w", err)
	}

	rawIntent := make(map[string][]float32, len(rows))
	rawContent := make(map[string][]float32, len(rows))
	for i, r := range rows {
		if i < len(intentEmbs) && len(intentEmbs[i]) == e.dim {
			rawIntent[r.Sticker.StickerID] = intentEmbs[i]
		}
		if i < len(contentEmbs) && len(contentEmbs[i]) == e.dim {
			rawContent[r.Sticker.StickerID] = contentEmbs[i]
		}
	}

	iIdx := vec.NewFlatF16Index(e.dim, float32(alpha))
	cIdx := vec.NewFlatF16Index(e.dim, float32(alpha))
	iIdx.RebuildCentering(rawIntent, float32(alpha))
	cIdx.RebuildCentering(rawContent, float32(alpha))

	for _, r := range rows {
		var ib, cb []byte
		if v, ok := iIdx.Vector(r.Sticker.StickerID); ok {
			ib = vec.EncodeFloat16(v)
		}
		if v, ok := cIdx.Vector(r.Sticker.StickerID); ok {
			cb = vec.EncodeFloat16(v)
		}
		if err := e.store.UpdateStickerVectors(ctx, r.Sticker.StickerID, ib, cb); err != nil {
			return 0, err
		}
	}
	return e.LoadIndex(ctx)
}

// EvalQuery is one benchmark item for `fatfish reindex --eval queries.yaml`.
type EvalQuery struct {
	Query       string   `yaml:"query"`
	ExpectedIDs []string `yaml:"expected_ids"`
}

// EvalReport summarizes Recall@5 and unique diversity for a given alpha.
type EvalReport struct {
	Alpha      float64
	RecallAt5  float64
	UniqueTop1 int
	Total      int
}

// EvaluateBenchmark measures Recall@5 and Top-1 diversity across queries.
func (e *Engine) EvaluateBenchmark(ctx context.Context, alpha float64, queries []EvalQuery) EvalReport {
	rep := EvalReport{Alpha: alpha, Total: len(queries)}
	if len(queries) == 0 {
		return rep
	}
	hits := 0
	top1Set := make(map[string]struct{})
	for _, q := range queries {
		res, err := e.Search(ctx, SearchOptions{Query: q.Query, TopK: 5})
		if err != nil || len(res) == 0 {
			continue
		}
		top1Set[res[0].Sticker.StickerID] = struct{}{}
		exp := make(map[string]struct{}, len(q.ExpectedIDs))
		for _, id := range q.ExpectedIDs {
			exp[id] = struct{}{}
		}
		for _, r := range res {
			if _, ok := exp[r.Sticker.StickerID]; ok {
				hits++
				break
			}
		}
	}
	rep.RecallAt5 = float64(hits) / float64(len(queries))
	rep.UniqueTop1 = len(top1Set)
	return rep
}

// HashFileSHA256 computes SHA-256 hex digest of a file on disk.
func HashFileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
