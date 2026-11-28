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
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/vec"
)

// SearchHit provides full scoring breakdown (also shown on the Console Sticker Manager).
type SearchHit struct {
	Sticker      model.Sticker `json:"sticker"`
	FinalScore   float64       `json:"final_score"`
	IntentScore  float64       `json:"intent_score"`
	ContentScore float64       `json:"content_score"`
	FTSScore     float64       `json:"fts_score"`
	RRFScore     float64       `json:"rrf_score"`
	RerankScore  float64       `json:"rerank_score"`
	DecayFactor  float64       `json:"decay_factor"`
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
	if mean := e.loadMeanMeta(ctx, "sticker_mean_intent"); len(mean) == e.dim {
		intentIdx.SetMean(mean, float32(cfg.DocCenteringAlpha))
	}
	if mean := e.loadMeanMeta(ctx, "sticker_mean_content"); len(mean) == e.dim {
		contentIdx.SetMean(mean, float32(cfg.DocCenteringAlpha))
	}

	e.mu.Lock()
	e.stickers = stMap
	e.intentIdx = intentIdx
	e.contentIdx = contentIdx
	e.mu.Unlock()
	return len(stMap), nil
}

func (e *Engine) saveMeanMeta(ctx context.Context, key string, mean []float32) {
	if e.store == nil || len(mean) != e.dim {
		return
	}
	raw := vec.EncodeFloat16(mean)
	_ = e.store.SetMeta(ctx, key, base64.StdEncoding.EncodeToString(raw))
}

func (e *Engine) loadMeanMeta(ctx context.Context, key string) []float32 {
	if e.store == nil {
		return nil
	}
	b64, err := e.store.GetMeta(ctx, key)
	if err != nil || b64 == "" {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != e.dim*2 {
		return nil
	}
	return vec.DecodeFloat16(raw)
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

	// Run FTS5 recall, Embed + parallel dual-vector scan, and recent-use decay queries concurrently.
	var (
		ftsIDs       []string
		intentHits   []vec.Hit
		contentHits  []vec.Hit
		recent       []string
		globalRecent []string
	)
	e.mu.RLock()
	hasVecs := e.intentIdx.Len() > 0 || e.contentIdx.Len() > 0
	e.mu.RUnlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ftsIDs, _ = e.store.SearchStickersFTS(ctx, opts.Query, opts.PackID, 20)
	}()
	go func() {
		defer wg.Done()
		globalRecent, _ = e.store.RecentStickerUsesGlobal(ctx, 12)
	}()
	if opts.ChannelID != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recent, _ = e.store.RecentStickerUses(ctx, opts.ChannelID, cfg.RecentWindow)
		}()
	}
	if e.hub != nil && hasVecs && strings.TrimSpace(opts.Query) != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if embs, err := e.hub.Embed(ctx, []string{opts.Query}); err == nil && len(embs) == 1 && len(embs[0]) == e.dim {
				q := embs[0]
				e.mu.RLock()
				// Note: caller holds e.mu.RLock() across both read-only vector scans; filter never re-locks e.mu.
				filter := func(id string) bool {
					if opts.PackID == "" {
						return true
					}
					st, ok := e.stickers[id]
					return ok && st.PackID == opts.PackID
				}
				var vwg sync.WaitGroup
				vwg.Add(2)
				go func() {
					defer vwg.Done()
					intentHits = e.intentIdx.Search(q, 30, filter)
				}()
				go func() {
					defer vwg.Done()
					contentHits = e.contentIdx.Search(q, 30, filter)
				}()
				vwg.Wait()
				e.mu.RUnlock()
			}
		}()
	}
	wg.Wait()

	ftsHits := make([]vec.Hit, 0, len(ftsIDs))
	for i, id := range ftsIDs {
		ftsHits = append(ftsHits, vec.Hit{ID: id, Score: float32(20 - i)})
	}

	// Build score breakdown maps for console diagnostics.
	ftsScoreMap := make(map[string]float64, len(ftsHits))
	for _, h := range ftsHits {
		ftsScoreMap[h.ID] = float64(h.Score)
	}
	intentScoreMap := make(map[string]float64, len(intentHits))
	for _, h := range intentHits {
		intentScoreMap[h.ID] = float64(h.Score)
	}
	contentScoreMap := make(map[string]float64, len(contentHits))
	for _, h := range contentHits {
		contentScoreMap[h.ID] = float64(h.Score)
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
				Sticker:      st,
				IntentScore:  intentScoreMap[h.ID],
				ContentScore: contentScoreMap[h.ID],
				FTSScore:     ftsScoreMap[h.ID],
				RRFScore:     float64(h.Score),
				FinalScore:   float64(h.Score),
				DecayFactor:  1.0,
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

	// 5. Exponential anti-repeat decay using pre-fetched recent-use lists (zero extra DB latency after Rerank).
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
				cands[i].DecayFactor *= decay
				cands[i].FinalScore *= decay
			}
		}
	}
	if len(globalRecent) > 0 {
		gPos := make(map[string]int, len(globalRecent))
		for idx, id := range globalRecent {
			if _, seen := gPos[id]; !seen {
				gPos[id] = idx
			}
		}
		for i := range cands {
			if pos, used := gPos[cands[i].Sticker.StickerID]; used {
				decay := 1.0 - 0.45*math.Exp(-float64(pos)/5.0)
				cands[i].DecayFactor *= decay
				cands[i].FinalScore *= decay
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

// PickOne selects a sticker from Top-8 using calibrated softmax sampling for natural variety,
// preferring files within cfg.MaxSendMiB to avoid OneBot base64 WS timeouts and memory spikes.
func (e *Engine) PickOne(ctx context.Context, opts SearchOptions) (*SearchHit, error) {
	opts.TopK = 8
	hits, err := e.Search(ctx, opts)
	if err != nil || len(hits) == 0 {
		return nil, err
	}
	if maxMiB := e.getConfig().MaxSendMiB; maxMiB > 0 {
		maxBytes := int64(maxMiB) << 20
		var within []SearchHit
		for _, h := range hits {
			if sz := e.stickerFileSize(h.Sticker); sz <= 0 || sz <= maxBytes {
				within = append(within, h)
			}
		}
		if len(within) > 0 {
			hits = within
		}
	}
	if len(hits) == 1 {
		return &hits[0], nil
	}
	temp := 0.25
	maxS := hits[0].FinalScore
	minS := hits[len(hits)-1].FinalScore
	if span := maxS - minS; span > 0 && span < 0.1 {
		// Calibrate softmax temperature when rerank was skipped and RRF scores are compressed (~0.01-0.04).
		temp = math.Max(span*0.55, 0.003)
	}
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

func (e *Engine) stickerFileSize(st model.Sticker) int64 {
	p := st.RelPath
	if !filepath.IsAbs(p) {
		p = filepath.Join(e.getConfig().PackDir, p)
	}
	fi, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return fi.Size()
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
	var enrichedCnt, taggedCnt atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for _, r := range rows {
		st := r.Sticker
		if st.Status == "ready" && strings.TrimSpace(st.IntentText) == "" && strings.TrimSpace(st.ContentText+st.OCRText) != "" {
			g.Go(func() error {
				resp, cerr := e.hub.Chat(gctx, "fast", infer.ChatRequest{
					Messages: []infer.ChatMessage{
						{Role: "system", Content: "Given a sticker visual description/OCR, output a concise comma-separated list of emotions, tones, and chat reply scenes (intent_text) in Chinese. Output plain text only."},
						{Role: "user", Content: strings.TrimSpace(st.ContentText + " " + st.OCRText)},
					},
				})
				if cerr == nil && strings.TrimSpace(resp.Content) != "" {
					if uerr := e.store.UpdateStickerStatus(gctx, st.StickerID, st.Status, st.OCRText, strings.TrimSpace(resp.Content), st.ContentText); uerr == nil {
						enrichedCnt.Add(1)
					}
				}
				return nil
			})
			continue
		}
		if tagUntagged && st.Status == "needs_review" && strings.TrimSpace(st.ContentText) == "" && strings.TrimSpace(st.IntentText) == "" {
			g.Go(func() error {
				raw, rerr := e.ReadStickerFile(st)
				if rerr != nil || len(raw) == 0 {
					return nil
				}
				dataURI := "data:" + MimeTypeFor(st) + ";base64," + base64.StdEncoding.EncodeToString(raw)
				resp, cerr := e.hub.Chat(gctx, "primary", infer.ChatRequest{
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
					return nil
				}
				var parsed struct {
					OCRText     string `json:"ocr_text"`
					IntentText  string `json:"intent_text"`
					ContentText string `json:"content_text"`
				}
				if json.Unmarshal([]byte(resp.Content), &parsed) == nil && (parsed.IntentText != "" || parsed.ContentText != "") {
					if uerr := e.store.UpdateStickerStatus(gctx, st.StickerID, "needs_review", strings.TrimSpace(parsed.OCRText), strings.TrimSpace(parsed.IntentText), strings.TrimSpace(parsed.ContentText)); uerr == nil {
						taggedCnt.Add(1)
					}
				}
				return nil
			})
		}
	}
	_ = g.Wait()
	return int(enrichedCnt.Load()), int(taggedCnt.Load()), nil
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
					alpha := float32(e.getConfig().DocCenteringAlpha)
					iv := embs[0]
					cv := embs[1]
					if mean := e.loadMeanMeta(ctx, "sticker_mean_intent"); len(mean) == e.dim && alpha > 0 {
						for j := range iv {
							iv[j] -= alpha * mean[j]
						}
					}
					if mean := e.loadMeanMeta(ctx, "sticker_mean_content"); len(mean) == e.dim && alpha > 0 {
						for j := range cv {
							cv[j] -= alpha * mean[j]
						}
					}
					vec.NormalizeInPlace(iv)
					vec.NormalizeInPlace(cv)
					_ = e.store.UpdateStickerVectors(ctx, stickerID, vec.EncodeFloat16(iv), vec.EncodeFloat16(cv))
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

	var intentEmbs, contentEmbs [][]float32
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var eerr error
		intentEmbs, eerr = e.hub.Embed(ectx, intentTexts)
		if eerr != nil {
			return fmt.Errorf("sticker: embed intent: %w", eerr)
		}
		return nil
	})
	eg.Go(func() error {
		var eerr error
		contentEmbs, eerr = e.hub.Embed(ectx, contentTexts)
		if eerr != nil {
			return fmt.Errorf("sticker: embed content: %w", eerr)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return 0, err
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
	intentMean := iIdx.RebuildCentering(rawIntent, float32(alpha))
	contentMean := cIdx.RebuildCentering(rawContent, float32(alpha))
	e.saveMeanMeta(ctx, "sticker_mean_intent", intentMean)
	e.saveMeanMeta(ctx, "sticker_mean_content", contentMean)

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
