package vec

import (
	"encoding/binary"
	"math"
	"sort"
	"sync"

	"github.com/x448/float16"
)

// Hit represents a single scored nearest-neighbor result.
type Hit struct {
	ID    string
	Score float32
}

// Index abstracts vector storage and search so flat float16 scan can be swapped later if needed.
type Index interface {
	Upsert(id string, v []float32)
	Delete(id string)
	Search(q []float32, k int, filter func(id string) bool) []Hit
	Len() int
}

// FlatF16Index stores L2-normalized vectors in a contiguous []uint16 slice (IEEE 754 binary16).
// It optionally applies document-only mean-centering: d'_i = norm(d_i - alpha * mu_doc)
// while leaving query vectors q strictly unshifted (only L2-normalized).
type FlatF16Index struct {
	mu    sync.RWMutex
	dim   int
	alpha float32
	mean  []float32 // corpus mean (doc-only centering)

	ids  []string
	pos  map[string]int
	data []uint16 // len = len(ids) * dim
}

// NewFlatF16Index creates a pure-Go float16 flat vector index.
func NewFlatF16Index(dim int, docCenteringAlpha float32) *FlatF16Index {
	return &FlatF16Index{
		dim:   dim,
		alpha: docCenteringAlpha,
		pos:   make(map[string]int),
	}
}

// SetMean configures the corpus mean vector used for document-only centering.
func (idx *FlatF16Index) SetMean(mean []float32, alpha float32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.alpha = alpha
	if len(mean) == idx.dim {
		idx.mean = append([]float32(nil), mean...)
	} else {
		idx.mean = nil
	}
}

// RebuildCentering recomputes mu_doc from all raw vectors and applies d'_i = norm(d_i - alpha * mu_doc).
func (idx *FlatF16Index) RebuildCentering(raw map[string][]float32, alpha float32) []float32 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.alpha = alpha
	idx.ids = idx.ids[:0]
	idx.data = idx.data[:0]
	idx.pos = make(map[string]int, len(raw))

	if len(raw) == 0 {
		idx.mean = nil
		return nil
	}

	mean := make([]float32, idx.dim)
	validCount := 0
	for _, v := range raw {
		if len(v) != idx.dim {
			continue
		}
		for i := 0; i < idx.dim; i++ {
			mean[i] += v[i]
		}
		validCount++
	}
	if validCount == 0 {
		idx.mean = nil
		return nil
	}
	inv := float32(1.0 / float64(validCount))
	for i := range mean {
		mean[i] *= inv
	}
	idx.mean = mean

	// Sort IDs for deterministic layout.
	sortedIDs := make([]string, 0, len(raw))
	for id, v := range raw {
		if len(v) == idx.dim {
			sortedIDs = append(sortedIDs, id)
		}
	}
	sort.Strings(sortedIDs)

	idx.ids = make([]string, 0, len(sortedIDs))
	idx.data = make([]uint16, 0, len(sortedIDs)*idx.dim)
	tmp := make([]float32, idx.dim)
	for _, id := range sortedIDs {
		v := raw[id]
		for i := 0; i < idx.dim; i++ {
			tmp[i] = v[i] - alpha*mean[i]
		}
		NormalizeInPlace(tmp)
		idx.pos[id] = len(idx.ids)
		idx.ids = append(idx.ids, id)
		for i := 0; i < idx.dim; i++ {
			idx.data = append(idx.data, float16.Fromfloat32(tmp[i]).Bits())
		}
	}
	return append([]float32(nil), mean...)
}

// Upsert inserts or replaces a document vector, applying doc-only centering if configured.
func (idx *FlatF16Index) Upsert(id string, v []float32) {
	if len(v) != idx.dim {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tmp := make([]float32, idx.dim)
	copy(tmp, v)
	if idx.alpha > 0 && len(idx.mean) == idx.dim {
		for i := 0; i < idx.dim; i++ {
			tmp[i] -= idx.alpha * idx.mean[i]
		}
	}
	NormalizeInPlace(tmp)

	if slot, exists := idx.pos[id]; exists {
		off := slot * idx.dim
		for i := 0; i < idx.dim; i++ {
			idx.data[off+i] = float16.Fromfloat32(tmp[i]).Bits()
		}
		return
	}

	slot := len(idx.ids)
	idx.pos[id] = slot
	idx.ids = append(idx.ids, id)
	for i := 0; i < idx.dim; i++ {
		idx.data = append(idx.data, float16.Fromfloat32(tmp[i]).Bits())
	}
}

// UpsertPreencoded inserts an already normalized/centered float16 vector directly (used on DB boot).
func (idx *FlatF16Index) UpsertPreencoded(id string, blob []byte) bool {
	if len(blob) != idx.dim*2 {
		return false
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if slot, exists := idx.pos[id]; exists {
		off := slot * idx.dim
		for i := 0; i < idx.dim; i++ {
			idx.data[off+i] = binary.LittleEndian.Uint16(blob[i*2:])
		}
		return true
	}
	slot := len(idx.ids)
	idx.pos[id] = slot
	idx.ids = append(idx.ids, id)
	for i := 0; i < idx.dim; i++ {
		idx.data = append(idx.data, binary.LittleEndian.Uint16(blob[i*2:]))
	}
	return true
}

// Delete removes id via swap-delete in O(1).
func (idx *FlatF16Index) Delete(id string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	slot, ok := idx.pos[id]
	if !ok {
		return
	}
	last := len(idx.ids) - 1
	if slot != last {
		lastID := idx.ids[last]
		idx.ids[slot] = lastID
		idx.pos[lastID] = slot
		copy(idx.data[slot*idx.dim:(slot+1)*idx.dim], idx.data[last*idx.dim:(last+1)*idx.dim])
	}
	delete(idx.pos, id)
	idx.ids = idx.ids[:last]
	idx.data = idx.data[:last*idx.dim]
}

// Len returns the number of indexed vectors.
func (idx *FlatF16Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.ids)
}

// Vector returns the decoded float32 vector for an indexed id (used by MMR diversity).
func (idx *FlatF16Index) Vector(id string) ([]float32, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	slot, ok := idx.pos[id]
	if !ok {
		return nil, false
	}
	out := make([]float32, idx.dim)
	off := slot * idx.dim
	for i := 0; i < idx.dim; i++ {
		out[i] = float16.Frombits(idx.data[off+i]).Float32()
	}
	return out, true
}

// Search performs a sequential L2-normalized dot product scan against unshifted query q.
func (idx *FlatF16Index) Search(q []float32, k int, filter func(id string) bool) []Hit {
	if k <= 0 || len(q) != idx.dim {
		return nil
	}
	qn := make([]float32, idx.dim)
	copy(qn, q)
	if !NormalizeInPlace(qn) {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	n := len(idx.ids)
	if n == 0 {
		return nil
	}
	if k > n {
		k = n
	}

	hits := make([]Hit, 0, n)
	dim := idx.dim
	data := idx.data
	for slot := 0; slot < n; slot++ {
		id := idx.ids[slot]
		if filter != nil && !filter(id) {
			continue
		}
		off := slot * dim
		var dot float32
		for i := 0; i < dim; i++ {
			dot += qn[i] * float16.Frombits(data[off+i]).Float32()
		}
		hits = append(hits, Hit{ID: id, Score: dot})
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].ID < hits[j].ID
		}
		return hits[i].Score > hits[j].Score
	})
	if len(hits) > k {
		hits = hits[:k]
	}
	return hits
}

// NormalizeInPlace L2-normalizes v in place. Returns false if norm is 0.
func NormalizeInPlace(v []float32) bool {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
		return false
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return true
}

// Dot computes the inner product of two equal-length vectors.
func Dot(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var s float32
	for i := 0; i < n; i++ {
		s += a[i] * b[i]
	}
	return s
}

// EncodeFloat16 serializes a float32 slice as little-endian IEEE 754 binary16 bytes.
func EncodeFloat16(v []float32) []byte {
	out := make([]byte, len(v)*2)
	for i, x := range v {
		binary.LittleEndian.PutUint16(out[i*2:], float16.Fromfloat32(x).Bits())
	}
	return out
}

// DecodeFloat16 deserializes little-endian IEEE 754 binary16 bytes into float32.
func DecodeFloat16(b []byte) []float32 {
	n := len(b) / 2
	out := make([]float32, n)
	for i := 0; i < n; i++ {
		out[i] = float16.Frombits(binary.LittleEndian.Uint16(b[i*2:])).Float32()
	}
	return out
}

// WeightedRRF fuses multiple ranked candidate lists via Reciprocal Rank Fusion.
// kConst is typically 60.
func WeightedRRF(lists [][]Hit, weights []float32, kConst float32, topK int) []Hit {
	if kConst <= 0 {
		kConst = 60
	}
	scores := make(map[string]float32)
	for li, list := range lists {
		w := float32(1.0)
		if li < len(weights) {
			w = weights[li]
		}
		for rank, h := range list {
			scores[h.ID] += w / (kConst + float32(rank+1))
		}
	}
	out := make([]Hit, 0, len(scores))
	for id, sc := range scores {
		out = append(out, Hit{ID: id, Score: sc})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].ID < out[j].ID
		}
		return out[i].Score > out[j].Score
	})
	if topK > 0 && len(out) > topK {
		out = out[:topK]
	}
	return out
}

// MMRRerank selects topK diverse candidates balancing relevance (lambda) and dissimilarity (1-lambda).
func MMRRerank(candidates []Hit, vecLookup func(id string) ([]float32, bool), lambda float32, topK int) []Hit {
	if len(candidates) == 0 || topK <= 0 {
		return nil
	}
	if topK > len(candidates) {
		topK = len(candidates)
	}
	if lambda <= 0 {
		lambda = 0.7
	}
	if lambda > 1 {
		lambda = 1
	}

	// Normalize relevance scores to [0, 1] for stable lambda blending.
	minS, maxS := candidates[0].Score, candidates[0].Score
	for _, c := range candidates[1:] {
		if c.Score < minS {
			minS = c.Score
		}
		if c.Score > maxS {
			maxS = c.Score
		}
	}
	normScore := func(s float32) float32 {
		if maxS == minS {
			return 1
		}
		return (s - minS) / (maxS - minS)
	}

	vecs := make(map[string][]float32, len(candidates))
	if vecLookup != nil {
		for _, c := range candidates {
			if v, ok := vecLookup(c.ID); ok {
				vecs[c.ID] = v
			}
		}
	}

	selected := make([]Hit, 0, topK)
	used := make([]bool, len(candidates))
	for len(selected) < topK {
		bestIdx := -1
		bestVal := float32(math.Inf(-1))
		for i, c := range candidates {
			if used[i] {
				continue
			}
			rel := normScore(c.Score)
			var maxSim float32
			if vc, ok := vecs[c.ID]; ok {
				for _, s := range selected {
					if vs, ok2 := vecs[s.ID]; ok2 {
						sim := Dot(vc, vs)
						if sim > maxSim {
							maxSim = sim
						}
					}
				}
			}
			score := lambda*rel - (1-lambda)*maxSim
			if score > bestVal {
				bestVal = score
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		used[bestIdx] = true
		selected = append(selected, candidates[bestIdx])
	}
	return selected
}
