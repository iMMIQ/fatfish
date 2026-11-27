package vec

import (
	"math"
	"testing"
)

func TestFlatF16IndexDocOnlyCentering(t *testing.T) {
	// Simulate a homogeneous IP sticker pack where all vectors share a strong common bias [10, 0, 0, 0].
	raw := map[string][]float32{
		"happy": {10.0, 1.0, 0.0, 0.0},
		"angry": {10.0, 0.0, 1.0, 0.0},
		"sleep": {10.0, 0.0, 0.0, 1.0},
	}
	idx := NewFlatF16Index(4, 0.7)
	idx.RebuildCentering(raw, 0.9)

	// Query is NOT shifted by mu_doc (only L2 normalized).
	hits := idx.Search([]float32{0.0, 0.0, 1.0, 0.0}, 3, nil)
	if len(hits) != 3 {
		t.Fatalf("expected 3 hits, got %d", len(hits))
	}
	if hits[0].ID != "angry" {
		t.Fatalf("expected angry to rank #1 with doc-only centering, got %+v", hits)
	}
	if hits[0].Score <= hits[1].Score+0.2 {
		t.Fatalf("expected strong margin after doc-only centering, got %+v", hits)
	}
}

func TestEncodeDecodeFloat16AndMMR(t *testing.T) {
	src := []float32{0.5, -0.25, 0.75, 0.125}
	b := EncodeFloat16(src)
	dst := DecodeFloat16(b)
	for i := range src {
		if math.Abs(float64(src[i]-dst[i])) > 1e-3 {
			t.Fatalf("float16 roundtrip mismatch at %d: %v vs %v", i, src[i], dst[i])
		}
	}

	idx := NewFlatF16Index(2, 0)
	idx.Upsert("a1", []float32{1, 0})
	idx.Upsert("a2", []float32{0.999, 0.01}) // near-duplicate of a1
	idx.Upsert("b1", []float32{0, 1})        // diverse

	cands := []Hit{
		{ID: "a1", Score: 0.95},
		{ID: "a2", Score: 0.94},
		{ID: "b1", Score: 0.90},
	}
	got := MMRRerank(cands, idx.Vector, 0.5, 2)
	if len(got) != 2 || got[0].ID != "a1" || got[1].ID != "b1" {
		t.Fatalf("expected MMR to pick diverse b1 over duplicate a2, got %+v", got)
	}
}
