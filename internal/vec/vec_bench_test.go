package vec

import (
	"fmt"
	"math"
	"testing"
)

func BenchmarkFlatF16IndexSearch2048d(b *testing.B) {
	const dim = 2048
	const n = 4096
	idx := NewFlatF16Index(dim, 0.0)
	idx.Reserve(n)
	v := make([]float32, dim)
	for i := 0; i < n; i++ {
		for j := 0; j < dim; j++ {
			v[j] = float32(math.Sin(float64(i*7+j) * 0.01))
		}
		idx.Upsert(fmt.Sprintf("doc_%04d", i), v)
	}
	q := make([]float32, dim)
	for j := 0; j < dim; j++ {
		q[j] = float32(math.Cos(float64(j) * 0.01))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hits := idx.Search(q, 20, nil)
		if len(hits) != 20 {
			b.Fatalf("expected 20 hits, got %d", len(hits))
		}
	}
}

func BenchmarkMMRRerankF16(b *testing.B) {
	const dim = 2048
	const n = 25
	idx := NewFlatF16Index(dim, 0.0)
	idx.Reserve(n)
	v := make([]float32, dim)
	cands := make([]Hit, n)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		for j := 0; j < dim; j++ {
			v[j] = float32(math.Sin(float64(i*13+j) * 0.02))
		}
		id := fmt.Sprintf("stk_%02d", i)
		idx.Upsert(id, v)
		ids[i] = id
		cands[i] = Hit{ID: id, Score: float32(n-i) / float32(n)}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f16s := idx.LookupF16Batch(ids)
		res := MMRRerankF16(cands, f16s, 0.7, 8)
		if len(res) != 8 {
			b.Fatalf("expected 8 results, got %d", len(res))
		}
	}
}
