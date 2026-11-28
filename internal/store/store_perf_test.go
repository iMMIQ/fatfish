package store

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

func TestStoreStreamingAndHistoricalPrune(t *testing.T) {
	st, err := Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")

	// 1. Episode embed stats & streaming
	blob := make([]byte, 8)
	binary.LittleEndian.PutUint16(blob[0:], 0x3c00) // 1.0
	ep := &model.Episode{
		ChannelID: ch,
		StartSeq:  1,
		EndSeq:    10,
		StartTS:   time.Now().Add(-time.Hour),
		EndTS:     time.Now(),
		Topic:     "测试主题",
		Summary:   "测试摘要",
	}
	if err := st.SaveEpisode(ctx, ep, blob); err != nil {
		t.Fatalf("SaveEpisode: %v", err)
	}
	cnt, maxID, err := st.EpisodeEmbedStats(ctx)
	if err != nil || cnt != 1 || maxID != ep.ID {
		t.Fatalf("EpisodeEmbedStats got cnt=%d maxID=%d err=%v", cnt, maxID, err)
	}
	streamed := 0
	if _, err := st.StreamEpisodeEmbeds(ctx, func(id int64, c model.ChannelID, b []byte) {
		if id == ep.ID && c == ch && len(b) == len(blob) {
			streamed++
		}
	}); err != nil || streamed != 1 {
		t.Fatalf("StreamEpisodeEmbeds got streamed=%d err=%v", streamed, err)
	}

	// 2. Idle open segments & historical prune
	oldSeg := &SegmentRecord{
		ChannelID:      ch,
		Epoch:          1,
		ScopeHash:      "s1",
		PinnedSnapshot: "snap",
		CreatedAt:      time.Now().Add(-2 * time.Hour),
		LastActiveAt:   time.Now().Add(-45 * time.Minute),
	}
	if err := st.CreateSegment(ctx, oldSeg); err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}
	idle, err := st.ListIdleOpenSegments(ctx, time.Now().Add(-30*time.Minute), 10)
	if err != nil || len(idle) != 1 || idle[0].ID != oldSeg.ID {
		t.Fatalf("ListIdleOpenSegments got %d err=%v", len(idle), err)
	}

	// Insert old KV metric (>30d) and verify PruneHistoricalData removes it.
	if err := st.RecordKVMetric(ctx, model.KVMetric{
		ChannelID: ch,
		SegmentID: oldSeg.ID,
		ModelID:   "m",
		Attempt:   1,
		CreatedAt: time.Now().AddDate(0, 0, -45),
	}); err != nil {
		t.Fatalf("RecordKVMetric: %v", err)
	}
	pruned, err := st.PruneHistoricalData(ctx, time.Now())
	if err != nil || pruned < 1 {
		t.Fatalf("PruneHistoricalData expected >=1 pruned, got %d err=%v", pruned, err)
	}

	// 3. Streaming RebuildFTS
	_ = st.AppendMessage(ctx, &model.Message{
		ChannelID:  ch,
		SenderID:   "u1",
		SenderName: "Alice",
		Parts:      []model.Part{{Kind: model.PartText, Text: "周末去杭州西湖喝龙井茶"}},
		CreatedAt:  time.Now(),
	})
	if err := st.RebuildFTS(ctx); err != nil {
		t.Fatalf("RebuildFTS: %v", err)
	}
	hits, err := st.SearchMessagesFTS(ctx, ch, "西湖", nil, 0, 0, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("expected 1 FTS hit after RebuildFTS, got %d err=%v", len(hits), err)
	}
	_ = math.NaN()
}

func BenchmarkTokenizeCJK(b *testing.B) {
	sample := "今天下午去杭州西湖边喝龙井茶顺便看看NVDA和AMD的财报k8s集群部署状态"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if out := TokenizeCJK(sample); out == "" {
			b.Fatal("empty tokens")
		}
	}
}
