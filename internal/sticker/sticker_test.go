package sticker

import (
	"context"
	"testing"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/vec"
)

func TestStickerHybridSearchAndAntiRepeatDecay(t *testing.T) {
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	s1 := model.Sticker{
		StickerID:   "stk_1",
		PackID:      "default",
		SHA256:      "aaa1",
		RelPath:     "1.png",
		Status:      "ready",
		OCRText:     "无语",
		IntentText:  "无语 摊手 敷衍",
		ContentText: "鱼 摊手",
	}
	s2 := model.Sticker{
		StickerID:   "stk_2",
		PackID:      "default",
		SHA256:      "aaa2",
		RelPath:     "2.png",
		Status:      "ready",
		OCRText:     "无语",
		IntentText:  "无语 白眼 嫌弃",
		ContentText: "鱼 翻白眼",
	}
	_ = st.UpsertSticker(ctx, s1, vec.EncodeFloat16([]float32{1, 0, 0, 0}), vec.EncodeFloat16([]float32{1, 0, 0, 0}))
	_ = st.UpsertSticker(ctx, s2, vec.EncodeFloat16([]float32{0, 1, 0, 0}), vec.EncodeFloat16([]float32{0, 1, 0, 0}))

	eng := NewEngine(st, nil, config.StickerConfig{
		Enabled:              true,
		DocCenteringAlpha:    0.7,
		ReactionIntentWeight: 0.75,
		ExplicitIntentWeight: 0.25,
		RecentWindow:         30,
		MMRLambda:            0.7,
	}, 4, nil)
	if n, err := eng.LoadIndex(ctx); err != nil || n != 2 {
		t.Fatalf("LoadIndex: n=%d err=%v", n, err)
	}

	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")
	// Record stk_1 as just used -> stk_2 should overtake stk_1 via anti-repeat decay!
	_ = st.RecordStickerUse(ctx, ch, "stk_1")

	hits, err := eng.Search(ctx, SearchOptions{ChannelID: ch, Query: "无语", TopK: 2})
	if err != nil || len(hits) != 2 {
		t.Fatalf("Search: hits=%+v err=%v", hits, err)
	}
	if hits[0].Sticker.StickerID != "stk_2" {
		t.Fatalf("expected stk_2 to rank ahead of recently-used stk_1, got %+v", hits)
	}
	if hits[1].DecayFactor >= 0.5 {
		t.Fatalf("expected strong decay penalty on recently-used stk_1, got %v", hits[1].DecayFactor)
	}
}
