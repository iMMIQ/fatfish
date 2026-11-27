package store

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
)

func TestTokenizeCJKAndFTS5(t *testing.T) {
	got := TokenizeCJK("今天NVDA暴涨了大肥鱼摸鱼")
	if got == "" {
		t.Fatal("empty tokens")
	}

	st, err := Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	ch1 := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")
	ch2 := model.MakeChannelID("onebot", "1", model.ChannelGroup, "200")

	m1 := &model.Message{
		ChannelID:  ch1,
		SenderID:   "u1",
		SenderName: "小明",
		Parts:      []model.Part{{Kind: model.PartText, Text: "今天NVDA财报超预期，大肥鱼在摸鱼"}},
		CreatedAt:  time.Now(),
	}
	m2 := &model.Message{
		ChannelID:  ch2,
		SenderID:   "u2",
		SenderName: "小红",
		Parts:      []model.Part{{Kind: model.PartText, Text: "今天NVDA在另一个群"}},
		CreatedAt:  time.Now(),
	}
	if err := st.AppendMessage(ctx, m1); err != nil {
		t.Fatalf("AppendMessage m1: %v", err)
	}
	if err := st.AppendMessage(ctx, m2); err != nil {
		t.Fatalf("AppendMessage m2: %v", err)
	}

	// Channel isolation must prevent ch2 message from appearing in ch1 search.
	hits, err := st.SearchMessagesFTS(ctx, ch1, "NVDA 摸鱼", nil, 0, 0, 10)
	if err != nil {
		t.Fatalf("SearchMessagesFTS: %v", err)
	}
	if len(hits) != 1 || hits[0].ChannelID != ch1 {
		t.Fatalf("expected 1 channel-isolated hit in ch1, got %+v", hits)
	}

	// Verify contentless FTS5 RebuildFTS and PatchMessageParts work cleanly.
	m1.Parts = append(m1.Parts, model.Part{Kind: model.PartAudio, Transcript: "语音转写测试", ASRStatus: "ok"})
	if err := st.PatchMessageParts(ctx, m1); err != nil {
		t.Fatalf("PatchMessageParts: %v", err)
	}
	if err := st.RebuildFTS(ctx); err != nil {
		t.Fatalf("RebuildFTS: %v", err)
	}
	hits2, err := st.SearchMessagesFTS(ctx, ch1, "语音转写", nil, 0, 0, 10)
	if err != nil || len(hits2) != 1 {
		t.Fatalf("SearchMessagesFTS after Patch+Rebuild: hits=%+v err=%v", hits2, err)
	}
}

func TestBlobStoreCanonicalAndChannelMediaOwnership(t *testing.T) {
	st, err := Open(t.TempDir(), 256, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	// Create an opaque 400x200 image -> should resize to 256x128 and encode as deterministic JPEG.
	img := image.NewRGBA(image.Rect(0, 0, 400, 200))
	for y := 0; y < 200; y++ {
		for x := 0; x < 400; x++ {
			img.Set(x, y, color.RGBA{R: 120, G: 180, B: 220, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("png encode: %v", err)
	}

	b1, err := st.Blobs().PutImage(buf.Bytes())
	if err != nil {
		t.Fatalf("PutImage 1: %v", err)
	}
	b2, err := st.Blobs().PutImage(buf.Bytes())
	if err != nil {
		t.Fatalf("PutImage 2: %v", err)
	}
	if b1.SHA256 != b2.SHA256 || b1.MimeType != "image/jpeg" || b1.Width != 256 || b1.Height != 128 {
		t.Fatalf("unexpected canonical blob: b1=%+v b2=%+v", b1, b2)
	}

	ctx := context.Background()
	ch1 := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")
	ch2 := model.MakeChannelID("onebot", "1", model.ChannelGroup, "200")
	asset, err := st.RegisterMediaAsset(ctx, ch1, "u1", b1, time.Hour)
	if err != nil {
		t.Fatalf("RegisterMediaAsset: %v", err)
	}
	if _, err := st.GetMediaAsset(ctx, ch1, asset.MediaID); err != nil {
		t.Fatalf("expected ch1 access to succeed: %v", err)
	}
	if _, err := st.GetMediaAsset(ctx, ch2, asset.MediaID); err == nil {
		t.Fatalf("expected cross-channel media access to fail")
	}
}
