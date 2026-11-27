package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
)

func TestMessagesJSONLAndStickerDirIdempotent(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	// Out-of-order messages JSONL
	jsonlPath := filepath.Join(dir, "history.jsonl")
	content := `{"channel_id":"onebot:1:group:100","sender_id":"u2","sender_name":"Bob","created_at_ms":1760000002000,"parts":[{"kind":"text","text":"second"}]}
{"channel_id":"onebot:1:group:100","sender_id":"u1","sender_name":"Alice","created_at_ms":1760000001000,"parts":[{"kind":"text","text":"first"}]}
`
	_ = os.WriteFile(jsonlPath, []byte(content), 0o600)

	ctx := context.Background()
	r1, err := MessagesJSONL(ctx, st, jsonlPath)
	if err != nil || r1.Inserted != 2 {
		t.Fatalf("first ingest: %+v err=%v", r1, err)
	}
	// Re-running ingest must be 100% idempotent.
	r2, err := MessagesJSONL(ctx, st, jsonlPath)
	if err != nil || r2.Inserted != 0 || r2.Skipped != 2 {
		t.Fatalf("second ingest should skip duplicates: %+v err=%v", r2, err)
	}
	msgs, _ := st.RecentMessages(ctx, model.ChannelID("onebot:1:group:100"), 10)
	if len(msgs) != 2 || msgs[0].PlainText() != "first" || msgs[1].PlainText() != "second" {
		t.Fatalf("expected chronological ordering, got %+v", msgs)
	}

	// Sticker pack ingest with manifest.jsonl
	stkSrc := filepath.Join(dir, "src_pack")
	_ = os.MkdirAll(stkSrc, 0o755)
	_ = os.WriteFile(filepath.Join(stkSrc, "01.png"), []byte("fake-png-bytes"), 0o600)
	_ = os.WriteFile(filepath.Join(stkSrc, "manifest.jsonl"), []byte(`{"file":"01.png","ocr_text":"摸鱼","intent_text":"slacking","content_text":"fish"}`+"\n"), 0o600)

	sr, err := StickerDir(ctx, st, stkSrc, filepath.Join(dir, "stickers"), "testpack")
	if err != nil || sr.Inserted != 1 {
		t.Fatalf("StickerDir: %+v err=%v", sr, err)
	}
	rows, _ := st.ListStickers(ctx, "ready")
	if len(rows) != 1 || rows[0].Sticker.OCRText != "摸鱼" {
		t.Fatalf("unexpected sticker rows: %+v", rows)
	}
}
