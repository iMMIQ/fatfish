package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/sticker"
	"github.com/iMMIQ/fatfish/internal/store"
)

// MessageLine matches docs/INGEST_FORMAT.md Section 1.
type MessageLine struct {
	ChannelID     string       `json:"channel_id"`
	PlatformMsgID string       `json:"platform_msg_id,omitempty"`
	SenderID      string       `json:"sender_id"`
	SenderName    string       `json:"sender_name"`
	FromBot       bool         `json:"from_bot,omitempty"`
	CreatedAtMs   int64        `json:"created_at_ms"`
	Parts         []model.Part `json:"parts"`
}

// StickerManifestLine matches docs/INGEST_FORMAT.md Section 2.
type StickerManifestLine struct {
	File        string `json:"file"`
	OCRText     string `json:"ocr_text,omitempty"`
	IntentText  string `json:"intent_text,omitempty"`
	ContentText string `json:"content_text,omitempty"`
	Caption     string `json:"caption,omitempty"` // Legacy single-field fallback
	Status      string `json:"status,omitempty"`
}

type Report struct {
	Total    int
	Inserted int
	Skipped  int
}

// MessagesJSONL imports generic normalized messages from a .jsonl file in chronological order,
// deduplicating idempotently via (channel_id, created_at_ms, sender_id, sha256(plain_text)).
func MessagesJSONL(ctx context.Context, st *store.Store, path string) (Report, error) {
	f, err := os.Open(path)
	if err != nil {
		return Report{}, err
	}
	defer f.Close()

	var lines []MessageLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var item MessageLine
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			return Report{}, fmt.Errorf("ingest: line %d invalid JSON: %w", lineNo, err)
		}
		if item.ChannelID == "" || item.SenderID == "" || len(item.Parts) == 0 {
			continue
		}
		lines = append(lines, item)
	}
	if err := sc.Err(); err != nil {
		return Report{}, err
	}

	// Sort chronologically per channel so monotonic seq numbers reflect historical order.
	sort.SliceStable(lines, func(i, j int) bool {
		if lines[i].ChannelID != lines[j].ChannelID {
			return lines[i].ChannelID < lines[j].ChannelID
		}
		return lines[i].CreatedAtMs < lines[j].CreatedAtMs
	})

	rep := Report{Total: len(lines)}
	msgs := make([]*model.Message, 0, len(lines))
	for _, it := range lines {
		msgs = append(msgs, &model.Message{
			ChannelID:     model.ChannelID(it.ChannelID),
			PlatformMsgID: it.PlatformMsgID,
			SenderID:      it.SenderID,
			SenderName:    it.SenderName,
			FromBot:       it.FromBot,
			Parts:         it.Parts,
			CreatedAt:     time.UnixMilli(it.CreatedAtMs),
		})
	}
	ins, skp, err := st.IngestMessagesBatch(ctx, msgs)
	rep.Inserted = ins
	rep.Skipped = skp
	return rep, err
}

// StickerDir imports a generic sticker pack directory with optional manifest.jsonl.
// Copies images into destPackDir/<packID>/ (if different) and deduplicates by SHA-256.
func StickerDir(ctx context.Context, st *store.Store, srcDir, destPackDir, packID string) (Report, error) {
	if packID == "" {
		packID = "default"
	}
	manifest := make(map[string]StickerManifestLine)
	manifestPath := filepath.Join(srcDir, "manifest.jsonl")
	if mf, err := os.Open(manifestPath); err == nil {
		sc := bufio.NewScanner(mf)
		sc.Buffer(make([]byte, 0, 512<<10), 8<<20)
		for sc.Scan() {
			raw := strings.TrimSpace(sc.Text())
			if raw == "" {
				continue
			}
			var ml StickerManifestLine
			if json.Unmarshal([]byte(raw), &ml) == nil && ml.File != "" {
				manifest[filepath.Clean(ml.File)] = ml
			}
		}
		mf.Close()
	}

	var files []string
	err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		switch ext {
		case ".png", ".jpg", ".jpeg", ".gif", ".webp":
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return Report{}, err
	}
	sort.Strings(files)

	targetDir := filepath.Join(destPackDir, packID)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return Report{}, err
	}

	rep := Report{Total: len(files)}
	for _, fullPath := range files {
		relSrc, _ := filepath.Rel(srcDir, fullPath)
		hash, err := sticker.HashFileSHA256(fullPath)
		if err != nil {
			rep.Skipped++
			continue
		}
		ext := strings.ToLower(filepath.Ext(fullPath))
		dstName := hash[:16] + ext
		dstFull := filepath.Join(targetDir, dstName)
		if _, err := os.Stat(dstFull); os.IsNotExist(err) {
			if err := copyFile(fullPath, dstFull); err != nil {
				return rep, err
			}
		}
		relStored := filepath.Join(packID, dstName)

		ml, hasManifest := manifest[filepath.Clean(relSrc)]
		if !hasManifest {
			ml, hasManifest = manifest[filepath.Base(fullPath)]
		}
		contentText := strings.TrimSpace(ml.ContentText)
		if contentText == "" && ml.Caption != "" {
			contentText = strings.TrimSpace(ml.Caption)
		}
		status := strings.TrimSpace(ml.Status)
		if status == "" {
			if contentText != "" || ml.IntentText != "" || ml.OCRText != "" {
				status = "ready"
			} else {
				status = "needs_review"
			}
		}

		stk := model.Sticker{
			StickerID:   "stk_" + hash[:12],
			PackID:      packID,
			SHA256:      hash,
			RelPath:     relStored,
			Status:      status,
			OCRText:     strings.TrimSpace(ml.OCRText),
			IntentText:  strings.TrimSpace(ml.IntentText),
			ContentText: contentText,
			CreatedAt:   time.Now(),
		}
		if err := st.UpsertSticker(ctx, stk, nil, nil); err != nil {
			return rep, err
		}
		rep.Inserted++
	}
	return rep, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
