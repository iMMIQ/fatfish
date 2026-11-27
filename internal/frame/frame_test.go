package frame

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
)

// TestKVCacheInvariants is a mandatory CI guard from SPEC 6.2.
// Simulates 10 multi-turn interactions with images, tool calls, mid-segment new members,
// and verifies tools, msg[0], msg[1] (Segment-Pinned Snapshot), and all FrozenFrame[0..i-1]
// remain 100% bit-for-bit identical across turns.
func TestKVCacheInvariants(t *testing.T) {
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer st.Close()

	cfg := config.FrameConfig{
		InputTokenBudget:   32768,
		TrimHighWater:      0.80,
		TrimLowWater:       0.50,
		SegmentIdleMinutes: 30,
		SummaryTokenBudget: 512,
		PinnedSnapshotBudgets: config.PinnedSnapshotBudgets{
			Lore:     120,
			Notes:    140,
			Profiles: 220,
		},
		MaxImagesPerRequest: 6,
		ImageTokenReserve:   1600,
	}
	builder := NewBuilder(st, cfg)
	ctx := context.Background()
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "55555")

	// Prepare one canonical test image.
	img := image.NewRGBA(image.Rect(0, 0, 64, 64))
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x * 3), G: 100, B: 200, A: 255})
		}
	}
	var imgBuf bytes.Buffer
	_ = png.Encode(&imgBuf, img)
	blob, err := st.Blobs().PutImage(imgBuf.Bytes())
	if err != nil {
		t.Fatalf("PutImage: %v", err)
	}
	asset, err := st.RegisterMediaAsset(ctx, ch, "u1", blob, time.Hour)
	if err != nil {
		t.Fatalf("RegisterMediaAsset: %v", err)
	}

	persona := "You are fatfish test persona."
	scopeHash := ScopeHash("persona_hash_v1", "tools_hash_frozen_v1", "test-model")

	// Frozen initial snapshot returned on segment creation.
	snapshotCalls := 0
	seg, _, err := builder.EnsureActiveSegment(ctx, ch, 1, scopeHash, func(context.Context) (SnapshotInput, error) {
		snapshotCalls++
		return SnapshotInput{
			Lore:        []model.LoreEntry{{Term: "摸鱼", Meaning: "slacking playfully"}},
			PinnedNotes: []model.Note{{Key: "rule", Content: "no spoilers"}},
			MemberCards: []string{"u1(Alice): engineer"},
		}, nil
	})
	if err != nil {
		t.Fatalf("EnsureActiveSegment: %v", err)
	}

	var prevSerialized [][]byte
	for turn := 1; turn <= 10; turn++ {
		// Even if DB notes/lore change mid-segment, EnsureActiveSegment MUST keep the same pinned snapshot.
		_ = st.SaveNote(ctx, model.Note{ChannelID: ch, Key: fmt.Sprintf("k%d", turn), Content: "mid-segment mutation", AuthorID: "u1", Pinned: true}, 50)

		seg2, _, err := builder.EnsureActiveSegment(ctx, ch, 1, scopeHash, func(context.Context) (SnapshotInput, error) {
			snapshotCalls++
			return SnapshotInput{PinnedNotes: []model.Note{{Key: "mutated", Content: "MUST NOT APPEAR"}}}, nil
		})
		if err != nil {
			t.Fatalf("turn %d EnsureActiveSegment: %v", turn, err)
		}
		if seg2.ID != seg.ID {
			t.Fatalf("turn %d unexpectedly rotated segment", turn)
		}

		// Turn 3 includes an image; turn 5 includes a mid-segment new member delta.
		var parts []model.Part
		parts = append(parts, model.Part{Kind: model.PartText, Text: fmt.Sprintf("turn %d hello", turn)})
		if turn == 3 {
			parts = append(parts, model.Part{Kind: model.PartImage, MediaID: asset.MediaID, SHA256: asset.SHA256, MimeType: asset.MimeType})
		}
		var delta []string
		if turn == 5 {
			delta = []string{"u2(Bob): likes coffee"}
		}

		userPayload := FormatUserTurn([]model.Message{{
			ChannelID:  ch,
			SenderID:   "u1",
			SenderName: "Alice",
			Parts:      parts,
			CreatedAt:  time.Unix(1760000000+int64(turn*60), 0),
		}}, time.UTC, delta)

		msgs, _, _, _, err := builder.Assemble(ctx, seg2, persona, &userPayload, "primary|test|m")
		if err != nil {
			t.Fatalf("turn %d Assemble: %v", turn, err)
		}

		// Assert EVERY message from previous turn prefix [0..len(prevSerialized)-1] is 100% bit-for-bit identical!
		for i := 0; i < len(prevSerialized); i++ {
			curBytes, _ := json.Marshal(msgs[i])
			if !bytes.Equal(curBytes, prevSerialized[i]) {
				t.Fatalf("KV prefix drift at turn %d msg[%d]!\nprev=%s\ncurr=%s", turn, i, string(prevSerialized[i]), string(curBytes))
			}
		}

		// Freeze this turn's user + optional tool call + assistant frames.
		toFreeze := []StoredFramePayload{userPayload}
		if turn == 4 {
			toFreeze = append(toFreeze,
				StoredFramePayload{
					Role: "assistant",
					ToolCalls: []infer.ToolCall{{
						ID:   "call_1",
						Type: "function",
						Function: infer.ToolCallFunction{
							Name:      "recall",
							Arguments: `{"mode":"recent"}`,
						},
					}},
				},
				StoredFramePayload{
					Role:       "tool",
					ToolCallID: "call_1",
					Name:       "recall",
					Text:       "recent history ok",
				},
			)
		}
		toFreeze = append(toFreeze, StoredFramePayload{
			Role: "assistant",
			Text: fmt.Sprintf("reply %d", turn),
		})

		if err := builder.AppendFrozenFrames(ctx, seg2, toFreeze...); err != nil {
			t.Fatalf("turn %d AppendFrozenFrames: %v", turn, err)
		}

		// Snapshot the now-frozen messages [0..end] to compare against turn+1.
		frozenMsgs, _, _, _, err := builder.Assemble(ctx, seg2, persona, nil, "primary|test|m")
		if err != nil {
			t.Fatalf("turn %d Assemble frozen: %v", turn, err)
		}
		prevSerialized = make([][]byte, len(frozenMsgs))
		for i, fm := range frozenMsgs {
			prevSerialized[i], _ = json.Marshal(fm)
		}
	}

	if snapshotCalls != 1 {
		t.Fatalf("expected Segment-Pinned Snapshot to build exactly ONCE, got %d", snapshotCalls)
	}
}
