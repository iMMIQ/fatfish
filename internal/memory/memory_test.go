package memory

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/store"
)

func TestAntiSarcasmCorroborationAndChannelIsolation(t *testing.T) {
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	svc := NewService(st, nil, 4, nil)
	ctx := context.Background()
	ch1 := model.MakeChannelID("onebot", "1", model.ChannelGroup, "100")
	ch2 := model.MakeChannelID("onebot", "1", model.ChannelGroup, "200")

	// Single episode fact must stay tentative and NOT appear in formatted bio card.
	if err := svc.MergeCandidateFact(ctx, ch1, "u1", "lives in Tokyo", 10, true); err != nil {
		t.Fatalf("MergeCandidateFact 1: %v", err)
	}
	p, _ := st.GetProfile(ctx, ch1, "u1")
	if len(p.Facts) != 1 || !p.Facts[0].Tentative {
		t.Fatalf("expected single-episode fact to remain tentative, got %+v", p.Facts)
	}
	if card := FormatMemberCard(*p, "Alice"); strings.Contains(card, "Tokyo") {
		t.Fatalf("tentative fact must not be promoted into member card: %q", card)
	}

	// Second distinct episode corroborates and graduates the fact.
	if err := svc.MergeCandidateFact(ctx, ch1, "u1", "lives in Tokyo", 11, true); err != nil {
		t.Fatalf("MergeCandidateFact 2: %v", err)
	}
	p, _ = st.GetProfile(ctx, ch1, "u1")
	if p.Facts[0].Tentative || len(p.Facts[0].EvidenceEpisodes) != 2 {
		t.Fatalf("expected corroborated fact to graduate, got %+v", p.Facts[0])
	}
	if card := FormatMemberCard(*p, "Alice"); !strings.Contains(card, "Tokyo") {
		t.Fatalf("expected graduated fact in member card, got %q", card)
	}

	// Jokes stay strictly separated in Jokes slice.
	_ = svc.AddProfileJoke(ctx, ch1, "u1", "self-proclaimed millionaire", 10)
	p, _ = st.GetProfile(ctx, ch1, "u1")
	if len(p.Jokes) != 1 || len(p.Facts) != 1 {
		t.Fatalf("jokes must remain separated from facts: %+v", p)
	}

	// Cross-channel profile must be completely isolated.
	p2, _ := st.GetProfile(ctx, ch2, "u1")
	if len(p2.Facts) != 0 || len(p2.Jokes) != 0 {
		t.Fatalf("expected empty profile in ch2, got %+v", p2)
	}
}

func TestProfileDecayAndCap(t *testing.T) {
	now := int64(1760000000000)
	dayMs := int64(24 * 3600 * 1000)

	var in []model.ProfileFact
	// Add 1 stale tentative (>60d) that must expire and 1 fresh tentative that must survive.
	in = append(in, model.ProfileFact{Text: "stale joke claim", Confidence: 0.4, Tentative: true, UpdatedAt: now - 75*dayMs})
	in = append(in, model.ProfileFact{Text: "fresh claim", Confidence: 0.4, Tentative: true, UpdatedAt: now - 5*dayMs})

	// Add 25 confirmed facts: oldest low-confidence facts must be evicted past cap (20).
	for i := 0; i < 25; i++ {
		in = append(in, model.ProfileFact{
			Text:       "fact_" + string(rune('a'+i)),
			Confidence: 0.75,
			Tentative:  false,
			UpdatedAt:  now - int64(i*15)*dayMs,
		})
	}

	out := CompactProfileFacts(in, now)
	confirmedCnt, tentativeCnt := 0, 0
	for _, f := range out {
		if f.Text == "stale joke claim" {
			t.Fatal("expected >60d tentative fact to expire")
		}
		if f.Tentative {
			tentativeCnt++
		} else {
			confirmedCnt++
		}
	}
	if confirmedCnt != maxConfirmedProfileFacts || tentativeCnt != 1 {
		t.Fatalf("expected %d confirmed and 1 tentative, got confirmed=%d tentative=%d",
			maxConfirmedProfileFacts, confirmedCnt, tentativeCnt)
	}
	if out[0].Text != "fact_a" {
		t.Fatalf("expected freshest confirmed fact_a ranked first, got %q", out[0].Text)
	}
}

func TestPartitionWindowsFiltersMediaSpamAndRecallFallback(t *testing.T) {
	now := time.Now()
	ch := model.MakeChannelID("onebot", "10001", model.ChannelGroup, "88888")

	// 12 pure-image messages should NOT produce an episode window.
	var spam []model.Message
	for i := 0; i < 12; i++ {
		spam = append(spam, model.Message{
			ChannelID: ch,
			Seq:       int64(i + 1),
			SenderID:  "u1",
			Parts:     []model.Part{{Kind: model.PartImage, MediaID: "m1"}},
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	if wins := PartitionWindows(spam, 30*time.Minute, 100, 8); len(wins) != 0 {
		t.Fatalf("expected pure-media spam to produce 0 windows, got %d", len(wins))
	}

	// 8 substantive text messages should produce 1 window.
	var realChat []model.Message
	for i := 0; i < 8; i++ {
		realChat = append(realChat, model.Message{
			ChannelID: ch,
			Seq:       int64(i + 1),
			SenderID:  "u1",
			Parts:     []model.Part{{Kind: model.PartText, Text: "discussion message line"}},
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	if wins := PartitionWindows(realChat, 30*time.Minute, 100, 8); len(wins) != 1 {
		t.Fatalf("expected substantive chat to produce 1 window, got %d", len(wins))
	}

	// Recall(mode="memory") cold-start fallback to L0 raw history by sender.
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	_ = st.UpsertMember(ctx, model.Member{ChannelID: ch, MemberID: "u42", DisplayName: "Alice", UpdatedAt: now})
	_ = st.AppendMessage(ctx, &model.Message{
		ChannelID:  ch,
		SenderID:   "u42",
		SenderName: "Alice",
		Parts:      []model.Part{{Kind: model.PartText, Text: "booked flight to Kyoto next week"}},
		CreatedAt:  now,
	})

	svc := NewService(st, nil, 4, nil)
	res, err := svc.Recall(ctx, RecallOptions{
		ChannelID: ch,
		Mode:      "memory",
		Query:     "do you recognize me",
		Sender:    "Alice",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if !strings.Contains(res.Summary, "[Raw Message Fallback]") || !strings.Contains(res.Summary, "Kyoto") {
		t.Fatalf("expected cold-start L0 raw fallback for sender, got %q", res.Summary)
	}
}

func TestFactDecayHalfLifeAndOverlappingEpisodeDedup(t *testing.T) {
	now := time.Now().UnixMilli()
	f := model.ProfileFact{
		Text:       "likes tea",
		Confidence: 0.8,
		UpdatedAt:  now - int64(90*24*time.Hour/time.Millisecond),
	}
	got := decayedFactScore(f, now)
	if got < 0.39 || got > 0.41 {
		t.Fatalf("expected 90d half-life decay to ~0.40 (50%% of 0.8), got %v", got)
	}

	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	ch := model.MakeChannelID("onebot", "1", model.ChannelGroup, "77")

	ep := &model.Episode{
		ChannelID: ch,
		StartSeq:  10,
		EndSeq:    20,
		StartTS:   time.Now().Add(-time.Hour),
		EndTS:     time.Now(),
		Topic:     "test",
		Summary:   "summary",
	}
	if err := st.SaveEpisode(ctx, ep, nil); err != nil {
		t.Fatalf("SaveEpisode: %v", err)
	}
	svc := NewService(st, nil, 4, nil)
	dupWin := []model.Message{{Seq: 12}, {Seq: 18}}
	// DistillWindow must skip overlapping seq range without calling hub or creating a duplicate episode.
	outEp, err := svc.DistillWindow(ctx, ch, dupWin, "distill")
	if err != nil || outEp != nil {
		t.Fatalf("expected overlapping window to be skipped (nil, nil), got ep=%+v err=%v", outEp, err)
	}

	// BuildSnapshotWithTime must include SessionTime, RecentHighlights, and member StyleNote.
	_ = st.UpsertMember(ctx, model.Member{ChannelID: ch, MemberID: "u9", DisplayName: "Bob", UpdatedAt: time.Now()})
	if err := svc.UpdateProfileStyle(ctx, ch, "u9", "爱互怼的熟人"); err != nil {
		t.Fatalf("UpdateProfileStyle: %v", err)
	}
	snap, err := svc.BuildSnapshotWithTime(ctx, ch, []string{"u9"}, time.Now(), time.UTC)
	if err != nil {
		t.Fatalf("BuildSnapshotWithTime: %v", err)
	}
	if snap.SessionTime == "" {
		t.Fatal("expected non-empty SessionTime")
	}
	if len(snap.RecentHighlights) == 0 || !strings.Contains(snap.RecentHighlights[0], "test") {
		t.Fatalf("expected recent 48h episode highlight, got %#v", snap.RecentHighlights)
	}
	if len(snap.MemberCards) == 0 || !strings.Contains(snap.MemberCards[0], "爱互怼的熟人") {
		t.Fatalf("expected StyleNote in member card, got %#v", snap.MemberCards)
	}
}
