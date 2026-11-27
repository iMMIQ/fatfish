package memory

import (
	"context"
	"strings"
	"testing"

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
