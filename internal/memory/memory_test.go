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
