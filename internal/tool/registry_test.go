package tool

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/transport"
)

type sampleNested struct {
	Tag   string `json:"tag" jsonschema:"description=Optional tag"`
	Limit int    `json:"limit,omitempty"`
}

type sampleArgs struct {
	Query  string       `json:"query" jsonschema:"required,description=Search query"`
	Filter sampleNested `json:"filter,omitempty"`
}

type sampleTool struct{ name string }

func (s sampleTool) Name() string        { return s.name }
func (s sampleTool) Description() string { return "sample" }
func (s sampleTool) ReadOnly() bool      { return true }
func (s sampleTool) Execute(_ context.Context, _ ToolContext, _ sampleArgs) (ToolResult, error) {
	return ToolResult{TextForLLM: "ok"}, nil
}

// TestSchemaNoRefs enforces SPEC 10.1: zero $ref/$defs/$schema/$id and deterministic ordering.
func TestSchemaNoRefs(t *testing.T) {
	reg := NewRegistry(5 * time.Second)
	if err := Register[sampleArgs](reg, sampleTool{name: "z_tool"}); err != nil {
		t.Fatalf("Register z_tool: %v", err)
	}
	if err := Register[sampleArgs](reg, sampleTool{name: "a_tool"}); err != nil {
		t.Fatalf("Register a_tool: %v", err)
	}
	reg.Freeze()

	defs := reg.Definitions()
	if len(defs) != 2 || defs[0].Function.Name != "a_tool" || defs[1].Function.Name != "z_tool" {
		t.Fatalf("expected lexicographical order [a_tool, z_tool], got %+v", defs)
	}

	raw := string(reg.FrozenJSON())
	for _, forbidden := range []string{"$ref", "$defs", "$schema", "$id"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("frozen tool schema contains forbidden token %q: %s", forbidden, raw)
		}
	}
}

type stubTransport struct {
	pokes int
}

func (s *stubTransport) Start(context.Context, func(*model.Message)) error             { return nil }
func (s *stubTransport) StopInbound() error                                            { return nil }
func (s *stubTransport) Stop() error                                                   { return nil }
func (s *stubTransport) Probe(context.Context) error                                   { return nil }
func (s *stubTransport) SelfID() string                                                { return "10001" }
func (s *stubTransport) SendText(context.Context, model.ChannelID, string, transport.SendOptions) (string, error) {
	return "1", nil
}
func (s *stubTransport) SendImage(context.Context, model.ChannelID, []byte, string, transport.SendOptions) (string, error) {
	return "2", nil
}
func (s *stubTransport) SendVideo(context.Context, model.ChannelID, []byte, transport.SendOptions) (string, error) {
	return "2v", nil
}
func (s *stubTransport) SendForward(context.Context, model.ChannelID, []transport.ForwardNode, string) (string, error) {
	return "3", nil
}
func (s *stubTransport) SendPoke(context.Context, model.ChannelID, string) error {
	s.pokes++
	return nil
}
func (s *stubTransport) SendReaction(context.Context, model.ChannelID, string, string) error { return nil }
func (s *stubTransport) FetchAudio(context.Context, string, string) ([]byte, string, error) {
	return nil, "", nil
}
func (s *stubTransport) GetMessage(context.Context, model.ChannelID, string) (*model.Message, error) {
	return nil, nil
}
func (s *stubTransport) GetForward(context.Context, string, int) ([]model.Part, error) { return nil, nil }

func TestInteractPokeMaxOncePerTurn(t *testing.T) {
	st := &stubTransport{}
	it := NewInteractTool(st, nil)
	tc := ToolContext{
		ChannelID:     model.MakeChannelID("onebot", "10001", model.ChannelGroup, "99"),
		SenderID:      "20001",
		PokedThisTurn: &atomic.Bool{},
	}
	if _, err := it.Execute(context.Background(), tc, InteractArgs{Action: "poke"}); err != nil {
		t.Fatalf("first poke should succeed: %v", err)
	}
	if _, err := it.Execute(context.Background(), tc, InteractArgs{Action: "poke"}); err == nil {
		t.Fatal("second poke in same turn must be rejected")
	}
	if st.pokes != 1 {
		t.Fatalf("expected exactly 1 poke sent, got %d", st.pokes)
	}
}
