package tool

import (
	"context"
	"strings"
	"testing"
	"time"
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
