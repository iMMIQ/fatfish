package tool

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/store"
)

// TestMCPToolCachePreservesKVDeclarations verifies SPEC Audit #2:
// when an MCP server is unreachable at boot, cached tool declarations from SQLite
// keep the OpenAI `tools` JSON identical (preventing KV Prefix Cache invalidation)
// and runtime invocations degrade gracefully without crashing.
func TestMCPToolCachePreservesKVDeclarations(t *testing.T) {
	st, err := store.Open(t.TempDir(), 512, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	cached := []CachedMCPTool{{
		RemoteName:  "search",
		FullName:    "mcp_web_search",
		Description: "Cached search tool",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`),
	}}
	raw, _ := json.Marshal(cached)
	if err := st.SaveMCPToolCache(ctx, "web", string(raw)); err != nil {
		t.Fatalf("SaveMCPToolCache: %v", err)
	}

	reg := NewRegistry(5 * time.Second)
	bridge := NewMCPBridge(st, nil)
	// Point at an unreachable local port so discovery fails and must fall back to SQLite cache.
	bridge.InitAndRegister(ctx, []config.MCPServerConfig{{
		Name:       "web",
		Transport:  "streamable_http",
		URL:        "http://127.0.0.1:1/mcp",
		TimeoutSec: 1,
	}}, reg)
	reg.Freeze()

	defs := reg.Definitions()
	if len(defs) != 1 || defs[0].Function.Name != "mcp_web_search" {
		t.Fatalf("expected cached MCP tool declaration to survive outage, got %+v", defs)
	}

	res, err := reg.Execute(ctx, ToolContext{}, "mcp_web_search", `{"q":"test"}`)
	if err != nil {
		t.Fatalf("expected graceful degradation error message, got hard err: %v", err)
	}
	if !strings.Contains(res.TextForLLM, "temporarily unavailable") {
		t.Fatalf("expected temporary unavailable notice, got %q", res.TextForLLM)
	}
}
