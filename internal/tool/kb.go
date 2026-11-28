package tool

import (
	"context"
	"errors"
	"strings"

	"github.com/iMMIQ/fatfish/internal/kb"
	"github.com/iMMIQ/fatfish/internal/netutil"
)

// KBSearchArgs defines the flat, $ref-free schema for the local read-only knowledge base search tool.
type KBSearchArgs struct {
	Query    string `json:"query" jsonschema:"required,description=Keywords or question to look up (e.g. '3天路线 住宿片区', '环岛动线 交通', '必去景点 历史文化')"`
	Entity   string `json:"entity,omitempty" jsonschema:"description=Optional destination, city, country, or topic name for precision filtering (e.g. '京都', '冰岛', '厦门', '黄山')"`
	Category string `json:"category,omitempty" jsonschema:"enum=all,enum=overview,enum=itinerary,enum=neighborhood,enum=sights,enum=transport,enum=food,enum=tips,enum=culture,description=Optional section category filter (default all)"`
	Limit    int    `json:"limit,omitempty" jsonschema:"description=Max reference chunks to return (default 4, max 6)"`
}

// KBSearchTool queries the optional local read-only curated knowledge base (SQLite FTS5 + Rerank, zero RAM vectors).
type KBSearchTool struct {
	svc        *kb.Service
	defaultMax int
}

func NewKBSearchTool(svc *kb.Service, defaultMax int) *KBSearchTool {
	if defaultMax <= 0 {
		defaultMax = 4
	}
	return &KBSearchTool{svc: svc, defaultMax: defaultMax}
}

func (t *KBSearchTool) Name() string   { return "kb_search" }
func (t *KBSearchTool) ReadOnly() bool { return true }
func (t *KBSearchTool) Description() string {
	return "Search the local read-only curated reference knowledge base (structured destination guides, regional route skeletons, neighborhood layouts, transport logic, cultural background, and custom notes). Ideal alongside web_search_prime: use kb_search for route structure/neighborhood vibe/cultural depth, and verify live prices, hours, bookings, and policies via web search."
}

func (t *KBSearchTool) Execute(ctx context.Context, _ ToolContext, args KBSearchArgs) (ToolResult, error) {
	if t.svc == nil || !t.svc.Enabled() {
		return ToolResult{TextForLLM: "Local reference knowledge base is not currently configured or available."}, nil
	}
	q := strings.TrimSpace(args.Query)
	ent := strings.TrimSpace(args.Entity)
	if q == "" && ent == "" {
		return ToolResult{}, errors.New("kb_search: query is required")
	}
	limit := args.Limit
	if limit <= 0 {
		limit = t.defaultMax
	}
	if limit > 6 {
		limit = 6
	}

	chunks, err := t.svc.Search(ctx, kb.SearchOptions{
		Query:    q,
		Entity:   ent,
		Category: args.Category,
		Limit:    limit,
	})
	if err != nil {
		return ToolResult{TextForLLM: "Local knowledge base lookup failed: " + err.Error(), SoftError: err.Error()}, nil
	}
	src := "kb_search"
	if ent != "" {
		src += ":" + ent
	}
	return ToolResult{TextForLLM: netutil.WrapExternalData(src, kb.FormatResults(chunks))}, nil
}
