package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/invopop/jsonschema"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
)

// ToolContext carries strictly verified execution metadata (never trusting LLM args for auth/scope).
type ToolContext struct {
	ChannelID     model.ChannelID
	SenderID      string
	SenderName    string
	IsAdmin       bool
	TriggerMsgID  string
	Epoch         int64
	SegmentID     int64
	PokedThisTurn *atomic.Bool // Enforces SPEC §10.2 interact.poke max once/turn
}

// ToolResult separates text digested by the LLM from binary media delivered directly to the channel.
type ToolResult struct {
	TextForLLM  string
	DirectMedia []model.DirectMedia
	SideEffect  string
	SoftError   string // Optional non-fatal degradation reason recorded in tool error metrics
}

// Tool is the strongly-typed generic interface implemented by all built-in tools.
type Tool[Args any] interface {
	Name() string
	Description() string
	ReadOnly() bool
	Execute(ctx context.Context, tc ToolContext, args Args) (ToolResult, error)
}

// DynamicReadOnlyTool may be implemented by tools whose read-only status depends on call arguments
// (e.g. note.list, schedule.list, travel.status/history/map/ticket).
type DynamicReadOnlyTool[Args any] interface {
	ReadOnlyArgs(args Args) bool
}

// Entry is the type-erased runtime representation stored in Registry.
type Entry struct {
	Name          string
	Description   string
	ReadOnly      bool
	ReadOnlyCheck func(rawArgs json.RawMessage) bool
	Parameters    json.RawMessage
	Invoke        func(ctx context.Context, tc ToolContext, rawArgs json.RawMessage) (ToolResult, error)
}

// Stat tracks per-tool latency and error counts for the console dashboard.
type Stat struct {
	Name       string `json:"name"`
	Calls      int64  `json:"calls"`
	Errors     int64  `json:"errors"`
	TotalMs    int64  `json:"total_ms"`
	AvgMs      int64  `json:"avg_ms"`
	LastErrStr string `json:"last_error,omitempty"`
}

// Registry maintains a lexicographically sorted, frozen set of OpenAI function tool declarations
// so the serialized [tools] prefix remains 100% byte-identical across turns.
type Registry struct {
	mu           sync.RWMutex
	entries      map[string]Entry
	frozen       bool
	frozenDefs   []infer.ToolDefinition
	frozenBytes  []byte
	frozenHash   string
	defaultTO    time.Duration
	stats        map[string]*Stat
}

func NewRegistry(defaultTimeout time.Duration) *Registry {
	if defaultTimeout <= 0 {
		defaultTimeout = 30 * time.Second
	}
	return &Registry{
		entries:   make(map[string]Entry),
		defaultTO: defaultTimeout,
		stats:     make(map[string]*Stat),
	}
}

// Register reflects a flattened, $ref-free JSON Schema for Args and registers the tool.
func Register[Args any](r *Registry, t Tool[Args]) error {
	var zero Args
	schemaBytes, err := FlattenedSchema(zero)
	if err != nil {
		return fmt.Errorf("tool: schema for %s: %w", t.Name(), err)
	}
	var roCheck func(rawArgs json.RawMessage) bool
	if dt, ok := any(t).(DynamicReadOnlyTool[Args]); ok {
		roCheck = func(rawArgs json.RawMessage) bool {
			var args Args
			if len(rawArgs) > 0 && string(rawArgs) != "null" && string(rawArgs) != "{}" {
				if err := json.Unmarshal(rawArgs, &args); err != nil {
					return false
				}
			}
			return dt.ReadOnlyArgs(args)
		}
	}
	return r.RegisterRaw(Entry{
		Name:          t.Name(),
		Description:   t.Description(),
		ReadOnly:      t.ReadOnly(),
		ReadOnlyCheck: roCheck,
		Parameters:    schemaBytes,
		Invoke: func(ctx context.Context, tc ToolContext, rawArgs json.RawMessage) (ToolResult, error) {
			var args Args
			if len(rawArgs) > 0 && string(rawArgs) != "null" && string(rawArgs) != "{}" {
				if err := json.Unmarshal(rawArgs, &args); err != nil {
					return ToolResult{}, fmt.Errorf("invalid arguments for %s: %w", t.Name(), err)
				}
			}
			return t.Execute(ctx, tc, args)
		},
	})
}

// RegisterRaw registers a pre-built Entry (used by built-ins and cached MCP bridge tools).
func (r *Registry) RegisterRaw(e Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return errors.New("tool: registry is already frozen")
	}
	if strings.TrimSpace(e.Name) == "" {
		return errors.New("tool: empty tool name")
	}
	if len(e.Parameters) == 0 {
		e.Parameters = json.RawMessage(`{"type":"object","properties":{}}`)
	}
	cleaned, err := StripSchemaMeta(e.Parameters)
	if err != nil {
		return fmt.Errorf("tool: normalize schema for %s: %w", e.Name, err)
	}
	e.Parameters = cleaned
	r.entries[e.Name] = e
	r.stats[e.Name] = &Stat{Name: e.Name}
	return nil
}

// Freeze sorts tools lexicographically and caches the canonical wire JSON and SHA-256 hash.
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	sort.Strings(names)

	defs := make([]infer.ToolDefinition, 0, len(names))
	for _, name := range names {
		e := r.entries[name]
		defs = append(defs, infer.ToolDefinition{
			Type: "function",
			Function: infer.ToolFunctionDefinition{
				Name:        e.Name,
				Description: e.Description,
				Parameters:  e.Parameters,
			},
		})
	}
	raw, _ := json.Marshal(defs)
	sum := sha256.Sum256(raw)

	r.frozenDefs = defs
	r.frozenBytes = raw
	r.frozenHash = hex.EncodeToString(sum[:])
	r.frozen = true
}

// Definitions returns the frozen sorted slice of OpenAI ToolDefinitions.
func (r *Registry) Definitions() []infer.ToolDefinition {
	r.mu.RLock()
	if !r.frozen {
		r.mu.RUnlock()
		r.Freeze()
		r.mu.RLock()
	}
	defer r.mu.RUnlock()
	return r.frozenDefs
}

// FrozenJSON returns the canonical serialized JSON bytes of all declared tools.
func (r *Registry) FrozenJSON() []byte {
	r.mu.RLock()
	if !r.frozen {
		r.mu.RUnlock()
		r.Freeze()
		r.mu.RLock()
	}
	defer r.mu.RUnlock()
	return r.frozenBytes
}

// Hash returns the SHA-256 hex digest of the frozen tool declarations.
func (r *Registry) Hash() string {
	r.mu.RLock()
	if !r.frozen {
		r.mu.RUnlock()
		r.Freeze()
		r.mu.RLock()
	}
	defer r.mu.RUnlock()
	return r.frozenHash
}

// Lookup returns a registered Entry by name.
func (r *Registry) Lookup(name string) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[name]
	return e, ok
}

// IsReadOnlyCall reports whether a specific tool invocation is safe to execute concurrently
// with sibling read-only calls in the same agent tool batch.
func (r *Registry) IsReadOnlyCall(name, rawArgs string) bool {
	e, ok := r.Lookup(name)
	if !ok {
		return false
	}
	if e.ReadOnly {
		return true
	}
	if e.ReadOnlyCheck != nil {
		b := json.RawMessage(strings.TrimSpace(rawArgs))
		if len(b) == 0 {
			b = json.RawMessage(`{}`)
		}
		return e.ReadOnlyCheck(b)
	}
	return false
}

// UpdateDefaultTimeout hot-reloads the default per-tool execution timeout.
func (r *Registry) UpdateDefaultTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	r.mu.Lock()
	r.defaultTO = d
	r.mu.Unlock()
}

// Execute invokes a tool with timeout enforcement and records latency/error metrics.
func (r *Registry) Execute(ctx context.Context, tc ToolContext, name string, rawArgs string) (ToolResult, error) {
	entry, ok := r.Lookup(name)
	if !ok {
		return ToolResult{}, fmt.Errorf("unknown tool %q", name)
	}
	r.mu.RLock()
	timeout := r.defaultTO
	r.mu.RUnlock()
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	argsBytes := json.RawMessage(strings.TrimSpace(rawArgs))
	if len(argsBytes) == 0 {
		argsBytes = json.RawMessage(`{}`)
	}
	res, err := entry.Invoke(runCtx, tc, argsBytes)
	elapsed := time.Since(start).Milliseconds()

	r.mu.Lock()
	if st := r.stats[name]; st != nil {
		st.Calls++
		st.TotalMs += elapsed
		if st.Calls > 0 {
			st.AvgMs = st.TotalMs / st.Calls
		}
		if err != nil {
			st.Errors++
			st.LastErrStr = err.Error()
		} else if res.SoftError != "" {
			st.Errors++
			st.LastErrStr = res.SoftError
		}
	}
	r.mu.Unlock()
	return res, err
}

// Stats returns a sorted snapshot of tool execution telemetry for the console.
func (r *Registry) Stats() []Stat {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Stat, 0, len(r.stats))
	for _, st := range r.stats {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// FlattenedSchema generates a completely inlined JSON Schema with zero $ref/$defs and no $schema/$id.
func FlattenedSchema(v any) (json.RawMessage, error) {
	ref := &jsonschema.Reflector{
		DoNotReference:            true,
		ExpandedStruct:            true,
		AllowAdditionalProperties: false,
	}
	s := ref.Reflect(v)
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return StripSchemaMeta(raw)
}

// StripSchemaMeta inlines local $defs/definitions references and removes $schema, $id, $defs, and definitions keys.
func StripSchemaMeta(raw json.RawMessage) (json.RawMessage, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	defs := make(map[string]any)
	for _, key := range []string{"$defs", "definitions"} {
		if d, ok := m[key].(map[string]any); ok {
			for name, val := range d {
				defs[name] = val
			}
		}
	}
	if len(defs) > 0 {
		m, _ = inlineSchemaRefs(m, defs, 0).(map[string]any)
	}
	delete(m, "$schema")
	delete(m, "$id")
	delete(m, "$defs")
	delete(m, "definitions")
	if _, ok := m["type"]; !ok {
		m["type"] = "object"
	}
	if _, ok := m["properties"]; !ok {
		m["properties"] = map[string]any{}
	}
	out, err := json.Marshal(m)
	return json.RawMessage(out), err
}

func inlineSchemaRefs(v any, defs map[string]any, depth int) any {
	if depth > 8 {
		return map[string]any{"type": "object"}
	}
	switch x := v.(type) {
	case map[string]any:
		if ref, ok := x["$ref"].(string); ok {
			for _, prefix := range []string{"#/$defs/", "#/definitions/"} {
				if strings.HasPrefix(ref, prefix) {
					if target, exists := defs[strings.TrimPrefix(ref, prefix)]; exists {
						return inlineSchemaRefs(target, defs, depth+1)
					}
				}
			}
			// Drop dangling $ref safely rather than leaving broken pointers.
			out := make(map[string]any, len(x))
			for k, val := range x {
				if k != "$ref" {
					out[k] = inlineSchemaRefs(val, defs, depth+1)
				}
			}
			if _, hasType := out["type"]; !hasType {
				out["type"] = "object"
			}
			return out
		}
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = inlineSchemaRefs(val, defs, depth+1)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = inlineSchemaRefs(item, defs, depth+1)
		}
		return out
	default:
		return v
	}
}
