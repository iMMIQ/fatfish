package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/netutil"
	"github.com/iMMIQ/fatfish/internal/store"
)

const defaultMCPMaxOutputRunes = 8000

// CachedMCPTool is persisted in SQLite `mcp_tool_cache` so transient MCP outages
// never cause declared OpenAI tool schemas to flap (preserving KV Prefix Cache).
type CachedMCPTool struct {
	RemoteName  string          `json:"remote_name"`
	FullName    string          `json:"full_name"`
	Description string          `json:"description"`
	ReadOnly    bool            `json:"read_only,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

// mcpManagedSession reference-counts active CallTool users so detaching a broken session
// never forcibly kills sibling concurrent calls that are still completing on it.
type mcpManagedSession struct {
	sess     *mcp.ClientSession
	mu       sync.Mutex
	active   int
	detached bool
}

func (m *mcpManagedSession) acquire() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.detached {
		return false
	}
	m.active++
	return true
}

func (m *mcpManagedSession) release() {
	m.mu.Lock()
	m.active--
	shouldClose := m.detached && m.active == 0
	m.mu.Unlock()
	if shouldClose && m.sess != nil {
		_ = m.sess.Close()
	}
}

func (m *mcpManagedSession) detach() {
	m.mu.Lock()
	m.detached = true
	shouldClose := m.active == 0
	m.mu.Unlock()
	if shouldClose && m.sess != nil {
		_ = m.sess.Close()
	}
}

type mcpServerConn struct {
	cfg       config.MCPServerConfig
	mu        sync.Mutex
	session   *mcpManagedSession
	connectSF singleflight.Group
}

// MCPBridge connects Streamable HTTP and stdio MCP servers and registers namespaced
// tools (`mcp.<server>.<tool>`) with SQLite cached manifest fallback.
type MCPBridge struct {
	store     *store.Store
	hub       *infer.Hub
	log       *slog.Logger
	transport *http.Transport
	mu        sync.Mutex
	servers   map[string]*mcpServerConn
}

func NewMCPBridge(st *store.Store, logger *slog.Logger) *MCPBridge {
	if logger == nil {
		logger = slog.Default()
	}
	return &MCPBridge{
		store: st,
		log:   logger,
		transport: &http.Transport{
			Proxy:               nil,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		servers: make(map[string]*mcpServerConn),
	}
}

// SetHub attaches an optional inference hub for semantic chunk reranking of oversized MCP outputs.
func (b *MCPBridge) SetHub(hub *infer.Hub) {
	b.mu.Lock()
	b.hub = hub
	b.mu.Unlock()
}

// InitAndRegister discovers all configured MCP servers in parallel, refreshes `mcp_tool_cache`
// on success, or falls back to cached tool declarations on failure so `tools` never flaps.
func (b *MCPBridge) InitAndRegister(ctx context.Context, servers []config.MCPServerConfig, reg *Registry) {
	discovered := make([][]CachedMCPTool, len(servers))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	for i, scfg := range servers {
		if strings.TrimSpace(scfg.Name) == "" {
			continue
		}
		idx := i
		cfg := scfg
		conn := &mcpServerConn{cfg: cfg}
		b.mu.Lock()
		b.servers[cfg.Name] = conn
		b.mu.Unlock()

		g.Go(func() error {
			tools, err := b.discoverServer(gctx, conn)
			if err != nil {
				b.log.Warn("mcp server unavailable at boot, loading cached manifest", "server", cfg.Name, "err", err)
				if cached := b.loadCached(gctx, cfg.Name); len(cached) > 0 {
					tools = cached
				}
			} else if len(tools) > 0 && b.store != nil {
				if raw, merr := json.Marshal(tools); merr == nil {
					_ = b.store.SaveMCPToolCache(gctx, cfg.Name, string(raw))
				}
			}
			discovered[idx] = tools
			return nil
		})
	}
	_ = g.Wait()

	// Register in deterministic config order.
	for i, scfg := range servers {
		if strings.TrimSpace(scfg.Name) == "" {
			continue
		}
		for _, ct := range discovered[i] {
			remoteName := ct.RemoteName
			serverName := scfg.Name
			timeout := time.Duration(scfg.TimeoutSec) * time.Second
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			maxRunes := scfg.MaxOutputRunes
			if maxRunes <= 0 {
				maxRunes = defaultMCPMaxOutputRunes
			}
			readOnly := ct.ReadOnly || scfg.ReadOnly || isKnownReadOnlyMCP(scfg.Name, ct.RemoteName)
			_ = reg.RegisterRaw(Entry{
				Name:        ct.FullName,
				Description: ct.Description,
				ReadOnly:    readOnly,
				Parameters:  ct.Parameters,
				Invoke: func(callCtx context.Context, _ ToolContext, rawArgs json.RawMessage) (ToolResult, error) {
					return b.invoke(callCtx, serverName, remoteName, rawArgs, timeout, maxRunes)
				},
			})
		}
	}
}

func (b *MCPBridge) discoverServer(ctx context.Context, conn *mcpServerConn) ([]CachedMCPTool, error) {
	sess, err := b.connect(ctx, conn.cfg)
	if err != nil {
		return nil, err
	}
	ms := &mcpManagedSession{sess: sess}
	ms.acquire()
	defer ms.release()

	conn.mu.Lock()
	old := conn.session
	conn.session = ms
	conn.mu.Unlock()
	if old != nil {
		old.detach()
	}

	res, err := sess.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		return nil, err
	}
	out := make([]CachedMCPTool, 0, len(res.Tools))
	for _, t := range res.Tools {
		params, _ := json.Marshal(t.InputSchema)
		if len(params) == 0 || string(params) == "null" {
			params = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		cleaned, err := StripSchemaMeta(params)
		if err != nil {
			cleaned = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		readOnly := false
		if t.Annotations != nil && t.Annotations.ReadOnlyHint {
			readOnly = true
		}
		out = append(out, CachedMCPTool{
			RemoteName:  t.Name,
			FullName:    fmt.Sprintf("mcp_%s_%s", sanitizeToolPart(conn.cfg.Name), sanitizeToolPart(t.Name)),
			Description: t.Description,
			ReadOnly:    readOnly,
			Parameters:  cleaned,
		})
	}
	return out, nil
}

func (b *MCPBridge) loadCached(ctx context.Context, serverName string) []CachedMCPTool {
	if b.store == nil {
		return nil
	}
	raw, err := b.store.LoadMCPToolCache(ctx, serverName)
	if err != nil || raw == "" {
		return nil
	}
	var tools []CachedMCPTool
	if err := json.Unmarshal([]byte(raw), &tools); err != nil {
		return nil
	}
	return tools
}

func (b *MCPBridge) acquireSession(ctx context.Context, conn *mcpServerConn, timeout time.Duration) (*mcpManagedSession, error) {
	for attempt := 0; attempt < 2; attempt++ {
		conn.mu.Lock()
		if ms := conn.session; ms != nil && ms.acquire() {
			conn.mu.Unlock()
			return ms, nil
		}
		conn.mu.Unlock()

		ch := conn.connectSF.DoChan("connect", func() (any, error) {
			conn.mu.Lock()
			if ms := conn.session; ms != nil {
				conn.mu.Unlock()
				return ms, nil
			}
			conn.mu.Unlock()

			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			defer cancel()
			sess, err := b.connect(cctx, conn.cfg)
			if err != nil {
				return nil, err
			}
			ms := &mcpManagedSession{sess: sess}
			conn.mu.Lock()
			if conn.session != nil {
				existing := conn.session
				conn.mu.Unlock()
				_ = sess.Close()
				return existing, nil
			}
			conn.session = ms
			conn.mu.Unlock()
			return ms, nil
		})

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-ch:
			if res.Err != nil {
				return nil, res.Err
			}
			ms := res.Val.(*mcpManagedSession)
			if ms.acquire() {
				return ms, nil
			}
		}
	}
	return nil, fmt.Errorf("mcp session rotated concurrently")
}

func (b *MCPBridge) invoke(ctx context.Context, serverName, remoteName string, rawArgs json.RawMessage, timeout time.Duration, maxRunes int) (ToolResult, error) {
	b.mu.Lock()
	conn := b.servers[serverName]
	hub := b.hub
	b.mu.Unlock()
	if conn == nil {
		return ToolResult{}, fmt.Errorf("mcp server %q temporarily unavailable", serverName)
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ms, err := b.acquireSession(callCtx, conn, timeout)
	if err != nil {
		return ToolResult{
			TextForLLM: fmt.Sprintf("MCP server %q is temporarily unavailable (%v). Continue without this tool.", serverName, err),
			SoftError:  fmt.Sprintf("connect %s: %v", serverName, err),
		}, nil
	}
	defer ms.release()

	var argMap map[string]any
	if len(rawArgs) > 0 && string(rawArgs) != "null" {
		_ = json.Unmarshal(rawArgs, &argMap)
	}
	if argMap == nil {
		argMap = map[string]any{}
	}

	res, err := ms.sess.CallTool(callCtx, &mcp.CallToolParams{
		Name:      remoteName,
		Arguments: argMap,
	})
	if err != nil {
		// Detach broken session so the next call reconnects cleanly, without closing it
		// underneath any sibling concurrent CallTool invocations still in flight.
		conn.mu.Lock()
		if conn.session == ms {
			conn.session = nil
			ms.detach()
		}
		conn.mu.Unlock()
		return ToolResult{
			TextForLLM: fmt.Sprintf("MCP tool %s.%s temporarily failed: %v", serverName, remoteName, err),
			SoftError:  fmt.Sprintf("call %s.%s: %v", serverName, remoteName, err),
		}, nil
	}

	var texts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok && tc.Text != "" {
			texts = append(texts, tc.Text)
		} else if raw, err := c.MarshalJSON(); err == nil {
			texts = append(texts, string(raw))
		}
	}
	joined := strings.Join(texts, "\n")
	if res.IsError {
		return ToolResult{
			TextForLLM: fmt.Sprintf("MCP tool error: %s", clipRunes(joined, 2000)),
			SoftError:  clipRunes(joined, 500),
		}, nil
	}
	joined = trimMCPOutput(ctx, hub, joined, argMap, maxRunes)
	return ToolResult{TextForLLM: netutil.WrapExternalData("mcp:"+serverName+":"+remoteName, joined)}, nil
}

func isKnownReadOnlyMCP(serverName, remoteName string) bool {
	s := strings.ToLower(strings.TrimSpace(serverName))
	r := strings.ToLower(strings.TrimSpace(remoteName))
	switch s {
	case "web_search_prime", "web_reader", "zread", "web_search":
		return true
	}
	if strings.HasPrefix(s, "web_search") || strings.HasPrefix(s, "web_reader") {
		return true
	}
	for _, prefix := range []string{"web_search", "web_reader", "search_", "read_", "get_", "fetch_"} {
		if strings.HasPrefix(r, prefix) {
			return true
		}
	}
	return false
}

func trimMCPOutput(ctx context.Context, hub *infer.Hub, content string, argMap map[string]any, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = defaultMCPMaxOutputRunes
	}
	if len([]rune(content)) <= maxRunes {
		return content
	}
	q := extractMCPQueryHint(argMap)
	if q != "" && hub != nil {
		return selectRelevantChunksWithHub(ctx, hub, content, q, 6, maxRunes)
	}
	return clipRunes(content, maxRunes)
}

func extractMCPQueryHint(argMap map[string]any) string {
	for _, k := range []string{"query", "search_query", "q", "keyword", "keywords", "topic"} {
		if v, ok := argMap[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func (b *MCPBridge) connect(ctx context.Context, scfg config.MCPServerConfig) (*mcp.ClientSession, error) {
	cli := mcp.NewClient(&mcp.Implementation{Name: "fatfish", Version: "1.0.0"}, nil)
	switch strings.ToLower(strings.TrimSpace(scfg.Transport)) {
	case "stdio":
		if strings.TrimSpace(scfg.Command) == "" {
			return nil, fmt.Errorf("mcp: stdio server %q missing command", scfg.Name)
		}
		cmd := exec.Command(scfg.Command, scfg.Args...)
		cmd.Env = os.Environ()
		for k, v := range scfg.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		return cli.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)

	case "streamable_http", "http", "":
		if strings.TrimSpace(scfg.URL) == "" {
			return nil, fmt.Errorf("mcp: http server %q missing url", scfg.Name)
		}
		baseRT := http.RoundTripper(b.transport)
		if baseRT == nil {
			baseRT = http.DefaultTransport
		}
		httpCli := &http.Client{
			Transport: &headerRoundTripper{
				base:    baseRT,
				headers: scfg.Headers,
			},
			Timeout: time.Duration(max(scfg.TimeoutSec, 30)) * time.Second,
		}
		return cli.Connect(ctx, &mcp.StreamableClientTransport{
			Endpoint:             scfg.URL,
			HTTPClient:           httpCli,
			DisableStandaloneSSE: true,
		}, nil)

	default:
		return nil, fmt.Errorf("mcp: unsupported transport %q", scfg.Transport)
	}
}

// Probe tests connectivity and tool listing for each configured MCP server concurrently (for `fatfish check`).
func (b *MCPBridge) Probe(ctx context.Context, servers []config.MCPServerConfig) map[string]error {
	results := make(map[string]error, len(servers))
	var resMu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(4)
	for _, scfg := range servers {
		if strings.TrimSpace(scfg.Name) == "" {
			continue
		}
		cfg := scfg
		g.Go(func() error {
			pctx, cancel := context.WithTimeout(gctx, 8*time.Second)
			defer cancel()
			sess, err := b.connect(pctx, cfg)
			if err == nil {
				_, err = sess.ListTools(pctx, &mcp.ListToolsParams{})
				_ = sess.Close()
			}
			resMu.Lock()
			results[cfg.Name] = err
			resMu.Unlock()
			return nil
		})
	}
	_ = g.Wait()
	return results
}

// Close cleanly closes all active MCP client sessions.
func (b *MCPBridge) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, conn := range b.servers {
		conn.mu.Lock()
		if conn.session != nil {
			conn.session.detach()
			conn.session = nil
		}
		conn.mu.Unlock()
	}
}

type headerRoundTripper struct {
	base    http.RoundTripper
	headers map[string]string
}

func (h *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	for k, v := range h.headers {
		clone.Header.Set(k, v)
	}
	return h.base.RoundTrip(clone)
}

func sanitizeToolPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
