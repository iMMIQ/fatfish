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

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/netutil"
	"github.com/iMMIQ/fatfish/internal/store"
)

// CachedMCPTool is persisted in SQLite `mcp_tool_cache` so transient MCP outages
// never cause declared OpenAI tool schemas to flap (preserving KV Prefix Cache).
type CachedMCPTool struct {
	RemoteName  string          `json:"remote_name"`
	FullName    string          `json:"full_name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type mcpServerConn struct {
	cfg     config.MCPServerConfig
	mu      sync.Mutex
	session *mcp.ClientSession
}

// MCPBridge connects Streamable HTTP and stdio MCP servers and registers namespaced
// tools (`mcp.<server>.<tool>`) with SQLite cached manifest fallback.
type MCPBridge struct {
	store   *store.Store
	log     *slog.Logger
	mu      sync.Mutex
	servers map[string]*mcpServerConn
}

func NewMCPBridge(st *store.Store, logger *slog.Logger) *MCPBridge {
	if logger == nil {
		logger = slog.Default()
	}
	return &MCPBridge{
		store:   st,
		log:     logger,
		servers: make(map[string]*mcpServerConn),
	}
}

// InitAndRegister connects to every configured MCP server, refreshes `mcp_tool_cache`
// on success, or falls back to cached tool declarations on failure so `tools` never flaps.
func (b *MCPBridge) InitAndRegister(ctx context.Context, servers []config.MCPServerConfig, reg *Registry) {
	for _, scfg := range servers {
		if strings.TrimSpace(scfg.Name) == "" {
			continue
		}
		conn := &mcpServerConn{cfg: scfg}
		b.mu.Lock()
		b.servers[scfg.Name] = conn
		b.mu.Unlock()

		tools, err := b.discoverServer(ctx, conn)
		if err != nil {
			b.log.Warn("mcp server unavailable at boot, loading cached manifest", "server", scfg.Name, "err", err)
			if cached := b.loadCached(ctx, scfg.Name); len(cached) > 0 {
				tools = cached
			}
		} else if len(tools) > 0 && b.store != nil {
			if raw, merr := json.Marshal(tools); merr == nil {
				_ = b.store.SaveMCPToolCache(ctx, scfg.Name, string(raw))
			}
		}

		for _, ct := range tools {
			remoteName := ct.RemoteName
			serverName := scfg.Name
			timeout := time.Duration(scfg.TimeoutSec) * time.Second
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			_ = reg.RegisterRaw(Entry{
				Name:        ct.FullName,
				Description: ct.Description,
				ReadOnly:    false,
				Parameters:  ct.Parameters,
				Invoke: func(callCtx context.Context, _ ToolContext, rawArgs json.RawMessage) (ToolResult, error) {
					return b.invoke(callCtx, serverName, remoteName, rawArgs, timeout)
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
	conn.mu.Lock()
	conn.session = sess
	conn.mu.Unlock()

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
		out = append(out, CachedMCPTool{
			RemoteName:  t.Name,
			FullName:    fmt.Sprintf("mcp_%s_%s", sanitizeToolPart(conn.cfg.Name), sanitizeToolPart(t.Name)),
			Description: t.Description,
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

func (b *MCPBridge) invoke(ctx context.Context, serverName, remoteName string, rawArgs json.RawMessage, timeout time.Duration) (ToolResult, error) {
	b.mu.Lock()
	conn := b.servers[serverName]
	b.mu.Unlock()
	if conn == nil {
		return ToolResult{}, fmt.Errorf("mcp server %q temporarily unavailable", serverName)
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn.mu.Lock()
	sess := conn.session
	conn.mu.Unlock()

	if sess == nil {
		var err error
		sess, err = b.connect(callCtx, conn.cfg)
		if err != nil {
			return ToolResult{
				TextForLLM: fmt.Sprintf("MCP server %q is temporarily unavailable (%v). Continue without this tool.", serverName, err),
			}, nil
		}
		conn.mu.Lock()
		conn.session = sess
		conn.mu.Unlock()
	}

	var argMap map[string]any
	if len(rawArgs) > 0 && string(rawArgs) != "null" {
		_ = json.Unmarshal(rawArgs, &argMap)
	}
	if argMap == nil {
		argMap = map[string]any{}
	}

	res, err := sess.CallTool(callCtx, &mcp.CallToolParams{
		Name:      remoteName,
		Arguments: argMap,
	})
	if err != nil {
		// Drop broken session so next call attempts clean reconnect.
		conn.mu.Lock()
		if conn.session == sess {
			_ = sess.Close()
			conn.session = nil
		}
		conn.mu.Unlock()
		return ToolResult{
			TextForLLM: fmt.Sprintf("MCP tool %s.%s temporarily failed: %v", serverName, remoteName, err),
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
		return ToolResult{TextForLLM: fmt.Sprintf("MCP tool error: %s", joined)}, nil
	}
	return ToolResult{TextForLLM: netutil.WrapExternalData("mcp:"+serverName+":"+remoteName, joined)}, nil
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
		httpCli := &http.Client{
			Transport: &headerRoundTripper{
				base:    http.DefaultTransport,
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

// Close cleanly closes all active MCP client sessions.
func (b *MCPBridge) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, conn := range b.servers {
		conn.mu.Lock()
		if conn.session != nil {
			_ = conn.session.Close()
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
