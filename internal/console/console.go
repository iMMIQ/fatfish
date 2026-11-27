package console

import (
	"context"
	"crypto/subtle"
	"fmt"
	"html"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/room"
	"github.com/iMMIQ/fatfish/internal/sticker"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/tool"
	"github.com/iMMIQ/fatfish/web"
)

const sessionCookieName = "fatfish_session"

type Dependencies struct {
	Listen   string
	Token    string
	Config   *config.Manager
	Store    *store.Store
	Hub      *infer.Hub
	Rooms    *room.Manager
	Registry *tool.Registry
	Stickers *sticker.Engine
	Logs     *LogBroadcaster
}

// Server serves the embedded HTMX admin console on 127.0.0.1:6185 with constant-time Token Auth.
type Server struct {
	deps Dependencies
	tmpl *template.Template
	http *http.Server
}

func NewServer(deps Dependencies) (*Server, error) {
	funcMap := template.FuncMap{
		"pct": func(a, b int) float64 {
			if b <= 0 {
				return 0
			}
			return float64(a) * 100.0 / float64(b)
		},
	}
	t, err := template.New("layout.html").Funcs(funcMap).ParseFS(web.Assets, "templates/layout.html")
	if err != nil {
		return nil, fmt.Errorf("console: parse templates: %w", err)
	}
	s := &Server{deps: deps, tmpl: t}
	mux := s.routes()
	s.http = &http.Server{
		Addr:              deps.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s, nil
}

// Handler exposes the http.Handler for unit testing without binding a TCP port.
func (s *Server) Handler() http.Handler { return s.http.Handler }

func (s *Server) Start() error {
	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	staticFS, _ := fs.Sub(web.Assets, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	mux.HandleFunc("GET /login", s.handleLoginPage)
	mux.HandleFunc("POST /login", s.handleLoginSubmit)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Protected routes
	mux.Handle("GET /{$}", s.auth(http.HandlerFunc(s.handleDashboard)))
	mux.Handle("GET /config", s.auth(http.HandlerFunc(s.handleConfigPage)))
	mux.Handle("POST /api/config/validate", s.auth(http.HandlerFunc(s.handleConfigValidate)))
	mux.Handle("POST /api/config/save", s.auth(http.HandlerFunc(s.handleConfigSave)))
	mux.Handle("POST /api/persona/save", s.auth(http.HandlerFunc(s.handlePersonaSave)))
	mux.Handle("GET /memory", s.auth(http.HandlerFunc(s.handleMemoryPage)))
	mux.Handle("POST /api/memory/note", s.auth(http.HandlerFunc(s.handleNoteSave)))
	mux.Handle("POST /api/memory/note/delete", s.auth(http.HandlerFunc(s.handleNoteDelete)))
	mux.Handle("GET /stickers", s.auth(http.HandlerFunc(s.handleStickersPage)))
	mux.Handle("GET /api/stickers/search", s.auth(http.HandlerFunc(s.handleStickerSearch)))
	mux.Handle("GET /api/stickers/image", s.auth(http.HandlerFunc(s.handleStickerImage)))
	mux.Handle("POST /api/stickers/approve", s.auth(http.HandlerFunc(s.handleStickerApprove)))
	mux.Handle("POST /api/stickers/reject", s.auth(http.HandlerFunc(s.handleStickerReject)))
	mux.Handle("GET /logs", s.auth(http.HandlerFunc(s.handleLogsPage)))
	mux.Handle("GET /api/logs/stream", s.auth(http.HandlerFunc(s.handleLogsStream)))

	return mux
}

func (s *Server) tokenMatch(candidate string) bool {
	expected := s.deps.Token
	if expected == "" || candidate == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(expected)) == 1
}

func (s *Server) isAuthenticated(r *http.Request) bool {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		if s.tokenMatch(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))) {
			return true
		}
	}
	if ck, err := r.Cookie(sessionCookieName); err == nil {
		if s.tokenMatch(ck.Value) {
			return true
		}
	}
	return false
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isAuthenticated(r) {
			if strings.HasPrefix(r.URL.Path, "/api/") || strings.Contains(r.Header.Get("Accept"), "application/json") {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleLoginPage(w http.ResponseWriter, _ *http.Request) {
	_ = s.tmpl.Execute(w, map[string]any{"Page": "login"})
}

func (s *Server) handleLoginSubmit(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimSpace(r.FormValue("token"))
	if !s.tokenMatch(tok) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = s.tmpl.Execute(w, map[string]any{"Page": "login", "Error": "Invalid console token"})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    tok,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var breakers []infer.BreakerSnapshot
	if s.deps.Hub != nil {
		breakers = s.deps.Hub.BreakerSnapshots()
	}
	var toolStats []tool.Stat
	if s.deps.Registry != nil {
		toolStats = s.deps.Registry.Stats()
	}
	activeRooms := 0
	if s.deps.Rooms != nil {
		activeRooms = len(s.deps.Rooms.ActiveChannels())
	}
	var kvMetrics []model.KVMetric
	var sumPrompt, sumCached int
	if s.deps.Store != nil {
		kvMetrics, _ = s.deps.Store.RecentKVMetrics(r.Context(), 30)
		for _, m := range kvMetrics {
			sumPrompt += m.PromptTokens
			sumCached += m.CachedTokens
		}
	}
	hitRate := 0.0
	if sumPrompt > 0 {
		hitRate = float64(sumCached) * 100.0 / float64(sumPrompt)
	}

	_ = s.tmpl.Execute(w, map[string]any{
		"Page":        "dashboard",
		"RSSMiB":      readProcessRSSMiB(ms),
		"HeapMiB":     float64(ms.Alloc) / (1024 * 1024),
		"Goroutines":  runtime.NumGoroutine(),
		"ActiveRooms": activeRooms,
		"KVHitRate":   hitRate,
		"Breakers":    breakers,
		"ToolStats":   toolStats,
		"KVMetrics":   kvMetrics,
	})
}

func readProcessRSSMiB(ms runtime.MemStats) float64 {
	if raw, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) >= 2 {
			if pages, err := strconv.ParseInt(fields[1], 10, 64); err == nil && pages > 0 {
				return float64(pages*int64(os.Getpagesize())) / (1024 * 1024)
			}
		}
	}
	return float64(ms.Sys) / (1024 * 1024)
}

func (s *Server) handleConfigPage(w http.ResponseWriter, _ *http.Request) {
	rawYAML := ""
	personaMD := ""
	if s.deps.Config != nil && s.deps.Config.Get() != nil {
		cfg := s.deps.Config.Get()
		if cfg.ConfigPath != "" {
			if b, err := os.ReadFile(cfg.ConfigPath); err == nil {
				rawYAML = string(b)
			}
		}
		if cfg.PersonaPath != "" {
			if b, err := os.ReadFile(cfg.PersonaPath); err == nil {
				personaMD = string(b)
			}
		} else {
			personaMD = cfg.PersonaText
		}
	}
	_ = s.tmpl.Execute(w, map[string]any{
		"Page":       "config",
		"ConfigYAML": rawYAML,
		"PersonaMD":  personaMD,
	})
}

func (s *Server) handleConfigValidate(w http.ResponseWriter, r *http.Request) {
	raw := []byte(r.FormValue("yaml"))
	baseDir := ""
	if s.deps.Config != nil && s.deps.Config.Get() != nil && s.deps.Config.Get().ConfigPath != "" {
		baseDir = filepath.Dir(s.deps.Config.Get().ConfigPath)
	}
	cfg, err := config.ParseBytes(raw, baseDir)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `<span class="badge err">Dry-Run Failed: %s</span>`, html.EscapeString(err.Error()))
		return
	}
	if err := policy.ValidateConfig(cfg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `<span class="badge err">Expr Validation Failed: %s</span>`, html.EscapeString(err.Error()))
		return
	}
	_, _ = w.Write([]byte(`<span class="badge ok">Dry-Run Validated (YAML + all expr rules OK)</span>`))
}

func (s *Server) handlePersonaSave(w http.ResponseWriter, r *http.Request) {
	if s.deps.Config == nil {
		http.Error(w, "config manager unavailable", http.StatusInternalServerError)
		return
	}
	text := r.FormValue("persona")
	if _, err := s.deps.Config.SavePersona(text); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `<span class="badge err">Save Failed: %s</span>`, html.EscapeString(err.Error()))
		return
	}
	_, _ = w.Write([]byte(`<span class="badge ok">Persona Saved & Hot-Reloaded</span>`))
}

func (s *Server) handleConfigSave(w http.ResponseWriter, r *http.Request) {
	raw := []byte(r.FormValue("yaml"))
	if s.deps.Config == nil {
		http.Error(w, "config manager unavailable", http.StatusInternalServerError)
		return
	}
	if _, err := s.deps.Config.SaveAndReload(raw); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `<span class="badge err">Rejected: %s</span>`, html.EscapeString(err.Error()))
		return
	}
	_, _ = w.Write([]byte(`<span class="badge ok">Validated & Hot-Reloaded</span>`))
}

func (s *Server) handleMemoryPage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var channels []store.ChannelState
	if s.deps.Store != nil {
		channels, _ = s.deps.Store.ListChannels(ctx)
	}
	selected := strings.TrimSpace(r.URL.Query().Get("channel"))
	if selected == "" && len(channels) > 0 {
		selected = string(channels[0].ChannelID)
	}

	var notes []model.Note
	var episodes []model.Episode
	var profiles []model.MemberProfile
	var lore []model.LoreEntry
	if selected != "" && s.deps.Store != nil {
		ch := model.ChannelID(selected)
		notes, _ = s.deps.Store.ListNotes(ctx, ch, false)
		episodes, _, _ = s.deps.Store.ListEpisodes(ctx, ch, 30)
		profiles, _ = s.deps.Store.ListProfiles(ctx, ch, 30)
		lore, _ = s.deps.Store.ListLore(ctx, ch, 50)
	}
	_ = s.tmpl.Execute(w, map[string]any{
		"Page":            "memory",
		"Channels":        channels,
		"SelectedChannel": selected,
		"Notes":           notes,
		"Episodes":        episodes,
		"Profiles":        profiles,
		"Lore":            lore,
	})
}

func (s *Server) handleNoteSave(w http.ResponseWriter, r *http.Request) {
	if s.deps.Store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	ch := model.ChannelID(strings.TrimSpace(r.FormValue("channel")))
	key := strings.TrimSpace(r.FormValue("key"))
	content := strings.TrimSpace(r.FormValue("content"))
	if ch == "" || key == "" || content == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("missing fields"))
		return
	}
	if err := s.deps.Store.SaveNote(r.Context(), model.Note{
		ChannelID: ch, Key: key, Content: content, AuthorID: "console", Pinned: true,
	}, 50); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(html.EscapeString(err.Error())))
		return
	}
	_, _ = w.Write([]byte(`<span class="badge ok">Saved (refresh to view updated table)</span>`))
}

func (s *Server) handleNoteDelete(w http.ResponseWriter, r *http.Request) {
	if s.deps.Store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	ch := model.ChannelID(strings.TrimSpace(r.FormValue("channel")))
	key := strings.TrimSpace(r.FormValue("key"))
	_, _ = s.deps.Store.DeleteNote(r.Context(), ch, key)
	_, _ = w.Write([]byte(`<span class="muted">deleted</span>`))
}

func (s *Server) handleStickersPage(w http.ResponseWriter, r *http.Request) {
	var review []model.Sticker
	if s.deps.Store != nil {
		rows, _ := s.deps.Store.ListStickers(r.Context(), "needs_review")
		for _, row := range rows {
			review = append(review, row.Sticker)
		}
	}
	_ = s.tmpl.Execute(w, map[string]any{
		"Page":           "stickers",
		"ReviewStickers": review,
	})
}

func (s *Server) handleStickerSearch(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if s.deps.Stickers == nil || q == "" {
		_, _ = w.Write([]byte(`<p class="muted">No results.</p>`))
		return
	}
	explicit := r.URL.Query().Get("explicit") == "1"
	hits, err := s.deps.Stickers.Search(r.Context(), sticker.SearchOptions{
		Query:          q,
		ExplicitLookup: explicit,
		TopK:           10,
	})
	if err != nil {
		_, _ = fmt.Fprintf(w, `<span class="badge err">%s</span>`, html.EscapeString(err.Error()))
		return
	}
	var b strings.Builder
	b.WriteString(`<table><thead><tr><th>ID</th><th>Pack</th><th>Final</th><th>v_intent</th><th>v_content</th><th>FTS</th><th>RRF</th><th>Rerank</th><th>Decay</th><th>Intent / Content</th></tr></thead><tbody>`)
	for _, h := range hits {
		fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.1f</td><td>%.3f</td><td>%.3f</td><td>%.2f</td><td>%s | %s</td></tr>`,
			html.EscapeString(h.Sticker.StickerID), html.EscapeString(h.Sticker.PackID),
			h.FinalScore, h.IntentScore, h.ContentScore, h.FTSScore, h.RRFScore, h.RerankScore, h.DecayFactor,
			html.EscapeString(h.Sticker.IntentText), html.EscapeString(h.Sticker.ContentText))
	}
	b.WriteString(`</tbody></table>`)
	_, _ = w.Write([]byte(b.String()))
}

func (s *Server) handleStickerImage(w http.ResponseWriter, r *http.Request) {
	if s.deps.Store == nil || s.deps.Stickers == nil {
		http.NotFound(w, r)
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.NotFound(w, r)
		return
	}
	for _, status := range []string{"needs_review", "ready"} {
		rows, err := s.deps.Store.ListStickers(r.Context(), status)
		if err != nil {
			continue
		}
		for _, row := range rows {
			if row.Sticker.StickerID == id {
				raw, rerr := s.deps.Stickers.ReadStickerFile(row.Sticker)
				if rerr != nil {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", sticker.MimeTypeFor(row.Sticker))
				w.Header().Set("Cache-Control", "private, max-age=300")
				_, _ = w.Write(raw)
				return
			}
		}
	}
	http.NotFound(w, r)
}

func (s *Server) handleStickerApprove(w http.ResponseWriter, r *http.Request) {
	if s.deps.Store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	id := strings.TrimSpace(r.FormValue("id"))
	ocr := strings.TrimSpace(r.FormValue("ocr"))
	intent := strings.TrimSpace(r.FormValue("intent"))
	content := strings.TrimSpace(r.FormValue("content"))
	if err := s.deps.Store.UpdateStickerStatus(r.Context(), id, "ready", ocr, intent, content); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(html.EscapeString(err.Error())))
		return
	}
	// Refresh live RAM index & embed vectors immediately so approved sticker is searchable right away.
	if s.deps.Stickers != nil {
		_ = s.deps.Stickers.ReindexOne(r.Context(), id)
	}
	_, _ = w.Write([]byte(`<span class="badge ok">Approved & Indexed</span>`))
}

func (s *Server) handleStickerReject(w http.ResponseWriter, r *http.Request) {
	if s.deps.Store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	id := strings.TrimSpace(r.FormValue("id"))
	if err := s.deps.Store.UpdateStickerStatus(r.Context(), id, "disabled", "", "", ""); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(html.EscapeString(err.Error())))
		return
	}
	_, _ = w.Write([]byte(`<span class="muted">Rejected</span>`))
}

func (s *Server) handleLogsPage(w http.ResponseWriter, _ *http.Request) {
	var recent []string
	if s.deps.Logs != nil {
		recent = s.deps.Logs.Recent()
	}
	_ = s.tmpl.Execute(w, map[string]any{
		"Page":       "logs",
		"RecentLogs": recent,
	})
}

func (s *Server) handleLogsStream(w http.ResponseWriter, r *http.Request) {
	if s.deps.Logs == nil {
		http.Error(w, "logs unavailable", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, unsub := s.deps.Logs.Subscribe()
	defer unsub()

	for {
		select {
		case <-r.Context().Done():
			return
		case line := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", line)
			flusher.Flush()
		}
	}
}
