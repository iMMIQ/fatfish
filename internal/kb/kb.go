package kb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/store"
)

// Chunk represents one structured section stored in a read-only external knowledge base (kb.db).
type Chunk struct {
	ID         int64
	Source     string
	BookID     string
	BookTitle  string
	Entity     string
	Breadcrumb string
	Category   string
	PubYear    int
	Priority   int
	Content    string
	Score      float64
}

// SearchOptions configures a read-only knowledge base lookup.
type SearchOptions struct {
	Query    string
	Entity   string
	Category string
	Limit    int
}

// Service provides zero-RAM-footprint read-only FTS5 retrieval + optional local Rerank
// against an externally built SQLite kb.db, automatically hot-reopening when kb.db is replaced on disk.
type Service struct {
	hub *infer.Hub
	log *slog.Logger

	mu      sync.Mutex
	path    string
	db      *sql.DB
	modTime time.Time
	size    int64
}

func NewService(dbPath string, hub *infer.Hub, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		hub:  hub,
		log:  logger,
		path: strings.TrimSpace(dbPath),
	}
}

// UpdatePath hot-swaps the configured kb.db path (closing any open handle so the next query opens cleanly).
func (s *Service) UpdatePath(dbPath string) {
	p := strings.TrimSpace(dbPath)
	s.mu.Lock()
	if s.path != p {
		if s.db != nil {
			_ = s.db.Close()
			s.db = nil
		}
		s.path = p
		s.modTime = time.Time{}
		s.size = 0
	}
	s.mu.Unlock()
}

// Enabled reports whether a non-empty kb.db path is configured and currently exists on disk.
func (s *Service) Enabled() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	p := s.path
	s.mu.Unlock()
	if p == "" {
		return false
	}
	st, err := os.Stat(p)
	return err == nil && !st.IsDir() && st.Size() > 0
}

// Close releases the underlying read-only SQLite connection.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		return err
	}
	return nil
}

// ensureDB opens kb.db read-only and transparently reopens it if the file was atomically replaced on disk.
func (s *Service) ensureDB() (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return nil, fmt.Errorf("kb: database path not configured")
	}
	fi, err := os.Stat(s.path)
	if err != nil {
		return nil, fmt.Errorf("kb: database unavailable: %w", err)
	}
	if s.db != nil && fi.ModTime().Equal(s.modTime) && fi.Size() == s.size {
		return s.db, nil
	}
	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
	u := &url.URL{Scheme: "file", Path: s.path}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_busy_timeout", "2000")
	u.RawQuery = q.Encode()

	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("kb: open sqlite: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("kb: ping sqlite: %w", err)
	}
	s.db = db
	s.modTime = fi.ModTime()
	s.size = fi.Size()
	return db, nil
}

// Meta returns key/value stats from kb_meta (e.g. total_books, total_chunks).
func (s *Service) Meta(ctx context.Context) (map[string]string, error) {
	db, err := s.ensureDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT key, value FROM kb_meta`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			out[k] = v
		}
	}
	return out, nil
}

// Search performs zero-RAM FTS5 candidate recall (entity-scoped first, then broadened fallback)
// followed by optional local Rerank precision ordering and section-level deduplication.
func (s *Service) Search(ctx context.Context, opts SearchOptions) ([]Chunk, error) {
	db, err := s.ensureDB()
	if err != nil {
		return nil, err
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 4
	}
	if limit > 8 {
		limit = 8
	}
	candLimit := limit * 5
	if candLimit < 20 {
		candLimit = 20
	}
	if candLimit > 32 {
		candLimit = 32
	}

	query := strings.TrimSpace(opts.Query)
	entity := strings.TrimSpace(opts.Entity)
	category := strings.ToLower(strings.TrimSpace(opts.Category))
	if category == "all" {
		category = ""
	}
	if query == "" && entity == "" {
		return nil, fmt.Errorf("kb: query or entity is required")
	}

	qExpr := store.BuildFTSQuery(query, false)
	eExpr := store.BuildFTSQuery(entity, false)

	var cands []Chunk
	seen := make(map[int64]struct{})
	appendUnique := func(rows []Chunk) {
		for _, c := range rows {
			if _, ok := seen[c.ID]; ok {
				continue
			}
			seen[c.ID] = struct{}{}
			cands = append(cands, c)
		}
	}

	// Stage 1A: strict entity + content match.
	if eExpr != "" && qExpr != "" {
		matchExpr := fmt.Sprintf("entity_tokens : (%s) AND tokens : (%s)", eExpr, qExpr)
		if rows, qerr := queryFTS(ctx, db, matchExpr, category, candLimit); qerr == nil {
			appendUnique(rows)
		}
	}

	// Stage 1B: entity-scoped match if strict match returned too few candidates.
	if len(cands) < limit*2 && eExpr != "" {
		matchExpr := fmt.Sprintf("entity_tokens : (%s)", eExpr)
		if rows, qerr := queryFTS(ctx, db, matchExpr, category, candLimit); qerr == nil {
			appendUnique(rows)
		}
	}

	// Stage 1C: relax category filter if category was too restrictive.
	if len(cands) < limit && category != "" && eExpr != "" {
		matchExpr := fmt.Sprintf("entity_tokens : (%s)", eExpr)
		if qExpr != "" {
			matchExpr = fmt.Sprintf("entity_tokens : (%s) AND tokens : (%s)", eExpr, qExpr)
		}
		if rows, qerr := queryFTS(ctx, db, matchExpr, "", candLimit); qerr == nil {
			appendUnique(rows)
		}
	}

	// Stage 1D: general full-text fallback across all chunks.
	if len(cands) == 0 {
		combExpr := store.BuildFTSQuery(strings.TrimSpace(entity+" "+query), false)
		if combExpr != "" {
			if rows, qerr := queryFTS(ctx, db, combExpr, category, candLimit); qerr == nil {
				appendUnique(rows)
			}
			if len(cands) == 0 && category != "" {
				if rows, qerr := queryFTS(ctx, db, combExpr, "", candLimit); qerr == nil {
					appendUnique(rows)
				}
			}
		}
	}

	if len(cands) == 0 {
		return nil, nil
	}
	if len(cands) > candLimit {
		cands = cands[:candLimit]
	}

	// Stage 2: precision rerank via local Rerank model (gracefully skipped when unavailable).
	rerankQ := strings.TrimSpace(entity + " " + query)
	if s.hub != nil && len(cands) > 1 && rerankQ != "" {
		docs := make([]string, len(cands))
		for i, c := range cands {
			docs[i] = fmt.Sprintf("[%s | %s]\n%s", c.BookTitle, c.Breadcrumb, clipRunesKB(c.Content, 600))
		}
		if rres, rerr := s.hub.Rerank(ctx, rerankQ, docs); rerr == nil && len(rres) > 0 {
			type ranked struct {
				chunk Chunk
				score float64
			}
			scored := make([]ranked, 0, len(rres))
			for _, r := range rres {
				if r.Index < 0 || r.Index >= len(cands) {
					continue
				}
				ch := cands[r.Index]
				// Gentle priority blend favors newer editions & custom curated notes on near-ties.
				blended := r.Score + float64(ch.Priority)/100.0*0.06
				ch.Score = blended
				scored = append(scored, ranked{chunk: ch, score: blended})
			}
			if len(scored) > 0 {
				sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
				reordered := make([]Chunk, len(scored))
				for i, sc := range scored {
					reordered[i] = sc.chunk
				}
				cands = reordered
			}
		}
	}

	// Stage 3: section-level diversity cap (at most 2 chunks per book+breadcrumb).
	out := make([]Chunk, 0, limit)
	bcCount := make(map[string]int)
	for _, c := range cands {
		key := c.BookID + "|" + c.Breadcrumb
		if bcCount[key] >= 2 {
			continue
		}
		bcCount[key]++
		out = append(out, c)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func queryFTS(ctx context.Context, db *sql.DB, matchExpr, category string, limit int) ([]Chunk, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT c.id, c.source, c.book_id, c.book_title, c.entity, c.breadcrumb, c.category, c.pub_year, c.priority, c.content,
		bm25(kb_chunks_fts, 1.0, 2.5) AS score
		FROM kb_chunks_fts f
		JOIN kb_chunks c ON c.id = f.rowid
		WHERE kb_chunks_fts MATCH ?`)
	args := []any{matchExpr}
	if category != "" {
		sb.WriteString(` AND c.category = ?`)
		args = append(args, category)
	}
	sb.WriteString(` ORDER BY score - (c.priority * 0.05) ASC LIMIT ?`)
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Chunk
	for rows.Next() {
		var c Chunk
		if err := rows.Scan(
			&c.ID, &c.Source, &c.BookID, &c.BookTitle, &c.Entity,
			&c.Breadcrumb, &c.Category, &c.PubYear, &c.Priority, &c.Content, &c.Score,
		); err == nil {
			out = append(out, c)
		}
	}
	return out, nil
}

// FormatResults formats retrieved chunks concisely for LLM grounding with clear freshness boundaries.
func FormatResults(chunks []Chunk) string {
	if len(chunks) == 0 {
		return "(no matching reference entries found in local knowledge base)"
	}
	var b strings.Builder
	b.WriteString("[Reference KB Note: Use for geographic structure, route skeletons, neighborhood vibe, and cultural depth. Always verify live prices, business open/closed status, ticket booking rules, and visa/tax policies via web search before quoting specifics.]\n\n")
	for i, c := range chunks {
		yr := "unknown"
		if c.PubYear > 0 {
			yr = fmt.Sprintf("%d", c.PubYear)
		}
		fmt.Fprintf(&b, "--- [%d] %s | %s (cat=%s, year=%s, src=%s) ---\n%s\n\n",
			i+1, c.BookTitle, c.Breadcrumb, c.Category, yr, c.Source, clipRunesKB(c.Content, 750))
	}
	return strings.TrimSpace(b.String())
}

func clipRunesKB(s string, max int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= max {
		return string(r)
	}
	return strings.TrimSpace(string(r[:max])) + "..."
}
