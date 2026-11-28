package kb

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/iMMIQ/fatfish/internal/store"
)

func seedTestKB(t *testing.T, dbPath string, rows []Chunk) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open test kb: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE kb_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		CREATE TABLE kb_chunks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source TEXT NOT NULL,
			book_id TEXT NOT NULL,
			book_title TEXT NOT NULL,
			entity TEXT NOT NULL,
			breadcrumb TEXT NOT NULL,
			category TEXT NOT NULL DEFAULT 'general',
			pub_year INTEGER NOT NULL DEFAULT 0,
			priority INTEGER NOT NULL DEFAULT 50,
			content TEXT NOT NULL
		);
		CREATE VIRTUAL TABLE kb_chunks_fts USING fts5(
			tokens, entity_tokens, category UNINDEXED, priority UNINDEXED, content='', tokenize='unicode61'
		);
		INSERT INTO kb_meta(key, value) VALUES ('total_books', '2'), ('total_chunks', '3');
	`)
	if err != nil {
		t.Fatalf("init test kb schema: %v", err)
	}
	for _, r := range rows {
		res, err := db.Exec(`INSERT INTO kb_chunks (source, book_id, book_title, entity, breadcrumb, category, pub_year, priority, content)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.Source, r.BookID, r.BookTitle, r.Entity, r.Breadcrumb, r.Category, r.PubYear, r.Priority, r.Content)
		if err != nil {
			t.Fatalf("insert chunk: %v", err)
		}
		id, _ := res.LastInsertId()
		toks := store.TokenizeCJK(r.BookTitle + " " + r.Breadcrumb + " " + r.Content)
		entToks := store.TokenizeCJK(r.Entity + " " + r.Breadcrumb)
		if _, err := db.Exec(`INSERT INTO kb_chunks_fts (rowid, tokens, entity_tokens, category, priority) VALUES (?, ?, ?, ?, ?)`,
			id, toks, entToks, r.Category, r.Priority); err != nil {
			t.Fatalf("insert fts: %v", err)
		}
	}
}

func TestKBSearchEntityScopingPriorityAndHotReload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "kb.db")
	seedTestKB(t, dbPath, []Chunk{
		{
			Source:     "travel_guide",
			BookID:     "japan_new",
			BookTitle:  "日本指南 (2024)",
			Entity:     "日本 京都 关西 Japan Kyoto Kansai",
			Breadcrumb: "日本 > 关西 > 京都 > 东山路线",
			Category:   "itinerary",
			PubYear:    2024,
			Priority:   85,
			Content:    "东山经典步行路线从清水寺出发，经二年坂三年坂直达八坂神社与祇园，清晨七点半前避开人流最舒适。",
		},
		{
			Source:     "travel_guide",
			BookID:     "iceland_new",
			BookTitle:  "冰岛指南 (2024)",
			Entity:     "冰岛 雷克雅未克 黄金圈 Iceland Reykjavik",
			Breadcrumb: "冰岛 > 西南部 > 黄金圈自驾",
			Category:   "itinerary",
			PubYear:    2024,
			Priority:   82,
			Content:    "黄金圈经典环线串联辛格维利尔国家公园、盖歇尔间歇泉与黄金瀑布，从雷克雅未克出发一天往返刚好。",
		},
		{
			Source:     "custom",
			BookID:     "custom_kyoto",
			BookTitle:  "京都精选住宿笔记",
			Entity:     "京都 日本 Kyoto",
			Breadcrumb: "京都住宿 > 片区选址",
			Category:   "neighborhood",
			PubYear:    2026,
			Priority:   100,
			Content:    "京都住宿优先选四条河原町或京都站周边，逛街换乘最顺手，避免频繁换酒店折腾行李。",
		},
	})

	svc := NewService(dbPath, nil, nil)
	defer svc.Close()
	if !svc.Enabled() {
		t.Fatalf("expected kb service enabled")
	}

	ctx := context.Background()
	meta, err := svc.Meta(ctx)
	if err != nil || meta["total_books"] != "2" {
		t.Fatalf("unexpected meta: %+v err=%v", meta, err)
	}

	// 1. Entity-scoped query must strictly return Kyoto entries, not Iceland.
	hits, err := svc.Search(ctx, SearchOptions{
		Entity:   "京都",
		Query:    "清水寺 东山 路线",
		Category: "itinerary",
		Limit:    3,
	})
	if err != nil || len(hits) == 0 {
		t.Fatalf("expected Kyoto itinerary hit, got %d err=%v", len(hits), err)
	}
	if !strings.Contains(hits[0].Content, "清水寺") {
		t.Fatalf("unexpected top hit: %+v", hits[0])
	}

	formatted := FormatResults(hits)
	if !strings.Contains(formatted, "Reference KB Note") || !strings.Contains(formatted, "清水寺") {
		t.Fatalf("unexpected formatted output:\n%s", formatted)
	}

	// 2. Atomic file swap on disk must hot-reopen without restarting service.
	newPath := filepath.Join(dir, "kb.db.new")
	seedTestKB(t, newPath, []Chunk{
		{
			Source:     "custom",
			BookID:     "custom_xiamen",
			BookTitle:  "厦门慢游笔记",
			Entity:     "厦门 鼓浪屿 Xiamen",
			Breadcrumb: "厦门岛 > 环岛路与沙坡尾",
			Category:   "itinerary",
			PubYear:    2026,
			Priority:   100,
			Content:    "厦门本岛推荐晨逛八市，下午沿环岛路骑行到沙坡尾看日落，节奏最舒服。",
		},
	})
	// Ensure mtime advances distinctly on fast filesystems.
	future := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(newPath, future, future)
	if err := os.Rename(newPath, dbPath); err != nil {
		t.Fatalf("atomic swap kb.db: %v", err)
	}

	hits2, err := svc.Search(ctx, SearchOptions{Entity: "厦门", Query: "沙坡尾 环岛路"})
	if err != nil || len(hits2) == 0 || !strings.Contains(hits2[0].Content, "沙坡尾") {
		t.Fatalf("expected hot-reopened厦门 hit, got %+v err=%v", hits2, err)
	}
}
