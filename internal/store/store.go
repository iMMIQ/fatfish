package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/iMMIQ/fatfish/internal/model"
)

// Store provides dual-pool SQLite persistence (writeDB=1 conn 8MiB, readDB=6 conns 12MiB)
// plus the SHA-256 canonical BlobStore.
type Store struct {
	writeDB *sql.DB
	readDB  *sql.DB
	blobs   *BlobStore
	dbPath  string
}

// Open initializes SQLite WAL databases, runs schema migrations, and initializes BlobStore.
func Open(dataDir string, imageMaxDim, maxImageMiB int) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir %s: %w", dataDir, err)
	}
	dbPath := filepath.Join(dataDir, "fatfish.db")

	writeDSN := buildDSN(dbPath, false)
	writeDB, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("store: open writeDB: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)

	if _, err := writeDB.Exec(schemaSQL); err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("store: migrate schema: %w", err)
	}
	// Idempotent additive migrations for existing SQLite databases.
	_, _ = writeDB.Exec(`ALTER TABLE profiles ADD COLUMN style_note TEXT NOT NULL DEFAULT ''`)
	_, _ = writeDB.Exec(`ALTER TABLE schedules ADD COLUMN market_days_only TEXT NOT NULL DEFAULT ''`)

	readDSN := buildDSN(dbPath, true)
	readDB, err := sql.Open("sqlite", readDSN)
	if err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("store: open readDB: %w", err)
	}
	readDB.SetMaxOpenConns(6)
	readDB.SetMaxIdleConns(6)

	blobs, err := NewBlobStore(filepath.Join(dataDir, "blobs"), imageMaxDim, maxImageMiB)
	if err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, err
	}

	return &Store{
		writeDB: writeDB,
		readDB:  readDB,
		blobs:   blobs,
		dbPath:  dbPath,
	}, nil
}

func buildDSN(dbPath string, readOnly bool) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "temp_store(MEMORY)")
	if readOnly {
		q.Add("mode", "ro")
		q.Add("_pragma", "cache_size(-2000)")
	} else {
		q.Add("mode", "rwc")
		q.Add("_pragma", "journal_mode(WAL)")
		q.Add("_pragma", "synchronous(NORMAL)")
		q.Add("_pragma", "foreign_keys(ON)")
		q.Add("_pragma", "cache_size(-8000)")
	}
	return "file:" + filepath.ToSlash(dbPath) + "?" + q.Encode()
}

// Blobs returns the SHA-256 canonical BlobStore.
func (s *Store) Blobs() *BlobStore { return s.blobs }

// Checkpoint executes PRAGMA wal_checkpoint(mode) ("PASSIVE" or "TRUNCATE").
func (s *Store) Checkpoint(ctx context.Context, mode string) error {
	if mode != "TRUNCATE" {
		mode = "PASSIVE"
	}
	_, err := s.writeDB.ExecContext(ctx, "PRAGMA wal_checkpoint("+mode+")")
	return err
}

// Close flushes WAL with PASSIVE checkpoint and closes both pools.
func (s *Store) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Checkpoint(ctx, "PASSIVE")
	err1 := s.readDB.Close()
	err2 := s.writeDB.Close()
	if err2 != nil {
		return err2
	}
	return err1
}

// --- Channel & Epoch State ---

type ChannelState struct {
	ChannelID       model.ChannelID
	Epoch           int64
	ActiveSegmentID int64
	MuteUntil       time.Time
	UpdatedAt       time.Time
}

func (s *Store) EnsureChannel(ctx context.Context, ch model.ChannelID) (ChannelState, error) {
	now := time.Now().UnixMilli()
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT OR IGNORE INTO channels (channel_id, epoch, active_segment_id, mute_until, updated_at) VALUES (?, 1, 0, 0, ?)`,
		string(ch), now,
	)
	if err != nil {
		return ChannelState{}, err
	}
	return s.GetChannel(ctx, ch)
}

func (s *Store) GetChannel(ctx context.Context, ch model.ChannelID) (ChannelState, error) {
	var st ChannelState
	var id string
	var muteMs, updMs int64
	err := s.readDB.QueryRowContext(ctx,
		`SELECT channel_id, epoch, active_segment_id, mute_until, updated_at FROM channels WHERE channel_id = ?`,
		string(ch),
	).Scan(&id, &st.Epoch, &st.ActiveSegmentID, &muteMs, &updMs)
	if err != nil {
		return ChannelState{}, err
	}
	st.ChannelID = model.ChannelID(id)
	if muteMs > 0 {
		st.MuteUntil = time.UnixMilli(muteMs)
	}
	st.UpdatedAt = time.UnixMilli(updMs)
	return st, nil
}

func (s *Store) ListChannels(ctx context.Context) ([]ChannelState, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT channel_id, epoch, active_segment_id, mute_until, updated_at FROM channels ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChannelState
	for rows.Next() {
		var st ChannelState
		var id string
		var muteMs, updMs int64
		if err := rows.Scan(&id, &st.Epoch, &st.ActiveSegmentID, &muteMs, &updMs); err != nil {
			return nil, err
		}
		st.ChannelID = model.ChannelID(id)
		if muteMs > 0 {
			st.MuteUntil = time.UnixMilli(muteMs)
		}
		st.UpdatedAt = time.UnixMilli(updMs)
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) BumpChannelEpoch(ctx context.Context, ch model.ChannelID) (int64, error) {
	now := time.Now().UnixMilli()
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO channels (channel_id, epoch, active_segment_id, mute_until, updated_at) VALUES (?, 2, 0, 0, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET epoch = epoch + 1, active_segment_id = 0, updated_at = excluded.updated_at`,
		string(ch), now,
	)
	if err != nil {
		return 0, err
	}
	st, err := s.GetChannel(ctx, ch)
	return st.Epoch, err
}

func (s *Store) SetChannelActiveSegment(ctx context.Context, ch model.ChannelID, segID int64) error {
	now := time.Now().UnixMilli()
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE channels SET active_segment_id = ?, updated_at = ? WHERE channel_id = ?`,
		segID, now, string(ch),
	)
	return err
}

func (s *Store) SetChannelMuteUntil(ctx context.Context, ch model.ChannelID, until time.Time) error {
	now := time.Now().UnixMilli()
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO channels (channel_id, epoch, active_segment_id, mute_until, updated_at) VALUES (?, 1, 0, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET mute_until = excluded.mute_until, updated_at = excluded.updated_at`,
		string(ch), until.UnixMilli(), now,
	)
	return err
}

// --- Members & Alias Ledger ---

func isPlaceholderDisplayName(name, memberID string) bool {
	t := strings.TrimSpace(name)
	if t == "" || t == memberID {
		return true
	}
	// Purely numeric names (e.g. raw QQ ID) are treated as placeholder names.
	for _, r := range t {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *Store) UpsertMember(ctx context.Context, m model.Member) error {
	if m.MemberID == "" {
		return nil
	}
	ts := m.UpdatedAt.UnixMilli()
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	role := m.Role
	if role == "" {
		role = "member"
	}
	name := strings.TrimSpace(m.DisplayName)
	if name == "" {
		name = m.MemberID
	}

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if isPlaceholderDisplayName(name, m.MemberID) {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO members (channel_id, member_id, display_name, role, last_seen_at) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(channel_id, member_id) DO UPDATE SET last_seen_at = MAX(last_seen_at, excluded.last_seen_at)`,
			string(m.ChannelID), m.MemberID, name, role, ts,
		)
	} else {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO members (channel_id, member_id, display_name, role, last_seen_at) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(channel_id, member_id) DO UPDATE SET
			   display_name = excluded.display_name,
			   role = CASE WHEN excluded.role != 'member' THEN excluded.role ELSE members.role END,
			   last_seen_at = MAX(last_seen_at, excluded.last_seen_at)`,
			string(m.ChannelID), m.MemberID, name, role, ts,
		)
		if err == nil {
			_, err = tx.ExecContext(ctx,
				`INSERT INTO member_aliases (channel_id, member_id, alias, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)
				 ON CONFLICT(channel_id, member_id, alias) DO UPDATE SET
				   first_seen = MIN(first_seen, excluded.first_seen),
				   last_seen = MAX(last_seen, excluded.last_seen)`,
				string(m.ChannelID), m.MemberID, name, ts, ts,
			)
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ResolveMembersByAlias(ctx context.Context, ch model.ChannelID, aliasLike string) ([]string, error) {
	pat := "%" + strings.TrimSpace(aliasLike) + "%"
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT DISTINCT member_id FROM member_aliases WHERE channel_id = ? AND alias LIKE ?
		 UNION
		 SELECT DISTINCT member_id FROM members WHERE channel_id = ? AND (display_name LIKE ? OR member_id = ?)`,
		string(ch), pat, string(ch), pat, strings.TrimSpace(aliasLike),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) GetMemberNames(ctx context.Context, ch model.ChannelID) (map[string]string, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT member_id, display_name FROM members WHERE channel_id = ?`, string(ch))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[id] = name
	}
	return out, rows.Err()
}

// --- L0 Messages & FTS5 ---

func (s *Store) AppendMessage(ctx context.Context, msg *model.Message) error {
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	partsJSON, err := json.Marshal(msg.Parts)
	if err != nil {
		return err
	}
	plain := msg.PlainText()
	dedup := model.DedupHash(plain)
	fromBot := 0
	if msg.FromBot {
		fromBot = 1
	}

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if msg.Seq <= 0 {
		var maxSeq int64
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(seq), 0) FROM messages WHERE channel_id = ?`, string(msg.ChannelID),
		).Scan(&maxSeq); err != nil {
			return err
		}
		msg.Seq = maxSeq + 1
	}

	res, err := tx.ExecContext(ctx,
		`INSERT INTO messages (channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, plain_text, dedup_hash, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(msg.ChannelID), msg.Seq, msg.PlatformMsgID, msg.SenderID, msg.SenderName,
		fromBot, string(partsJSON), plain, dedup, msg.CreatedAt.UnixMilli(),
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	msg.ID = id

	toks := TokenizeCJK(msg.SenderName + " " + plain)
	if toks != "" {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO messages_fts (rowid, tokens, channel_id, sender_id, created_at) VALUES (?, ?, ?, ?, ?)`,
			id, toks, string(msg.ChannelID), msg.SenderID, msg.CreatedAt.UnixMilli(),
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// IngestMessagesBatch inserts historical messages in transactional chunks (e.g. 2000 rows/tx)
// for 50x-100x faster bulk ingest while remaining 100% idempotent on (channel_id, created_at, sender_id, dedup_hash).
func (s *Store) IngestMessagesBatch(ctx context.Context, msgs []*model.Message) (inserted int, skipped int, err error) {
	const chunkSize = 2000
	maxSeqByCh := make(map[string]int64)

	for start := 0; start < len(msgs); start += chunkSize {
		end := start + chunkSize
		if end > len(msgs) {
			end = len(msgs)
		}
		chunk := msgs[start:end]

		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return inserted, skipped, err
		}

		checkStmt, err := tx.PrepareContext(ctx,
			`SELECT 1 FROM messages WHERE channel_id = ? AND created_at = ? AND sender_id = ? AND dedup_hash = ? LIMIT 1`)
		if err != nil {
			_ = tx.Rollback()
			return inserted, skipped, err
		}
		insMsgStmt, err := tx.PrepareContext(ctx,
			`INSERT INTO messages (channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, plain_text, dedup_hash, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			checkStmt.Close()
			_ = tx.Rollback()
			return inserted, skipped, err
		}
		insFTSStmt, err := tx.PrepareContext(ctx,
			`INSERT INTO messages_fts (rowid, tokens, channel_id, sender_id, created_at) VALUES (?, ?, ?, ?, ?)`)
		if err != nil {
			checkStmt.Close()
			insMsgStmt.Close()
			_ = tx.Rollback()
			return inserted, skipped, err
		}
		memStmt, err := tx.PrepareContext(ctx,
			`INSERT INTO members (channel_id, member_id, display_name, role, last_seen_at) VALUES (?, ?, ?, 'member', ?)
			 ON CONFLICT(channel_id, member_id) DO UPDATE SET
			   display_name = CASE WHEN excluded.display_name != excluded.member_id THEN excluded.display_name ELSE members.display_name END,
			   last_seen_at = MAX(last_seen_at, excluded.last_seen_at)`)
		if err != nil {
			checkStmt.Close()
			insMsgStmt.Close()
			insFTSStmt.Close()
			_ = tx.Rollback()
			return inserted, skipped, err
		}
		aliasStmt, err := tx.PrepareContext(ctx,
			`INSERT INTO member_aliases (channel_id, member_id, alias, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(channel_id, member_id, alias) DO UPDATE SET
			   first_seen = MIN(first_seen, excluded.first_seen),
			   last_seen = MAX(last_seen, excluded.last_seen)`)
		if err != nil {
			checkStmt.Close()
			insMsgStmt.Close()
			insFTSStmt.Close()
			memStmt.Close()
			_ = tx.Rollback()
			return inserted, skipped, err
		}

		for _, msg := range chunk {
			chStr := string(msg.ChannelID)
			tsMs := msg.CreatedAt.UnixMilli()
			plain := msg.PlainText()
			dedup := model.DedupHash(plain)

			var exists int
			if qerr := checkStmt.QueryRowContext(ctx, chStr, tsMs, msg.SenderID, dedup).Scan(&exists); qerr == nil {
				skipped++
				continue
			}

			seq, ok := maxSeqByCh[chStr]
			if !ok {
				_ = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM messages WHERE channel_id = ?`, chStr).Scan(&seq)
			}
			seq++
			maxSeqByCh[chStr] = seq
			msg.Seq = seq

			partsJSON, _ := json.Marshal(msg.Parts)
			fromBot := 0
			if msg.FromBot {
				fromBot = 1
			}
			res, ierr := insMsgStmt.ExecContext(ctx, chStr, seq, msg.PlatformMsgID, msg.SenderID, msg.SenderName,
				fromBot, string(partsJSON), plain, dedup, tsMs)
			if ierr != nil {
				checkStmt.Close()
				insMsgStmt.Close()
				insFTSStmt.Close()
				memStmt.Close()
				aliasStmt.Close()
				_ = tx.Rollback()
				return inserted, skipped, ierr
			}
			rowID, _ := res.LastInsertId()
			msg.ID = rowID
			inserted++

			if toks := TokenizeCJK(msg.SenderName + " " + plain); toks != "" {
				_, _ = insFTSStmt.ExecContext(ctx, rowID, toks, chStr, msg.SenderID, tsMs)
			}
			name := strings.TrimSpace(msg.SenderName)
			if name == "" {
				name = msg.SenderID
			}
			_, _ = memStmt.ExecContext(ctx, chStr, msg.SenderID, name, tsMs)
			if !isPlaceholderDisplayName(name, msg.SenderID) {
				_, _ = aliasStmt.ExecContext(ctx, chStr, msg.SenderID, name, tsMs, tsMs)
			}
		}

		checkStmt.Close()
		insMsgStmt.Close()
		insFTSStmt.Close()
		memStmt.Close()
		aliasStmt.Close()

		// Ensure channel rows exist for all touched channels so ListChannels sees ingested channels.
		nowMs := time.Now().UnixMilli()
		for chStr := range maxSeqByCh {
			_, _ = tx.ExecContext(ctx,
				`INSERT OR IGNORE INTO channels (channel_id, epoch, active_segment_id, mute_until, updated_at) VALUES (?, 1, 0, 0, ?)`,
				chStr, nowMs)
		}

		if cerr := tx.Commit(); cerr != nil {
			return inserted, skipped, cerr
		}
	}
	return inserted, skipped, nil
}

// IngestMessage inserts a historical message idempotently (deduplicating on channel, ts, sender, text hash).
func (s *Store) IngestMessage(ctx context.Context, msg *model.Message) (inserted bool, err error) {
	plain := msg.PlainText()
	dedup := model.DedupHash(plain)
	var exists int
	err = s.readDB.QueryRowContext(ctx,
		`SELECT 1 FROM messages WHERE channel_id = ? AND created_at = ? AND sender_id = ? AND dedup_hash = ? LIMIT 1`,
		string(msg.ChannelID), msg.CreatedAt.UnixMilli(), msg.SenderID, dedup,
	).Scan(&exists)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err := s.AppendMessage(ctx, msg); err != nil {
		return false, err
	}
	_ = s.UpsertMember(ctx, model.Member{
		ChannelID:   msg.ChannelID,
		MemberID:    msg.SenderID,
		DisplayName: msg.SenderName,
		UpdatedAt:   msg.CreatedAt,
	})
	return true, nil
}

// PatchMessageParts updates a persisted message's parts (e.g. async ASR backfill) and re-indexes FTS5.
func (s *Store) PatchMessageParts(ctx context.Context, msg *model.Message) error {
	partsJSON, err := json.Marshal(msg.Parts)
	if err != nil {
		return err
	}
	plain := msg.PlainText()
	dedup := model.DedupHash(plain)

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`UPDATE messages SET parts_json = ?, plain_text = ?, dedup_hash = ? WHERE id = ?`,
		string(partsJSON), plain, dedup, msg.ID,
	)
	if err != nil {
		return err
	}
	// Delete old FTS row and insert refreshed tokens.
	_, _ = tx.ExecContext(ctx, `DELETE FROM messages_fts WHERE rowid = ?`, msg.ID)
	toks := TokenizeCJK(msg.SenderName + " " + plain)
	if toks != "" {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO messages_fts (rowid, tokens, channel_id, sender_id, created_at) VALUES (?, ?, ?, ?, ?)`,
			msg.ID, toks, string(msg.ChannelID), msg.SenderID, msg.CreatedAt.UnixMilli(),
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) RecentMessages(ctx context.Context, ch model.ChannelID, limit int) ([]model.Message, error) {
	if limit <= 0 {
		limit = 30
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, created_at
		 FROM messages WHERE channel_id = ? ORDER BY seq DESC LIMIT ?`,
		string(ch), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	// Reverse into chronological order.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (s *Store) MessagesAfterSeq(ctx context.Context, ch model.ChannelID, afterSeq int64, limit int) ([]model.Message, error) {
	return s.MessagesAfterSeqSince(ctx, ch, afterSeq, 0, limit)
}

func (s *Store) MessagesAfterSeqSince(ctx context.Context, ch model.ChannelID, afterSeq int64, sinceMs int64, limit int) ([]model.Message, error) {
	if limit <= 0 {
		limit = 500
	}
	var rows *sql.Rows
	var err error
	if sinceMs > 0 {
		rows, err = s.readDB.QueryContext(ctx,
			`SELECT id, channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, created_at
			 FROM messages WHERE channel_id = ? AND seq > ? AND created_at >= ? ORDER BY seq ASC LIMIT ?`,
			string(ch), afterSeq, sinceMs, limit,
		)
	} else {
		rows, err = s.readDB.QueryContext(ctx,
			`SELECT id, channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, created_at
			 FROM messages WHERE channel_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`,
			string(ch), afterSeq, limit,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *Store) MessagesInRange(ctx context.Context, ch model.ChannelID, startTS, endTS time.Time, limit int) ([]model.Message, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, created_at
		 FROM messages WHERE channel_id = ? AND created_at >= ? AND created_at <= ? ORDER BY seq ASC LIMIT ?`,
		string(ch), startTS.UnixMilli(), endTS.UnixMilli(), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func (s *Store) GetMessageByPlatformID(ctx context.Context, ch model.ChannelID, platformMsgID string) (*model.Message, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, seq, platform_msg_id, sender_id, sender_name, from_bot, parts_json, created_at
		 FROM messages WHERE channel_id = ? AND platform_msg_id = ? LIMIT 1`,
		string(ch), platformMsgID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, sql.ErrNoRows
	}
	return &msgs[0], nil
}

// SearchMessagesFTS performs channel-scoped FTS5 lookup with optional sender and time filters.
func (s *Store) SearchMessagesFTS(ctx context.Context, ch model.ChannelID, query string, senderIDs []string, sinceMs, untilMs int64, limit int) ([]model.Message, error) {
	if limit <= 0 {
		limit = 20
	}
	ftsQ := BuildFTSQuery(query, false)
	var args []any
	var where []string
	where = append(where, "m.channel_id = ?")
	args = append(args, string(ch))

	if len(senderIDs) > 0 {
		ph := make([]string, len(senderIDs))
		for i, id := range senderIDs {
			ph[i] = "?"
			args = append(args, id)
		}
		where = append(where, "m.sender_id IN ("+strings.Join(ph, ",")+")")
	}
	if sinceMs > 0 {
		where = append(where, "m.created_at >= ?")
		args = append(args, sinceMs)
	}
	if untilMs > 0 {
		where = append(where, "m.created_at <= ?")
		args = append(args, untilMs)
	}

	var sqlStr string
	if ftsQ != "" {
		args = append([]any{ftsQ}, args...)
		args = append(args, limit)
		sqlStr = `SELECT m.id, m.channel_id, m.seq, m.platform_msg_id, m.sender_id, m.sender_name, m.from_bot, m.parts_json, m.created_at
			FROM messages_fts f
			JOIN messages m ON m.id = f.rowid
			WHERE f.tokens MATCH ? AND ` + strings.Join(where, " AND ") + `
			ORDER BY bm25(messages_fts), m.created_at DESC LIMIT ?`
	} else {
		args = append(args, limit)
		sqlStr = `SELECT m.id, m.channel_id, m.seq, m.platform_msg_id, m.sender_id, m.sender_name, m.from_bot, m.parts_json, m.created_at
			FROM messages m
			WHERE ` + strings.Join(where, " AND ") + `
			ORDER BY m.created_at DESC LIMIT ?`
	}

	rows, err := s.readDB.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMessages(rows)
}

func scanMessages(rows *sql.Rows) ([]model.Message, error) {
	var out []model.Message
	for rows.Next() {
		var m model.Message
		var chID, pmsgID, partsJSON string
		var fromBot int
		var tsMs int64
		if err := rows.Scan(&m.ID, &chID, &m.Seq, &pmsgID, &m.SenderID, &m.SenderName, &fromBot, &partsJSON, &tsMs); err != nil {
			return nil, err
		}
		m.ChannelID = model.ChannelID(chID)
		m.PlatformMsgID = pmsgID
		m.FromBot = fromBot != 0
		m.CreatedAt = time.UnixMilli(tsMs)
		_ = json.Unmarshal([]byte(partsJSON), &m.Parts)
		if m.ReplyToMsgID == "" {
			for _, p := range m.Parts {
				if p.Kind == model.PartQuote && p.RefMsgID != "" {
					m.ReplyToMsgID = p.RefMsgID
					break
				}
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// --- Channel-Scoped Media Assets ---

func (s *Store) RegisterMediaAsset(ctx context.Context, ch model.ChannelID, senderID string, blob CanonicalBlob, retention time.Duration) (model.MediaAsset, error) {
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	now := time.Now()
	exp := now.Add(retention)

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return model.MediaAsset{}, err
	}
	defer tx.Rollback()

	var count int
	_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM media_assets WHERE channel_id = ?`, string(ch)).Scan(&count)
	mediaID := fmt.Sprintf("m_%04d", count+1)
	// Ensure uniqueness inside the serialized write transaction if old assets were pruned.
	for attempt := 0; attempt < 16; attempt++ {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO media_assets (channel_id, media_id, sha256, mime_type, byte_size, width, height, sender_id, created_at, expires_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(ch), mediaID, blob.SHA256, blob.MimeType, blob.ByteSize, blob.Width, blob.Height,
			senderID, now.UnixMilli(), exp.UnixMilli(),
		)
		if err == nil {
			if cerr := tx.Commit(); cerr != nil {
				return model.MediaAsset{}, cerr
			}
			return model.MediaAsset{
				ChannelID: ch,
				MediaID:   mediaID,
				SHA256:    blob.SHA256,
				MimeType:  blob.MimeType,
				ByteSize:  blob.ByteSize,
				Width:     blob.Width,
				Height:    blob.Height,
				SenderID:  senderID,
				CreatedAt: now,
				ExpiresAt: exp,
			}, nil
		}
		mediaID = fmt.Sprintf("m_%04d_%d", count+1+attempt, (now.UnixNano()+int64(attempt))%10000)
	}
	return model.MediaAsset{}, errors.New("store: failed to allocate unique media_id")
}

// GetMediaAsset strictly enforces (channel_id, media_id) ownership at the DB layer.
func (s *Store) GetMediaAsset(ctx context.Context, ch model.ChannelID, mediaID string) (model.MediaAsset, error) {
	var a model.MediaAsset
	var chID string
	var createdMs, expiresMs int64
	err := s.readDB.QueryRowContext(ctx,
		`SELECT channel_id, media_id, sha256, mime_type, byte_size, width, height, sender_id, created_at, expires_at
		 FROM media_assets WHERE channel_id = ? AND media_id = ?`,
		string(ch), mediaID,
	).Scan(&chID, &a.MediaID, &a.SHA256, &a.MimeType, &a.ByteSize, &a.Width, &a.Height, &a.SenderID, &createdMs, &expiresMs)
	if err != nil {
		return model.MediaAsset{}, err
	}
	a.ChannelID = model.ChannelID(chID)
	a.CreatedAt = time.UnixMilli(createdMs)
	a.ExpiresAt = time.UnixMilli(expiresMs)
	return a, nil
}

// PruneExpiredMedia removes expired media rows and deletes unreferenced blob files.
func (s *Store) PruneExpiredMedia(ctx context.Context, now time.Time) (int, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT channel_id, media_id, sha256, mime_type FROM media_assets WHERE expires_at < ?`, now.UnixMilli())
	if err != nil {
		return 0, err
	}
	type item struct{ ch, mid, sha, mime string }
	var expired []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ch, &it.mid, &it.sha, &it.mime); err == nil {
			expired = append(expired, it)
		}
	}
	rows.Close()

	deleted := 0
	for _, it := range expired {
		_, err := s.writeDB.ExecContext(ctx,
			`DELETE FROM media_assets WHERE channel_id = ? AND media_id = ?`, it.ch, it.mid)
		if err != nil {
			continue
		}
		deleted++
		var refs int
		_ = s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM media_assets WHERE sha256 = ?`, it.sha).Scan(&refs)
		if refs == 0 {
			_ = s.blobs.Delete(it.sha, it.mime)
		}
	}
	return deleted, nil
}

// EnforceMediaQuotas enforces soft per-channel (e.g. 256 MiB) and hard global (e.g. 1024 MiB)
// byte quotas via oldest-first (LRU) eviction of media_assets and unreferenced blobs (SPEC §4.4).
func (s *Store) EnforceMediaQuotas(ctx context.Context, roomMaxBytes, totalMaxBytes int64) (int, error) {
	if roomMaxBytes <= 0 {
		roomMaxBytes = 256 << 20
	}
	if totalMaxBytes <= 0 {
		totalMaxBytes = 1024 << 20
	}
	deleted := 0

	deleteAsset := func(ch, mid, sha, mime string) {
		if _, err := s.writeDB.ExecContext(ctx,
			`DELETE FROM media_assets WHERE channel_id = ? AND media_id = ?`, ch, mid); err != nil {
			return
		}
		deleted++
		var refs int
		_ = s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM media_assets WHERE sha256 = ?`, sha).Scan(&refs)
		if refs == 0 {
			_ = s.blobs.Delete(sha, mime)
		}
	}

	// 1. Per-channel soft quota.
	chRows, err := s.readDB.QueryContext(ctx,
		`SELECT channel_id, SUM(byte_size) FROM media_assets GROUP BY channel_id HAVING SUM(byte_size) > ?`, roomMaxBytes)
	if err != nil {
		return 0, err
	}
	type chUsage struct {
		ch    string
		bytes int64
	}
	var overCh []chUsage
	for chRows.Next() {
		var u chUsage
		if err := chRows.Scan(&u.ch, &u.bytes); err == nil {
			overCh = append(overCh, u)
		}
	}
	chRows.Close()

	for _, u := range overCh {
		rows, err := s.readDB.QueryContext(ctx,
			`SELECT media_id, sha256, mime_type, byte_size FROM media_assets WHERE channel_id = ? ORDER BY created_at ASC`, u.ch)
		if err != nil {
			continue
		}
		type item struct {
			mid, sha, mime string
			size           int64
		}
		var items []item
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.mid, &it.sha, &it.mime, &it.size); err == nil {
				items = append(items, it)
			}
		}
		rows.Close()
		cur := u.bytes
		for _, it := range items {
			if cur <= roomMaxBytes {
				break
			}
			deleteAsset(u.ch, it.mid, it.sha, it.mime)
			cur -= it.size
		}
	}

	// 2. Global hard quota.
	var totalBytes int64
	_ = s.readDB.QueryRowContext(ctx, `SELECT COALESCE(SUM(byte_size), 0) FROM media_assets`).Scan(&totalBytes)
	if totalBytes > totalMaxBytes {
		rows, err := s.readDB.QueryContext(ctx,
			`SELECT channel_id, media_id, sha256, mime_type, byte_size FROM media_assets ORDER BY created_at ASC`)
		if err != nil {
			return deleted, err
		}
		type gItem struct {
			ch, mid, sha, mime string
			size               int64
		}
		var items []gItem
		for rows.Next() {
			var it gItem
			if err := rows.Scan(&it.ch, &it.mid, &it.sha, &it.mime, &it.size); err == nil {
				items = append(items, it)
			}
		}
		rows.Close()
		for _, it := range items {
			if totalBytes <= totalMaxBytes {
				break
			}
			deleteAsset(it.ch, it.mid, it.sha, it.mime)
			totalBytes -= it.size
		}
	}

	return deleted, nil
}

// --- Segments & FrozenFrames (KV Prefix Cache) ---

type SegmentRecord struct {
	ID             int64
	ChannelID      model.ChannelID
	Epoch          int64
	ScopeHash      string
	PinnedSnapshot string
	SummaryText    string
	CreatedAt      time.Time
	LastActiveAt   time.Time
	ClosedAt       time.Time
}

type FrameRecord struct {
	ID          int64
	SegmentID   int64
	ChannelID   model.ChannelID
	FrameIndex  int
	Role        string
	PayloadJSON string
	EstTokens   int
	CreatedAt   time.Time
}

func (s *Store) CreateSegment(ctx context.Context, seg *SegmentRecord) error {
	now := time.Now()
	if seg.CreatedAt.IsZero() {
		seg.CreatedAt = now
	}
	if seg.LastActiveAt.IsZero() {
		seg.LastActiveAt = now
	}
	res, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO segments (channel_id, epoch, scope_hash, pinned_snapshot, summary_text, created_at, last_active_at, closed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
		string(seg.ChannelID), seg.Epoch, seg.ScopeHash, seg.PinnedSnapshot, seg.SummaryText,
		seg.CreatedAt.UnixMilli(), seg.LastActiveAt.UnixMilli(),
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	seg.ID = id
	return s.SetChannelActiveSegment(ctx, seg.ChannelID, id)
}

func (s *Store) GetSegment(ctx context.Context, segID int64) (*SegmentRecord, error) {
	var r SegmentRecord
	var chID string
	var createdMs, lastMs, closedMs int64
	err := s.readDB.QueryRowContext(ctx,
		`SELECT id, channel_id, epoch, scope_hash, pinned_snapshot, summary_text, created_at, last_active_at, closed_at
		 FROM segments WHERE id = ?`, segID,
	).Scan(&r.ID, &chID, &r.Epoch, &r.ScopeHash, &r.PinnedSnapshot, &r.SummaryText, &createdMs, &lastMs, &closedMs)
	if err != nil {
		return nil, err
	}
	r.ChannelID = model.ChannelID(chID)
	r.CreatedAt = time.UnixMilli(createdMs)
	r.LastActiveAt = time.UnixMilli(lastMs)
	if closedMs > 0 {
		r.ClosedAt = time.UnixMilli(closedMs)
	}
	return &r, nil
}

func (s *Store) LatestClosedSegmentSummary(ctx context.Context, ch model.ChannelID) (string, error) {
	var sum string
	err := s.readDB.QueryRowContext(ctx,
		`SELECT summary_text FROM segments WHERE channel_id = ? AND closed_at > 0 AND summary_text != '' ORDER BY id DESC LIMIT 1`,
		string(ch),
	).Scan(&sum)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return sum, err
}

func (s *Store) TouchSegment(ctx context.Context, segID int64, at time.Time) error {
	_, err := s.writeDB.ExecContext(ctx, `UPDATE segments SET last_active_at = ? WHERE id = ?`, at.UnixMilli(), segID)
	return err
}

func (s *Store) CloseSegment(ctx context.Context, segID int64, summary string) error {
	now := time.Now().UnixMilli()
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE segments SET summary_text = CASE WHEN ? != '' THEN ? ELSE summary_text END, closed_at = ? WHERE id = ?`,
		summary, summary, now, segID,
	)
	return err
}

func (s *Store) AppendFrame(ctx context.Context, f *FrameRecord) error {
	if f.CreatedAt.IsZero() {
		f.CreatedAt = time.Now()
	}
	res, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO frames (segment_id, channel_id, frame_index, role, payload_json, est_tokens, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		f.SegmentID, string(f.ChannelID), f.FrameIndex, f.Role, f.PayloadJSON, f.EstTokens, f.CreatedAt.UnixMilli(),
	)
	if err != nil {
		return err
	}
	f.ID, _ = res.LastInsertId()
	return nil
}

func (s *Store) ListFrames(ctx context.Context, segID int64) ([]FrameRecord, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, segment_id, channel_id, frame_index, role, payload_json, est_tokens, created_at
		 FROM frames WHERE segment_id = ? ORDER BY frame_index ASC`, segID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FrameRecord
	for rows.Next() {
		var f FrameRecord
		var chID string
		var tsMs int64
		if err := rows.Scan(&f.ID, &f.SegmentID, &chID, &f.FrameIndex, &f.Role, &f.PayloadJSON, &f.EstTokens, &tsMs); err != nil {
			return nil, err
		}
		f.ChannelID = model.ChannelID(chID)
		f.CreatedAt = time.UnixMilli(tsMs)
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *Store) DeleteFramesUpToIndex(ctx context.Context, segID int64, maxFrameIndex int) error {
	_, err := s.writeDB.ExecContext(ctx,
		`DELETE FROM frames WHERE segment_id = ? AND frame_index <= ?`, segID, maxFrameIndex)
	return err
}

// CommitTurnFrames atomically trims obsolete frames (if trimUpToIdx >= 0), appends all new
// FrozenFrames with contiguous frame_index values, and touches segments.last_active_at in ONE tx.
func (s *Store) CommitTurnFrames(ctx context.Context, segID int64, trimUpToIdx int, recs []FrameRecord, touchAt time.Time) error {
	if touchAt.IsZero() {
		touchAt = time.Now()
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if trimUpToIdx >= 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM frames WHERE segment_id = ? AND frame_index <= ?`, segID, trimUpToIdx); err != nil {
			return err
		}
	}
	var maxIdx int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(frame_index), -1) FROM frames WHERE segment_id = ?`, segID).Scan(&maxIdx); err != nil {
		return err
	}
	nextIdx := maxIdx + 1
	if len(recs) > 0 {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO frames (segment_id, channel_id, frame_index, role, payload_json, est_tokens, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for i := range recs {
			created := recs[i].CreatedAt
			if created.IsZero() {
				created = touchAt
			}
			if _, err := stmt.ExecContext(ctx, segID, string(recs[i].ChannelID), nextIdx, recs[i].Role, recs[i].PayloadJSON, recs[i].EstTokens, created.UnixMilli()); err != nil {
				return err
			}
			nextIdx++
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE segments SET last_active_at = ? WHERE id = ?`, touchAt.UnixMilli(), segID); err != nil {
		return err
	}
	return tx.Commit()
}

// --- Delivery Ledger & KV Metrics ---

func (s *Store) CreateDelivery(ctx context.Context, plan model.TurnPlan) (int64, error) {
	planBytes, err := json.Marshal(plan)
	if err != nil {
		return 0, err
	}
	res, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO deliveries (channel_id, epoch, segment_id, status, plan_json, sent_json, created_at)
		 VALUES (?, ?, ?, ?, ?, '[]', ?)`,
		string(plan.ChannelID), plan.Epoch, plan.SegmentID, string(model.DeliveryPlanned),
		string(planBytes), time.Now().UnixMilli(),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) UpdateDelivery(ctx context.Context, id int64, status model.DeliveryStatus, sent []model.SentItem) error {
	sentBytes, err := json.Marshal(sent)
	if err != nil {
		return err
	}
	_, err = s.writeDB.ExecContext(ctx,
		`UPDATE deliveries SET status = ?, sent_json = ? WHERE id = ?`,
		string(status), string(sentBytes), id,
	)
	return err
}

func (s *Store) RecordKVMetric(ctx context.Context, m model.KVMetric) error {
	ts := m.CreatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO kv_metrics (channel_id, segment_id, model_id, attempt, prompt_tokens, cached_tokens, completion_tokens, prefix_hash, miss_reason, latency_ms, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(m.ChannelID), m.SegmentID, m.ModelID, m.Attempt, m.PromptTokens, m.CachedTokens,
		m.CompletionTokens, m.PrefixHash, m.MissReason, m.LatencyMs, ts.UnixMilli(),
	)
	return err
}

func (s *Store) RecentKVMetrics(ctx context.Context, limit int) ([]model.KVMetric, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, segment_id, model_id, attempt, prompt_tokens, cached_tokens, completion_tokens, prefix_hash, miss_reason, latency_ms, created_at
		 FROM kv_metrics ORDER BY id DESC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.KVMetric
	for rows.Next() {
		var m model.KVMetric
		var chID string
		var tsMs int64
		if err := rows.Scan(&m.ID, &chID, &m.SegmentID, &m.ModelID, &m.Attempt, &m.PromptTokens, &m.CachedTokens,
			&m.CompletionTokens, &m.PrefixHash, &m.MissReason, &m.LatencyMs, &tsMs); err != nil {
			return nil, err
		}
		m.ChannelID = model.ChannelID(chID)
		m.CreatedAt = time.UnixMilli(tsMs)
		out = append(out, m)
	}
	return out, rows.Err()
}

// --- L1 Episodes & L2 Profiles / Lore / Notes ---

// HasOverlappingEpisode reports whether an episode already covers any portion of [startSeq, endSeq]
// in channel ch, preventing duplicate episodes and anti-sarcasm double-counting between online
// segment-close distillation and hourly incremental distillation.
func (s *Store) HasOverlappingEpisode(ctx context.Context, ch model.ChannelID, startSeq, endSeq int64) (bool, error) {
	if startSeq <= 0 || endSeq <= 0 {
		return false, nil
	}
	var cnt int
	err := s.readDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM episodes WHERE channel_id = ? AND start_seq <= ? AND end_seq >= ?`,
		string(ch), endSeq, startSeq,
	).Scan(&cnt)
	return cnt > 0, err
}

func (s *Store) SaveEpisode(ctx context.Context, ep *model.Episode, embedF16 []byte) error {
	if ep.CreatedAt.IsZero() {
		ep.CreatedAt = time.Now()
	}
	partJSON, _ := json.Marshal(ep.Participants)
	notableJSON, _ := json.Marshal(ep.Notable)

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO episodes (channel_id, start_seq, end_seq, start_ts, end_ts, topic, summary, participants_json, notable_json, embed_f16, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(ep.ChannelID), ep.StartSeq, ep.EndSeq, ep.StartTS.UnixMilli(), ep.EndTS.UnixMilli(),
		ep.Topic, ep.Summary, string(partJSON), string(notableJSON), embedF16, ep.CreatedAt.UnixMilli(),
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	ep.ID = id

	toks := TokenizeCJK(ep.Topic + " " + ep.Summary + " " + strings.Join(ep.Notable, " "))
	if toks != "" {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO episodes_fts (rowid, tokens, channel_id) VALUES (?, ?, ?)`,
			id, toks, string(ep.ChannelID),
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) UpdateEpisodeEmbed(ctx context.Context, id int64, embedF16 []byte) error {
	_, err := s.writeDB.ExecContext(ctx, `UPDATE episodes SET embed_f16 = ? WHERE id = ?`, embedF16, id)
	return err
}

func (s *Store) ListEpisodes(ctx context.Context, ch model.ChannelID, limit int) ([]model.Episode, [][]byte, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, start_seq, end_seq, start_ts, end_ts, topic, summary, participants_json, notable_json, embed_f16, created_at
		 FROM episodes WHERE channel_id = ? ORDER BY end_ts DESC LIMIT ?`,
		string(ch), limit,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	return scanEpisodes(rows)
}

// RecentEpisodesSince returns recent channel episodes ending at or after sinceTS (newest first).
func (s *Store) RecentEpisodesSince(ctx context.Context, ch model.ChannelID, sinceTS time.Time, limit int) ([]model.Episode, error) {
	if limit <= 0 {
		limit = 3
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, start_seq, end_seq, start_ts, end_ts, topic, summary, participants_json, notable_json, NULL, created_at
		 FROM episodes WHERE channel_id = ? AND end_ts >= ? ORDER BY end_ts DESC LIMIT ?`,
		string(ch), sinceTS.UnixMilli(), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	eps, _, err := scanEpisodes(rows)
	return eps, err
}

func (s *Store) AllEpisodesWithEmbeds(ctx context.Context) ([]model.Episode, [][]byte, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, start_seq, end_seq, start_ts, end_ts, topic, summary, participants_json, notable_json, embed_f16, created_at
		 FROM episodes ORDER BY id ASC`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	return scanEpisodes(rows)
}

func (s *Store) SearchEpisodesFTS(ctx context.Context, ch model.ChannelID, query string, limit int) ([]model.Episode, error) {
	if limit <= 0 {
		limit = 20
	}
	ftsQ := BuildFTSQuery(query, false)
	if ftsQ == "" {
		eps, _, err := s.ListEpisodes(ctx, ch, limit)
		return eps, err
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT e.id, e.channel_id, e.start_seq, e.end_seq, e.start_ts, e.end_ts, e.topic, e.summary, e.participants_json, e.notable_json, NULL, e.created_at
		 FROM episodes_fts f
		 JOIN episodes e ON e.id = f.rowid
		 WHERE f.tokens MATCH ? AND e.channel_id = ?
		 ORDER BY bm25(episodes_fts), e.end_ts DESC LIMIT ?`,
		ftsQ, string(ch), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	eps, _, err := scanEpisodes(rows)
	return eps, err
}

func (s *Store) GetEpisodeByID(ctx context.Context, id int64) (*model.Episode, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, start_seq, end_seq, start_ts, end_ts, topic, summary, participants_json, notable_json, NULL, created_at
		 FROM episodes WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	eps, _, err := scanEpisodes(rows)
	if err != nil || len(eps) == 0 {
		if err == nil {
			err = sql.ErrNoRows
		}
		return nil, err
	}
	return &eps[0], nil
}

// GetEpisodesByIDs batch-fetches channel-scoped episodes by ID in a single query, avoiding N+1 lookups.
func (s *Store) GetEpisodesByIDs(ctx context.Context, ch model.ChannelID, ids []int64) (map[int64]model.Episode, error) {
	out := make(map[int64]model.Episode, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	ph := make([]string, len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, string(ch))
	for i, id := range ids {
		ph[i] = "?"
		args = append(args, id)
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, start_seq, end_seq, start_ts, end_ts, topic, summary, participants_json, notable_json, NULL, created_at
		 FROM episodes WHERE channel_id = ? AND id IN (`+strings.Join(ph, ",")+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	eps, _, err := scanEpisodes(rows)
	if err != nil {
		return nil, err
	}
	for _, ep := range eps {
		out[ep.ID] = ep
	}
	return out, nil
}

func scanEpisodes(rows *sql.Rows) ([]model.Episode, [][]byte, error) {
	var eps []model.Episode
	var blobs [][]byte
	for rows.Next() {
		var ep model.Episode
		var chID, pJSON, nJSON string
		var startMs, endMs, createdMs int64
		var blob []byte
		if err := rows.Scan(&ep.ID, &chID, &ep.StartSeq, &ep.EndSeq, &startMs, &endMs,
			&ep.Topic, &ep.Summary, &pJSON, &nJSON, &blob, &createdMs); err != nil {
			return nil, nil, err
		}
		ep.ChannelID = model.ChannelID(chID)
		ep.StartTS = time.UnixMilli(startMs)
		ep.EndTS = time.UnixMilli(endMs)
		ep.CreatedAt = time.UnixMilli(createdMs)
		_ = json.Unmarshal([]byte(pJSON), &ep.Participants)
		_ = json.Unmarshal([]byte(nJSON), &ep.Notable)
		eps = append(eps, ep)
		blobs = append(blobs, blob)
	}
	return eps, blobs, rows.Err()
}

func (s *Store) GetProfile(ctx context.Context, ch model.ChannelID, memberID string) (*model.MemberProfile, error) {
	var fJSON, jJSON, styleNote string
	var updMs int64
	err := s.readDB.QueryRowContext(ctx,
		`SELECT facts_json, jokes_json, style_note, updated_at FROM profiles WHERE channel_id = ? AND member_id = ?`,
		string(ch), memberID,
	).Scan(&fJSON, &jJSON, &styleNote, &updMs)
	if errors.Is(err, sql.ErrNoRows) {
		return &model.MemberProfile{ChannelID: ch, MemberID: memberID}, nil
	}
	if err != nil {
		return nil, err
	}
	p := &model.MemberProfile{
		ChannelID: ch,
		MemberID:  memberID,
		StyleNote: styleNote,
		UpdatedAt: time.UnixMilli(updMs),
	}
	_ = json.Unmarshal([]byte(fJSON), &p.Facts)
	_ = json.Unmarshal([]byte(jJSON), &p.Jokes)
	return p, nil
}

func (s *Store) SaveProfile(ctx context.Context, p *model.MemberProfile) error {
	fJSON, _ := json.Marshal(p.Facts)
	jJSON, _ := json.Marshal(p.Jokes)
	now := time.Now().UnixMilli()
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO profiles (channel_id, member_id, facts_json, jokes_json, style_note, updated_at) VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id, member_id) DO UPDATE SET facts_json = excluded.facts_json, jokes_json = excluded.jokes_json, style_note = excluded.style_note, updated_at = excluded.updated_at`,
		string(p.ChannelID), p.MemberID, string(fJSON), string(jJSON), strings.TrimSpace(p.StyleNote), now,
	)
	return err
}

func (s *Store) ListProfiles(ctx context.Context, ch model.ChannelID, limit int) ([]model.MemberProfile, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT member_id, facts_json, jokes_json, style_note, updated_at FROM profiles WHERE channel_id = ? ORDER BY updated_at DESC LIMIT ?`,
		string(ch), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.MemberProfile
	for rows.Next() {
		var p model.MemberProfile
		var fJSON, jJSON, styleNote string
		var updMs int64
		if err := rows.Scan(&p.MemberID, &fJSON, &jJSON, &styleNote, &updMs); err != nil {
			return nil, err
		}
		p.ChannelID = ch
		p.StyleNote = styleNote
		p.UpdatedAt = time.UnixMilli(updMs)
		_ = json.Unmarshal([]byte(fJSON), &p.Facts)
		_ = json.Unmarshal([]byte(jJSON), &p.Jokes)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) UpsertLore(ctx context.Context, entry model.LoreEntry) error {
	now := time.Now().UnixMilli()
	conf := entry.Confidence
	if conf <= 0 {
		conf = 0.45
	}
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO lore (channel_id, term, meaning, confidence, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id, term) DO UPDATE SET
		   meaning = excluded.meaning,
		   confidence = MIN(0.95, MAX(confidence, excluded.confidence) + 0.15),
		   updated_at = excluded.updated_at`,
		string(entry.ChannelID), entry.Term, entry.Meaning, conf, now,
	)
	return err
}

// PruneStaleLore removes uncorroborated (confidence < 0.55) lore older than expireLowConfDays
// and caps each channel to at most maxPerChannel entries ordered by confidence & recency.
func (s *Store) PruneStaleLore(ctx context.Context, maxPerChannel, expireLowConfDays int) (int64, error) {
	var total int64
	if expireLowConfDays > 0 {
		cutoff := time.Now().AddDate(0, 0, -expireLowConfDays).UnixMilli()
		res, err := s.writeDB.ExecContext(ctx,
			`DELETE FROM lore WHERE confidence < 0.55 AND updated_at < ?`, cutoff)
		if err != nil {
			return 0, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			total += n
		}
	}
	if maxPerChannel > 0 {
		rows, err := s.readDB.QueryContext(ctx,
			`SELECT channel_id FROM lore GROUP BY channel_id HAVING COUNT(*) > ?`, maxPerChannel)
		if err != nil {
			return total, err
		}
		var overChs []string
		for rows.Next() {
			var ch string
			if rows.Scan(&ch) == nil {
				overChs = append(overChs, ch)
			}
		}
		rows.Close()
		for _, ch := range overChs {
			res, err := s.writeDB.ExecContext(ctx,
				`DELETE FROM lore WHERE channel_id = ? AND id NOT IN (
				   SELECT id FROM lore WHERE channel_id = ? ORDER BY confidence DESC, updated_at DESC LIMIT ?
				 )`, ch, ch, maxPerChannel)
			if err != nil {
				return total, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				total += n
			}
		}
	}
	return total, nil
}

func (s *Store) ListLore(ctx context.Context, ch model.ChannelID, limit int) ([]model.LoreEntry, error) {
	if limit <= 0 {
		limit = 30
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, term, meaning, confidence, updated_at FROM lore WHERE channel_id = ? ORDER BY confidence DESC, updated_at DESC LIMIT ?`,
		string(ch), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.LoreEntry
	for rows.Next() {
		var l model.LoreEntry
		var updMs int64
		if err := rows.Scan(&l.ID, &l.Term, &l.Meaning, &l.Confidence, &updMs); err != nil {
			return nil, err
		}
		l.ChannelID = ch
		l.UpdatedAt = time.UnixMilli(updMs)
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *Store) SaveNote(ctx context.Context, note model.Note, maxPerChannel int) error {
	if maxPerChannel <= 0 {
		maxPerChannel = 50
	}
	var exists int
	err := s.readDB.QueryRowContext(ctx,
		`SELECT 1 FROM notes WHERE channel_id = ? AND key = ?`, string(note.ChannelID), note.Key,
	).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		var cnt int
		_ = s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM notes WHERE channel_id = ?`, string(note.ChannelID)).Scan(&cnt)
		if cnt >= maxPerChannel {
			return fmt.Errorf("store: channel note limit (%d) reached", maxPerChannel)
		}
	}
	now := time.Now().UnixMilli()
	pinned := 0
	if note.Pinned {
		pinned = 1
	}
	_, err = s.writeDB.ExecContext(ctx,
		`INSERT INTO notes (channel_id, key, content, author_id, pinned, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id, key) DO UPDATE SET content = excluded.content, author_id = excluded.author_id, pinned = excluded.pinned, updated_at = excluded.updated_at`,
		string(note.ChannelID), note.Key, note.Content, note.AuthorID, pinned, now, now,
	)
	return err
}

func (s *Store) ListNotes(ctx context.Context, ch model.ChannelID, pinnedOnly bool) ([]model.Note, error) {
	q := `SELECT id, key, content, author_id, pinned, created_at, updated_at FROM notes WHERE channel_id = ?`
	if pinnedOnly {
		q += ` AND pinned = 1`
	}
	q += ` ORDER BY pinned DESC, updated_at DESC`
	rows, err := s.readDB.QueryContext(ctx, q, string(ch))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Note
	for rows.Next() {
		var n model.Note
		var pinned int
		var cMs, uMs int64
		if err := rows.Scan(&n.ID, &n.Key, &n.Content, &n.AuthorID, &pinned, &cMs, &uMs); err != nil {
			return nil, err
		}
		n.ChannelID = ch
		n.Pinned = pinned != 0
		n.CreatedAt = time.UnixMilli(cMs)
		n.UpdatedAt = time.UnixMilli(uMs)
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) DeleteNote(ctx context.Context, ch model.ChannelID, key string) (bool, error) {
	res, err := s.writeDB.ExecContext(ctx, `DELETE FROM notes WHERE channel_id = ? AND key = ?`, string(ch), key)
	if err != nil {
		return false, err
	}
	aff, _ := res.RowsAffected()
	return aff > 0, nil
}

// --- Stickers ---

func (s *Store) UpsertSticker(ctx context.Context, st model.Sticker, intentF16, contentF16 []byte) error {
	ts := st.CreatedAt.UnixMilli()
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	if st.Status == "" {
		st.Status = "ready"
	}

	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO stickers (sticker_id, pack_id, sha256, rel_path, status, ocr_text, intent_text, content_text, intent_f16, content_f16, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(sha256) DO UPDATE SET
		   pack_id = excluded.pack_id,
		   rel_path = excluded.rel_path,
		   status = excluded.status,
		   ocr_text = CASE WHEN excluded.ocr_text != '' THEN excluded.ocr_text ELSE stickers.ocr_text END,
		   intent_text = CASE WHEN excluded.intent_text != '' THEN excluded.intent_text ELSE stickers.intent_text END,
		   content_text = CASE WHEN excluded.content_text != '' THEN excluded.content_text ELSE stickers.content_text END,
		   intent_f16 = COALESCE(excluded.intent_f16, stickers.intent_f16),
		   content_f16 = COALESCE(excluded.content_f16, stickers.content_f16)`,
		st.StickerID, st.PackID, st.SHA256, st.RelPath, st.Status, st.OCRText, st.IntentText, st.ContentText,
		intentF16, contentF16, ts,
	)
	if err != nil {
		return err
	}

	var rowid int64
	if err := tx.QueryRowContext(ctx, `SELECT rowid FROM stickers WHERE sha256 = ?`, st.SHA256).Scan(&rowid); err != nil {
		return err
	}
	_, _ = tx.ExecContext(ctx, `DELETE FROM stickers_fts WHERE rowid = ?`, rowid)
	toks := TokenizeCJK(st.OCRText + " " + st.IntentText + " " + st.ContentText)
	if toks != "" {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO stickers_fts (rowid, tokens, pack_id) VALUES (?, ?, ?)`,
			rowid, toks, st.PackID,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) UpdateStickerVectors(ctx context.Context, stickerID string, intentF16, contentF16 []byte) error {
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE stickers SET intent_f16 = ?, content_f16 = ? WHERE sticker_id = ?`,
		intentF16, contentF16, stickerID,
	)
	return err
}

func (s *Store) UpdateStickerStatus(ctx context.Context, stickerID, status, ocr, intent, content string) error {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`UPDATE stickers SET status = ?, ocr_text = ?, intent_text = ?, content_text = ? WHERE sticker_id = ?`,
		status, ocr, intent, content, stickerID,
	)
	if err != nil {
		return err
	}
	var rowid int64
	var packID string
	if err := tx.QueryRowContext(ctx, `SELECT rowid, pack_id FROM stickers WHERE sticker_id = ?`, stickerID).Scan(&rowid, &packID); err != nil {
		return err
	}
	_, _ = tx.ExecContext(ctx, `DELETE FROM stickers_fts WHERE rowid = ?`, rowid)
	toks := TokenizeCJK(ocr + " " + intent + " " + content)
	if toks != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO stickers_fts (rowid, tokens, pack_id) VALUES (?, ?, ?)`, rowid, toks, packID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type StickerRow struct {
	Sticker    model.Sticker
	IntentF16  []byte
	ContentF16 []byte
}

func (s *Store) ListStickers(ctx context.Context, status string) ([]StickerRow, error) {
	q := `SELECT sticker_id, pack_id, sha256, rel_path, status, ocr_text, intent_text, content_text, intent_f16, content_f16, created_at FROM stickers`
	var args []any
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY sticker_id ASC`
	rows, err := s.readDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StickerRow
	for rows.Next() {
		var r StickerRow
		var tsMs int64
		if err := rows.Scan(&r.Sticker.StickerID, &r.Sticker.PackID, &r.Sticker.SHA256, &r.Sticker.RelPath,
			&r.Sticker.Status, &r.Sticker.OCRText, &r.Sticker.IntentText, &r.Sticker.ContentText,
			&r.IntentF16, &r.ContentF16, &tsMs); err != nil {
			return nil, err
		}
		r.Sticker.CreatedAt = time.UnixMilli(tsMs)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) SearchStickersFTS(ctx context.Context, query string, packID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	ftsQ := BuildFTSQuery(query, false)
	if ftsQ == "" {
		return nil, nil
	}
	q := `SELECT s.sticker_id FROM stickers_fts f JOIN stickers s ON s.rowid = f.rowid WHERE f.tokens MATCH ? AND s.status = 'ready'`
	args := []any{ftsQ}
	if packID != "" {
		q += ` AND s.pack_id = ?`
		args = append(args, packID)
	}
	q += ` ORDER BY bm25(stickers_fts) LIMIT ?`
	args = append(args, limit)
	rows, err := s.readDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) RecordStickerUse(ctx context.Context, ch model.ChannelID, stickerID string) error {
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO sticker_uses (channel_id, sticker_id, used_at) VALUES (?, ?, ?)`,
		string(ch), stickerID, time.Now().UnixMilli(),
	)
	return err
}

func (s *Store) RecentStickerUses(ctx context.Context, ch model.ChannelID, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 30
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT sticker_id FROM sticker_uses WHERE channel_id = ? ORDER BY used_at DESC LIMIT ?`,
		string(ch), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RecentStickerUsesGlobal returns the most recently sent sticker IDs across all channels
// so cross-group sends avoid repeating the exact same sticker back-to-back.
func (s *Store) RecentStickerUsesGlobal(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 12
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT sticker_id FROM sticker_uses ORDER BY used_at DESC LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// MemberActivityCounts returns non-bot message counts per member in ch since the given timestamp.
func (s *Store) MemberActivityCounts(ctx context.Context, ch model.ChannelID, memberIDs []string, since time.Time) (map[string]int, error) {
	out := make(map[string]int, len(memberIDs))
	if len(memberIDs) == 0 {
		return out, nil
	}
	ph := make([]string, len(memberIDs))
	args := make([]any, 0, len(memberIDs)+2)
	args = append(args, string(ch), since.UnixMilli())
	for i, id := range memberIDs {
		ph[i] = "?"
		args = append(args, id)
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT sender_id, COUNT(*) FROM messages
		 WHERE channel_id = ? AND from_bot = 0 AND created_at >= ? AND sender_id IN (`+strings.Join(ph, ",")+`)
		 GROUP BY sender_id`,
		args...,
	)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var cnt int
		if err := rows.Scan(&id, &cnt); err != nil {
			return out, err
		}
		out[id] = cnt
	}
	return out, rows.Err()
}

// --- Schedules & Daily Digests ---

func (s *Store) CreateSchedule(ctx context.Context, sc *model.Schedule, maxPerChannel int) error {
	if maxPerChannel <= 0 {
		maxPerChannel = 10
	}
	var active int
	_ = s.readDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schedules WHERE channel_id = ? AND enabled = 1`, string(sc.ChannelID),
	).Scan(&active)
	if active >= maxPerChannel {
		return fmt.Errorf("store: channel active schedule limit (%d) reached", maxPerChannel)
	}
	now := time.Now()
	sc.CreatedAt = now
	res, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO schedules (channel_id, creator_id, kind, cron_expr, market_days_only, next_run_at, prompt, enabled, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)`,
		string(sc.ChannelID), sc.CreatorID, sc.Kind, sc.CronExpr, sc.MarketDaysOnly, sc.NextRunAt.UnixMilli(), sc.Prompt, now.UnixMilli(),
	)
	if err != nil {
		return err
	}
	sc.ID, _ = res.LastInsertId()
	sc.Enabled = true
	return nil
}

func (s *Store) ListSchedules(ctx context.Context, ch model.ChannelID, enabledOnly bool) ([]model.Schedule, error) {
	q := `SELECT id, channel_id, creator_id, kind, cron_expr, market_days_only, next_run_at, prompt, enabled, created_at FROM schedules WHERE channel_id = ?`
	if enabledOnly {
		q += ` AND enabled = 1`
	}
	q += ` ORDER BY next_run_at ASC`
	rows, err := s.readDB.QueryContext(ctx, q, string(ch))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func (s *Store) DueSchedules(ctx context.Context, now time.Time) ([]model.Schedule, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, creator_id, kind, cron_expr, market_days_only, next_run_at, prompt, enabled, created_at
		 FROM schedules WHERE enabled = 1 AND next_run_at <= ? ORDER BY next_run_at ASC`,
		now.UnixMilli(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func (s *Store) UpdateScheduleNextRun(ctx context.Context, id int64, next time.Time, enabled bool) error {
	en := 0
	if enabled {
		en = 1
	}
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE schedules SET next_run_at = ?, enabled = ? WHERE id = ?`,
		next.UnixMilli(), en, id,
	)
	return err
}

func (s *Store) CancelSchedule(ctx context.Context, ch model.ChannelID, id int64, requesterID string, isAdmin bool) (bool, error) {
	var res sql.Result
	var err error
	if isAdmin {
		res, err = s.writeDB.ExecContext(ctx,
			`UPDATE schedules SET enabled = 0 WHERE id = ? AND channel_id = ?`, id, string(ch))
	} else {
		res, err = s.writeDB.ExecContext(ctx,
			`UPDATE schedules SET enabled = 0 WHERE id = ? AND channel_id = ? AND creator_id = ?`,
			id, string(ch), requesterID,
		)
	}
	if err != nil {
		return false, err
	}
	aff, _ := res.RowsAffected()
	return aff > 0, nil
}

func scanSchedules(rows *sql.Rows) ([]model.Schedule, error) {
	var out []model.Schedule
	for rows.Next() {
		var sc model.Schedule
		var chID string
		var nextMs, createdMs int64
		var en int
		if err := rows.Scan(&sc.ID, &chID, &sc.CreatorID, &sc.Kind, &sc.CronExpr, &sc.MarketDaysOnly, &nextMs, &sc.Prompt, &en, &createdMs); err != nil {
			return nil, err
		}
		sc.ChannelID = model.ChannelID(chID)
		sc.NextRunAt = time.UnixMilli(nextMs)
		sc.Enabled = en != 0
		sc.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, sc)
	}
	return out, rows.Err()
}

// --- Zero-LLM Price Alerts ---

func (s *Store) CreatePriceAlert(ctx context.Context, a *model.PriceAlert, maxPerChannel int) error {
	if maxPerChannel <= 0 {
		maxPerChannel = 20
	}
	var active int
	_ = s.readDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM price_alerts WHERE channel_id = ? AND enabled = 1`, string(a.ChannelID),
	).Scan(&active)
	if active >= maxPerChannel {
		return fmt.Errorf("store: channel active price alert limit (%d) reached", maxPerChannel)
	}
	now := time.Now()
	a.CreatedAt = now
	res, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO price_alerts (channel_id, creator_id, symbol, display_name, market, condition, threshold, cooldown_min, last_triggered_at, prompt, enabled, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 1, ?)`,
		string(a.ChannelID), a.CreatorID, a.Symbol, a.DisplayName, a.Market, a.Condition, a.Threshold, a.CooldownMin, a.Prompt, now.UnixMilli(),
	)
	if err != nil {
		return err
	}
	a.ID, _ = res.LastInsertId()
	a.Enabled = true
	return nil
}

func (s *Store) ListPriceAlerts(ctx context.Context, ch model.ChannelID, enabledOnly bool) ([]model.PriceAlert, error) {
	q := `SELECT id, channel_id, creator_id, symbol, display_name, market, condition, threshold, cooldown_min, last_triggered_at, prompt, enabled, created_at FROM price_alerts WHERE channel_id = ?`
	if enabledOnly {
		q += ` AND enabled = 1`
	}
	q += ` ORDER BY id ASC`
	rows, err := s.readDB.QueryContext(ctx, q, string(ch))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPriceAlerts(rows)
}

func (s *Store) AllEnabledPriceAlerts(ctx context.Context) ([]model.PriceAlert, error) {
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT id, channel_id, creator_id, symbol, display_name, market, condition, threshold, cooldown_min, last_triggered_at, prompt, enabled, created_at
		 FROM price_alerts WHERE enabled = 1 ORDER BY id ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPriceAlerts(rows)
}

func (s *Store) MarkPriceAlertTriggered(ctx context.Context, id int64, triggeredAt time.Time, keepEnabled bool) error {
	en := 0
	if keepEnabled {
		en = 1
	}
	_, err := s.writeDB.ExecContext(ctx,
		`UPDATE price_alerts SET last_triggered_at = ?, enabled = ? WHERE id = ?`,
		triggeredAt.UnixMilli(), en, id,
	)
	return err
}

func (s *Store) CancelPriceAlert(ctx context.Context, ch model.ChannelID, id int64, requesterID string, isAdmin bool) (bool, error) {
	var res sql.Result
	var err error
	if isAdmin {
		res, err = s.writeDB.ExecContext(ctx,
			`UPDATE price_alerts SET enabled = 0 WHERE id = ? AND channel_id = ?`, id, string(ch))
	} else {
		res, err = s.writeDB.ExecContext(ctx,
			`UPDATE price_alerts SET enabled = 0 WHERE id = ? AND channel_id = ? AND creator_id = ?`,
			id, string(ch), requesterID,
		)
	}
	if err != nil {
		return false, err
	}
	aff, _ := res.RowsAffected()
	return aff > 0, nil
}

func scanPriceAlerts(rows *sql.Rows) ([]model.PriceAlert, error) {
	var out []model.PriceAlert
	for rows.Next() {
		var a model.PriceAlert
		var chID string
		var trigMs, createdMs int64
		var en int
		if err := rows.Scan(&a.ID, &chID, &a.CreatorID, &a.Symbol, &a.DisplayName, &a.Market, &a.Condition,
			&a.Threshold, &a.CooldownMin, &trigMs, &a.Prompt, &en, &createdMs); err != nil {
			return nil, err
		}
		a.ChannelID = model.ChannelID(chID)
		if trigMs > 0 {
			a.LastTriggeredAt = time.UnixMilli(trigMs)
		}
		a.Enabled = en != 0
		a.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) SaveDailyDigest(ctx context.Context, d model.DailyDigest, embedF16 []byte) error {
	ts := d.CreatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO daily_digests (channel_id, day_key, report, stats_json, embed_f16, created_at) VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id, day_key) DO UPDATE SET report = excluded.report, stats_json = excluded.stats_json, embed_f16 = excluded.embed_f16, created_at = excluded.created_at`,
		string(d.ChannelID), d.DayKey, d.Report, d.StatsJSON, embedF16, ts.UnixMilli(),
	)
	return err
}

func (s *Store) ListDailyDigests(ctx context.Context, ch model.ChannelID, limit int) ([]model.DailyDigest, error) {
	if limit <= 0 {
		limit = 14
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT channel_id, day_key, report, stats_json, created_at FROM daily_digests WHERE channel_id = ? ORDER BY day_key DESC LIMIT ?`,
		string(ch), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.DailyDigest
	for rows.Next() {
		var d model.DailyDigest
		var chID string
		var tsMs int64
		if err := rows.Scan(&chID, &d.DayKey, &d.Report, &d.StatsJSON, &tsMs); err != nil {
			return nil, err
		}
		d.ChannelID = model.ChannelID(chID)
		d.CreatedAt = time.UnixMilli(tsMs)
		out = append(out, d)
	}
	return out, rows.Err()
}

// --- MCP Cached Tool Manifest ---

func (s *Store) SaveMCPToolCache(ctx context.Context, serverName, toolsJSON string) error {
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO mcp_tool_cache (server_name, tools_json, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT(server_name) DO UPDATE SET tools_json = excluded.tools_json, updated_at = excluded.updated_at`,
		serverName, toolsJSON, time.Now().UnixMilli(),
	)
	return err
}

func (s *Store) LoadMCPToolCache(ctx context.Context, serverName string) (string, error) {
	var toolsJSON string
	err := s.readDB.QueryRowContext(ctx,
		`SELECT tools_json FROM mcp_tool_cache WHERE server_name = ?`, serverName,
	).Scan(&toolsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return toolsJSON, err
}

// --- Budget Calibrator Samples ---

type CalibratorSample struct {
	ModelKey   string
	EstText    int
	ActualTok  int
	ImageCount int
	CreatedAt  time.Time
}

func (s *Store) AddCalibratorSample(ctx context.Context, sample CalibratorSample) error {
	ts := sample.CreatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO calibrator_samples (model_key, est_text, actual_tok, image_count, created_at) VALUES (?, ?, ?, ?, ?)`,
		sample.ModelKey, sample.EstText, sample.ActualTok, sample.ImageCount, ts.UnixMilli(),
	)
	if err != nil {
		return err
	}
	// Keep rolling window bounded per model key (last 128 rows).
	_, _ = s.writeDB.ExecContext(ctx,
		`DELETE FROM calibrator_samples WHERE model_key = ? AND id NOT IN (
			SELECT id FROM calibrator_samples WHERE model_key = ? ORDER BY id DESC LIMIT 128
		)`, sample.ModelKey, sample.ModelKey,
	)
	return nil
}

func (s *Store) RecentCalibratorSamples(ctx context.Context, modelKey string, limit int) ([]CalibratorSample, error) {
	if limit <= 0 {
		limit = 96
	}
	rows, err := s.readDB.QueryContext(ctx,
		`SELECT model_key, est_text, actual_tok, image_count, created_at FROM calibrator_samples WHERE model_key = ? ORDER BY id DESC LIMIT ?`,
		modelKey, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CalibratorSample
	for rows.Next() {
		var cs CalibratorSample
		var tsMs int64
		if err := rows.Scan(&cs.ModelKey, &cs.EstText, &cs.ActualTok, &cs.ImageCount, &tsMs); err != nil {
			return nil, err
		}
		cs.CreatedAt = time.UnixMilli(tsMs)
		out = append(out, cs)
	}
	return out, rows.Err()
}

// --- Metadata KV (e.g. Distill Checkpoints) ---

func (s *Store) GetMeta(ctx context.Context, key string) (string, error) {
	var v string
	err := s.readDB.QueryRowContext(ctx, `SELECT value FROM kv_meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	_, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO kv_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

func (s *Store) GetDistillCheckpoint(ctx context.Context, ch model.ChannelID) (int64, error) {
	v, err := s.GetMeta(ctx, "distill_seq:"+string(ch))
	if err != nil || v == "" {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func (s *Store) SetDistillCheckpoint(ctx context.Context, ch model.ChannelID, seq int64) error {
	return s.SetMeta(ctx, "distill_seq:"+string(ch), strconv.FormatInt(seq, 10))
}

// RebuildFTS rebuilds messages_fts, episodes_fts, and stickers_fts from source tables.
func (s *Store) RebuildFTS(ctx context.Context) error {
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, tbl := range []string{"messages_fts", "episodes_fts", "stickers_fts"} {
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+tbl+"("+tbl+") VALUES('delete-all')"); err != nil {
			return err
		}
	}

	// Messages
	mRows, err := tx.QueryContext(ctx, `SELECT id, channel_id, sender_id, sender_name, plain_text, created_at FROM messages`)
	if err != nil {
		return err
	}
	type mItem struct {
		id, ts              int64
		ch, sid, sname, txt string
	}
	var msgs []mItem
	for mRows.Next() {
		var it mItem
		if err := mRows.Scan(&it.id, &it.ch, &it.sid, &it.sname, &it.txt, &it.ts); err != nil {
			mRows.Close()
			return err
		}
		msgs = append(msgs, it)
	}
	mRows.Close()
	for _, it := range msgs {
		if toks := TokenizeCJK(it.sname + " " + it.txt); toks != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO messages_fts (rowid, tokens, channel_id, sender_id, created_at) VALUES (?, ?, ?, ?, ?)`,
				it.id, toks, it.ch, it.sid, it.ts); err != nil {
				return err
			}
		}
	}

	// Episodes
	eRows, err := tx.QueryContext(ctx, `SELECT id, channel_id, topic, summary, notable_json FROM episodes`)
	if err != nil {
		return err
	}
	type eItem struct {
		id                         int64
		ch, topic, summary, notRaw string
	}
	var eps []eItem
	for eRows.Next() {
		var it eItem
		if err := eRows.Scan(&it.id, &it.ch, &it.topic, &it.summary, &it.notRaw); err != nil {
			eRows.Close()
			return err
		}
		eps = append(eps, it)
	}
	eRows.Close()
	for _, it := range eps {
		var notable []string
		_ = json.Unmarshal([]byte(it.notRaw), &notable)
		if toks := TokenizeCJK(it.topic + " " + it.summary + " " + strings.Join(notable, " ")); toks != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO episodes_fts (rowid, tokens, channel_id) VALUES (?, ?, ?)`,
				it.id, toks, it.ch); err != nil {
				return err
			}
		}
	}

	// Stickers
	sRows, err := tx.QueryContext(ctx, `SELECT rowid, pack_id, ocr_text, intent_text, content_text FROM stickers`)
	if err != nil {
		return err
	}
	type sItem struct {
		rowid                  int64
		pack, ocr, intent, cnt string
	}
	var stks []sItem
	for sRows.Next() {
		var it sItem
		if err := sRows.Scan(&it.rowid, &it.pack, &it.ocr, &it.intent, &it.cnt); err != nil {
			sRows.Close()
			return err
		}
		stks = append(stks, it)
	}
	sRows.Close()
	for _, it := range stks {
		if toks := TokenizeCJK(it.ocr + " " + it.intent + " " + it.cnt); toks != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO stickers_fts (rowid, tokens, pack_id) VALUES (?, ?, ?)`,
				it.rowid, toks, it.pack); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
