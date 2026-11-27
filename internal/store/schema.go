package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS channels (
    channel_id        TEXT PRIMARY KEY,
    epoch             INTEGER NOT NULL DEFAULT 1,
    active_segment_id INTEGER NOT NULL DEFAULT 0,
    mute_until        INTEGER NOT NULL DEFAULT 0,
    updated_at        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
    channel_id   TEXT NOT NULL,
    member_id    TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role         TEXT NOT NULL DEFAULT 'member',
    last_seen_at INTEGER NOT NULL,
    PRIMARY KEY (channel_id, member_id)
);

CREATE TABLE IF NOT EXISTS member_aliases (
    channel_id TEXT NOT NULL,
    member_id  TEXT NOT NULL,
    alias      TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    PRIMARY KEY (channel_id, member_id, alias)
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id      TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    platform_msg_id TEXT,
    sender_id       TEXT NOT NULL,
    sender_name     TEXT NOT NULL,
    from_bot        INTEGER NOT NULL DEFAULT 0,
    parts_json      TEXT NOT NULL,
    plain_text      TEXT NOT NULL,
    dedup_hash      TEXT NOT NULL DEFAULT '',
    created_at      INTEGER NOT NULL,
    UNIQUE (channel_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_messages_chan_ts ON messages(channel_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_chan_sender ON messages(channel_id, sender_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_platform_id ON messages(channel_id, platform_msg_id);
CREATE INDEX IF NOT EXISTS idx_messages_dedup ON messages(channel_id, created_at, sender_id, dedup_hash);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    tokens,
    channel_id UNINDEXED,
    sender_id  UNINDEXED,
    created_at UNINDEXED,
    content='',
    contentless_delete=1,
    tokenize='unicode61'
);

CREATE TABLE IF NOT EXISTS media_assets (
    channel_id TEXT NOT NULL,
    media_id   TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    mime_type  TEXT NOT NULL,
    byte_size  INTEGER NOT NULL,
    width      INTEGER NOT NULL,
    height     INTEGER NOT NULL,
    sender_id  TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (channel_id, media_id)
);
CREATE INDEX IF NOT EXISTS idx_media_expires ON media_assets(expires_at);

CREATE TABLE IF NOT EXISTS segments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id      TEXT NOT NULL,
    epoch           INTEGER NOT NULL,
    scope_hash      TEXT NOT NULL,
    pinned_snapshot TEXT NOT NULL,
    summary_text    TEXT NOT NULL DEFAULT '',
    created_at      INTEGER NOT NULL,
    last_active_at  INTEGER NOT NULL,
    closed_at       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_segments_chan ON segments(channel_id, id DESC);

CREATE TABLE IF NOT EXISTS frames (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id    INTEGER NOT NULL REFERENCES segments(id),
    channel_id    TEXT NOT NULL,
    frame_index   INTEGER NOT NULL,
    role          TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    est_tokens    INTEGER NOT NULL,
    created_at    INTEGER NOT NULL,
    UNIQUE (segment_id, frame_index)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id   TEXT NOT NULL,
    epoch        INTEGER NOT NULL,
    segment_id   INTEGER NOT NULL,
    status       TEXT NOT NULL,
    plan_json    TEXT NOT NULL,
    sent_json    TEXT NOT NULL DEFAULT '[]',
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kv_metrics (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id        TEXT NOT NULL,
    segment_id        INTEGER NOT NULL,
    model_id          TEXT NOT NULL,
    attempt           INTEGER NOT NULL,
    prompt_tokens     INTEGER NOT NULL,
    cached_tokens     INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    prefix_hash       TEXT NOT NULL,
    miss_reason       TEXT NOT NULL DEFAULT '',
    latency_ms        INTEGER NOT NULL,
    created_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kv_metrics_ts ON kv_metrics(created_at DESC);

CREATE TABLE IF NOT EXISTS episodes (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id        TEXT NOT NULL,
    start_seq         INTEGER NOT NULL,
    end_seq           INTEGER NOT NULL,
    start_ts          INTEGER NOT NULL,
    end_ts            INTEGER NOT NULL,
    topic             TEXT NOT NULL,
    summary           TEXT NOT NULL,
    participants_json TEXT NOT NULL,
    notable_json      TEXT NOT NULL,
    embed_f16         BLOB,
    created_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_episodes_chan_ts ON episodes(channel_id, end_ts DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS episodes_fts USING fts5(
    tokens, channel_id UNINDEXED, content='', contentless_delete=1, tokenize='unicode61'
);

CREATE TABLE IF NOT EXISTS profiles (
    channel_id TEXT NOT NULL,
    member_id  TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    jokes_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (channel_id, member_id)
);

CREATE TABLE IF NOT EXISTS lore (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    term       TEXT NOT NULL,
    meaning    TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0.5,
    updated_at INTEGER NOT NULL,
    UNIQUE (channel_id, term)
);

CREATE TABLE IF NOT EXISTS notes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    key        TEXT NOT NULL,
    content    TEXT NOT NULL,
    author_id  TEXT NOT NULL,
    pinned     INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (channel_id, key)
);

CREATE TABLE IF NOT EXISTS stickers (
    sticker_id   TEXT PRIMARY KEY,
    pack_id      TEXT NOT NULL,
    sha256       TEXT NOT NULL UNIQUE,
    rel_path     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'ready',
    ocr_text     TEXT NOT NULL DEFAULT '',
    intent_text  TEXT NOT NULL DEFAULT '',
    content_text TEXT NOT NULL DEFAULT '',
    intent_f16   BLOB,
    content_f16  BLOB,
    created_at   INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS stickers_fts USING fts5(
    tokens, pack_id UNINDEXED, content='', contentless_delete=1, tokenize='unicode61'
);

CREATE TABLE IF NOT EXISTS sticker_uses (
    channel_id TEXT NOT NULL,
    sticker_id TEXT NOT NULL,
    used_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sticker_uses_chan ON sticker_uses(channel_id, used_at DESC);

CREATE TABLE IF NOT EXISTS schedules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  TEXT NOT NULL,
    creator_id  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    cron_expr   TEXT NOT NULL DEFAULT '',
    next_run_at INTEGER NOT NULL,
    prompt      TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_run_at);

CREATE TABLE IF NOT EXISTS daily_digests (
    channel_id TEXT NOT NULL,
    day_key    TEXT NOT NULL,
    report     TEXT NOT NULL,
    stats_json TEXT NOT NULL,
    embed_f16  BLOB,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (channel_id, day_key)
);

CREATE TABLE IF NOT EXISTS mcp_tool_cache (
    server_name TEXT PRIMARY KEY,
    tools_json  TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS calibrator_samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_key   TEXT NOT NULL,
    est_text    INTEGER NOT NULL,
    actual_tok  INTEGER NOT NULL,
    image_count INTEGER NOT NULL,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_calibrator_model ON calibrator_samples(model_key, id DESC);

CREATE TABLE IF NOT EXISTS kv_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
`
