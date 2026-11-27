# fatfish Generic Ingest Format

`fatfish ingest` imports historical messages and sticker packs from platform-agnostic files. Keep any legacy-system converters outside the public repository and emit the standard formats below.

---

## 1. Historical Messages (`--messages history.jsonl`)

UTF-8 newline-delimited JSON (`.jsonl`), one normalized message per line. Lines may be in any order; `fatfish ingest` deduplicates by `(channel_id, created_at_ms, sender_id, sha256(text))` and assigns monotonic per-channel `seq` numbers in chronological order.

### Schema

```json
{
  "channel_id": "onebot:100000001:group:123456789",
  "platform_msg_id": "987654321",
  "sender_id": "200000001",
  "sender_name": "Alice",
  "from_bot": false,
  "created_at_ms": 1760000000000,
  "parts": [
    { "kind": "text", "text": "Hello everyone!" }
  ]
}
```

### Field Notes
- `channel_id` *(required)*: Canonical channel identifier `<platform>:<bot_id>:<group|dm>:<native_id>`.
- `sender_id` *(required)*: Immutable platform user ID (never a nickname).
- `sender_name` *(required)*: Display name/card at the time the message was sent (historical aliases are recorded automatically).
- `from_bot` *(optional, default `false`)*: `true` if sent by the bot itself.
- `created_at_ms` *(required)*: Unix timestamp in milliseconds (UTC).
- `parts` *(required)*: Array of normalized `Part` objects (`text`, `image`, `audio`, `card`, `file`, `video`, `poke`, `quote`, `forward`). For pure-text legacy logs, a single `{"kind":"text","text":"..."}` entry suffices.

---

## 2. Sticker Pack Directory (`--stickers ./pack-dir --pack default`)

A directory containing image files (`.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`) and an optional `manifest.jsonl` describing existing captions/OCR/tags (if omitted, `fatfish ingest` queues untagged images for vision tagging).

### Optional `manifest.jsonl` Line Schema

```json
{
  "file": "0001.webp",
  "ocr_text": "working hard",
  "intent_text": "sarcastic slacking at work, tired sigh, playful reply to overtime complaints",
  "content_text": "cartoon fish staring blankly at a laptop screen",
  "status": "ready"
}
```

### Notes
- Deduplication uses file SHA-256; re-running `fatfish ingest` is idempotent.
- If `intent_text` and `content_text` are both populated, `fatfish reindex` builds both `v_intent` and `v_content` dual vectors directly.
- If only a legacy single `caption` field is provided (`"caption": "..."`), `fatfish ingest` stores it as `content_text` and marks `intent_text` for optional LLM enrichment via `fatfish reindex --enrich`.
