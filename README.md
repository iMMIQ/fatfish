# fatfish

A clean-sheet, **Pure Go (`CGO_ENABLED=0`)** multimodal group & personal AI assistant runtime engineered for **KV Prefix Cache stability**, low memory footprint (**60–100 MiB RSS**), native vision & voice perception, hybrid memory, dual-vector sticker reactions, sandboxed Starlark computation & CJK PNG charting, and MCP extensibility.

## Highlights

- **Zero-CGO Single Binary**: Static Go binary + `debian:bookworm-slim` + `poppler-utils` + bundled TrueType `.ttf` CJK font.
- **KV Prefix Cache First**: Frozen lexicographical OpenAI tool schemas (with SQLite `mcp_tool_cache`), Segment-Pinned Memory Snapshots (`msg[1]` frozen once per segment), canonical SHA-256 JPEG/PNG BlobStore, 80%→50% whole-turn batch trim, and an Adaptive Token Budget Calibrator.
- **Dual-Pool SQLite + Zero-RAM CJK FTS5 + `float16` Vector Engine**: `writeDB (1 conn, 8 MiB)` + `readDB (3 conns, 12 MiB)` + pure-Go CJK bigram FTS5 + flat `float16` vector search with document-only tunable mean-centering and MMR diversity.
- **Per-Channel Actor & `expr` Policy Engine**: Non-blocking OneBot v11 WebSocket ingress, bounded image/ASR perception waits, hot-reloadable `expr` rules, thread continuation, `+1` repeater, smart reply decoration, epoch cancellation, and delivery ledger.
- **10 Built-in High-Leverage Tools + Cached MCP Bridge**: `recall`, `note`, `web_search`, `read_source` (Bilibili/arXiv/HF/PDF/XLSX/DOCX), `compute` (Starlark + AST OOM DoS guard + CJK PNG charts), `quote` (US/HK/A-share), `schedule`, `send_sticker`, `send_media`, `interact`, plus Streamable HTTP & stdio MCP servers.
- **Embedded Admin Console (`:6185`)**: Zero-build HTMX UI with constant-time Token Auth, live KV hit-rate dashboard, circuit breaker telemetry, config/policy editor, memory explorer, sticker search lab, and SSE log stream.

## Quick Start

```bash
cp configs/fatfish.example.yaml configs/fatfish.yaml
# Edit configs/fatfish.yaml and export required env vars (LLM_BASE_URL, LLM_API_KEY, ONEBOT_TOKEN, ...)

CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o fatfish ./cmd/fatfish
./fatfish check -c configs/fatfish.yaml
./fatfish serve -c configs/fatfish.yaml
```

## Documentation

- [Detailed System Design Specification](docs/SPEC.md)
- [Generic Ingest Format (JSONL & Sticker Packs)](docs/INGEST_FORMAT.md)

## License

`fatfish` is licensed under the **GNU Lesser General Public License v3.0 or later (`LGPL-3.0-or-later`)**. See [LICENSE](LICENSE) ([COPYING.LESSER](COPYING.LESSER)) and [COPYING](COPYING) for the full license texts.

Bundled third-party assets retain their respective licenses (e.g. Noto Sans SC under SIL OFL 1.1; HTMX under BSD-2-Clause).
