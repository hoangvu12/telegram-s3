# Throughput roadmap — self-contained resume plan

This document is the operational source of truth for a multi-phase migration
that ends with the gateway running entirely on Telegram's MTProto protocol
(via `gotd/td`) instead of the Bot HTTP API. It is written so that **a fresh
Claude session in this working directory (with no prior chat context) can
complete any unfinished phase from where the previous session stopped.**

The design rationale and the original approved plan live in
`C:\Users\HP MEDIA\.claude\plans\polymorphic-mixing-metcalfe.md`. This file is
the operational version: less prose, more checklists.

---

## Where things stand (last edit: 2026-05-25)

- Branch: `master`. Git HEAD: `fc5347c` (`Phase 4: MTProto backend with dual-transport dispatcher + grace-delete sweeper`).
- **Phase 0 COMMITTED** as `d784b58`.
- **Phase 1 COMMITTED** as `18b611a`.
- **Phase 2 COMMITTED** as `23e35bf`.
- **Phase 3 COMMITTED** as `789819f`.
- **Phase 4 COMMITTED** as `fc5347c`. Working tree is clean except for the long-standing
  untracked `PHASES.md`, `S3-COMPAT-ACCEPTANCE.md`, `scripts/`. Tests
  green at HEAD: `go vet ./...` clean, `go build ./...` clean,
  `go test ./... -count=1 -timeout 120s` → all packages pass
  (`cache 0.85s`, `config 0.74s`, `metadata 2.18s`, `migrate 1.51s`,
  `reader 0.94s`, `s3api 6.67s`, `storage 0.67s`, `telegram 4.29s`,
  `mtproto 1.53s`). `-race` not run locally (Windows host has no
  cgo/gcc); run in CI. Deploy still ships with `TELEGRAM_TRANSPORT=bot`
  (no-op default) — flip to `dual` post-deploy after confirming
  MTProto bots authenticated.
- **Phase 2 invariants now baked in (do not regress)**:
  - `storage.Backend` takes `ChunkRef`, not bare `fileID string` / `messageID int64`.
  - `Chunk` (both `storage.Chunk` and `metadata.Chunk`) carries `Transport` + `BotIndex`. `Transport == ""` is implicitly `"bot"` — the dispatcher in Phase 4 relies on this for any row that somehow escaped the column normalization.
  - All reap paths call `backend.DeleteBatch`; per-chunk `Delete` loops are gone (Phase 4 batches MTProto's `channels.deleteMessages` at 100/call without further call-site churn).
  - Object GETs go through `internal/reader.Reader` (Phase 4 swaps in an MTProto `ChunkSource` without touching `streamObject`/`openObject`/`copyObject`/`uploadPartCopy`).
  - `Reader.Prime()` must be called before any `writeHeaders()` — that's the 502-vs-truncated-200 invariant the tests pin.
- **Phase 3 invariants now baked in (do not regress)**:
  - `BotStorage` is built on a `*BotPool` of `botClient{token, *http.Client}`
    entries. Uploads round-robin via `Pool.Pick(BotOpUpload)`; the index is
    persisted in `Chunk.BotIndex` so the same bot resolves the chunk on
    read (Bot HTTP API `file_id` is bot-bound). The two pool counters
    (`stream`/`upload`) are independent atomics so a burst of one op does
    not skew the other (teldrive's model).
  - `object_chunks`, `multipart_part_chunks`, and `objects` each carry
    `transport TEXT NOT NULL DEFAULT 'bot'` and
    `bot_index INTEGER NOT NULL DEFAULT 0` columns. Additive only —
    `ensureColumn` is the single helper that issues `ALTER TABLE ADD COLUMN`.
  - The migrate step backfills every legacy single-message row
    (`objects.size > 0`, no matching `object_chunks` row) as a one-row
    chunk `(seq=0, offset=0, size=obj.Size, transport='bot', bot_index=0)`.
    Idempotent: the `NOT EXISTS` clause skips already-chunked rows.
  - The chunk map is now the sole source of truth on every read/delete
    path. The three pre-Phase-3 fallback branches in `(*Handler).planRead`,
    `deleteOneObject`, and `reapSupersededChunks` are deleted.
  - `TELEGRAM_BOT_TOKENS` (comma-separated) is the required env var.
    `TELEGRAM_BOT_TOKEN` (singular) is a soft fallback that auto-promotes
    to a one-element pool when the plural is unset — eases partial
    deploys but is not the recommended steady state.
- **Phase 4 invariants now baked in (do not regress)**:
  - `storage.Dispatcher` is the only `Backend` the handler sees. It
    routes by `ref.Transport`: `""`/`"bot"` → `BotStorage`,
    `"mtproto"` → `mtproto.Storage`. `Upload` always routes to the
    `cfg.TelegramTransport`-selected backend.
  - `internal/storage/telegram/mtproto/` holds the MTProto Backend.
    `client.go` runs one long-lived `*telegram.Client` per bot with a
    cached `*tg.InputChannel`. `upload.go` mirrors `BotStorage.Upload`
    in shape (same chunk size, same per-chunk rotation). `download.go`
    aligns arbitrary `(offset, length)` reads to MTProto's
    power-of-2 [4KiB, 1MiB] window, caches
    `(messageID, botIndex) → *tg.InputDocumentFileLocation` with a
    30m TTL, refreshes once on `FILE_REFERENCE_EXPIRED`, treats
    `CHANNEL_PRIVATE`/`CHAT_FORBIDDEN`/`CHANNEL_INVALID` as fatal
    (no round-robin), and falls back across the pool on any other
    error. `delete.go` batches `ChannelsDeleteMessages` at 100/call.
  - SQLite schema additions: `tg_sessions(key, value, updated_at)`
    holds per-bot gotd session blobs; `bot_chunks_pending_delete
    (message_id, bot_index, swapped_at)` is the grace-delete buffer
    decision #14 mandates. Both via the standard ensureColumn pattern.
  - `internal/migrate/sweeper.go` runs two passes per tick when
    `cfg.TelegramTransport == "dual"`. Pass-1 (rate-capped at
    `cfg.MigrationRate` rows/day) atomically swaps `transport='bot'`
    rows to `'mtproto'` and enqueues the old `(message_id, bot_index)`
    in `bot_chunks_pending_delete`. Pass-2 reaps pending rows older
    than `cfg.BotDeleteGrace` (default 1h, clamped to >= 1m in prod).
    The grace window prevents 404'ing readers that fetched the chunk
    map just before the swap. Tests pin both passes and the
    concurrent-read-during-swap invariant.
  - `TELEGRAM_TRANSPORT=bot` (default) is a no-op — no MTProto deps
    boot, no app creds needed. `dual` requires `TELEGRAM_APP_ID` +
    `TELEGRAM_APP_HASH` (from my.telegram.org) and brings up every
    bot's `*telegram.Client` in parallel with a 200ms × index
    initial-auth stagger.
- **Phase 4 deploy/rollback**: ship the binary first with
  `TELEGRAM_TRANSPORT=bot`. Confirm boot logs show all MTProto bots
  authenticated. Flip env to `dual` and watch sweeper logs for
  pass-1 progress. To pause migration during an incident, set
  `MIGRATION_RATE=0` (pass-2 keeps draining anything pass-1 already
  wrote). NEVER roll back the binary past the point where any
  `'mtproto'` chunks exist without first draining them back —
  PHASES.md rollback notes apply.
- **Next phase to start: post-Phase-4 monitoring + eventual
  `TELEGRAM_TRANSPORT=mtproto` switch + BotStorage removal.** These
  are operational, not code phases.

## How a fresh session resumes

1. Run the verification commands below to confirm the world is as this doc says:
   ```
   git status --short
   git log --oneline -3
   go vet ./...
   go build ./...
   go test ./... -count=1 -timeout 90s
   ```
2. If `git status` shows the 6 Phase-0 files above as `M` and the build/tests
   pass, you are exactly at the end of Phase 0. Ask the user whether to commit
   Phase 0 before starting Phase 1, or to roll it into a single commit at end
   of Phase 1.
3. If `git status` is clean and HEAD is a commit later than `6ccc793`, read the
   most recent commit messages to determine which phase already shipped.
4. Read the "Current state" line above and the matching Phase section to find
   what to do next.
5. Use `TaskCreate` to lay out the substeps for the active phase, then execute.

## Production / deployment context

- Live at `https://s3.nguyenvu.dev` (Easypanel project `telegram-s3`,
  service `api`). See `HANDOFF.md` for the operational details.
- A real bucket `send` holds Gokapi share files (keyed by SHA1) used by
  `https://send.nguyenvu.dev`. **These reads must keep working through every
  phase.** This is the hardest constraint in the entire roadmap and shapes
  Phase 4's dual-transport design.
- SQLite metadata lives in `/app/data/telegram-s3.db` on the deployed
  container; locally it's `telegram-s3.db` at repo root.
- Today: single `TELEGRAM_BOT_TOKEN`, single `TELEGRAM_CHAT_ID`,
  `TELEGRAM_API_BASE_URL` defaults to `https://api.telegram.org`.

## Cross-cutting design decisions (apply to every phase)

Carry these forward — they are the conclusions of the upfront research and
should not be re-litigated.

1. **`Backend` interface honestly admits transport heterogeneity.** Phase 2
   replaces `Backend.Download(ctx, fileID string)` etc. with `Backend.Download(ctx, ref ChunkRef)`
   where `ChunkRef{Transport, BotFileID, MessageID, BotIndex}`. The Bot-API
   path fills `BotFileID`; MTProto fills `MessageID` + `BotIndex`. Also add
   `Backend.DeleteBatch(ctx, []ChunkRef) error` (Bot impl fan-outs; MTProto
   batches `ChannelsDeleteMessages` at 100/call).

2. **Survives-cutover reader.** Phase 2 introduces a transport-agnostic
   `ChunkSource` interface (`Chunk(ctx, offset, limit) ([]byte, error)` +
   `ChunkSize(start, end) int64`). The parallel-prefetch reader binds to
   that. In Phase 4, the MTProto download path is just a new `ChunkSource`
   impl — the reader code does not move.

3. **Survives-cutover cache.** Phase 1's `(fileID → filePath, expiresAt)`
   TTL cache is built as a generic `Cache[K comparable, V any]` so Phase 4
   can reuse the type for `(messageID, botIndex) → *tg.InputDocumentFileLocation`.
   No Redis. sync.Map + per-entry expiresAt + periodic sweep goroutine.

4. **SQLite: writer/reader split (Phase 0).** Two `*sql.DB` to the same
   WAL file: writer `SetMaxOpenConns(1)`, reader `SetMaxOpenConns(N)`. This
   is the right pattern per modernc.org/sqlite community guidance — a single
   pool with N conns just adds `SQLITE_BUSY` contention; WAL allows many
   concurrent readers + one writer. Already done.

5. **Multi-bot identity.** Telegram `file_id` is bot-bound under Bot API.
   Under MTProto the message ID is bot-agnostic but `FileReference` is
   session-scoped. We persist `bot_index` per chunk (advisory hint about
   which bot uploaded it). Downloads try the original bot first; on bot
   failure fall back to round-robin (every channel-member bot can re-resolve).

6. **Two separate round-robin counters** (`stream`, `upload`) per teldrive's
   model — prevents an upload burst from starving range reads.

7. **Migration cliff (Phase 4 only).** Add `transport TEXT NOT NULL DEFAULT 'bot'`
   to chunk tables in Phase 3. Phase 4 dispatcher picks the backend per chunk.
   New uploads → MTProto; legacy `'bot'` rows → `BotStorage` indefinitely.
   A throttled background sweeper re-uploads `'bot'` chunks through MTProto
   and atomically swaps the row. When `'bot'` row count hits zero, rip out
   `BotStorage`. The original token (renamed to `TELEGRAM_LEGACY_BOT_TOKEN`)
   must stay valid until then. Named constant `legacyBotIndex = 0`.

8. **Legacy single-message backfill (Phase 3).** Pre-Phase-3 objects have no
   `object_chunks` rows — only `objects.telegram_file_id`/`telegram_message_id`
   populated. Phase 2 refactored the GET path so the legacy fallback now lives
   in the second branch of `(*Handler).planRead` in `internal/s3api/handler.go`
   (`if obj.Size > 0 { return []reader.ChunkLoc{{...obj.Telegram*...}}, ... }`),
   and the legacy delete fallbacks live in `deleteOneObject`
   (`else if obj.Size > 0 && obj.TelegramMessageID != 0`) and
   `reapSupersededChunks` (the `len(oldChunks) == 0 && prev.Size > 0 && ...`
   branch). In Phase 3 backfill legacy rows as one-row
   `object_chunks(seq=0, offset=0, size=obj.Size, ...)` entries and delete
   all three legacy fallbacks. Worth the one-time backfill.

9. **`FILE_REFERENCE_EXPIRED` handling (Phase 4).** Wrap `UploadGetFile` with
   explicit retry on `tgerr.Is(err, tgerr.FileReferenceExpired, tgerr.FileReferenceInvalid)`:
   invalidate the cache key, re-call `ChannelsGetMessages`, rebuild
   `InputDocumentFileLocation`, retry once. ~10 lines; strictly more correct
   than teldrive's cache-TTL-only approach.

10. **Channel model stays trivial.** Single `TELEGRAM_CHAT_ID`. No rollover
    (500K msgs × 2 GB = ~1 PB capacity per channel). Cache the resolved
    `*tg.InputChannel` (with its `AccessHash`) once per bot at boot.

11. **Bot membership errors do not round-robin.** On `CHANNEL_PRIVATE` /
    `CHAT_FORBIDDEN` during download fallback, return a clear error rather
    than silently trying other bots — every other bot will fail the same way.

12. **`TELEGRAM_BOT_TOKEN` → `TELEGRAM_BOT_TOKENS` in Phase 3** is a breaking
    env change. The deploy must update Easypanel env vars in the same step.
    Document the old value before the deploy so rollback is possible.

13. **Schema migrations are additive only** — `ALTER TABLE ADD COLUMN`,
    never drops. Use a `pragma_table_info`-driven `ensureColumn` helper
    (SQLite doesn't support `IF NOT EXISTS` on `ADD COLUMN`).

14. **Grace-delete during migration sweep (Phase 4).** The sweeper's
    row-swap and the bot-message delete are NOT done in the same
    transaction. After the row flips `'bot'→'mtproto'`, the old bot
    `(message_id, bot_index)` is recorded in
    `bot_chunks_pending_delete` and reaped by a second sweeper pass
    after `cfg.BotDeleteGrace` (default 1h). Reason: a reader that
    fetched the chunk map just before the swap holds a `transport='bot'`
    ref by value; if we delete the bot message immediately, that
    in-flight read 404s permanently. The grace window gives any
    concurrent reader time to finish. Cost is trivial — one duplicate
    Telegram message per migrated chunk for the grace duration.

## Phase 0 — Foundation tuning [DONE]

Concrete changes that landed (verify with `git diff HEAD` against the file
list at the top of this doc):

- `internal/storage/telegram/bot.go`: `NewBotStorage` now delegates to a new
  `NewBotStorageWithOptions(token, chatID, baseURL, idlePerHost, logger)`.
  The `http.Client` uses a tuned `http.Transport`: `MaxIdleConnsPerHost`,
  `MaxConnsPerHost`, `MaxIdleConns = 4 × idlePerHost`, `IdleConnTimeout = 90s`,
  `TLSHandshakeTimeout = 10s`, `ExpectContinueTimeout = 1s`,
  `ResponseHeaderTimeout = 30s`, `ForceAttemptHTTP2 = true`, custom dialer
  (`Timeout: 10s`, `KeepAlive: 30s`). No client-wide `Timeout` — per-request
  deadlines flow through ctx.
- `internal/metadata/store.go`: `Store` struct now has `write *sql.DB` and
  `read *sql.DB`. `OpenWithOptions(path, readerConns)` opens both against
  the same WAL file. Every `Exec`/`BeginTx` routes through `s.write`; every
  `Query`/`QueryRow` through `s.read`. `Open(path)` defaults `readerConns = 8`.
  `Close()` closes both.
- `internal/config/config.go`: `HTTPMaxIdleConnsPerHost` (default 32),
  `SQLiteReaderConns` (default 8). Both env-overridable. New `getInt`
  helper. `strconv` import added.
- `cmd/telegram-s3/main.go`: uses `metadata.OpenWithOptions` and
  `telegram.NewBotStorageWithOptions` with the config values.
- `.env.example`: documented the two new optional env vars.

If `git status` shows these 6 files as `M`, Phase 0 is correctly applied but
uncommitted. If they have already been committed, look for a commit message
starting "Phase 0" in `git log`.

## Phase 1 — getFile path cache + local Bot API support [NEXT]

**Goal:** stop calling `getFile` once per chunk read. Cache `(fileID → filePath)`
with a TTL; on the subsequent file GET, invalidate on 404 and re-resolve.
Also: support raising `MaxChunkSize` when the user runs a local
`tdlib/telegram-bot-api` server (which removes the 20 MB Bot API download cap).

**Files touched:**

- **New:** `internal/cache/cache.go` — generic `Cache[K comparable, V any]`
  with TTL eviction. Backed by `sync.Map` + per-entry `expiresAt`. Provides
  `Get(key) (V, bool)`, `Set(key, value, ttl time.Duration)`, `Delete(key)`,
  `Len() int`. A background sweep goroutine (started by constructor) wakes
  every TTL/4 (min 1 min) and evicts expired entries. Stop with `Close()`.
  No external dep.
- **New:** `internal/cache/cache_test.go` — set/get, expiry, invalidation,
  concurrent access (race detector via `go test -race`).
- `internal/storage/telegram/bot.go::DownloadRange` — before the `getFile`
  POST, look up `(fileID → filePath)` in a per-storage cache field. On
  miss, call `getFile`, store with `cfg.LocationCacheTTL`. On HTTP 404
  from the subsequent file GET, invalidate the entry and resolve once more
  (one retry only). `NewBotStorageWithOptions` takes an optional
  `*cache.Cache[string, string]`; `BotStorage` holds it. If nil, behavior is
  unchanged (no cache).
- `internal/storage/telegram/bot_test.go` (existing or new) — a unit test
  that fakes the HTTP backend, GETs the same fileID 5 times, asserts
  `getFile` called once.
- `internal/config/config.go` — add `LocationCacheTTL` (default 30m,
  `time.Duration`) and `TelegramMaxChunkSize` (default 18 << 20). Env vars:
  `LOCATION_CACHE_TTL`, `TELEGRAM_MAX_CHUNK_SIZE` (parsed via
  `humanize.ParseBytes` or a small inline parser supporting `1.9GB`, `1900000000`).
- `internal/storage/telegram/bot.go` — `chunkSize` field becomes
  configurable via `NewBotStorageWithOptions`. Cap stays at 18 MiB by
  default; user can raise to `~1.9GB` when running local Bot API.
- `cmd/telegram-s3/main.go` — construct the cache, pass it and the new
  config values into `NewBotStorageWithOptions`.
- `.env.example` — document `LOCATION_CACHE_TTL` and `TELEGRAM_MAX_CHUNK_SIZE`.

**Acceptance:**

- `go test ./...` green. The existing integration tests must not change
  observable behavior (default cache TTL set; if test backends return stable
  file paths, caching is a no-op for correctness).
- New cache tests cover TTL expiry and concurrent access (`-race`).
- New integration test confirms `getFile` is invoked once across N reads of
  the same `fileID`.

**Notes:**

- Do not pull in `github.com/dustin/go-humanize` just for byte parsing — it
  is already an indirect dep in `go.sum`. A 30-line inline parser is fine
  too. Whichever keeps `go.mod` minimal.
- The cache must be transport-agnostic in shape — Phase 4 reuses
  `Cache[K,V]` with a struct key. Resist the urge to specialize it to
  `string → string`.

## Phase 2 — Parallel prefetch reader

**Goal:** stop opening segments strictly sequentially in `streamSegments` /
`openObject`. Fan out N chunks concurrently, deliver in order. This is the
load-bearing phase: most user-visible speedup, and the code survives
Phase 4 unchanged (transport-agnostic via `ChunkSource`).

**Files touched:**

- **New:** `internal/reader/reader.go` — `ChunkSource` interface
  (`Chunk(ctx, offset, limit) ([]byte, error)` and `ChunkSize(start, end) int64`),
  `prefetchReader` struct (port of teldrive `internal/reader/tg_reader.go::tgMultiReader`,
  encryption stripped). `errgroup` fan-out, bounded `bufferChan` channel,
  `leftCut`/`rightCut` boundary trimming, `offset := start - (start % chunkSize)`
  alignment. Constructor takes `(ctx, ChunkSource, start, end, concurrency, buffers, chunkTimeout)`.
- **New:** `internal/reader/buffer.go` — port teldrive's tiny helper
  (`buf []byte; offset int` with `isEmpty()`, `buffer()`, `increment(n)`).
- **New:** `internal/reader/bot_source.go` — `ChunkSource` implementation
  wrapping `BotStorage.DownloadRange`. Reads the full chunk into a `[]byte`
  per call. Larger chunk size than MTProto's 1 MiB cap is fine here — the
  Bot API has no alignment constraint, so this can do e.g. 4 MiB chunks.
- **New:** `internal/reader/reader_test.go` — covers:
  - Ordering invariant: chunk N+1 returned before chunk N must still
    deliver bytes in order (fake `ChunkSource` with controllable delays).
  - Concurrency: instrumented counter of in-flight `Chunk()` calls hits
    `≥ 2` during a multi-segment read.
  - **Critical:** first-segment-fail surfaces as a non-nil error
    *before any bytes are written*, so a handler wrapping this can return
    502 without truncating a 200/206 response.
  - Late-segment-fail aborts the stream cleanly (Close releases buffers).
  - Empty stream → immediate EOF.
- `internal/storage/storage.go` — refactor `Backend` interface:
  ```
  type ChunkRef struct {
      Transport string  // "bot" | "mtproto"
      BotFileID string  // populated for "bot"
      MessageID int64
      BotIndex  int
  }
  type Backend interface {
      Upload(ctx, name, contentType string, body io.Reader) ([]Chunk, error)
      Download(ctx, ref ChunkRef) (io.ReadCloser, error)
      DownloadRange(ctx, ref ChunkRef, offset, length int64) (io.ReadCloser, error)
      Delete(ctx, ref ChunkRef) error
      DeleteBatch(ctx, refs []ChunkRef) error  // NEW
  }
  ```
  `Chunk` struct grows a `Transport string` and `BotIndex int` field (zero
  values OK; Phase 3 populates them properly).
- `internal/storage/telegram/bot.go` — methods take `ChunkRef` (read
  `BotFileID`). `DeleteBatch` is a serial fan-out over `Delete` (Phase 4
  introduces the real batched MTProto path).
- `internal/metadata/store.go::Chunk` — gain `Transport` and `BotIndex`
  fields (NOT YET persisted to DB; that's Phase 3's schema migration).
- `internal/s3api/handler.go` — `streamSegments` (currently line 526) and
  `openObject` (line 464) build a `prefetchReader` over the segments. First
  chunk is awaited synchronously **before** `writeHeaders()` runs so a
  backend failure still produces 502 (not truncated 200). `objectSegments`
  (line 424) and `readSegment` (line 549) thread `ChunkRef` instead of bare
  `fileID string`.
- `internal/s3api/copy.go` — `UploadPartCopy` already calls `openObject`;
  it inherits the new reader.
- `internal/s3api/multipart.go` — uses the new `DeleteBatch` for abort and
  reap paths.
- `internal/s3api/handler.go::reapSupersededChunks`, `deleteChunks` — call
  `backend.DeleteBatch` instead of looping per message.
- `internal/config/config.go` — `StreamConcurrency` (default 4), `StreamBuffers`
  (default 8), `ChunkTimeout` (default 30s). Env vars
  `STREAM_CONCURRENCY`, `STREAM_BUFFERS`, `CHUNK_TIMEOUT`.

**Acceptance:**

- `go test ./...` green including the new reader tests with `-race`.
- A manual end-to-end test (recorded in commit message): PUT a 100 MB
  object, GET it, time it before/after Phase 2.
- The existing `internal/s3api/range_test.go` and `phase7_live_test.go`
  must continue to pass.

## Phase 3 — Multi-bot pool + schema migration [DONE]

**Goal:** N bot tokens, round-robin per operation type, BotIndex stored per
chunk. Schema additions for Phase 4 land here so Phase 4 can deploy without
a migration window. Legacy single-message objects get backfilled into
one-row `object_chunks` entries; the legacy code branch is deleted.

**Files touched:**

- `internal/metadata/store.go::migrate` — extend with a small
  `ensureColumn(table, col, type, defaultExpr)` helper that reads
  `pragma_table_info(table)` and `ALTER TABLE ... ADD COLUMN` only if
  missing. Apply to:
  - `object_chunks`: `transport TEXT NOT NULL DEFAULT 'bot'`,
    `bot_index INTEGER NOT NULL DEFAULT 0`.
  - `multipart_part_chunks`: same two columns.
  - `objects`: `transport TEXT NOT NULL DEFAULT 'bot'`,
    `bot_index INTEGER NOT NULL DEFAULT 0`.
- `internal/metadata/store.go` — one-shot backfill called at the end of
  `migrate()`: for every `objects` row with `Size > 0` AND no matching
  `object_chunks` row, insert a one-row chunk
  `(seq=0, offset=0, size=obj.Size, file_id=obj.telegram_file_id,
  message_id=obj.telegram_message_id, transport='bot', bot_index=0)`.
- `internal/s3api/handler.go::planRead` — delete the second branch
  (`if obj.Size > 0 { return []reader.ChunkLoc{{...obj.Telegram*...}}, ... }`)
  once the backfill is in place. Same for the legacy delete fallbacks in
  `deleteOneObject` (`else if obj.Size > 0 && obj.TelegramMessageID != 0`)
  and `reapSupersededChunks` (the `len(oldChunks) == 0 && prev.Size > 0 ...`
  branch). After Phase 3 every read/delete path consumes the chunk map only.
- **New:** `internal/storage/telegram/botpool.go`:
  ```
  type BotOp string
  const (BotOpStream BotOp = "stream"; BotOpUpload BotOp = "upload")

  type BotPool struct {
      bots    []*botClient
      streamIdx, uploadIdx atomic.Int64
  }
  func (p *BotPool) Pick(op BotOp) (idx int, c *botClient)
  ```
  `botClient` holds `{token string, client *http.Client}`. All bots share
  the same `baseURL`.
- `internal/storage/telegram/bot.go` — `BotStorage` holds a `*BotPool`
  instead of a single token. `Upload(...)` picks a bot via
  `pool.Pick(BotOpUpload)` and threads the resulting `BotIndex` into
  every returned `Chunk`. `DownloadRange(ctx, ref)` uses `ref.BotIndex`
  first; on bot-side error (network or 4xx other than 404), fall back to
  round-robin across remaining bots (best-effort).
- `internal/config/config.go` —
  - Add `TelegramBotTokens []string` (parsed from `TELEGRAM_BOT_TOKENS`
    comma-separated; required).
  - Rename `TelegramBotToken string` → `TelegramLegacyBotToken string`
    (still read from `TELEGRAM_BOT_TOKEN` — kept around to cover
    `bot_index=0` legacy rows until Phase 4 migration drains). This is the
    breaking env change.
  - Add `legacyBotIndex = 0` constant in the storage package.
- `cmd/telegram-s3/main.go` — pass the token list and legacy token to
  `BotStorage`.
- `.env.example` — replace `TELEGRAM_BOT_TOKEN=...` with
  `TELEGRAM_BOT_TOKENS=token1,token2,token3`. Keep a comment about
  `TELEGRAM_BOT_TOKEN` being the legacy single-token fallback for
  bot_index=0 reads.
- `HANDOFF.md` — update the "Configuration" section to mention plural
  tokens and the migration cliff.
- Tests:
  - `botpool_test.go`: round-robin order with multiple bots, separate
    counters per op (an upload sequence doesn't affect stream sequence).
  - Migration test: open a pre-Phase-3 SQLite file, run `Open`, verify
    new columns exist, verify backfill turned a legacy single-message row
    into a one-row `object_chunks` entry, verify second `Open` is a no-op.
  - End-to-end: 2 tokens, PUT 6 objects, verify `object_chunks.bot_index`
    is `[0,1,0,1,0,1]` (or however round-robin rolls).

**Production deploy note:** the env var change must be applied in the same
Easypanel deploy as the binary update. Record the current
`TELEGRAM_BOT_TOKEN` value somewhere safe before the deploy — rolling
back the binary will require putting it back.

## Phase 4 — MTProto backend (destination state) [DONE]

**Goal:** replace Bot HTTP API with `gotd/td` MTProto for new uploads,
keep Bot API alive for legacy chunks, migrate in the background.

**This is the biggest phase by an order of magnitude.** Estimated 1000+
lines of new Go + dependency additions. Plan to ship behind
`TELEGRAM_TRANSPORT=bot` (no-op default), then flip to `dual`, then
eventually `mtproto`.

**Files touched:**

- **New package** `internal/storage/telegram/mtproto/`:
  - `client.go` — one `MTProtoBot` per token. Holds `*telegram.Client`,
    `api *tg.Client`, a `*pool.Pool` (lift teldrive's `internal/pool/pool.go`
    verbatim — they credit `iyear/tdl`), cached `*tg.InputChannel`
    resolved at boot. Long-lived `client.Run(ctx, ...)` goroutine with a
    `ready := make(chan error, 1)` for handshake completion. Middlewares:
    `floodwait.NewSimpleWaiter()`, `ratelimit.New(rate.Every(100*time.Millisecond), 5)`,
    `retry.New(10)`.
  - `session.go` — `session.Storage` interface impl backed by a new SQLite
    table `tg_sessions(key TEXT PRIMARY KEY, value BLOB, updated_at TEXT)`.
    Upserts in a transaction (partial writes would force re-auth and trip
    `auth.importBotAuthorization` flood control). Stagger initial bot
    auth at 200 ms intervals on cold start.
  - Migration also adds `bot_chunks_pending_delete(message_id INTEGER NOT
    NULL, bot_index INTEGER NOT NULL, swapped_at TEXT NOT NULL,
    PRIMARY KEY(message_id, bot_index))` — sweeper's grace-delete buffer.
    See design decision #14 for why the bot delete is decoupled from
    the row swap.
  - `upload.go` — `uploader.NewUploader(api).WithThreads(cfg.UploadThreads).WithPartSize(512*1024)`
    → `message.NewSender(api).To(channelPeer).Media(ctx, message.UploadedDocument(upload).Filename(name).ForceFile(true))`.
    Extract `*tg.Message` from updates; return
    `Chunk{MessageID, BotIndex, Size, Offset, Transport: "mtproto"}`.
    **No `FileID` persisted** — chunk identity under MTProto is
    `(MessageID, BotIndex)`.
  - `download.go` — implements `reader.ChunkSource` (so Phase 2's
    `prefetchReader` plugs in unchanged):
    - `Chunk(ctx, offset, limit)`: cache `(messageID, botIndex) → *tg.InputDocumentFileLocation`
      via the Phase-1 generic `Cache` with 30 m TTL. On miss, call
      `client.ChannelsGetMessages(channel, [messageID])` →
      `media.Document.AsInputDocumentFileLocation()` → cache. Call
      `api.UploadGetFile(ctx, &tg.UploadGetFileRequest{Offset, Limit, Location, Precise: true})`.
      On `tgerr.Is(err, tgerr.FileReferenceExpired, tgerr.FileReferenceInvalid)`:
      invalidate the cache key, re-resolve, retry once. On
      `CHANNEL_PRIVATE` / `CHAT_FORBIDDEN`: surface clean error, don't
      round-robin. On other RPC / transport errors: fall back to
      round-robin across the rest of the pool.
    - `ChunkSize(start, end)`: returns up to 1 MiB (MTProto cap),
      halving for tiny ranges — port teldrive `tgc.CalculateChunkSize`.
  - `delete.go` — implements `Backend.DeleteBatch` via
    `ChannelsDeleteMessages` batched at 100/call (port teldrive
    `tgc/helpers.go::DeleteMessages`).
- **New:** `internal/storage/dispatcher.go` — top-level `Backend` impl
  that switches on `ref.Transport` and forwards to `BotStorage` (`"bot"`)
  or `MTProtoStorage` (`"mtproto"`). `Upload` always routes to
  `MTProtoStorage` when `cfg.TelegramTransport != "bot"`. `DeleteBatch`
  groups refs by transport and dispatches each group.
- **New:** `internal/migrate/sweeper.go` — two-pass opportunistic
  background goroutine. Active when `cfg.TelegramTransport == "dual"`.
  - **Pass 1 (migrate):** scan `object_chunks WHERE transport='bot'`
    in age order at `cfg.MigrationRate` rows/day. For each: download
    via `BotStorage`, re-upload via `MTProtoStorage`, then in ONE tx:
    update the row to `transport='mtproto'` + new `message_id` +
    `bot_index` + `file_id=''`, AND insert
    `bot_chunks_pending_delete(message_id, bot_index, swapped_at=NOW)`.
    Per-row failures are logged and the row is left for tomorrow.
  - **Pass 2 (reap):** scan `bot_chunks_pending_delete WHERE
    swapped_at < NOW - cfg.BotDeleteGrace`. For each: `BotStorage.Delete`
    the message, then delete the pending-delete row. Failures leave
    the row in place for the next pass.
  - **Why two passes:** see design decision #14. A reader that fetched
    the chunk map before the swap holds a stale `'bot'` ref; immediate
    delete would 404 it permanently. The grace window lets concurrent
    reads finish before the bot message goes away.
- `cmd/telegram-s3/main.go` — boot all bots in parallel with staggered
  auth, wait for all `ready` channels, register shutdown to `Run`-cancel
  each bot. Construct the dispatcher and pass it to the handler.
- `internal/config/config.go`:
  - `TelegramTransport string` (default `"bot"`; flipped via env to
    `"dual"` after smoke test; eventually `"mtproto"`).
  - `TelegramAppID int`, `TelegramAppHash string` (required when transport
    != "bot").
  - `TelegramPoolSize int` (default 4).
  - `TelegramUploadThreads int` (default 8).
  - `MigrationRate int` (default 100 rows/day, 0 disables the sweeper).
  - `BotDeleteGrace time.Duration` (default 1h, minimum 1m). How long
    a migrated bot message lingers before the sweeper reaps it. Set
    higher if your GETs commonly take >1h. Setting to 0 forces
    immediate delete and re-introduces the read-during-swap race
    (decision #14) — useful only for tests.
- `go.mod` additions: `github.com/gotd/td`, `github.com/gotd/contrib`,
  `go.uber.org/zap`, `golang.org/x/sync`, `golang.org/x/time`,
  `golang.org/x/net/proxy`, `github.com/go-faster/errors`,
  `github.com/cenkalti/backoff/v4`. Plus a tiny `slog → zap` adapter so
  gotd's logger emits through the existing slog handler.
- Tests:
  - Backend interface conformance suite that exercises `BotStorage`,
    `MTProtoStorage`, and the dispatcher uniformly.
  - File-reference refresh: mock `*tg.Client` returns `FILE_REFERENCE_EXPIRED`
    once; assert one re-resolve + one successful retry.
  - Bot revocation: mock `CHANNEL_PRIVATE`; assert clean error.
  - Opportunistic migration end-to-end (two-pass): PUT with
    `TELEGRAM_TRANSPORT=bot`, flip to `dual`, run pass-1 → row's
    `transport` flips to `'mtproto'`, GET serves via mtproto, original
    bot message is NOT yet deleted (within grace), `pending_delete`
    has one row. Fast-forward past `BotDeleteGrace` and run pass-2 →
    bot message deleted, `pending_delete` drained. A concurrent reader
    that holds a pre-swap ref keeps succeeding throughout the grace
    window (the load-bearing test for decision #14).

**Acceptance:**

- Full existing test suite passes with `TELEGRAM_TRANSPORT=bot` (the
  default; Phase 4 deploy is a no-op until flipped).
- New MTProto test suite passes (requires a real test channel + bot
  tokens, or a recorded fixture).
- Pre-deploy: `TELEGRAM_TRANSPORT=mtproto` against staging channel passes
  the full `internal/s3api/*_test.go` suite.
- Production deploy: ship with `TELEGRAM_TRANSPORT=bot`. After verifying
  boot logs show all MTProto bots authenticated, flip to `dual` via
  Easypanel env var update. Watch sweeper logs.

## Pre-existing research (do NOT re-research)

These findings are already verified and baked into the plan:

- **gotd/td library**: `/gotd/td` on Context7. Current version (as of
  2026-05): v0.134.0. High source reputation, 158 code snippets. The
  `*telegram.Client` requires a long-lived `client.Run(ctx, fn)` goroutine;
  other goroutines borrow the `*tg.Client` API via a `ready := make(chan error, 1)`
  pattern. See teldrive `internal/tgc/client_pool.go` for the reference
  implementation.
- **Local Bot API server limits** (`tdlib/telegram-bot-api`): 2 GB upload
  via `sendDocument`, *no documented per-request download cap*. The legacy
  "20 MB" limit applies only to the convenience direct HTTP URL on the
  public Bot API, not to `getFile`/`file_path` downloads through a
  self-hosted local server.
- **modernc.org/sqlite + WAL**: do NOT use a single `*sql.DB` with
  `SetMaxOpenConns(8)` — SQLite serializes the writer regardless, and you
  just get `SQLITE_BUSY` contention. Two `*sql.DB` (write `MaxOpenConns=1`,
  read `MaxOpenConns=N`) is the right pattern. Already done in Phase 0.
- **`FILE_REFERENCE_EXPIRED` in gotd**: returned as a normal RPC error;
  detect via `tgerr.Is(err, tgerr.FileReferenceExpired, tgerr.FileReferenceInvalid)`.
  Fix = invalidate cache, re-call `messages.getMessages` (or
  `channels.getMessages`), rebuild `InputDocumentFileLocation`, retry once.
- **MTProto `UploadGetFile` alignment**: `Offset` must be 0-aligned to a
  power-of-2 chunk size from 4 KiB to 1 MiB; `Limit` must equal that chunk
  size. The Phase-2 reader's `offset := start - (start % chunkSize)` handles
  this. Always pass `Precise: true` for byte-precision.
- **Upload part-count limit**: MTProto caps a single document at 4000 parts.
  At 512 KiB part size = 2 GiB document, matches Telegram's per-document
  ceiling. Stay at 512 KiB × 4000 = 2 GiB. Don't drop part size below
  512 KiB.
- **Teldrive design conventions to copy**: `internal/pool/pool.go` (session
  multiplexing), `internal/reader/tg_reader.go::tgMultiReader` (parallel
  reader), `internal/tgc/bot_selector.go` (round-robin with per-op
  counters), `internal/tgc/helpers.go::DeleteMessages` (batch 100/call),
  `internal/cache/keys.go` (cache-key composition).

## Verification commands (one-liners)

```
# Sanity / starting state
git status --short
git log --oneline -3
go vet ./...
go build ./...
go test ./... -count=1 -timeout 90s

# After each phase
go test ./... -count=1 -timeout 90s -race
go test ./internal/<changed-pkg>/... -count=1 -run <TestName> -v

# Manual smoke (requires running gateway)
aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp <file> s3://test/<key>
aws --endpoint-url http://localhost:9000 s3 cp s3://test/<key> ./out
```

## Existing repo conventions (carry forward)

- Code comments explain **why**, not what. See `internal/s3api/handler.go`
  comments for the style — they reference design plans (`§6.4`, `P7.5`)
  and prior incidents (the OOM-on-bulk-upload comment in `bot.go::Upload`).
  Match that voice in new code.
- Tests live next to the code they cover, with names like
  `internal/s3api/range_test.go`, `phase7_live_test.go`. Phase tests
  follow this naming.
- No new top-level dependencies unless a phase explicitly approves them.
  Phase 1 adds none. Phase 4 adds the gotd tree (approved in plan).
- `go.mod` is currently Go 1.25.6.
- Windows-friendly: paths in code use forward slashes; tests use
  `filepath.Join`. The shell here is PowerShell.

## Rollback per phase

- **Phase 0:** revert single commit; no schema change.
- **Phase 1:** revert. Cache misses already fall back to the original
  `getFile` call, so a stuck cache is not load-bearing.
- **Phase 2:** revert. The old `streamSegments` was sequential; semantics
  restored. **Make sure no `ChunkRef`-typed values were persisted before
  reverting** — the `Chunk` struct gains fields but the DB schema doesn't
  change until Phase 3.
- **Phase 3:** schema additions are additive — old binary ignores the new
  columns. The env var `TELEGRAM_BOT_TOKEN` was removed; rolling back the
  binary requires re-adding it (record value before the deploy).
- **Phase 4:** deploy first with `TELEGRAM_TRANSPORT=bot` (no behavior
  change). Flip to `dual` once boot logs show all MTProto bots
  authenticated. Migration is opportunistic and can be paused via
  `MIGRATION_RATE=0`. Roll back by flipping `TELEGRAM_TRANSPORT` back to
  `bot` — `'mtproto'` chunk rows then become unreadable until next
  forward deploy, **so do not regress past the point where any
  `'mtproto'` chunks exist without a clear plan.**
