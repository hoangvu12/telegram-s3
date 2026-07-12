# Next phases — BotStorage cleanup + Channel rollover

Self-contained implementation plan for two follow-ups after Phase 4
shipped (`master` HEAD `256346a`, transport=mtproto live, drain at 0).
A fresh Claude Code session in this working directory can pick up from
here without prior chat context.

**Read in this order:**

1. This file (scope, decisions, step-by-step for both phases).
2. `PHASES.md` (Phases 0–4 design rationale + invariants).
3. Auto-memory `mtproto-migration-roadmap`, `phase4-test-strategy`,
   `phase-commit-shape`, `reader-diverges-from-teldrive`.

---

## TL;DR

- **Phase 5: BotStorage cleanup.** Delete the now-dead Bot HTTP API
  backend, dispatcher's "bot"/"dual" modes, migration sweeper,
  and related env vars. ~500–800 LoC removed, no behavior change.
  Single squashed commit per `phase-commit-shape`.
- **Phase 6: Channel rollover (manual-create + auto-routing).**
  Multi-channel awareness: chunk rows carry `channel_id`; operator
  creates new channels manually in Telegram + registers via a CLI
  subcommand; uploads route to whichever channel is "selected"; reads
  resolve per-chunk's `channel_id`. Single squashed commit.
- **Phase 7 (deferred, optional): userbot for auto-create.** The
  Telegram-side restriction that *bots cannot create channels* means
  full automation requires a user-account MTProto session. ~150 LoC +
  one-time human auth. Worth it only if channel churn becomes
  operationally painful. Not blocking Phase 6.

Order matters. Ship Phase 5 first — it removes confusion before
Phase 6 adds new channel-routing code. Don't bundle them.

---

## Resume verification (run first — confirm world matches this doc)

```sh
git status --short                     # expect: untracked HANDOFF-PHASE4.md, PHASES.md, S3-COMPAT-ACCEPTANCE.md, NEXT-PHASES.md, scripts/
git log --oneline -3                   # expect 256346a at top (Phase 4 perf), then 5ba3fda, f7b804c
curl -sS -o /dev/null -w '%{http_code}\n' https://s3.nguyenvu.dev/healthz  # 204
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=10 --grep="drain snapshot|server listening"
# expect: server listening transport=mtproto, bot_chunks_remaining=0, pending_deletes=1854 (or similar)
go vet ./... && go build ./... && go test ./... -count=1 -timeout 180s   # clean
```

If transport != mtproto or bot_chunks_remaining > 0 → **stop and read
HANDOFF-PHASE4.md instead** — the Phase 4 migration is not done.

---

## Phase 5: BotStorage cleanup

### Goal

Delete the dead `BotStorage` code path now that production runs
`TELEGRAM_TRANSPORT=mtproto` and zero chunk rows have `transport='bot'`.
No behavior change: every read/write/delete already routes through
MTProto.

### What gets deleted

| Path | Reason |
|---|---|
| `internal/storage/telegram/bot.go` | Bot HTTP API client (~300 LoC) |
| `internal/storage/telegram/bot_test.go` | Tests for above |
| `internal/storage/telegram/botpool.go` | Round-robin pool for Bot HTTP clients |
| `internal/storage/telegram/botpool_test.go` | Tests for above |
| `internal/storage/dispatcher.go` | Routing layer collapses (only one backend left) |
| `internal/storage/dispatcher_test.go` | Tests for above |
| `internal/migrate/` (entire package) | Sweeper has no work in mtproto-only |

### What gets modified

| Path | Change |
|---|---|
| `cmd/telegram-s3/main.go` | Stop constructing BotStorage, Dispatcher, Sweeper. Pass `*mtproto.Storage` directly as `storage.Backend` to the handler. |
| `internal/storage/storage.go` | Drop `TransportBot` constant; keep `TransportMTProto` for chunk-row literal. Keep `Chunk` struct as-is (additive schema invariant). Drop `Chunk.Ref()`'s `transport == "" → "bot"` fallback — every row in the wild is mtproto. |
| `internal/config/config.go` | Drop fields: `TelegramTransport`, `TelegramAPIBaseURL`, `MigrationRate`, `MigrationWorkers`, `BotDeleteGrace`. Drop their env-load + validation. `TELEGRAM_APP_ID`/`TELEGRAM_APP_HASH` become unconditionally required. |
| `internal/config/config_test.go` | Drop transport/migration test cases. |
| `internal/s3api/handler.go` | If it references `cfg.TelegramTransport` for any branching, simplify. |
| `internal/metadata/store.go` | Drop: `ListBotChunksOldestFirst`, `SwapBotChunkToMtproto`, `CountBotChunks` (if exists), `BotMigrationSnapshot`, `BotChunkLoc`. Drop the `pending_delete` helpers (`PendingDeletesOlderThan`, `DeletePendingDelete`). |
| `.env.example` | Drop: `TELEGRAM_TRANSPORT`, `TELEGRAM_BOT_TOKEN` (legacy singular — promote `TELEGRAM_BOT_TOKENS` to the only form), `TELEGRAM_API_BASE_URL`, `MIGRATION_RATE`, `MIGRATION_WORKERS`, `BOT_DELETE_GRACE`. |
| `README.md` | Drop migration-mode sections; promote mtproto-only as the only mode. |

### Schema decisions

**Keep, do not drop:**

- `object_chunks.transport` column — additive-only invariant
  (PHASES.md decision #8). All rows hold `'mtproto'` literal now.
- `object_chunks.bot_index` column — still meaningful (multi-bot
  MTProto pool round-robin index).
- `bot_chunks_pending_delete` table — 1854 dormant zombie rows.
  Leave as historical record; no read or write path touches it
  after Phase 5.
- `tg_sessions` table — MTProto session storage; still required.

**Drop never.** The cost of an empty table is zero; the cost of a
broken schema migration on production data is real.

### What CANNOT be removed (subtle)

- `TELEGRAM_BOT_TOKENS` env — MTProto bots authenticate with bot
  tokens (each bot calls `auth.importBotAuthorization`). The token
  is still required; just no longer used by Bot HTTP API.
- `TELEGRAM_CHAT_ID` env — bootstrap channel ID. Phase 6 changes
  semantics (only the FIRST channel; the rest come from the
  `channels` table), but the var stays.
- `internal/storage/storage.go` `Backend` interface — still the
  shape mtproto.Storage implements.

### Step-by-step

1. Delete the four bot files + dispatcher + dispatcher_test.
2. Delete `internal/migrate/` directory entirely.
3. Edit `cmd/telegram-s3/main.go`:
   - Remove `botBackend := telegram.NewBotStorageWithOptions(...)`.
   - Remove `dispatcher, err = storage.NewDispatcher(...)` — handler
     now gets the MTProto storage directly.
   - Remove `migrate.NewSweeper(...)` + the `go sweeper.Run(...)` goroutine.
4. Edit `internal/config/config.go`:
   - Drop the five fields listed in the table.
   - Drop their `getInt`/`getDuration` loads.
   - Drop the `TelegramTransport` switch validation.
   - Make `TELEGRAM_APP_ID > 0` and `TELEGRAM_APP_HASH != ""` always required.
   - Drop the legacy `TELEGRAM_BOT_TOKEN` soft-fallback (require `TELEGRAM_BOT_TOKENS`).
5. Edit `internal/config/config_test.go` to match.
6. Edit `internal/metadata/store.go`:
   - Remove migration helpers + the BotChunkLoc/BotMigrationSnapshot
     types. Search-and-delete by symbol name.
7. Edit `internal/storage/storage.go`:
   - Drop `TransportBot` constant + Ref()'s fallback.
8. Edit `.env.example` + `README.md`.
9. `go vet ./... && go build ./... && go test ./... -count=1`.
10. Manual smoke: `aws s3 cp` round-trip via prod endpoint, same as
    the Phase 4 completion test (HANDOFF-PHASE4.md §"Smoke-test gap").

### Test plan

No new tests. Existing tests on `internal/storage/telegram/mtproto/*`,
`internal/reader/*`, `internal/s3api/*` cover all retained paths.
Tests that referenced BotStorage/Dispatcher/Sweeper get deleted with
their subjects.

### Commit shape

One squashed commit. Suggested message:

```
Phase 5: drop dead BotStorage + dispatcher + migration sweeper

After Phase 4 (256346a) flipped TELEGRAM_TRANSPORT=mtproto in
production with bot_chunks_remaining=0, every read/write/delete
routes through MTProto. The Bot HTTP API client, dispatcher
routing layer, and migration sweeper are dead code.

Removed:
- internal/storage/telegram/{bot,botpool}{,_test}.go
- internal/storage/dispatcher{,_test}.go
- internal/migrate/

Config drops: TELEGRAM_TRANSPORT, TELEGRAM_API_BASE_URL,
MIGRATION_RATE, MIGRATION_WORKERS, BOT_DELETE_GRACE,
TELEGRAM_BOT_TOKEN (singular legacy form).

DB schema unchanged per the additive-only invariant:
bot_chunks_pending_delete still holds 1854 zombie rows; no
read/write path touches it after this.
```

### Rollback

Revert the commit. Schema is unchanged so revert is purely code.

---

## Phase 6: Channel rollover (manual-create + auto-routing)

### Goal

Defend against the day a single Telegram channel approaches a
structural limit (channel message ID ceiling ~2B, practical
client-UI thresholds well before that). Operator creates a new
channel in Telegram + registers it via CLI; the gateway routes
new uploads there; existing chunks keep reading from whichever
channel they were stored in.

### Why manual-create, not auto-create (Phase 7)

**Hard Telegram constraint:** `channels.createChannel` requires user
authorization, not bot authorization. Bots cannot create channels
via the MTProto API. Teldrive's auto-create uses the *user's*
MTProto session (their multi-tenant SaaS model where each user
signs in with their own Telegram account).

Our deploy uses bot tokens only. Auto-create requires adding a
user-account MTProto session (a "userbot") — that's Phase 7,
deferred. Phase 6 ships the *routing + registry* layer with a CLI
subcommand for the operator to register channels they created in
Telegram by hand. This delivers the structural benefit (multi-
channel awareness) without the userbot complexity.

When Phase 6 is in production, the operator workflow is:
1. Channel near limit → operator creates new channel in Telegram
   desktop client (one-time, ~30 seconds).
2. Operator adds every bot in `TELEGRAM_BOT_TOKENS` as channel
   admin with delete + post permissions (manual via Telegram UI).
3. Operator runs `telegram-s3 channels add --id=-100xxx --name="storage_2" --select`.
4. New uploads start landing in the new channel; reads of old
   data keep using the old channel.

### Scope summary

| Area | Change |
|---|---|
| Schema | New `channels` table; `channel_id` column added (additive) to `object_chunks` + `multipart_part_chunks`; bootstrap insert + backfill at migrate-time. |
| New package | `internal/channels/manager.go` — selection, capacity check, registration, in-process mutex for serialization. |
| MTProto code | `MTProtoBot` swaps single `*tg.InputChannel` cache → keyed-by-channelID LRU; upload/download/delete take a `channelID` arg. Pool fans channels through bots same as before. |
| Storage interface | `Backend.Upload` returns chunks tagged with channel_id (already returns the message-id; add channel-id). `Backend.DownloadRange`/`Delete` take ChunkRef with channel_id. |
| Metadata store | Chunk CRUD takes/returns channel_id. `Current()` returns selected channel. `Register(channel)` inserts into channels table. `PartCountByChannel(channelID)` for capacity check. |
| Config | New: `CHANNEL_PART_LIMIT` (default 500000 — match teldrive's default). `TELEGRAM_CHAT_ID` becomes bootstrap-only: only used when `channels` table is empty at boot. |
| CLI | New cobra/flag subcommand `telegram-s3 channels {list,add,select,remove}`. |
| Tests | New `metadata` tests for the channels table + chunk channel_id; new `channels` package tests for capacity check + selection serialization; new `mtproto` tests for keyed channel resolution. Existing tests get a channelID argument plumbed in. |

### Schema migration

Additive only. SQLite has no `ADD COLUMN IF NOT EXISTS`, so use the
existing `ensureColumn` helper (search `internal/metadata/store.go`).

```sql
-- New table
CREATE TABLE IF NOT EXISTS channels (
    channel_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    selected INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_channels_selected ON channels(selected) WHERE selected = 1;

-- Additive columns. Default 0 means "not yet backfilled".
-- The migrate-at-boot backfill sets all 0s to the bootstrap channel ID.
ALTER TABLE object_chunks ADD COLUMN channel_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE multipart_part_chunks ADD COLUMN channel_id INTEGER NOT NULL DEFAULT 0;
```

**Bootstrap migration logic at `Open()`:**

```go
// Pseudocode — runs once on every boot, idempotent.
if countChannels() == 0 {
    bootstrapID := cfg.TelegramChatID  // env-supplied
    insertChannel(bootstrapID, "bootstrap", selected=true)
}
backfillCount := count("object_chunks WHERE channel_id = 0")
if backfillCount > 0 {
    bootstrap := getSelectedChannelID()
    UPDATE object_chunks SET channel_id = bootstrap WHERE channel_id = 0
    UPDATE multipart_part_chunks SET channel_id = bootstrap WHERE channel_id = 0
}
```

The backfill runs ONCE on first boot after the Phase 6 deploy.
Idempotent: subsequent boots find 0 rows with channel_id=0 and
no-op.

### New package: `internal/channels`

```go
// Manager wraps the channels table + provides serialized
// "current channel + register-new" semantics.
type Manager struct {
    store    *metadata.Store
    limit    int64  // CHANNEL_PART_LIMIT
    mu       sync.Mutex  // serialize selection updates within process
    cached   atomic.Int64  // last-known current channel ID
    logger   *slog.Logger
}

// Current returns the selected channel ID. Cached in-process; the
// cache invalidates on Register/Select.
func (m *Manager) Current(ctx context.Context) (int64, error)

// AtCapacity reports whether the current channel's part count is
// at or above the configured limit.
func (m *Manager) AtCapacity(ctx context.Context, channelID int64) (bool, error)

// Register adds a channel to the registry. If selected, atomically
// flips selected=false on every other row.
func (m *Manager) Register(ctx context.Context, channelID int64, name string, selected bool) error

// List returns every registered channel.
func (m *Manager) List(ctx context.Context) ([]Channel, error)
```

**Why an in-process mutex (not DB advisory lock):** we're single-
instance SQLite. Teldrive uses Postgres advisory locks because
they support multi-instance deploys. A `sync.Mutex` is the
equivalent for our shape; if we ever go multi-instance, swap to
SQLite `BEGIN IMMEDIATE` or app-level lease via the DB.

### MTProto changes

Current shape (`internal/storage/telegram/mtproto/client.go`):
each `MTProtoBot` caches one `*tg.InputChannel` resolved at
`StartBot` from `cfg.TelegramChatID`.

New shape:
- Replace single cached `*tg.InputChannel` with `sync.Map` (or
  small LRU) keyed by channelID.
- New method `bot.Channel(ctx, channelID int64) (*tg.InputChannel, error)`
  resolves via `channels.GetChannels` on first ask; caches result.
- Upload/download/delete take `channelID` as a parameter; the bot
  resolves the InputChannel on demand.
- `Pool.Upload(ctx, channelID, ...)` and `Pool.Download(ctx, channelID, ref)`
  — channelID becomes part of the call surface.

`channels.GetChannels` returns `*tg.MessagesChats`; pull
`(*tg.Channel).AsInput()` to get the input form. Cache the
access hash with it (input channels need both ID + access hash).

Failure modes:
- Channel ID not found / bot not admin → MTProto returns
  `CHANNEL_PRIVATE` or `CHANNEL_INVALID`. Treat as fatal for that
  request (route the chunk error up to the S3 handler as 500).
- Race where channel was just created but not yet registered with
  bots — register CLI subcommand should poll-verify admin status
  before persisting to DB.

### Storage interface changes

```go
// Chunk gains a ChannelID field.
type Chunk struct {
    Seq       int
    FileID    string
    MessageID int64
    Size      int64
    Offset    int64
    Transport string
    BotIndex  int
    ChannelID int64  // NEW — which Telegram channel holds this chunk
}

// ChunkRef same.
type ChunkRef struct {
    Transport string
    BotFileID string
    MessageID int64
    BotIndex  int
    ChannelID int64  // NEW
}

// Backend.Upload takes a channelID — the dispatcher (or its
// post-Phase-5 replacement) calls Manager.Current() and passes
// the result.
Upload(ctx context.Context, channelID int64, name, contentType string, body io.Reader) ([]Chunk, error)
```

Yes, this is a breaking-shape change to the Backend interface.
That's fine — only mtproto.Storage implements it (post-Phase 5).

### Metadata store changes

- `PutObject(ctx, obj, chunks)` — chunks now carry ChannelID;
  persist it.
- `GetObjectChunks(ctx, bucket, key)` — return ChannelID populated.
- New: `PartCountByChannel(ctx, channelID) → int64` for capacity check.
- New: `RegisterChannel(ctx, id, name, selected)` — transactional;
  if selected, `UPDATE channels SET selected=0 WHERE selected=1` first.
- New: `SelectedChannel(ctx) → (int64, error)`.
- New: `ListChannels(ctx) → []Channel`.

### Config changes

Add:

```
CHANNEL_PART_LIMIT=500000   # rollover threshold; matches teldrive default
```

Existing `TELEGRAM_CHAT_ID` keeps its name but its semantics narrow:
"bootstrap channel ID, used only when the `channels` table is empty
at boot." Document this in `.env.example` + README.

### Operator CLI

Add a subcommand-style entrypoint. Minimal impl using `flag`:

```sh
telegram-s3 channels list
# 2026-03-01  -1003909089400  bootstrap        [SELECTED]
# 2026-04-15  -1003912345678  storage_2

telegram-s3 channels add --id=-1003999888777 --name="storage_3" --select
# Adds the channel to the registry. --select flips it to the
# upload target. Verifies all bots have admin rights first.

telegram-s3 channels select --id=-1003912345678
# Flip the upload target without adding a new one.

telegram-s3 channels remove --id=-1003999888777
# Refuses if any chunk row still references the channel. Otherwise
# drops the row (existing chunks keep working; only new uploads
# stop landing here).
```

The CLI shares the same `Open(cfg.DatabasePath)` path as the
gateway. Must not run while the gateway is running on the same DB
file (SQLite WAL handles concurrent readers but a CLI write could
race with the gateway). Easypanel pattern: stop service → run CLI
→ start service.

Alternative: add a `/admin/channels` HTTP endpoint on the gateway
itself, gated by an `ADMIN_TOKEN` env. Simpler to operate (no
service stop). Recommend.

### Step-by-step

1. Schema migration in `internal/metadata/store.go` (`ensureColumn` +
   `CREATE TABLE` + bootstrap backfill).
2. Add `Channel` type + CRUD methods on `*metadata.Store`.
3. Create `internal/channels/manager.go` (Manager + cache + mutex).
4. Add `ChannelID` field to `storage.Chunk` + `storage.ChunkRef`.
5. Extend `Backend.Upload`/`DownloadRange`/`Delete` signatures.
6. Update `mtproto.Storage`:
   - Channel resolution cache keyed by ID.
   - Pool.Upload routes through `Manager.Current()` → passes channelID
     to bot.
   - Read/delete use ref.ChannelID.
7. Update `internal/s3api/handler.go`:
   - PutObject path: `mgr.Current()` → if `mgr.AtCapacity(current)`
     log a WARN telling operator to register a new channel.
     (No auto-create in Phase 6.)
8. Add CLI subcommand OR admin HTTP endpoint (pick one in advance).
9. Update `.env.example` + README to document `CHANNEL_PART_LIMIT`
   + the manual rollover procedure.
10. `go vet ./... && go build ./... && go test ./... -count=1`.
11. Smoke test: upload 1 file, register a second test channel,
    confirm uploads route to it and reads of the first file still
    work.

### Test plan

Mocks-only per `phase4-test-strategy` memory. New tests:

- `internal/metadata/channels_test.go` — table CRUD, bootstrap
  backfill idempotency, `RegisterChannel` selected-flip atomicity.
- `internal/channels/manager_test.go` — Current() caching;
  AtCapacity() arithmetic at boundary; concurrent Register calls
  serialize cleanly.
- `internal/storage/telegram/mtproto/storage_channel_test.go` —
  channel cache hits on second resolve, miss-and-fetch on first,
  passes the right channelID to ChannelsDeleteMessages /
  uploads. Use the existing `*tg.Client` mock pattern from
  `mtproto/retry_test.go`.
- `internal/s3api/multipart_test.go` extension — multipart upload
  routes every part to the same channel (don't split a single
  multipart across a rollover boundary mid-upload).

### Bootstrap migration verification

On first boot after Phase 6 deploy, expect logs:
```
channels table empty, bootstrapping with TELEGRAM_CHAT_ID=-1003909089400
backfilled N object_chunks with channel_id=-1003909089400
backfilled M multipart_part_chunks with channel_id=-1003909089400
```

On second boot: no log lines (idempotent no-op).

If `TELEGRAM_CHAT_ID` is unset on first boot → fatal startup error.
If `TELEGRAM_CHAT_ID` is changed after bootstrap → ignored (the
DB-stored bootstrap channel wins). Document.

### Commit shape

One squashed commit. Suggested message:

```
Phase 6: channel rollover registry + auto-routing (manual create)

Adds multi-channel awareness so the gateway is not pinned to a
single Telegram channel forever:

- New channels table; chunk rows carry channel_id (additive
  schema migration + bootstrap backfill from TELEGRAM_CHAT_ID).
- internal/channels.Manager handles selection + capacity check
  with in-process mutex serialization.
- MTProtoBot resolves channels by ID (LRU cache); upload/download/
  delete plumb a channelID arg through Backend.
- Admin endpoint /admin/channels (or CLI subcommand) for the
  operator to register channels they created manually in Telegram
  + flip the selected upload target.
- Config: new CHANNEL_PART_LIMIT (default 500000, matching
  teldrive). TELEGRAM_CHAT_ID narrows to bootstrap-only semantics.

Bots cannot create channels via MTProto (Telegram API restriction
on bot identities). Auto-create requires a userbot session and is
deferred to Phase 7.
```

### Rollback

The schema is additive — revert the binary and the gateway falls
back to single-channel behavior (chunk rows still have a
`channel_id` column, but the old code ignores it). Reads of
chunks that landed in non-bootstrap channels would 404 after a
revert. So rollback is *safe only if* no chunks have been written
to non-bootstrap channels yet. After the first rollover happens,
revert means data loss for new-channel chunks.

Defensive: after deploying Phase 6, leave the binary in production
for 24h with logging before registering any new channels.

---

## Phase 7 (deferred, optional): userbot for auto-create

When you'd want it:
- Channel rollover starts firing often enough that operator
  manual-create is friction (e.g., once a week or more).
- You want the gateway to handle bursts of small files without
  human intervention.

What it adds:
- A user-account MTProto session (separate from bot sessions).
  One-time auth: human signs in via Telegram desktop, scripts pull
  the session string, stored in a new `userbot_sessions` table or
  env var.
- `userbot.CreateChannel(title)` calls `channels.createChannel`
  with user auth.
- After create, automatically promotes every bot in
  `TELEGRAM_BOT_TOKENS` to admin via `channels.editAdmin` (bot
  auth is fine for the *editing* side once the user-created
  channel exists).
- Channel registry insert happens automatically inside the same
  flow.

Operational cost:
- Userbot accounts can be flagged for "automation" if they create
  too many channels too fast. Real risk for accounts that aren't
  established. Use an aged personal account or be conservative
  with rollover triggering.
- Telegram terms-of-service grey area. Read the latest TOS before
  shipping. Manual is always safe.

Skip until manual genuinely hurts. Most likely never.

---

## Cross-phase: invariants to preserve

Pulled forward from `PHASES.md` §"Critical do-not-regress
constraints" — these still hold through Phase 5 + 6.

1. **Object GETs go through `internal/reader.Reader`.** Phase 6
   adds `channel_id` to ChunkRef; the reader's plumbing changes
   minimally — same per-index channels + drainer (per
   `reader-diverges-from-teldrive` memory; do not port teldrive's
   `bufferChan` model).
2. **`Reader.Prime()` runs before `writeHeaders()`.** Sacred. The
   502-vs-truncated-200 invariant.
3. **Schema changes are additive only.** Phase 5 removes ZERO
   columns/tables. Phase 6 adds `channel_id` columns + `channels`
   table.
4. **Chunk map is the SOLE source of truth on read/delete.** No
   fallback branches. Phase 6 widens "chunk map" to include
   channel_id — same principle.
5. **`storage.Backend` interface is small.** Phase 6 adds
   `channelID` to Upload/DownloadRange/Delete signatures but
   doesn't add new methods (no `RotateChannel`, no
   `CreateChannel` — those live in `internal/channels`).
6. **Tests stay mocks-only.** No live Telegram E2E suite per
   `phase4-test-strategy`.
7. **Each phase ships as one squashed commit.** Per
   `phase-commit-shape`. Don't split Phase 6 into
   "schema commit + code commit" — atomicity matters for revert.

## Cross-phase: do NOT do

- Don't try to drop `bot_chunks_pending_delete` or any other
  table in Phase 5 (additive-only).
- Don't try to backfill `channel_id` to anything but the
  bootstrap channel ID. The mapping for legacy chunks is
  unambiguous (they were all in the single env-supplied channel).
- Don't add an "automatic" channel selection heuristic (least-
  full, round-robin, etc.). The selected channel is operator-
  driven. Auto only happens via Phase 7 userbot create-and-select.
- Don't add a "default per-bucket channel" config. The whole
  point is centralized routing — per-bucket overrides re-
  fragment what we just consolidated.
- Don't reorder Phase 5 and Phase 6. Phase 6 on top of dead
  `Dispatcher`/`BotStorage` would be confused review.

---

## Open questions for the operator (you)

1. **Admin endpoint vs CLI subcommand for channel registration?**
   Endpoint is simpler to operate (no service stop). CLI is
   safer (no exposed mutation API on the gateway). Recommend
   endpoint behind `ADMIN_TOKEN` env. Decide before Phase 6
   implementation.

2. **`CHANNEL_PART_LIMIT` default?** Teldrive uses 500000.
   Reasonable for our scale. Use as-is unless you have a
   specific reason.

3. **Phase 6 spike test channel.** Before flipping production,
   want to test channel rollover end-to-end against a non-
   production Telegram channel? Cheap: create a test channel,
   add bots, register via admin endpoint, upload a test file,
   GET it back, register a second test channel, flip selected,
   upload another file, GET both. ~15 minutes wall time.

---

## When you're done

After Phase 6 ships and at least one rollover has been verified
in production, this doc is exhausted. Tidy-ups:

1. Delete `NEXT-PHASES.md` (untracked, like HANDOFF-PHASE4.md was).
2. Update `PHASES.md` to add Phase 5 + Phase 6 sections matching
   the [DONE] format of Phases 0–4.
3. Update `mtproto-migration-roadmap` auto-memory.

If Phase 5 shipped but Phase 6 didn't, update the TL;DR at the
top of this file so the next session sees an accurate state.
