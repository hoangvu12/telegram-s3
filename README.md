# telegram-s3

S3-compatible gateway backed by Telegram. Objects are chunked and
stored as documents in a Telegram channel; reads stream the chunks
back through the S3 API.

Personal-scale only. Telegram is not a real object-storage service —
no SLA, no per-object durability guarantees, throughput is bounded
by Telegram's per-bot rate caps. Use this if you have a Telegram
channel you trust and a use case (file dump, share-link backend,
mostly-static asset host) that fits the trade-offs.

## What works

- Buckets: create, list, delete
- Objects: PUT, GET, HEAD, DELETE, COPY, list
- **Range reads** (parallel prefetch — multiple chunks fetched
  concurrently per GET, delivered in order)
- **Multipart uploads** (S3 multipart API; each part lands as its
  own chunked upload)
- **Large objects** — chunks are stitched, no Telegram document
  size limit applies to objects
- **Dual transport** — Bot HTTP API for legacy data + MTProto for
  new writes, with a background sweeper migrating between them
- Static AWS SigV4 credentials
- SQLite metadata with WAL writer/reader split
- Per-bot session multiplexing (gotd connection pool)
- Health endpoint at `GET /healthz` (returns 204)

## Not implemented

Server-side encryption, ACLs, versioning, lifecycle policies, object
tags, CDN-redirect download path (Telegram rarely serves bot uploads
from CDN; if your deployment hits this, see `download.go`'s
"unexpected response" branch).

## Quick start

### 1. Create a Telegram bot

1. Talk to `@BotFather` on Telegram → `/newbot` → save the token.
2. Talk to `@BotFather` → `/setprivacy` → select your bot →
   **Disable** (so it can see all messages, not just direct
   commands — optional but recommended for some flows).

### 2. Get MTProto app credentials

Required for `TELEGRAM_TRANSPORT=dual` or `mtproto`. Skip if you're
running pure Bot-API mode (`TELEGRAM_TRANSPORT=bot`, the default).

1. Visit <https://my.telegram.org/auth> and log in with your phone
   number.
2. → "API development tools" → fill in a basic app description.
3. Note the **App api_id** (numeric) and **App api_hash** (hex).
   These are different from the bot token.

### 3. Create the storage channel

1. In Telegram, create a **private supergroup or channel**.
2. Add your bot as an **administrator** with these rights:
   - **Post Messages** — required, otherwise the bot can't upload
   - **Delete Messages** — recommended (also see "Zombie messages"
     limitation below)
   - Other rights are optional; the bot doesn't use them.
3. Get the channel's chat_id. Easiest way: post a message in the
   channel, forward it to `@RawDataBot`, copy `forward_from_chat.id`.
   It will look like `-1001234567890` (the `-100` prefix means
   supergroup/channel).

### 4. Configure & run

```bash
cp .env.example .env
# Edit .env — at minimum set TELEGRAM_BOT_TOKEN (or TELEGRAM_BOT_TOKENS),
# TELEGRAM_CHAT_ID, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY.
# If using TELEGRAM_TRANSPORT=dual or mtproto, also set
# TELEGRAM_APP_ID and TELEGRAM_APP_HASH.

go run ./cmd/telegram-s3
```

Default port is `:9000`. Health check: `curl http://localhost:9000/healthz`.

### 5. Test with AWS CLI

```bash
aws configure set aws_access_key_id "$S3_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$S3_SECRET_ACCESS_KEY"
aws configure set region us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp ./some-file s3://test/
aws --endpoint-url http://localhost:9000 s3 ls s3://test
aws --endpoint-url http://localhost:9000 s3 cp s3://test/some-file ./downloaded
```

## Transport modes

| Mode | New uploads | Old reads | Use when |
|---|---|---|---|
| `bot` (default) | Bot HTTP API | Bot HTTP API | Bootstrap, simplest setup, no MTProto creds needed |
| `dual` | MTProto | Bot OR MTProto (per chunk) + background sweeper migrates `bot → mtproto` | Migrating an existing deploy off Bot API |
| `mtproto` | MTProto | MTProto only | Once the sweeper has drained all `bot` chunks |

**Going from `bot` to `mtproto` is a two-step migration:**

1. Flip env to `TELEGRAM_TRANSPORT=dual`. New uploads land via
   MTProto; old chunks stay readable via Bot API; sweeper drains in
   the background.
2. Watch `bot_chunks_remaining` (logged in `drain snapshot` lines).
   When it hits 0, flip env to `TELEGRAM_TRANSPORT=mtproto`.

The sweeper rate is bounded by `MIGRATION_RATE` (rows/day). Realistic
production numbers per single bot:

| MIGRATION_RATE | Effective | Note |
|---|---|---|
| 1000 | ~1000/day | Conservative; very low Telegram load |
| 10000 | ~8640/day | Hits the 60s tick-interval clamp; one bot's effective ceiling for migration throughput |
| 20000+ | same as 10000 | Single-bot ceiling; for higher, add more tokens via `TELEGRAM_BOT_TOKENS=tok1,tok2,...` |

## Configuration reference

The full env surface is documented inline in `.env.example`. Highlights:

- **Tokens:** `TELEGRAM_BOT_TOKENS=tok1,tok2,...` (comma-separated)
  enables multi-bot round-robin for both uploads and reads. The
  legacy singular `TELEGRAM_BOT_TOKEN` is supported as a fallback.
- **Tuning:** `HTTP_MAX_IDLE_CONNS_PER_HOST`, `SQLITE_READER_CONNS`,
  `STREAM_CONCURRENCY`, `STREAM_BUFFERS`, `STREAM_CHUNK_SIZE`,
  `CHUNK_TIMEOUT`, `LOCATION_CACHE_TTL`, `TELEGRAM_MAX_CHUNK_SIZE`,
  `TELEGRAM_POOL_SIZE`, `TELEGRAM_UPLOAD_THREADS` — all have safe
  defaults; tweak only if profiling says so.
- **Migration:** `MIGRATION_RATE`, `BOT_DELETE_GRACE`.

## Known limitations

### Zombie messages after migration

When migrating `bot → mtproto`, the sweeper's pass-1 atomically
swaps the chunk row to point at the new MTProto-uploaded copy. The
old bot-API-uploaded message in the channel is supposed to be
deleted in a separate pass-2 sweep after a grace window.

**Telegram refuses to let bots delete sufficiently-old messages**,
even with `can_delete_messages` admin right. The Bot API surfaces
this as a 48h limit; MTProto returns `MESSAGE_DELETE_FORBIDDEN`. No
admin permission overrides it for bot identities.

Workaround: set `BOT_DELETE_GRACE=999h` (or any very large value).
Pass-2 never finds rows to reap. The old bot messages stay in the
channel forever as "zombies" — they're inaccessible via S3 (chunk
map points to the new mtproto copies) but cost ~1 message slot
each in the channel. For most users this is fine; Telegram channels
have effectively unlimited message capacity.

If channel clutter matters, you can run a user-account MTProto
script (Telethon, etc.) to delete them — user accounts have looser
delete rules and can clean admin/bot messages in chats they own.
This isn't built into this project.

### Telegram is not real object storage

- No SLA, no per-object durability promises.
- Bot tokens are rate-limited; throughput per bot is bounded by
  Telegram's flood-control. Run multiple bots for parallelism.
- Bot can be revoked; the bot can be kicked from the channel.
  Either makes all data inaccessible. Have a backup strategy if
  this matters.
- File reference tokens (the MTProto download handle) expire after
  ~30 minutes; the gateway handles refresh-on-expired automatically.

### Not a drop-in S3 replacement

Several S3 features aren't implemented (encryption, ACLs,
versioning, lifecycle, presigned-URL upload, etc.). The implemented
subset is enough for typical "static-asset host" or "file-share
backend" use cases.

## Architecture

```
            ┌─────────────────────┐
HTTP S3 ───▶│   internal/s3api    │
            └──────────┬──────────┘
                       │  ChunkRef
                       ▼
            ┌─────────────────────┐
            │ storage.Dispatcher  │── routes by ref.Transport
            └─────┬───────────┬───┘
                  │           │
       transport=bot      transport=mtproto
                  │           │
                  ▼           ▼
            ┌──────────┐  ┌──────────┐
            │BotStorage│  │ MTProto  │
            │(HTTP API)│  │ (gotd/td)│
            └──────────┘  └──────────┘
                  │           │
                  └─────┬─────┘
                        ▼
              Telegram channel
              (single supergroup)
```

- **`internal/s3api`** — S3 API surface (SigV4 auth, request
  parsing, response shapes). Calls into the storage layer through
  the `Backend` interface.
- **`internal/reader`** — parallel-prefetch reader. N concurrent
  chunk fetches, ordered delivery.
- **`internal/storage`** — `Backend` interface + dispatcher.
- **`internal/storage/telegram`** — Bot HTTP API backend.
- **`internal/storage/telegram/mtproto`** — MTProto backend via
  `gotd/td`. Includes session pool for multiplexing per-bot RPCs.
- **`internal/migrate`** — background two-pass sweeper that drains
  `transport='bot'` rows into MTProto.
- **`internal/metadata`** — SQLite metadata (objects, chunks,
  multipart parts, MTProto session storage, pending-delete queue).
- **`internal/cache`** — generic TTL cache. Bot-API uses it for
  `(file_id → file_path)`; MTProto uses it for
  `(message_id, bot_index → InputDocumentFileLocation)`.

## Development

```bash
# Run the full test suite (Windows host can skip -race; CI runs with it):
go test ./... -count=1 -timeout 180s

# Build a local binary:
go build -o telegram-s3 ./cmd/telegram-s3
```

Tests are mocks-only against `*tg.Client` for the MTProto layer; no
live Telegram needed. The migration sweeper has its own in-memory
backend fake so two-pass invariants can be exercised without standing
up a real channel.

## Credits

Several patterns lifted (with attribution in code comments) from
[tgdrive/teldrive](https://github.com/tgdrive/teldrive):

- `gotd/pool.Pool` session multiplexing (originally from
  [iyear/tdl](https://github.com/iyear/tdl))
- Parallel-prefetch reader (their `tgMultiReader`)
- Multi-bot round-robin with per-op counters
- Retry / recovery middleware list of transient error strings
- 100-batch delete grouping
- Upload size-verification round-trip

## License

This repo doesn't currently declare a license. Treat as
all-rights-reserved unless one is added.
