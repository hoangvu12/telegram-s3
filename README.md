# telegram-s3

S3-compatible gateway backed by Telegram. Objects are chunked and
stored as documents in a Telegram channel; reads stream the chunks
back through the S3 API. Storage is MTProto-only (via `gotd/td`); the
Bot HTTP API path was removed in Phase 5 after the migration drained.

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
- Static AWS SigV4 credentials
- SQLite metadata with WAL writer/reader split
- Per-bot session multiplexing (gotd connection pool)
- Multi-bot round-robin for both uploads and reads
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

1. Visit <https://my.telegram.org/auth> and log in with your phone
   number.
2. → "API development tools" → fill in a basic app description.
3. Note the **App api_id** (numeric) and **App api_hash** (hex).
   These are different from the bot token.

### 3. Create the storage channel

1. In Telegram, create a **private supergroup or channel**.
2. Add your bot as an **administrator** with these rights:
   - **Post Messages** — required, otherwise the bot can't upload
   - **Delete Messages** — recommended
   - Other rights are optional; the bot doesn't use them.
3. Get the channel's chat_id. Easiest way: post a message in the
   channel, forward it to `@RawDataBot`, copy `forward_from_chat.id`.
   It will look like `-1001234567890` (the `-100` prefix means
   supergroup/channel).

### 4. Configure & run

```bash
cp .env.example .env
# Edit .env — at minimum set TELEGRAM_BOT_TOKENS, TELEGRAM_CHAT_ID,
# TELEGRAM_APP_ID, TELEGRAM_APP_HASH, S3_ACCESS_KEY_ID,
# S3_SECRET_ACCESS_KEY.

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

## Configuration reference

The full env surface is documented inline in `.env.example`. Highlights:

- **Tokens:** `TELEGRAM_BOT_TOKENS=tok1,tok2,...` (comma-separated)
  enables multi-bot round-robin for both uploads and reads. Each bot
  authenticates its own MTProto session.
- **MTProto:** `TELEGRAM_APP_ID`, `TELEGRAM_APP_HASH`,
  `TELEGRAM_POOL_SIZE`, `TELEGRAM_UPLOAD_THREADS`.
- **Tuning:** `HTTP_MAX_IDLE_CONNS_PER_HOST`, `SQLITE_READER_CONNS`,
  `STREAM_CONCURRENCY`, `STREAM_BUFFERS`, `STREAM_CHUNK_SIZE`,
  `CHUNK_TIMEOUT`, `LOCATION_CACHE_TTL`, `TELEGRAM_MAX_CHUNK_SIZE`
  — all have safe defaults; tweak only if profiling says so.

## Known limitations

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
            ┌──────────────────────────┐
            │ mtproto.Storage (gotd/td)│
            └────────────┬─────────────┘
                         │
                         ▼
                Telegram channel
                (single supergroup)
```

- **`internal/s3api`** — S3 API surface (SigV4 auth, request
  parsing, response shapes). Calls into the storage layer through
  the `Backend` interface.
- **`internal/reader`** — parallel-prefetch reader. N concurrent
  chunk fetches, ordered delivery.
- **`internal/storage`** — `Backend` interface.
- **`internal/storage/telegram/mtproto`** — MTProto backend via
  `gotd/td`. Includes session pool for multiplexing per-bot RPCs.
- **`internal/metadata`** — SQLite metadata (objects, chunks,
  multipart parts, MTProto session storage).
- **`internal/cache`** — generic TTL cache for
  `(message_id, bot_index → InputDocumentFileLocation)`.

## Development

```bash
# Run the full test suite (Windows host can skip -race; CI runs with it):
go test ./... -count=1 -timeout 180s

# Build a local binary:
go build -o telegram-s3 ./cmd/telegram-s3
```

Tests are mocks-only against `*tg.Client` for the MTProto layer; no
live Telegram needed.

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
