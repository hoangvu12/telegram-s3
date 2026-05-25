# Telegram S3 — Handoff

S3-compatible object gateway backed by Telegram (Go), plus a Gokapi
quick-share UI in front of it. Both live and working as of 2026-05-19.

- Repo: https://github.com/hoangvu12/telegram-s3 (public, branch `master`)
- Local: `C:\Users\HP MEDIA\Desktop\nguyenvu\telegram-s3`

## Deployments

**telegram-s3 gateway** — Easypanel project `telegram-s3`, service `api`
- `https://s3.nguyenvu.dev` (health: `/healthz` → 204), internal port 9000
- Dockerfile from GitHub; volume `data -> /app/data`; SQLite at `/app/data/telegram-s3.db`

**Gokapi share UI** — Easypanel project `send`, service `app`
- `https://send.nguyenvu.dev`, image `docker.io/f0rc3/gokapi:latest`, internal port 53842
- Volumes: `send-data -> /app/data`, `send-config -> /app/config` (config.json persists here)
- Bucket `send` on the gateway holds Gokapi objects (keyed by SHA1)
- Env:
  - `GOKAPI_AWS_BUCKET=send`
  - `GOKAPI_AWS_REGION=us-east-1` (value irrelevant; gateway recomputes SigV4 from credential scope)
  - `GOKAPI_AWS_KEY=telegram-s3-local`
  - `GOKAPI_AWS_KEY_SECRET=<same as gateway S3_SECRET_ACCESS_KEY>`
  - `GOKAPI_AWS_ENDPOINT=https://s3.nguyenvu.dev`
  - `GOKAPI_AWS_PROXY_DOWNLOAD=true`
  - `GOKAPI_USE_CLOUDFLARE=true`
  - `GOKAPI_DISABLE_CORS_CHECK=true` (gateway has no GetBucketCors)
  - `GOKAPI_MAX_FILESIZE=5`
  - `GOKAPI_PORT=53842`, `GOKAPI_DATA_DIR=/app/data`, `GOKAPI_CONFIG_DIR=/app/config`, `TZ=UTC`
- Wizard gotchas (already set correctly — keep them): Use SSL = **No**
  (TLS is at Cloudflare/Traefik; enabling Gokapi SSL breaks the proxy);
  storage = Cloud/S3 + Proxy downloads; encryption = **Level 0**
  (never Level 3 E2E — needs S3 CORS; never "Key on startup" — breaks
  Docker auto-start).

## Gateway code + behavior that matters

- `internal/s3api/handler.go` — routing, SigV4 gate, public reads, cache headers
- `internal/storage/telegram/bot.go` — Telegram Bot API backend
- `internal/metadata/store.go` — SQLite metadata
- Any `GET`/`HEAD` on bucket+key is **public, unauthenticated** (`handler.go:44`),
  signature ignored on reads. The Gokapi SHA1 key is the de-facto secret.
- Not implemented (returns `501`): S3 multipart upload, `Range` reads,
  CopyObject. Upload buffers whole file in memory.

## Next work — S3 compatibility (the 5 MB cap)

The focus from here is extending the gateway's S3 implementation.

First target: **S3 multipart upload** in `internal/s3api/handler.go`. Why
it matters: AWS SDK `s3manager` uses a single `PutObject` only for ≤5 MB;
larger uploads switch to multipart, which the gateway currently `501`s, so
`GOKAPI_MAX_FILESIZE=5` is the working ceiling. After multipart works,
add `Range` reads and CopyObject, then raise `GOKAPI_MAX_FILESIZE`. Next
ceiling after that is Telegram's ~20 MB Bot API download limit (beyond it
needs a self-hosted local Bot API server and/or splitting one object
across multiple Telegram messages).

## Phase 3 (multi-bot pool)

- `TELEGRAM_BOT_TOKENS` is now the required comma-separated list of bot
  tokens. New uploads round-robin across the pool; each chunk's
  `bot_index` is persisted so reads route back through the same bot
  (Bot API `file_id` is bot-bound). Put the legacy token first so
  backfilled rows (`bot_index=0`) keep resolving.
- `TELEGRAM_BOT_TOKEN` (singular) is kept as a soft fallback: if
  `TELEGRAM_BOT_TOKENS` is unset and the singular is set, the gateway
  boots in single-bot mode. Preserved primarily as a rollback target —
  record the production value before a Phase 3 deploy so a binary revert
  has the token to restore.
- Migration is automatic at startup: legacy single-message objects (no
  `object_chunks` row) get a one-row entry inserted as
  `(seq=0, offset=0, transport='bot', bot_index=0)`. The legacy fallback
  branches in `(*Handler).planRead` / `deleteOneObject` /
  `reapSupersededChunks` are removed; the chunk map is now the sole
  source of truth on every read/delete path.

## Ops notes

- Easypanel host `138.2.82.133`. Helper:
  `node C:\Users\HP MEDIA\Desktop\nguyenvu\easypanel-skill\scripts\easypanel.mjs ...`
- Easypanel `update*` mutations replace whole fields — inspect first, send full payload.
- `.env` is gitignored — do not commit.
