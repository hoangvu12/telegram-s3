# Phase 4 — Handoff (drain in progress, pass-2 dormant by policy)

Self-contained resume plan for the Phase 4 MTProto migration. A
fresh Claude Code session in this working directory can finish from
here without prior chat context.

**Read in this order:**

1. This file (orientation, current state, scenarios).
2. `PHASES.md` (full migration roadmap, design decisions).
3. `HANDOFF.md` (general deploy context, Gokapi UI, send bucket).

Auto-memory entries `mtproto-migration-roadmap`,
`bot-delete-old-messages-blocked`, `phase4-test-strategy`,
`phase-commit-shape`, `easypanel-skill-limits`,
`reader-diverges-from-teldrive` are loaded automatically into every
session and capture cross-session preferences.

---

## TL;DR — state as of 2026-05-26 ~21:00 UTC

- **Code:** master HEAD is `f7b804c` (Phase 4 cleanup: remove
  admin-rights diagnostic; document grace=999h policy). Recent
  commits:
  - `f7b804c` — Phase 4: remove admin-rights diagnostic
  - `3115374` — Phase 4 diagnostic: log bot's AdminRights at boot (now
    removed in f7b804c; existed only to confirm `delete_messages=true`)
  - `ca6807e` — Phase 4 fix: pass-2 reap via MTProto (Bot API 48h
    limit) — see "Pass-2 zombie policy" below for why this didn't
    actually solve the underlying issue
  - `98739d8` — Phase 4 hardening: teldrive-parity retry/recovery
    middleware + upload size verification
  - `738b50c` — Drain-snapshot logging
  - `47ef4f6` — Per-bot MTProto session pool
  - `fc5347c` — Phase 4 core: MTProto backend + dispatcher + sweeper
- **Production:** running `f7b804c` at `s3.nguyenvu.dev` in
  `TELEGRAM_TRANSPORT=dual` with `MIGRATION_RATE=10000` and
  `BOT_DELETE_GRACE=999h`. Sweeper actively swapping
  `transport='bot'` rows to `transport='mtproto'`. Pass-2 is dormant
  by design (see policy below).
- **Drain progress:** at ~21:00 UTC, `bot_chunks_remaining=1712`,
  `pending_deletes=142+ (growing harmlessly)`. With rate=10000 the
  per-tick budget is ~6 rows / 60s, so **drain ETA is ~02:00 UTC**
  (~09:00 Vietnam time, 2026-05-27).
- **Only mandatory pending action:** flip
  `TELEGRAM_TRANSPORT=mtproto` once `bot_chunks_remaining=0`. See
  Scenario B below.

---

## Pass-2 zombie policy (the load-bearing finding of 2026-05-26)

**TL;DR:** Telegram refuses to let bots delete sufficiently-old
messages, regardless of `can_delete_messages` admin rights. We
accept that and leave the old bot messages in the channel as
inaccessible-by-S3 zombies. The chunk map points to the swapped
mtproto copies; reads are unaffected.

**What we learned:**

- Bot HTTP API `deleteMessage` rejects messages >48h old with
  `"Bad Request: message can't be deleted"`. Documented limit.
- MTProto `channels.deleteMessages` also rejects old bot messages
  with `MESSAGE_DELETE_FORBIDDEN` (rpc error code 403) — surprise.
- Verified via `channels.getParticipant` on 2026-05-26 that the bot
  HAS `AdminRights.DeleteMessages=true` server-side AND the Telegram
  UI shows the toggle on. The block is content-level, not
  permission-level. No admin permission exists that overrides it for
  bot identities. See `memory/bot_delete_messages_permission.md`.

**The policy:**

- `BOT_DELETE_GRACE=999h` stays in env permanently.
- Pass-2 runs every tick but finds no rows where
  `swapped_at < now - 999h` → no-op.
- `pending_deletes` count just grows monotonically with pass-1.
  It's a SQLite row count, not Telegram messages. Harmless.
- The original ~1850 bot messages stay in the channel forever as
  inaccessible-by-S3 zombies. They cost ~1850 message slots in a
  channel with effectively unlimited capacity.

**The escape hatch (not implemented):** a user-account MTProto
session (Telethon / gotd-as-user) could delete these — user accounts
have looser delete rules and can clean admin/bot messages in chats
they own. ~100 LoC + new env var + one-time human auth via Telegram
client. Worth doing only if channel clutter becomes a real problem.

---

## Verification (run first — confirm world matches this doc)

```sh
git status --short                     # expect only untracked: HANDOFF-PHASE4.md, PHASES.md, S3-COMPAT-ACCEPTANCE.md, scripts/
git log --oneline -5                   # expect f7b804c at top, then 3115374, ca6807e, 98739d8, 738b50c
go vet ./... && go build ./...         # clean
go test ./... -count=1 -timeout 180s   # all packages pass
curl -sS -o /dev/null -w '%{http_code}\n' https://s3.nguyenvu.dev/healthz  # 204
```

If anything diverges, **stop and investigate** — the world has moved
since this was written.

---

## How to check drain progress (do this first when you sit down)

```sh
# Snapshot only:
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=10 --grep="drain snapshot"

# Snapshot + any active warnings (handy for sanity):
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=10 --grep="drain snapshot|level=ERROR|panic|FATAL"
```

You'll see lines like:

```
time=...Z level=INFO msg="drain snapshot" bot_chunks_remaining=1500 pending_deletes=250 latest_swap=...Z since_latest_swap=4s
```

- `bot_chunks_remaining` is the migration target. **When it hits 0,
  fire Scenario B (flip TRANSPORT=mtproto).**
- `pending_deletes` should keep growing during the drain (pass-1
  enqueues, pass-2 dormant). After flip + drain done, this number is
  the count of zombie messages we accept and don't try to delete.
- `since_latest_swap` should usually be small (single-digit seconds).
  Large values means pass-1 stalled — see Scenario E.

---

## Tooling pointers

- **Easypanel skill:** `../easypanel-skill/`. tRPC calls via
  `node ../easypanel-skill/scripts/easypanel.mjs <procedure> <input>`.
  Procedure docs in the skill's `SKILL.md`.
  ⚠️ **Wipe-on-omit footgun:** every `update*` mutation REPLACES the
  field wholesale. Always `inspectService` first → build the full
  payload → `updateEnv`. Don't send a diff.
- **Log tail:**
  `../easypanel-skill/scripts/easypanel-logs.mjs <project> <service>
  [--duration=N] [--grep=PATTERN]` — opens `wss://<panel>/ws/serviceLogs`
  (note the `/ws/` prefix, NOT `/serviceLogs`) and prints frames.
- **Creds:** `~/.easypanel/config.json` has the panel URL + API token.

---

## What to do — by scenario

### Scenario A: "How's the drain going?" (most common)

```sh
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=10 --grep="drain snapshot"
```

If `bot_chunks_remaining` is decreasing on every tick (~6 per ~60s
at rate=10000), the sweeper is healthy. If it's stuck, see
"Recovery paths" / Scenario E.

### Scenario B: Drain hit 0 — fire the transport flip

When `bot_chunks_remaining=0`:

```sh
# 1. Read current env so we can preserve everything.
node ../easypanel-skill/scripts/easypanel.mjs services.app.inspectService projectName=telegram-s3 serviceName=api | grep '"env"'

# 2. Build a JSON file with TELEGRAM_TRANSPORT=mtproto in place of dual.
#    Keep every other line exactly (especially the bot token, app id/hash,
#    BOT_DELETE_GRACE=999h, MIGRATION_RATE — though you can also drop
#    MIGRATION_RATE since dual-only feature once flipped to mtproto).
#
#    The current env block:
#      LISTEN_ADDR=:9000
#      DATABASE_PATH=/app/data/telegram-s3.db
#      S3_ACCESS_KEY_ID=telegram-s3-local
#      S3_SECRET_ACCESS_KEY=<redacted — read from inspectService>
#      TELEGRAM_BOT_TOKEN=<redacted — read from inspectService>
#      TELEGRAM_CHAT_ID=-1003909089400
#      TELEGRAM_APP_ID=25591132
#      TELEGRAM_APP_HASH=1fe59f76ff0bd58a2e3344aaf2d3d749
#      TELEGRAM_TRANSPORT=dual          ← change to mtproto
#      MIGRATION_RATE=10000             ← can drop or set 0 once drained
#      BOT_DELETE_GRACE=999h            ← keep for safety
#
# 3. Apply + restart.
node ../easypanel-skill/scripts/easypanel.mjs services.app.updateEnv --file env.json
node ../easypanel-skill/scripts/easypanel.mjs services.app.restartService projectName=telegram-s3 serviceName=api

# 4. Confirm:
curl -sS -o /dev/null -w '%{http_code}\n' https://s3.nguyenvu.dev/healthz
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=5 --grep="server listening|mtproto bot ready"
```

The expected boot log is `"server listening ... transport=mtproto"`.

### Scenario C: Bump (or pause) MIGRATION_RATE

Same `inspectService → updateEnv → restartService` pattern. Useful
numbers:

| Rate | Effective | Per-tick budget | Drain time for 1700 chunks |
|---|---|---|---|
| 0 | pass-1 disabled (pass-2 still dormant via grace) | 0 | freeze |
| 1000 | 1000/day | 1/tick at 86s ticks | ~30h (original setting) |
| 10000 (current) | ~8640/day | 6/tick at 60s | ~5h |
| 20000 | ~13/tick at 60s | ~2.5h |
| Above 14400 | tick-interval clamp at 60s caps throughput | — | — |

Per-tick budget formula: `floor(rate × 60 / 86400)`. The 60s minimum
tick interval is what caps real throughput at ~14400/day for a
single bot. With more bots in the pool, this scales linearly.

### Scenario D: Boot fails after a restart

If `restartService` brings the service back up failing healthz / not
serving / endlessly restarting:

```sh
# Most likely cause: a bad env flip. Rollback to last-known-good values
# (probably TELEGRAM_TRANSPORT=dual + MIGRATION_RATE=10000 +
# BOT_DELETE_GRACE=999h — those are the values that were live as of
# this doc).
node ../easypanel-skill/scripts/easypanel.mjs services.app.updateEnv --file env.json
node ../easypanel-skill/scripts/easypanel.mjs services.app.restartService projectName=telegram-s3 serviceName=api
```

If the binary itself is bad (a new commit broke boot), redeploy the
prior commit:

```sh
git push origin <last-good-sha>:master --force-with-lease   # ONLY with user approval
node ../easypanel-skill/scripts/easypanel.mjs services.app.deployService projectName=telegram-s3 serviceName=api
```

Force-push is destructive — always confirm with the user before
running it.

### Scenario E: Sweeper stuck (pass-1 swap count not moving)

Tail logs at WARN level:

```sh
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=30 --grep="WARN|ERROR|FLOOD"
```

Common causes:

- **FLOOD_WAIT_n** repeatedly logged — Telegram is rate-limiting.
  Lower MIGRATION_RATE. The new `floodwait` middleware should absorb
  these silently, but if you see them at WARN it means they're
  hitting middleware-bypassed RPCs (rare).
- **`migrate pass-1 chunk failed`** with a specific bucket/key — a
  particular chunk's bot message is missing/inaccessible. Pass-1
  will keep retrying it on every tick. Either let it ride (it doesn't
  block other chunks) or manually fix the row via SQLite.
- **`mtproto verify ... size mismatch`** — the upload-verification
  path added in `98739d8` caught a short/zero-byte document. Pass-1
  deletes the bad mtproto msg and leaves the row at `transport='bot'`
  for next tick. Watch for repeated occurrences on the same key.
- **`pass-2 reap failed ... MESSAGE_DELETE_FORBIDDEN`** — if you see
  this, `BOT_DELETE_GRACE` was set to something low. Bump it back to
  `999h`. This is the policy, see "Pass-2 zombie policy" above.

---

## Items status (original handoff list — historic record)

| # | Description | Status |
|---|---|---|
| 1 | Set `TELEGRAM_APP_ID` + `_HASH` in Easypanel | **DONE** 2026-05-25 |
| 2 | Deploy Phase 4 binary in `TRANSPORT=bot` | **DONE** 2026-05-25 |
| 3 | Flip `TRANSPORT=dual`, smoke-test boot | **DONE** 2026-05-25 |
| 4 | Monitor sweeper drain | **DONE** — drain-snapshot logging at `738b50c` |
| 5 | Flip `TRANSPORT=mtproto` once drained | **PENDING — fires when bot_chunks_remaining=0.** See Scenario B. |
| 6 | Session pool (teldrive's `pool.Pool`) | **DONE** — `47ef4f6` |
| 7 | `retry` middleware | **DONE differently** — `98739d8` shipped a hand-rolled retry list (`Timedout`, `RPC_CALL_FAIL`, etc.), NOT generic `gotd/contrib/retry.New(10)` |
| 8 | CDN redirect path | **NOT NEEDED** — teldrive doesn't implement either; our loud-fail is upstream parity |
| 9 | `golang.org/x/net/proxy` dep | OPTIONAL. Skip unless proxy needed. |
| 10 | `recovery` middleware | **DONE** — `98739d8` |
| 11 | Upload size verification | **DONE** — `98739d8` |
| 12 | Pass-2 reap via MTProto (was: via Bot API) | **DONE** — `ca6807e`, but pass-2 is dormant by policy anyway (see zombie section above) |

---

## Smoke-test gap (worth doing if time permits)

Nobody has directly verified a fresh client write goes via MTProto.
Healthz proves the gateway booted; migration logs prove the sweeper
works on existing data; but a fresh user PUT → GET round trip hasn't
been confirmed end-to-end against the deployed binary. To close it:

```sh
# Using awscli with creds from the Easypanel env block:
aws --endpoint-url=https://s3.nguyenvu.dev \
    s3 cp ./some-10MiB-file.bin s3://test-bucket/test-mtproto

# Tail to confirm the chunk landed as transport=mtproto:
node ../easypanel-skill/scripts/easypanel-logs.mjs telegram-s3 api --duration=5 --grep="bucket=test-bucket"

# Round-trip:
aws --endpoint-url=https://s3.nguyenvu.dev \
    s3 cp s3://test-bucket/test-mtproto ./roundtripped.bin
diff ./some-10MiB-file.bin ./roundtripped.bin
```

The S3 creds are in the production env block: `S3_ACCESS_KEY_ID` and
`S3_SECRET_ACCESS_KEY`. `inspectService` exposes both.

---

## Suboptimal but tolerated (not blocking item 5)

- **Single bot.** Env still uses singular `TELEGRAM_BOT_TOKEN`
  (legacy). Config soft-falls-back to a 1-element pool. Adding a
  second token via plural `TELEGRAM_BOT_TOKENS=tok1,tok2,...` would
  let the multi-bot fan-out from Phase 3 actually do something.
  Doesn't matter today; flag for scaling.
- **Zombie messages in the channel.** ~1850 bot-uploaded messages
  cannot be deleted (Telegram-side restriction on bot delete of old
  messages). Accepted policy. See "Pass-2 zombie policy" above.

---

## Critical "do not regress" constraints

These are the invariants earlier phases pinned.

1. **`storage.Backend` takes `ChunkRef`.** Never reintroduce bare
   `fileID string` parameters.
2. **`Chunk.Transport == ""` is implicitly `"bot"`.** Dispatcher's
   `pick()` and `Chunk.Ref()` normalize.
3. **All reap paths use `backend.DeleteBatch`.** No per-chunk
   `Delete` loops at call sites.
4. **Object GETs go through `internal/reader.Reader`.**
5. **`Reader.Prime()` runs before `writeHeaders()`.** This is the
   502-vs-truncated-200 invariant.
6. **Chunk map is the SOLE source of truth on read/delete.** No
   pre-Phase-3 fallback branches.
7. **Sweeper grace-delete is decoupled from row swap.** PHASES.md
   decision #14. Never collapse pass-1 + pass-2 into a single tx.
8. **Schema changes are additive only.** `ensureColumn` helper.
9. **`TELEGRAM_TRANSPORT=bot` must remain a true no-op** that boots
   without `TELEGRAM_APP_ID` / `TELEGRAM_APP_HASH`.
10. **Gokapi `send` bucket reads must keep working through every
    transition.** Hardest constraint in the project.
11. **Middleware chain order matters** (`98739d8`): `floodwait → recovery →
    retry → ratelimit`. Floodwait outermost so FLOOD_WAIT sleeps
    don't burn retry budget. Don't reorder.
12. **Pass-2 reap goes through MTProto** (`ca6807e`), not Bot API.
    Even with grace=999h dormant, if grace is ever lowered, the
    MTProto path is strictly better (cleaner error, batched 100/call).

---

## Common operations cookbook

```sh
# --- Local dev ---

# Run the test suite (Windows host has no cgo, so no -race; CI does that):
go test ./... -count=1 -timeout 180s

# Run a single package's tests verbose:
go test ./internal/migrate/... -v -count=1

# Build + run locally with bot-only mode (no MTProto creds needed):
go build -o telegram-s3.exe ./cmd/telegram-s3
TELEGRAM_TRANSPORT=bot ./telegram-s3.exe

# --- Easypanel ops ---

# List recent actions on telegram-s3/api:
node ../easypanel-skill/scripts/easypanel.mjs actions.listActions '{}' \
  | node -e "let d='';process.stdin.on('data',c=>d+=c);process.stdin.on('end',()=>{const j=JSON.parse(d.split('\n').slice(2).join('\n'));for(const a of j.filter(x=>x.projectName==='telegram-s3'&&x.serviceName==='api').slice(0,5))console.log(a.createdAt,a.type,a.status)})"

# Service stats (CPU/RAM/network):
node ../easypanel-skill/scripts/easypanel.mjs monitor.getServiceStats \
  '{"projectName":"telegram-s3","serviceName":"api","serviceType":"app"}'

# Trigger a deploy (rebuild from master):
node ../easypanel-skill/scripts/easypanel.mjs services.app.deployService \
  projectName=telegram-s3 serviceName=api

# Restart (no rebuild):
node ../easypanel-skill/scripts/easypanel.mjs services.app.restartService \
  projectName=telegram-s3 serviceName=api

# Read env (for env-update safety check):
node ../easypanel-skill/scripts/easypanel.mjs services.app.inspectService \
  projectName=telegram-s3 serviceName=api | node -e "let d='';process.stdin.on('data',c=>d+=c);process.stdin.on('end',()=>{const lines=d.split('\n');const idx=lines.findIndex(l=>l.startsWith('{'));const j=JSON.parse(lines.slice(idx).join('\n'));console.log(j.env);})"

# --- Container DB (only when log tail isn't enough) ---

# Inspect SQLite via panel terminal UI or SSH→docker exec. DB at
# /app/data/telegram-s3.db. Example queries:
#   SELECT transport, COUNT(*) FROM object_chunks GROUP BY transport;
#   SELECT COUNT(*) FROM bot_chunks_pending_delete;
#   SELECT MAX(swapped_at) FROM bot_chunks_pending_delete;
```

---

## When you're done

Once `bot_chunks_remaining=0` AND `TELEGRAM_TRANSPORT=mtproto` is in
the env block, this handoff is exhausted. Two final tidy-ups:

1. Update `PHASES.md` §"Where things stand" to note migration done.
2. Delete this `HANDOFF-PHASE4.md` (it's untracked anyway).
3. Optional cleanup: drop the `BotStorage` code entirely — once
   transport=mtproto is permanent and there are no
   `transport='bot'` rows, the BotStorage backend is dead code. This
   is the proper "Phase 5" if you want to take it.

If you only made partial progress (e.g. bumped rate, drain not done
yet), just update the TL;DR block at the top of this file so the
next session sees an accurate picture.
