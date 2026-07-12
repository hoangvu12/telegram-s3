# Telegram‑S3 — Live Acceptance Runbook (self‑contained)

> **You do not need any prior chat context.** This is the executable plan for
> the **remaining live acceptance** of the S3 gateway. Phases 1–7 are
> implemented, unit‑verified, committed and pushed (commit
> `193a962 Implement S3 compatibility Phases 1-7`, branch `master`) and
> **deployed** at `https://s3.nguyenvu.dev`.
>
> Companion docs in‑repo: `S3-COMPAT-PROGRESS.md` (state of all phases),
> `S3-COMPAT-PHASE7-PLAN.md` (what Phase 7 did), `HANDOFF.md` (deploy/ops).

---

## A. What is already verified (do not redo)

- **Unit:** every phase, full suite green (`go test ./...`).
- **Local live, real Telegram:** `go test -tags s3live -run Live` —
  `TestLiveChunkedRoundTrip`, `TestLiveUnsignedPayloadRoundTrip`,
  `TestLivePhase7/*` (subresource, DeleteObjects, CopyObject byte‑identity,
  metadata+`If-None-Match`→304, vhost, request‑id) **all pass**.
- **Production (read‑only, no creds):** `GET /healthz`→204; `OPTIONS /`→200
  with `Access-Control-Allow-Origin: *` + `x-amz-request-id`;
  `GET /send/<missing>`→404 with `<RequestId>` in the body. P7.3 + P7.6
  confirmed live in prod.

## B. What is still owed (this runbook)

The plan's §5 says **real default clients are the spec**. Owed:

1. `aws` CLI v2 acceptance against a **throwaway bucket** (never `send`).
2. `rclone` + `restic` round‑trips (optional but recommended).
3. **Scale:** a `>18 MiB` multi‑chunk object; a `≥2500`‑key paginated list.
4. **Gokapi Level‑3 E2E** (manual — UI driven; the CORS payoff).

---

## C. Prerequisites

- `aws` CLI v2 installed (`aws --version`). `rclone`, `restic`, `curl`,
  `python3` optional — the script skips any missing tool.
- The **production S3 secret** goes in `.env` (git-ignored — never committed).
  Add these two lines to `C:\Users\HP MEDIA\Desktop\nguyenvu\telegram-s3\.env`:

  ```
  S3_ACCESS_KEY_ID=telegram-s3-local
  S3_SECRET_ACCESS_KEY=<the telegram-s3/api S3_SECRET_ACCESS_KEY from Easypanel>
  ```

  `telegram-s3-local` is the access key id (HANDOFF.md). The secret is the
  Easypanel env var on the `telegram-s3`/`api` service — inspect it with
  `node "C:\Users\HP MEDIA\Desktop\nguyenvu\easypanel-skill\scripts\easypanel.mjs" ...`
  or get it from whoever deployed. The script auto‑sources `.env` and maps
  `S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY` → its `S3_ACCESS_KEY`/`S3_SECRET_KEY`.

> **Safety:** the script hard‑refuses to run if the target bucket is `send`
> (Gokapi’s data). It only ever touches its own throwaway bucket and deletes
> exactly what it created.

---

## D. Run it

Creds come from `.env` (see §C) — nothing secret on the command line:

```bash
cd "C:\Users\HP MEDIA\Desktop\nguyenvu\telegram-s3"

# Optional knobs (all have safe defaults; endpoint defaults to prod):
export ACCEPT_BIG_MIB=20          # >18 → forces multi-chunk (P3) + multipart (P4)
export ACCEPT_LIST_N=2500         # plan target; lower (e.g. 200) for a quick pass
export RUN_RCLONE=1 RUN_RESTIC=0  # toggle optional clients

bash scripts/acceptance.sh
```

Defaults if unset: `S3_ENDPOINT=https://s3.nguyenvu.dev`,
`TEST_BUCKET=phase7-accept` (the script hard‑refuses `send`). Override either
by `export`‑ing it before the run.

Exit code `0` = every executed check passed. Each check prints
`PASS`/`FAIL`/`SKIP`; a summary is printed at the end. The script is
idempotent — safe to re‑run; it recreates and finally deletes the bucket.

### A fresh Claude session

> Read `S3-COMPAT-ACCEPTANCE.md` and run `bash scripts/acceptance.sh` from the
> repo root. The script auto‑sources `.env` for `S3_SECRET_ACCESS_KEY` /
> `S3_ACCESS_KEY_ID` — if it exits with "set S3_SECRET_ACCESS_KEY in .env",
> ask the user for the secret (do not guess). Report the PASS/FAIL summary;
> on any FAIL capture the failing command + response body and stop. Then walk
> the manual Gokapi Level‑3 steps (§F) with the user and record the outcome
> in `S3-COMPAT-PROGRESS.md` §7.

---

## E. What each check proves

| Check | Exercises |
|---|---|
| create‑bucket / head‑bucket | bucket lifecycle |
| put → get small (default checksums) | P1 aws‑chunked de‑frame, ETag=MD5 |
| metadata: `--content-type/--content-disposition/--metadata` + `head-object` | P7.5 persist+echo |
| `get-object --response-content-disposition` | P7.5 response‑* override |
| conditional `curl -H 'If-None-Match'` → 304; `curl -H 'If-Match: bad'` → 412 | P7.5 conditionals |
| `curl -r 0-3` → 206 + Content‑Range | P5 Range GET |
| `aws s3 cp s3://b/a s3://b/b` (server‑side) | P7.4 CopyObject |
| `aws s3 cp big.bin s3://b/` (`>ACCEPT_BIG_MIB`) + download `cmp` | P4 multipart + P3 multi‑chunk + P5 ranged reassembly |
| upload `ACCEPT_LIST_N` keys + paginated `list-objects-v2` count | P6 pagination/listing at scale |
| `get-bucket-location` / `get-bucket-versioning` ok; `get-bucket-tagging` → `NoSuchTagSet` | P7.1 subresource probes |
| `aws s3 rm s3://b --recursive` | P7.2 bulk DeleteObjects |
| `rclone lsd` / `rclone copy` / `rclone purge` | real‑client (rclone) |
| `restic init/backup/snapshots/forget --prune` | real‑client (restic) |
| Gokapi Level‑3 (manual, §F) | CORS payoff, regression guard |

## F. Gokapi Level‑3 E2E (manual — cannot be scripted)

The regression guard + the CORS payoff. Do this in a browser:

1. `https://send.nguyenvu.dev` → upload a small file. It should succeed
   (path‑style / UNSIGNED‑PAYLOAD regression guard still holds post‑deploy).
2. Open the share link, download it → bytes identical.
3. If the Gokapi instance is set to **encryption Level 3 (E2E)** (HANDOFF.md
   warns it was kept at Level 0 because the old gateway had no CORS): with
   Phase 7 CORS now live you may switch it to Level 3 and repeat 1–2. The
   browser `fetch`/XHR to `s3.nguyenvu.dev` must not be CORS‑blocked.
   *(Changing Gokapi encryption level is an ops decision — confirm with the
   owner first; it is not required to accept the gateway itself.)*

## G. On failure

- A 403 on signed calls → wrong `S3_SECRET_KEY` (must equal the gateway’s
  `S3_SECRET_ACCESS_KEY` Easypanel env).
- A 502 `TelegramUploadFailed`/`TelegramDownloadFailed` → Telegram backend /
  bot‑token issue on the server, not the S3 layer.
- Multi‑chunk download mismatch → investigate `internal/storage/telegram`
  ranged `getFile` (Bot API ~20 MB cap; see `S3-COMPAT-PROGRESS.md` §2.2).
- Record results in `S3-COMPAT-PROGRESS.md` §7 (replace the "still owed"
  bullet with the date + outcome).
