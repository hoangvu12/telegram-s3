# Telegram‑S3 — S3 Compatibility Plan

> Self‑contained engineering plan. You do **not** need any prior chat context to
> execute this. It states the system, the diagnosis, a verified protocol
> reference (so you don't have to re‑research the wire formats), and a
> dependency‑ordered work plan with acceptance criteria.
>
> Companion doc: `HANDOFF.md` (deployment/ops). This doc is the engineering roadmap.
> Last researched/verified: 2026‑05‑19.

---

## 1. What this system is

An S3‑compatible object storage gateway written in Go, backed by Telegram (a
Telegram bot uploads each object as a `document` to a chat; object metadata lives
in SQLite). A Gokapi share UI sits in front of it in production.

- Repo: `https://github.com/hoangvu12/telegram-s3` (public, branch `master`)
- Local: `C:\Users\HP MEDIA\Desktop\nguyenvu\telegram-s3`
- Live gateway: `https://s3.nguyenvu.dev` (`/healthz` → 204), internal port 9000
- No GitHub issues/PRs exist; all planning lives in repo markdown.

### Source map (all ~500 LOC of Go)

| File | Responsibility |
|---|---|
| `cmd/telegram-s3/main.go` | wiring, HTTP server, graceful shutdown |
| `internal/config/config.go` | env config (`Config` struct); has unused `TempDir`, `PublicEndpointURL` |
| `internal/s3api/handler.go` | **all** routing, SigV4, S3 verbs, XML responses |
| `internal/storage/storage.go` | `Backend` interface: `Upload`, `Download` only |
| `internal/storage/telegram/bot.go` | Telegram Bot API backend |
| `internal/metadata/store.go` | SQLite: buckets + objects tables, CRUD |

### What currently works (and the exact routing)

`handler.go:ServeHTTP` (around `handler.go:37`) dispatches by method + whether
`bucket`/`key` are empty:

- `GET /` → ListBuckets
- `PUT /{bucket}` → CreateBucket; `HEAD` → HeadBucket; `DELETE` → DeleteBucket
- `PUT /{bucket}/{key}` → PutObject (single, **whole body buffered in RAM**)
- `GET /{bucket}/{key}` → GetObject; `HEAD` → HeadObject; `DELETE` → DeleteObject
- `GET /{bucket}` (any other) → ListObjects **v1**
- everything else → `501 NotImplemented` (`handler.go:71`)

Key behaviors that matter:

- **Reads are public/unauthenticated by design**: any `GET`/`HEAD` on
  bucket+key skips the SigV4 gate (`handler.go:44`). *Keep this* — signed reads
  still pass (signature is simply ignored), so it does not hurt client
  compatibility; it is only a data‑exposure property to stay aware of.
- Object delete is **soft** (`store.go:166` sets `deleted_at`); the Telegram
  message is never deleted, so storage is never reclaimed.
- `Backend` interface has no `Delete`, no range, no multipart.

---

## 2. The core diagnosis — why it fails with "most S3 stuff"

The gateway speaks a ~2015 dialect of S3. Every modern client (AWS CLI v2,
boto3/botocore, aws‑sdk‑go‑v2 incl. its transfer manager, rclone, restic)
speaks a 2024+ dialect **by default**. The single most damaging gap:

### 2.1 KEYSTONE BUG: `aws-chunked` streaming uploads are stored corrupted

**Verified (Context7, AWS SDK for Go v2 developer guide,
`s3-checksums.html`):** the S3 module **v1.74.1+ automatically calculates and
sends a CRC32 checksum even when the caller specifies no algorithm**. Default
is `request_checksum_calculation = when_supported`. botocore (AWS CLI v2 /
boto3) shipped the same default in late 2024.

For any streaming / non‑seekable body — which is what `aws s3 cp`, the
s3manager transfer manager, rclone, and most SDK `PutObject` calls produce —
the client therefore frames the body as `Content-Encoding: aws-chunked` with a
trailing checksum. The current `putObject` (`handler.go:135`) does
`io.TeeReader(r.Body, hasher)` and streams the **raw framed bytes** straight
into Telegram. Result: every object uploaded by a modern client is stored as
*chunk framing + data + trailer*, with a wrong MD5 ETag and wrong size.

**Why Gokapi works anyway:** Gokapi uses `aws-sdk-go` **v1**, which over HTTPS
signs `PutObject` as `UNSIGNED-PAYLOAD` with **no** chunk wrapper and **no**
CRC32 trailer — the one shape this gateway happens to handle. That is
survivorship bias, not compatibility. Until §Phase 1 is done, every other
improvement is built on corrupted uploads.

### 2.2 SigV4 correctness gaps

- `canonicalQuery` (`handler.go:337`) uses Go's `url.Values.Encode()`. AWS
  SigV4 requires its own URI encoding (space = `%20` not `+`; empty value still
  gets `=`; sort by **encoded** key). **AWS's own docs explicitly warn**: "The
  standard UriEncode functions provided by your development platform may not
  work … We recommend that you write your own custom UriEncode function."
  Latent today only because Gokapi keys are SHA1 hex (no special chars); any
  key/query with a space or special char → `SignatureDoesNotMatch`.
- `X-Amz-Expires` is never enforced (`authorizedPresigned`, `handler.go:285`):
  presigned URLs never expire. Correctness + security bug. Verified bounds:
  min `1`, max `604800` (7 days).
- No clock‑skew/date validation; `X-Amz-Content-Sha256` accepted but never
  verified against the body.

### 2.3 Missing operations, ranked by real‑world breakage

| Rank | Operation | Without it |
|---|---|---|
| 1 | **Multipart upload** (`POST ?uploads`, `PUT ?partNumber&uploadId`, `POST ?uploadId`, `DELETE ?uploadId`, `GET ?uploadId`) | `aws s3 cp` >8 MB, s3manager (default 5 MiB parts), rclone, restic → all `501`. This *is* the `GOKAPI_MAX_FILESIZE=5` ceiling. |
| 2 | **Range GET** (`handler.go:176` hard‑`501`s it) | video/audio streaming, restic, s3fs‑fuse, rclone VFS, resume in Cyberduck/Transmit, browser `<video>` seeking. |
| 3 | **ListObjectsV2 + `delimiter`/`CommonPrefixes` + real pagination** | `IsTruncated` hard‑coded `false` (`handler.go:239`), listings silently cap at 1000 → **silent data loss** in any paginating client; no `delimiter` → no folder browsing in rclone/s3fs/Cyberduck/web consoles. |
| 4 | **DeleteObjects bulk** (`POST /{bucket}?delete`) | `aws s3 rm --recursive`, rclone purge, restic prune → `501` (POST is unrouted). |
| 5 | **CopyObject** (`PUT` + `x-amz-copy-source`) and `UploadPartCopy` | `aws s3 cp` same‑endpoint, rclone server‑side move/rename. |
| 6 | **Bucket subresource probes** (`?location`, `?versioning`, `?acl`, `?cors`, `?tagging`, `?policy`, …) | routing bug: every `GET /{bucket}?<subresource>` falls through to `listObjects` and returns a bogus `200 ListBucketResult` instead of the real doc / clean 404. Confuses rclone/Cyberduck/Veeam during probe phase. Cheap to fix with canned XML. |
| 7 | **CORS / `OPTIONS` preflight** | all browser clients. Also unlocks Gokapi Level‑3 E2E (HANDOFF.md notes it needs S3 CORS). |
| 8 | **Virtual‑hosted addressing** (`bucket.host/key`) | `parsePath` is path‑style only; SDKs not set to path‑style send `Host: bucket.s3.nguyenvu.dev` → 404. |

Note: `GetBucketLocation` is **tier 6, not a top blocker** — modern SDKs do not
call it before every op. (An earlier research pass over‑stated this; corrected
here.)

### 2.4 Correctness bugs inside operations that "work"

- `Size: r.ContentLength` (`handler.go:163`) is `-1` for chunked/streaming
  uploads → HEAD/GET later emit `Content-Length: -1`. Must come from
  `X-Amz-Decoded-Content-Length` or a byte count.
- Soft‑delete orphans Telegram data forever (`store.go:166`; `Backend` has no
  `Delete`).
- No object metadata stored: `x-amz-meta-*`, `Content-Disposition`,
  `Content-Encoding`, `Cache-Control`, `response-content-*` query overrides.
- Conditional requests (`If-None-Match`, `If-Modified-Since`, `If-Match`)
  ignored → no CDN/browser revalidation, no optimistic concurrency.
- Error XML lacks `<RequestId>`/`<Resource>`.

### 2.5 The physical ceiling — the Telegram backend

- `bot.go:34` `Upload` buffers the **entire object** into a `bytes.Buffer`
  before POSTing → memory blows up on large objects; cannot stream.
- Telegram Bot API limits: **sendDocument upload ≈ 50 MB**, **getFile download
  ≈ 20 MB** (the binding limit). These are the long‑standing public Bot API
  limits. **Verified verbatim** from Telegram docs for a self‑hosted *local Bot
  API server*: "Download files without a size limit." and "Upload files up to
  2000 MB." → a local Bot API server is the config‑only escape hatch.
- The structural escape hatch: **split one object across multiple Telegram
  messages** (a chunk map in SQLite). This single data structure
  simultaneously enables (a) objects ≫20 MB, (b) *true* `Range` reads (map a
  byte range → a subset of chunk messages, fetch only those), and (c) a natural
  home for S3 multipart parts. Multipart + Range + large‑object all converge
  here — design them together, not as separate passes.

---

## 3. Verified protocol reference (so you don't re‑research)

All of the following was pulled verbatim from primary sources on 2026‑05‑19
(AWS S3 API docs `sigv4-streaming.html`, `sigv4-streaming-trailers.html`,
`sigv4-query-string-auth.html`; Telegram Bot API docs; AWS SDK for Go v2 guide
via Context7).

### 3.1 `aws-chunked` request shape

Request headers the client sends (any of these → body is chunk‑framed):

```
Content-Encoding: aws-chunked
x-amz-decoded-content-length: <real object size in bytes>
Content-Length: <larger framed size>          # or Transfer-Encoding: chunked
x-amz-content-sha256: <one of the STREAMING-* values below>
x-amz-trailer: x-amz-checksum-crc32            # only in the *-TRAILER modes
```

`x-amz-content-sha256` values and what each means:

| Value | Body framed? | Per‑chunk signature? | Trailer? |
|---|---|---|---|
| `UNSIGNED-PAYLOAD` | **No** (raw body) | no | no |
| `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | yes | no | yes |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | yes | yes | no |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` | yes | yes | yes |

Each chunk on the wire (verbatim grammar from AWS docs):

```
string(IntHexBase(chunk-size)) + ";chunk-signature=" + signature + \r\n + chunk-data + \r\n
```

…except `STREAMING-UNSIGNED-PAYLOAD-TRAILER` chunks omit `;chunk-signature=…`
(just `<hexsize>\r\n<data>\r\n`). Final data chunk has size `0`:

```
0;chunk-signature=<sig>\r\n           # signed modes
0\r\n                                  # unsigned-trailer mode
```

Trailer chunk (only in `*-TRAILER` modes), sent **after** the 0‑byte chunk:

```
x-amz-checksum-crc32:<base64 value>\r\n
x-amz-trailer-signature:<sig>\r\n      # ONLY in the signed-trailer mode
```

**To recover real object bytes:** read `<hexsize>` (base‑16), strip the
`;chunk-signature=…` up to `\r\n`, read exactly that many data bytes, consume
the trailing `\r\n`, repeat until size `0`, then discard the trailer block.
True object length = `x-amz-decoded-content-length`.

### 3.2 Per‑chunk signature (only needed for the *signed* streaming modes)

Seed signature = the `Signature=` in the `Authorization` header (computed by
the normal SigV4 path, but with `x-amz-content-sha256` = the streaming constant
and the chunked headers in `SignedHeaders`).

Each data chunk's string‑to‑sign:

```
AWS4-HMAC-SHA256-PAYLOAD\n
<amzDate (yyyymmddThhmmssZ)>\n
<date>/<region>/s3/aws4_request\n
<previous-signature-hex>\n
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n   # SHA256("") constant
<hex SHA256(chunk-data)>
```

`chunk-signature = hex(HMAC-SHA256(signingKey, stringToSign))`, chained
(previous‑signature starts at the seed). Trailer chunk uses the constant
`AWS4-HMAC-SHA256-TRAILER` and its last line is
`hex SHA256("x-amz-checksum-crc32:<b64>\n")` (header name, colon, b64 value,
single `\n`, no spaces). Verified against AWS's official PUT‑object test
vectors (seed `4f232c43…` for the non‑trailer example).

> Practical scope decision: most clients default to the **UNSIGNED‑TRAILER**
> mode (no per‑chunk signature). Phase 1 only needs de‑framing for the unsigned
> modes; per‑chunk signature verification (the signed modes) can be a later,
> optional hardening step. The CRC32 trailer value may be validated or ignored.

### 3.3 Presigned URL rules (verified)

- Required query params: `X-Amz-Algorithm=AWS4-HMAC-SHA256`,
  `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`,
  `X-Amz-Signature`. Canonical query string must include all of them **except**
  `X-Amz-Signature`. Payload hash constant is `UNSIGNED-PAYLOAD`. `host` is
  always a signed header.
- `X-Amz-Expires`: integer seconds, min `1`, max `604800` (7 days). Expiry =
  `X-Amz-Date + X-Amz-Expires` vs. now. **Enforce this.**

### 3.4 Telegram limits (verified)

- Public Bot API: send (upload) ≈ **50 MB**, download via `getFile` ≈ **20 MB**.
- Self‑hosted local Bot API server (verbatim): downloads "without a size
  limit"; uploads "up to 2000 MB". Config‑only change
  (`api.telegram.org` → local base URL) — plumb a base‑URL config option.

### 3.5 AWS SDK defaults (verified via Context7)

- s3manager `Uploader` default part size 5 MiB (configurable via
  `u.PartSize`); switches to multipart for large/non‑seekable bodies.
- AWS CLI v2 default `multipart_threshold` 8 MiB.
- aws‑sdk‑go‑v2 S3 module **≥ v1.74.1**: auto CRC32 checksum when none
  specified (the §2.1 keystone).

---

## 4. Work plan (dependency‑ordered)

Each phase has a concrete target and an acceptance test. Phases 3–5 share one
data structure (the chunk map) — design them together.

### Phase 1 — De‑frame `aws-chunked` uploads  ★ highest leverage, do first

- In `putObject` (`handler.go:135`), before reading the body: if
  `Content-Encoding` contains `aws-chunked` **or** `x-amz-content-sha256`
  starts with `STREAMING-`, wrap `r.Body` in a de‑chunking `io.Reader` that
  yields only real object bytes (per §3.1).
- Object size = `X-Amz-Decoded-Content-Length` (fall back to counting bytes).
  Fix `Size:` (`handler.go:163`) to use it, not `r.ContentLength`.
- ETag = MD5 of the **decoded** stream (keep the `TeeReader`, but tee the
  decoded reader).
- Scope: handle the two **unsigned** modes (`UNSIGNED-PAYLOAD` passthrough,
  `STREAMING-UNSIGNED-PAYLOAD-TRAILER` de‑frame). Signed‑payload per‑chunk
  verification is optional hardening (§3.2) — for now de‑frame it too but don't
  require the chunk signatures to validate.
- **Acceptance:** `aws s3 cp ./5MB.bin s3://send/test.bin` then
  `aws s3 cp s3://send/test.bin ./out.bin` with default AWS CLI v2 →
  `sha256sum` matches the original; stored size correct; ETag is MD5 of the
  real content.

### Phase 2 — SigV4 correctness

- Replace `canonicalQuery` (`handler.go:337`) with a custom encoder: AWS
  UriEncode (unreserved `A-Za-z0-9-_.~` literal, everything else `%XX`
  uppercase; space → `%20`), empty value still emits `key=`, sort by encoded
  key. Apply the same encoder to the canonical URI path.
- Enforce `X-Amz-Expires` in `authorizedPresigned` (`handler.go:285`):
  reject if `now > X-Amz-Date + X-Amz-Expires`, or expires out of `[1,604800]`.
- Add a request‑date skew check (e.g. ±15 min) on signed (non‑public) requests.
- **Acceptance:** `aws s3 presign s3://send/test.bin --expires-in 60` → URL
  works; the same URL after 61 s → 403. A signed request with a space in the
  key (`aws s3 cp ./x s3://send/"a b.txt"`) succeeds.

### Phase 3 — Chunked Telegram storage (structural foundation)

- New SQLite table `object_chunks(bucket, key, part_seq, telegram_file_id,
  telegram_message_id, size, offset)` (migration in `store.go:migrate`).
- Extend `storage.Backend`: add streaming upload that emits multiple Telegram
  messages of ≤ N MB (N below the send limit, e.g. 18 MB to stay under the
  20 MB download limit), a ranged download (`fileID`, `offset`, `len`), and a
  `Delete(messageID)`.
- `bot.go:Upload`: stop buffering the whole body into `bytes.Buffer`; stream
  via `io.Pipe` or spool to `cfg.TempDir`, splitting into chunk messages.
- Add config for a local Bot API base URL (raises 50/20 MB → 2000 MB/∞) — see
  `config.go`; default stays `api.telegram.org`.
- Make object delete **hard**: delete Telegram messages for all chunks, then
  remove rows (replace soft‑delete at `store.go:166`).
- **Acceptance:** upload a 30 MB object via `aws s3 cp` (multipart, Phase 4) or
  a raw PUT; it round‑trips byte‑identical even though each Telegram message is
  ≤18 MB. Deleting it removes the Telegram messages.

### Phase 4 — S3 multipart upload

- Route `POST /{bucket}/{key}?uploads` (CreateMultipartUpload → return
  `<UploadId>`), `PUT …?partNumber=N&uploadId=…` (UploadPart → store part,
  return part ETag), `POST …?uploadId=…` (CompleteMultipartUpload → assemble
  in `part_seq` order), `DELETE …?uploadId=…` (Abort), and `GET …?uploadId=…`
  (ListParts). Add a `multipart_uploads`/`multipart_parts` table.
- Map S3 parts onto Phase‑3 chunk messages (a part may itself be multiple
  Telegram messages). Final object ETag = `hex(MD5(concat of part MD5s))-N`.
- Note POST is currently unrouted (`handler.go:71` → 501); add a POST arm.
- **Acceptance:** `aws s3 cp ./200MB.bin s3://send/big.bin` (forces multipart
  at CLI default 8 MiB) round‑trips byte‑identical; `aws s3api list-parts`
  works mid‑upload; abort frees Telegram messages.

### Phase 5 — Range GET

- In `getObject` (`handler.go:175`), parse `Range: bytes=a-b` /`a-`/`-b`,
  resolve against the Phase‑3 chunk map, fetch only the covering chunks, slice
  edges, respond `206` with `Content-Range` + correct `Content-Length`; honor
  `If-Range`; `416` on unsatisfiable. Remove the `501` at `handler.go:176`.
- **Acceptance:** `curl -r 1000000-2000000` on a >20 MB object returns exactly
  those bytes with `206`; restic and an HTML5 `<video>` seek work.

### Phase 6 — Listing correctness

- Implement ListObjectsV2 (`list-type=2`): `delimiter` → `<CommonPrefixes>`,
  real `IsTruncated`, `ContinuationToken`/`NextContinuationToken`,
  `StartAfter`, `KeyCount`, `MaxKeys`. Keep v1 (`marker`/`NextMarker`) too.
  Fix `store.ListObjects` to support delimiter + a real cursor; stop lying
  about truncation (`handler.go:239`).
- **Acceptance:** a bucket with 2500 keys lists fully via paginated
  `aws s3api list-objects-v2`; `--delimiter /` returns prefixes;
  `rclone lsd` shows folders.

### Phase 7 — Remaining compatibility surface

- `POST /{bucket}?delete` (DeleteObjects bulk) with `<Deleted>`/`<Error>` XML.
- CopyObject (`PUT` + `x-amz-copy-source`); UploadPartCopy.
- Bucket subresource GETs return canned "not configured" XML or proper 404
  instead of falling through to `listObjects` (fix routing).
- `OPTIONS` + CORS response headers (also unlocks Gokapi Level‑3 E2E).
- Object metadata: persist `x-amz-meta-*`, `Content-Disposition`,
  `Content-Encoding`, `Cache-Control`; honor `response-content-*` query
  overrides on GET. Conditional requests (`If-None-Match` etc.).
- Virtual‑hosted addressing: derive bucket from `Host` when it isn't the bare
  endpoint host. Add `<RequestId>` to error XML.

---

## 5. Testing strategy

- **Real clients are the spec.** Per‑phase acceptance uses default‑configured
  AWS CLI v2 (do *not* disable checksums — the defaults are the point) plus
  rclone and restic where relevant. Point them at the live gateway bucket
  `send` or a scratch bucket.
- Add Go unit tests for the de‑framer (use AWS's published test vectors:
  non‑trailer seed `4f232c43…`, trailer seed `106e2a8a…`,
  SHA256("")=`e3b0c442…`) and for the custom UriEncode/canonical query.
- Regression guard: Gokapi (aws‑sdk‑go v1, path‑style, UNSIGNED‑PAYLOAD) must
  keep working through every phase — it exercises the non‑chunked path.
- `.env` is gitignored — never commit it. Easypanel `update*` mutations replace
  whole fields; inspect then send the full payload (see HANDOFF.md ops notes).

## 6. Open design decisions (resolve before Phase 3)

1. **Chunk size:** ≤18 MB keeps public‑Bot‑API download working (20 MB limit).
   If standardizing on a local Bot API server, chunks can be far larger
   (fewer messages, less metadata). Decide whether local server is a hard
   requirement or an optimization.
2. **Streaming vs. spooling uploads:** `io.Pipe` (true streaming, lower disk,
   trickier error handling) vs. spool to `cfg.TempDir` (simpler, needs disk ≥
   largest object). Spooling is the lower‑risk first cut.
3. **Signed streaming payloads (§3.2):** verify per‑chunk signatures, or just
   de‑frame and rely on TLS + the public‑read model? De‑frame‑only is
   acceptable given reads are already public by design; revisit if write‑path
   auth hardening becomes a goal.
4. **SQLite concurrency:** `store.go` pins `SetMaxOpenConns(1)`. Multipart +
   chunk maps add write pressure; consider WAL mode / a small pool before
   Phase 4.
