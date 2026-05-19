# Telegram‑S3 — S3 Compatibility: Execution Progress

> **Self‑contained continuation doc.** You do **not** need any prior chat
> context. This records what has been *implemented* against the roadmap in
> `S3-COMPAT-PLAN.md`, the decisions made (so they are not relitigated), the
> exact working‑tree state, and the precise remaining work with acceptance
> criteria.
>
> Companion docs (still authoritative, persist in repo):
> - `S3-COMPAT-PLAN.md` — the roadmap + **§3 verified protocol reference**
>   (SigV4/aws‑chunked/Telegram wire formats) + §4 Phase 5/6/7 detail. Still
>   valid; **this doc supersedes its wording only where noted** (size handling,
>   storage architecture).
> - `HANDOFF.md` — deployment/ops (Easypanel, Gokapi front‑end, env).
>
> Status as of 2026‑05‑19: **Phases 1–7 implemented and unit‑verified.
> Nothing committed. Live data‑path smoke‑verified end‑to‑end through real
> Telegram (local gateway, `s3live` harness — see §7): Phase 1 chunked +
> UNSIGNED‑PAYLOAD round‑trips and the Phase 7 marquee ops (subresource,
> DeleteObjects, CopyObject, metadata+conditional, vhost, request‑id) all
> pass. Remaining owed: acceptance with the actual default clients
> (aws‑cli v2 / rclone / restic) + Gokapi Level‑3 E2E, and the scale checks
> (>18 MiB multi‑chunk, 2500‑key pagination).**

---

## 1. Quick verification

```
cd C:\Users\HP MEDIA\Desktop\nguyenvu\telegram-s3
gofmt -l ./internal ./cmd            # expect: no output
go build ./...                       # expect: clean
go vet ./... && go vet -tags s3live ./internal/s3api/
go test ./...                        # expect: all ok (Phases 1–4 unit tests)
```

Live acceptance (needs a running gateway + creds — see §6):

```
TELEGRAM_S3_ENDPOINT=http://localhost:9000 \
TELEGRAM_S3_ACCESS_KEY=... TELEGRAM_S3_SECRET_KEY=... TELEGRAM_S3_BUCKET=send \
TELEGRAM_S3_ITEST_SIZE=33554432 \
  go test -tags s3live -run Live -v ./internal/s3api/
```

---

## 2. Decisions made (do not relitigate)

1. **Object size = counted decoded bytes, with `X-Amz-Decoded-Content-Length`
   enforced.** Researched against MinIO (`cmd/object-handlers.go`,
   `internal/hash/reader.go`) + SeaweedFS/Perplexity consensus. The header is a
   *declared contract*; the actual decoded byte count is ground truth; a
   mismatch → **`400 IncompleteBody`** (never silently stored). This
   **supersedes `S3-COMPAT-PLAN.md` §Phase 1's literal "use the header, fall
   back to counting"** wording. Implemented in `s3api/handler.go`
   `decodeUpload` + `validateDecodedSize`.
2. **Storage architecture = Bot API multi‑message chunking** (NOT MTProto).
   Chosen over an MTProto pivot (gotd/td) deliberately: incremental, no new
   auth/session/dependency, preserves the Gokapi regression path, keeps the
   repo small. **Known trade‑off:** Bot API `getFile` is hard‑capped ~20 MB, so
   each chunk is ≤18 MiB to stay independently downloadable; ranged reads are
   approximated via chunk boundaries (not native offset reads). **Revisit
   MTProto only if** true large single‑document storage or native seekable
   reads become a hard requirement — that is a separate, larger re‑architecture
   of `internal/storage/telegram`.
3. **Upload buffering = per‑chunk bounded (`io.ReadFull` window) streamed via
   `io.Pipe`+multipart.** Peak RAM ≈ one chunk (18 MiB) regardless of object
   size; no temp disk, no whole‑object buffer. Research confirmed Telegram
   `sendDocument` accepts a streamed multipart body (no Content‑Length needed).
4. **Signed streaming payloads: de‑frame only, do not verify per‑chunk
   signatures** (Phase 1 scope; `S3-COMPAT-PLAN.md` §3.2). Acceptable because
   reads are public by design and transport is TLS.
5. **SQLite: WAL + `busy_timeout=5000`, kept `SetMaxOpenConns(1)`** (§6.4).
   Resolved before Phase 4 as planned.
6. **Schema migrations are additive only.** `buckets`/`objects` are untouched;
   `object_chunks` and the three `multipart_*` tables were *added*. Existing
   production rows (legacy single‑message Gokapi objects) keep working via a
   fallback read/delete path. This is the regression guard — preserve it.
7. **Local Bot API server is config‑only.** `config.TelegramAPIBaseURL`
   (env `TELEGRAM_API_BASE_URL`, default `https://api.telegram.org`). Pointing
   it at a self‑hosted server raises the 50/20 MB ceilings; default unchanged.

---

## 3. What is implemented (Phases 1–4)

### Phase 1 — de‑frame `aws-chunked` uploads ✅
- `internal/s3api/chunked.go`: `awsChunkedReader` (handles unsigned‑trailer
  *and* signed framing by ignoring the chunk extension), `isAWSChunked`,
  `countingReader`. Truncation → `io.ErrUnexpectedEOF` (never silent).
- `putObject` de‑frames, ETag = MD5(decoded), size = counted bytes, enforces
  `X-Amz-Decoded-Content-Length`. Gokapi `UNSIGNED-PAYLOAD` path passes through
  untouched.

### Phase 2 — SigV4 correctness ✅
- `awsURIEncode` (custom AWS UriEncode; space→`%20`, uppercase `%XX`, slash
  literal only in key path), `canonicalQuery` rewrite (explicit `=` for empty,
  sort by encoded name then value), `canonicalURI`.
- `X-Amz-Expires` enforced in `authorizedPresigned` (int in `[1,604800]`,
  reject past `X-Amz-Date+Expires`). ±15 min clock‑skew on header auth.

### Phase 3 — chunked Telegram storage ✅
- `storage.Backend` redesigned: `Upload→[]Chunk`, `Download`,
  `DownloadRange(fileID,offset,length)`, `Delete(messageID)`.
- `telegram/bot.go`: streaming chunked upload (≤`MaxChunkSize`=18 MiB,
  `chunkSize` field overridable in tests), `DownloadRange` via
  `getFile`+HTTP `Range`, `deleteMessage`, configurable base URL, best‑effort
  reap of sent chunks on mid‑upload failure.
- `metadata`: `object_chunks` table; `PutObject(obj,chunks)` transactional;
  `GetObjectChunks`; `DeleteObject` now **hard** (drops both tables).
- `getObject` reassembles chunks in seq order; **legacy single‑message
  fallback** (no chunk rows + size>0) and **empty‑object** (size 0) branches.
  `deleteObject` reaps Telegram messages then metadata.

### Phase 4 — S3 multipart upload ✅
- `internal/s3api/multipart.go`: Create/UploadPart/List/Complete/Abort.
  Object ETag = `MD5(concat of part MD5s)-N`. Parts validated for ascending
  order + ETag match. `FinalizeMultipartUpload` atomically materializes the
  assembled `object_chunks` map and drops bookkeeping.
- Tables: `multipart_uploads`, `multipart_parts`, `multipart_part_chunks`.
- Routing: a POST arm + query‑aware multipart dispatch added to `ServeHTTP`
  ahead of the generic object verbs. Phase 1 decode logic factored into shared
  `decodeUpload`/`validateDecodedSize`/`toMetaChunks` (used by `putObject` and
  `uploadPart` — they cannot diverge).

### Phase 5 — Range GET ✅
- `getObject` (`handler.go`) rewritten: the `Range → 501` guard is gone.
  `parseByteRange` resolves a single RFC 7233 range (`a-b` / `a-` / `-b`)
  against `obj.Size`; `ifRangeAllows` gates on `If-Range` (quoted/weak ETag
  *or* HTTP‑date, unparseable → full object). Unsatisfiable (incl. any range
  on a size‑0 object) → **`416` + `Content-Range: bytes */size`** with
  `InvalidRange` XML; malformed/multi‑range/wrong‑unit → ignored, full `200`
  (S3/RFC behavior); satisfiable → **`206` + `Content-Range` + ranged
  `Content-Length`**.
- The full‑object, ranged, and legacy single‑message paths now share one
  `streamSegments` helper over a `[]readSegment` (`{fileID, off, length}`;
  `length 0` == whole chunk per the `Backend` contract). Range→chunk mapping:
  skip chunks fully outside `[start,end]`, else `localOff = max(0,
  start-c.Offset)`, read to `min(c.Size-1, end-c.Offset)`. First source is
  opened before the status line so a backend failure is still `502`, not a
  truncated `206`. Legacy path issues a single `DownloadRange(fileID, start,
  len)`; the full read is every chunk at `(0,0)` (byte‑identical to the old
  `Download` path — regression guard intact).
- `headObject` now sends `Accept-Ranges: bytes` (also set on `200`/`206`/`416`)
  so restic / HTML5 `<video>` discover range support before the ranged GET.

### Phase 6 — Listing correctness ✅
- `store.ListObjects` → `store.ListObjectsPage(ListParams) ListPage`: prefix is
  now a **half‑open byte‑range scan** (`key >= prefix AND key <
  prefixUpperBound`, *not* `LIKE` — keys may contain `%`/`_`); rows stream
  lazily and stop as soon as the page is full. Real delimiter rollup +
  `IsTruncated` + a resume cursor (`NextAfter` = last underlying key
  *consumed*, incl. keys folded into a `CommonPrefix`, so resuming never
  duplicates or skips data through a rollup). SQLite default TEXT collation is
  BINARY → byte order == S3's UTF‑8 key order.
- `handler.listObjects` serves **both v1 and ListObjectsV2** (`list-type=2`):
  v1 `marker`/`NextMarker`; v2 `continuation-token`/`NextContinuationToken`
  (opaque = base64 of the resume key, so a page is a pure function of its
  token — no server cursor state), `start-after`, `KeyCount`. `delimiter` →
  `<CommonPrefixes>`. `encoding-type=url` applies AWS UriEncode to the
  key‑ish fields + echoes `<EncodingType>`. Real `IsTruncated` (the
  hard‑coded `false` is gone). `<ListBucketResult>` now carries the S3
  `xmlns`; `LastModified` is ISO8601‑millis. `deleteBucket`'s emptiness
  probe moved to `ListObjectsPage` (MaxKeys 1).

### Phase 7 — Remaining compatibility surface ✅

Per `S3-COMPAT-PHASE7-PLAN.md`, all six items implemented + unit‑verified:

- **P7.1 Bucket subresource probes** (`s3api/subresource.go`): routing arm
  before createBucket/deleteBucket/listObjects — `GET /{bucket}?<sub>`
  returns canned config XML (`?location|?versioning|?acl|?accelerate|
  ?requestPayment|?notification|?logging`) or a clean S3 404
  (`?cors→NoSuchCORSConfiguration`, `?tagging→NoSuchTagSet`, …) instead of a
  bogus `ListBucketResult`; `?uploads` is a real ListMultipartUploads from
  `store.ListMultipartUploads`. `PUT/DELETE /{bucket}?<sub>` → 200/204 no‑op.
  `BucketExists` checked first → `404 NoSuchBucket`. POST excluded so P7.2 is
  unaffected. `delete` is intentionally **not** a subresource key.
- **P7.2 DeleteObjects bulk** (`s3api/deleteobjects.go`): `POST
  /{bucket}?delete` arm before the multipart POST arms. `deleteObject` core
  extracted to `deleteOneObject` (idempotent: missing key → nil) shared by
  both paths. Parses `<Delete>` (cap 1000), honors `<Quiet>`, always
  `200 <DeleteResult>`; a missing key reports `Deleted`, not `Error`.
- **P7.3 CORS + OPTIONS** (`setCORS` in `handler.go`): `OPTIONS` answered
  before the auth gate (no creds, no routing) with `200`; `setCORS` runs for
  every response — `Access-Control-Allow-Origin: *`, methods, echoed
  `Allow-Headers` (→ `*`), `Expose-Headers`, `Max-Age: 3000`. Unlocks Gokapi
  Level‑3 E2E.
- **P7.4 CopyObject + UploadPartCopy** (`s3api/copy.go`): `parseCopySource`
  (URL‑decoded, `?versionId` stripped). Shared `openObject`/`segmentReader`
  reader over the chunk map (legacy single‑message + empty handled);
  `getObject`'s segment switch refactored into the shared `objectSegments`
  (Phase‑5 behavior byte‑identical). Copy = re‑upload through MD5;
  destination's superseded chunks reaped on overwrite. UploadPartCopy honors
  `x-amz-copy-source-range`; response is `<CopyPartResult>` XML.
  `x-amz-metadata-directive` COPY/REPLACE honored (folds into P7.5).
- **P7.5 Object metadata + conditionals** (`s3api/objectmeta.go`, schema):
  new additive `object_metadata` + `multipart_upload_metadata` tables; the
  delete+insert folds into the existing `PutObject` /
  `FinalizeMultipartUpload` txns and the delete into `DeleteObject` (mirrors
  the `object_chunks` pattern — **no `ALTER`**). Persists
  `content-disposition|content-encoding|cache-control|expires|x-amz-meta-*`;
  GET/HEAD echo them; `response-*` GET overrides applied; conditional
  `If-Match/If-Unmodified-Since/If-None-Match/If-Modified-Since` with RFC 7232
  precedence (→ 412 / bodyless 304). MPU metadata captured at create, carried
  on complete.
- **P7.6 Virtual‑hosted + RequestId** (`splitBucketKey` in `handler.go`):
  `endpointHost` parsed from `cfg.PublicEndpointURL` in `NewHandler`;
  `<bucket>.<endpointHost>` → vhost, everything else (incl. bare endpoint /
  unset) → path‑style (Gokapi byte‑identical). SigV4 unchanged
  (`canonicalHeaders` signs `r.Host`). `x-amz-request-id` set centrally for
  every response; `errorResponse` carries a matching `<RequestId>`.

---

## 4. Working‑tree state (NOTHING COMMITTED)

Last commit: `72afb08 Cache public object HEAD responses`. Branch `master`.

Modified: `cmd/telegram-s3/main.go`, `internal/config/config.go`,
`internal/metadata/store.go`, `internal/s3api/handler.go`,
`internal/storage/storage.go`, `internal/storage/telegram/bot.go`.

New (untracked): `internal/s3api/chunked.go`, `internal/s3api/multipart.go`,
`internal/s3api/subresource.go`, `internal/s3api/deleteobjects.go`,
`internal/s3api/copy.go`, `internal/s3api/objectmeta.go` (Phase 7 sources),
`internal/s3api/chunked_test.go`, `internal/s3api/sigv4_test.go`,
`internal/s3api/multipart_test.go`, `internal/s3api/range_test.go`,
`internal/s3api/list_test.go`, `internal/s3api/subresource_test.go`,
`internal/s3api/deleteobjects_test.go`, `internal/s3api/cors_test.go`,
`internal/s3api/copy_test.go`, `internal/s3api/objectmeta_test.go`,
`internal/s3api/vhost_test.go` (Phase 7 tests),
`internal/s3api/integration_test.go` + `internal/s3api/phase7_live_test.go`
(build tag `s3live`),
`internal/metadata/store_test.go`, `internal/storage/telegram/bot_test.go`,
plus `S3-COMPAT-PLAN.md`, `S3-COMPAT-PHASE7-PLAN.md`, `HANDOFF.md`, this file.

`.env` is gitignored — never commit it.

---

## 5. Test inventory (all pass)

- `s3api/chunked_test.go` — de‑framer vs AWS §3.2 vectors, unsigned/signed
  trailer, tiny‑buffer splits, corruption matrix, detection truth table.
- `s3api/sigv4_test.go` — `awsURIEncode` (incl. AWS doc example + UTF‑8),
  canonical query, canonical URI, presigned expiry, clock skew, space‑in‑key.
- `s3api/multipart_test.go` — fake `Backend` driving full create→parts→list→
  complete→reassembled GET (multipart ETag/size), abort frees messages, error
  matrix. Reusable rig (`newMPRig`, `signHeaderAuth`).
- `s3api/list_test.go` — Phase 6 on the same rig (signed GETs): v1/v2 basics,
  prefix, delimiter rollup, prefix+delimiter, v2 token + v1 marker pagination
  round‑trips, **pagination through a delimiter rollup** (one new prefix per
  page, no dup/skip), `start-after`, `encoding-type=url` (incl. raw‑vs‑encoded
  matrix), empty bucket, and the `deleteBucket` emptiness regression.
- `s3api/range_test.go` — Phase 5 on the same rig: full‑GET regression,
  satisfiable matrix (mid cross‑chunk / single byte / open‑ended / suffix /
  end‑past‑size clamp / suffix>size / explicit‑whole), 416 matrix (+`bytes
  */size`/`InvalidRange`), malformed/multi‑range→200 fallback, `If-Range`
  match vs stale, empty object, legacy single‑message ranged + full.
- `s3api/subresource_test.go` — Phase 7 P7.1: `?location/?versioning/?acl`
  200, `?tagging/?cors/?policy/…` clean 404 matrix, `NoSuchBucket`,
  PUT/DELETE no‑op, real ListMultipartUploads, and the Phase‑6 listing
  non‑regression (subresource arm only fires on a config key).
- `s3api/deleteobjects_test.go` — P7.2: subset bulk delete + survivors,
  `<Quiet>`, missing‑key‑is‑Deleted (idempotent), malformed body → 400.
- `s3api/cors_test.go` — P7.3: OPTIONS preflight (200, ACAO `*`, echoed
  Allow‑Headers, no body), ACAO on a normal GET, `*` fallback.
- `s3api/copy_test.go` — P7.4: CopyObject ETag = `md5hex(src)` + identical
  bytes, missing source → NoSuchKey, overwrite reaps superseded chunks,
  UploadPartCopy full + `x-amz-copy-source-range` then complete reassembles.
- `s3api/objectmeta_test.go` — P7.5: metadata echo on GET/HEAD, `response-*`
  overrides, conditional matrix (304/412 + precedence), MPU metadata carry,
  copy COPY vs REPLACE directive.
- `s3api/vhost_test.go` — P7.6: `<bucket>.<endpointHost>` served, path‑style
  still works, vhost disabled when endpoint unset, `x-amz-request-id` header
  == `<RequestId>` in the error body.
- `metadata/store_test.go` — chunk round‑trip, overwrite‑replaces‑map, hard
  delete, legacy compat, WAL active, idempotent delete‑missing.
- `telegram/bot_test.go` — mock Bot API: split/reassemble/delete, empty body,
  exact‑multiple boundary, cleanup‑on‑failure, ranged read.
- `s3api/integration_test.go` (`//go:build s3live`) — live single‑PUT chunked
  + UNSIGNED‑PAYLOAD round‑trip, ETag/Content‑Length asserts. Skips without env.
- `s3api/phase7_live_test.go` (`//go:build s3live`) — live Phase 7 acceptance
  against a running gateway (signs with the package's own canonical funcs so
  query subresources verify): subresource probes, DeleteObjects, CopyObject
  byte‑identity, metadata echo + `If-None-Match`→304, vhost, request‑id +
  `<RequestId>`. **Run 2026‑05‑19 against a local gateway + real Telegram:
  all pass** (`TestLiveChunkedRoundTrip`, `TestLiveUnsignedPayloadRoundTrip`,
  `TestLivePhase7/*`).

---

## 6. Known caveats / open warts

1. **Overwrite‑orphan** (Phase 3+4): `PutObject` / `FinalizeMultipartUpload`
   replace `object_chunks` transactionally, but the *superseded* object
   version's Telegram messages are not reaped (orphaned forever). Pre‑Phase‑3
   this orphaned 1 message; now N. **Cheap follow‑up:** before replacing, read
   old chunks (`GetObjectChunks`), and after a successful store `backend.Delete`
   those message IDs (they are distinct messages from the new upload, safe to
   delete).
2. **Abandoned multipart uploads never expire** — rows + Telegram messages
   persist until an explicit Abort. No lifecycle/janitor. Follow‑up: sweep
   `multipart_uploads` older than N and abort.
3. **No 5 MiB min‑part enforcement** — AWS returns `EntityTooSmall` for
   undersized non‑last parts; we accept them. Not a round‑trip correctness
   issue; add only if a client depends on the error.
4. **Malformed `aws-chunked` body → 502** (not 400) because the de‑framer error
   propagates through `backend.Upload`. No corruption stored. Tightening needs
   distinguishing the error class at the handler.
5. **Range GET ✅ done (Phase 5).** Single‑range only by design; multi‑range
   requests intentionally fall back to a full `200` (matches S3). Ranged reads
   remain chunk‑boundary slices on top of Bot‑API `getFile` + HTTP `Range`
   (decision §2.2), not native seekable reads. Still **not live‑verified**
   (see caveat 7).
6. **ListParts is under the public‑read auth bypass** (GET+bucket+key). Consistent
   with the gateway's "reads are public" design; signed requests still pass.
7. **Live acceptance not run** — Phases 1–6 acceptance needs a running gateway.
   The `s3live` harness covers single‑PUT; set `TELEGRAM_S3_ITEST_SIZE`
   >18 MiB to force multi‑chunk (Phase 3). **Multipart live check is manual**
   (`aws s3 cp ./200MB.bin s3://send/big.bin`, `aws s3api list-parts`) or
   extend the harness with a multipart flow. Phase 6 owes the 2500‑key
   paginated `aws s3api list-objects-v2` / `rclone lsd` live pass.
8. **Listing deviations (minor, intentional):** `max-keys=0` is treated as the
   default 1000 (AWS returns an empty page) — kept so `deleteBucket`'s
   `MaxKeys:1` probe and old callers are unaffected; tighten only if a client
   depends on `0`. v1 `NextMarker` is emitted on **every** truncation, not
   only with a delimiter (AWS omits it otherwise) — a robustness superset that
   no client breaks on.
9. **CopyObject = re‑upload (P7.4).** Copy streams the source bytes into a
   fresh set of Telegram messages (decision §2 / plan §B.1: message
   ref‑counting would double‑free on delete). Costs duplicate storage;
   accepted. The destination's prior chunks **are** reaped on overwrite, so
   copy does not add to the overwrite‑orphan wart (#1).
10. **No bucket‑level config is persisted (P7.1).** `?acl/?versioning/…`
    reads are the canned "default/absent" responses an unconfigured bucket
    would give; `PUT/DELETE` of a subresource is a silent no‑op (so
    `put-bucket-cors` etc. don't 501). Versioning/ACL/policy/lifecycle/CORS
    are genuinely unimplemented, not stored.
11. **DeleteObjects always 200 (P7.2).** Per‑key idempotent (missing →
    `Deleted`); a bucket that does not exist still yields a 200
    `<DeleteResult>` (all keys "deleted") rather than `NoSuchBucket` — matches
    the idempotent-delete design and the targets (`aws s3 rm`, rclone purge).
12. **Virtual‑hosted only `<bucket>.<endpointHost>` (P7.6).** Derived from
    `PUBLIC_ENDPOINT_URL`; empty/​bare‑host → path‑style. No path‑style‑in‑
    vhost‑Host heuristics. SigV4 needs the client to have signed the host it
    used (standard).
13. **CORS is permissive `*` (P7.3).** Safe here — reads are public by
    design, transport is TLS, SigV4 rides in headers/query (not cookies).
    Not configurable per‑bucket.

---

## 7. Remaining work (dependency‑ordered)

For unchanged protocol detail see `S3-COMPAT-PLAN.md` §3 (verified reference)
and §4 (Phase 5/6/7). Acceptance = real default‑configured AWS CLI v2 / rclone
/ restic against the live gateway (`S3-COMPAT-PLAN.md` §5).

### Phase 5 — Range GET ✅ DONE (unit‑verified; see §3, §5)
- Implemented per the original plan; `handler.go` + `range_test.go`.
- **Live acceptance still owed:** `curl -r 1000000-2000000` on a >18 MiB
  multi‑chunk object returns exactly those bytes with `206`; HTML5 `<video>`
  seek + `restic` against the live gateway. Fold into the §6.7 live pass.

### Phase 6 — Listing correctness ✅ DONE (unit‑verified; see §3, §5, §6.8)
- Implemented per the original plan; `store.go` + `handler.go` + `list_test.go`.
- **Live acceptance still owed:** 2500‑key bucket lists fully via paginated
  `aws s3api list-objects-v2`; `--delimiter /` returns prefixes; `rclone lsd`
  shows folders. Fold into the §6.7 live pass.

### Phase 7 — Remaining compatibility surface ✅ DONE (unit‑verified; see §3, §5)
- Implemented per `S3-COMPAT-PHASE7-PLAN.md` (P7.1→P7.6); sources
  `s3api/subresource.go|deleteobjects.go|copy.go|objectmeta.go` +
  `handler.go`/`multipart.go`/`metadata/store.go`, tests per §5.
- With Phases 5–6 also done, **the S3 surface is feature‑complete.**
- **Live data‑path smoke DONE (2026‑05‑19):** local gateway + real Telegram,
  `go test -tags s3live -run Live` — `TestLiveChunkedRoundTrip`,
  `TestLiveUnsignedPayloadRoundTrip`, and `TestLivePhase7` (subresource /
  DeleteObjects / CopyObject byte‑identity / metadata+`If-None-Match`→304 /
  vhost / request‑id) all green. Plus no‑auth checks: `/healthz`→204,
  `OPTIONS`→200 with ACAO `*` + `x-amz-request-id`.
- **Still owed** — acceptance with the *actual default clients* (the plan's
  §5 "real clients are the spec": aws‑cli v2 / rclone / restic) + Gokapi
  **Level‑3 E2E**, and scale (>18 MiB multi‑chunk, 2500‑key pagination):
  - `aws s3api get-bucket-location/get-bucket-versioning` succeed;
    `get-bucket-tagging` → `NoSuchTagSet`; `rclone`/`Cyberduck` connect clean.
  - `aws s3 rm s3://send --recursive`, rclone `purge`, restic `prune`.
  - `curl -X OPTIONS` → 200 + ACAO; browser `fetch` PUT; Gokapi **Level‑3
    E2E** (HANDOFF.md) end‑to‑end.
  - `aws s3 cp s3://send/a s3://send/b`, rclone server‑side move, a
    large‑object multipart copy.
  - `head-object` shows `--content-type/--content-disposition/--metadata`;
    `get-object --response-content-disposition` overrides; conditional GET
    `If-None-Match` → 304 (CDN/restic cache).
  - An SDK with **default (vhost) addressing** at `PUBLIC_ENDPOINT_URL`
    works (`bucket.host/key`); errors carry `x-amz-request-id` + `<RequestId>`.

### Follow‑ups (any time; see §6)
- Overwrite‑orphan reaping (small; do with Phase 5 or standalone).
- Abandoned‑multipart janitor.
- Optional: min‑part‑size `EntityTooSmall`; malformed‑chunked → 400.

---

## 8. Key file map

| File | Role |
|---|---|
| `internal/s3api/handler.go` | routing, SigV4 (Phase 2), put/get/head/delete, shared decode + `objectSegments`/`openObject`/`segmentReader`, `setCORS`, `splitBucketKey`, conditional `errorResponse` w/ `<RequestId>` |
| `internal/s3api/chunked.go` | aws‑chunked de‑framer, counting reader (Phase 1) |
| `internal/s3api/multipart.go` | multipart handlers + XML types (Phase 4), `newUploadID`/`newRequestID` |
| `internal/s3api/subresource.go` | P7.1 bucket subresource probes + ListMultipartUploads render |
| `internal/s3api/deleteobjects.go` | P7.2 bulk DeleteObjects |
| `internal/s3api/copy.go` | P7.4 CopyObject / UploadPartCopy, `parseCopySource`, `loadSource` |
| `internal/s3api/objectmeta.go` | P7.5 metadata capture/echo, `response-*` overrides, conditional eval |
| `internal/storage/storage.go` | `Backend` interface, `Chunk` |
| `internal/storage/telegram/bot.go` | chunked upload / ranged download / delete |
| `internal/metadata/store.go` | SQLite: objects + object_chunks + multipart_* + **object_metadata** + **multipart_upload_metadata**; WAL. New: `ListMultipartUploads`, `GetObjectMetadata`, `replaceObjectMetadataTx`, `Put/GetMultipartUploadMetadata`; `Object.Metadata` |
| `internal/config/config.go` | `TelegramAPIBaseURL`, `PublicEndpointURL` (P7.6 vhost) |
| `cmd/telegram-s3/main.go` | wiring (passes base URL) |

Regression guard (verify every phase): Gokapi (aws‑sdk‑go v1, path‑style,
UNSIGNED‑PAYLOAD, SHA1 keys) must keep working; schema additive; legacy
single‑message read/delete path preserved.
