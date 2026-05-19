# Telegram‑S3 — Phase 7 Execution Plan (self‑contained)

> **You do not need any prior chat context.** This file is the executable plan
> for **Phase 7 (remaining S3 compatibility surface)**. Phases 1–6 are already
> implemented, unit‑verified, and uncommitted in the working tree.
>
> Companion docs in‑repo (background, still authoritative):
> - `S3-COMPAT-PROGRESS.md` — state of Phases 1–6, decisions, key file map.
> - `S3-COMPAT-PLAN.md` — original roadmap + §3 verified protocol reference;
>   §4 "Phase 7" and the gap table (lines ~106–130) enumerate the targets.
> - `HANDOFF.md` — deployment/ops (Easypanel, Gokapi front‑end). Notes that
>   Gokapi **Level‑3 E2E needs S3 CORS** (relevant to P7.3).
>
> Execute the work items **in order** (P7.1 → P7.6); each is independently
> shippable and unit‑tested. Run the gate (§A) before starting and after each
> item. Update the docs (§D) when done.

---

## A. Verification gate (run first, and after every item)

```
cd C:\Users\HP MEDIA\Desktop\nguyenvu\telegram-s3
gofmt -l ./internal ./cmd                                   # expect: no output
go build ./...                                              # expect: clean
go vet ./... && go vet -tags s3live ./internal/s3api/       # expect: clean
go test ./...                                               # expect: all ok
```

Live acceptance (optional, needs a running gateway + creds) is described per
item; it is **not** required to land the code, but record what's owed.

---

## B. Current state & invariants (do not break)

- Branch `master`. Last commit `72afb08 Cache public object HEAD responses`.
  **NOTHING is committed.** Phases 1–6 live only in the working tree.
- **Do not commit** unless the user explicitly asks — the established rhythm
  across Phases 5–6 was "implement + unit‑verify, leave uncommitted".
- `.env` is gitignored — never commit it.
- **Regression guard (re‑verify for every item):** Gokapi must keep working —
  `aws-sdk-go` **v1**, **path‑style**, `UNSIGNED-PAYLOAD`, SHA1‑hex keys, no
  aws‑chunked. Anything that isn't a `GET`/`HEAD` of `bucket+key` requires
  valid SigV4 (the public‑read bypass is exactly
  `publicObjectRead := (GET||HEAD) && bucket!="" && key!=""` in
  `ServeHTTP`). Do not widen or narrow that.
- **Schema is additive only.** `buckets`/`objects`/`object_chunks`/
  `multipart_*` are untouched. New state → a **new** `CREATE TABLE IF NOT
  EXISTS` in `store.migrate()`. **Do not `ALTER` `objects`** (preserves legacy
  single‑message rows verbatim).
- All XML responses go through `h.writeXML` (writes `xml.Header` then encodes).
  List/error XML now carry `xmlns` via `s3XMLNS`
  (`http://s3.amazonaws.com/doc/2006-03-01/`). Reuse it on new XML roots.

### Key files

| File | Role |
|---|---|
| `internal/s3api/handler.go` | `ServeHTTP` routing (~L39–92), SigV4, put/get/head/delete, list (v1+v2), error/XML helpers, `awsURIEncode`, `s3XMLNS`, `awsListTimeFormat` |
| `internal/s3api/multipart.go` | multipart handlers + XML type pattern to copy |
| `internal/s3api/chunked.go` | aws‑chunked de‑framer (`decodeUpload`) |
| `internal/metadata/store.go` | SQLite; `migrate()`, `ListObjectsPage`, object/chunk/multipart methods, WAL |
| `internal/storage/storage.go` | `Backend` iface: `Upload`/`Download`/`DownloadRange`/`Delete`; `Chunk` |
| `internal/storage/telegram/bot.go` | chunked upload / ranged download / delete |
| `internal/config/config.go` | `Config.PublicEndpointURL` (env `PUBLIC_ENDPOINT_URL`) — used by P7.6 |
| `cmd/telegram-s3/main.go` | wiring |

### Routing facts (handler.go `ServeHTTP`)

- `parsePath(r.URL.Path)` is **path‑style only**: `/{bucket}/{key}`.
- `q := r.URL.Query()` is already computed; `hasUploads := q["uploads"]`,
  `uploadID := q.Get("uploadId")` exist.
- Order matters. Multipart arms precede generic object verbs. **`case GET &&
  bucket != "":` → `listObjects` currently catches every `GET /{bucket}?<sub>`
  probe** (the P7.1 bug). POST is routed only for multipart (`key!=""`); a
  bucket‑level `POST /{bucket}?delete` currently hits `default` → 501.
- `OPTIONS` currently → `default` 501 **and** is gated by the auth check.

### Test infrastructure (reuse — same `s3api` package)

- `multipart_test.go`: `newMPRig(t) *mpRig` → `{h *Handler, be *fakeBackend}`;
  `(*mpRig).do(method, target, body)` signs write verbs (SigV4 header auth via
  `signHeaderAuth`/`amz`); `md5hex`. `fakeBackend` implements the full
  `Backend` (Upload/Download/DownloadRange/Delete), splits at `chunkSize=4`.
- `range_test.go`: `getWith(r, target, headers map)` — **unsigned** object GET
  (public‑read path).
- `list_test.go`: `signedGet(r, target)` — **signed** GET (needed for any
  non‑public verb); `seedBucket(t,r,keys...)`; `eq`, `keysOf`, `prefixesOf`.
- Put new tests in `internal/s3api/phase7_test.go` (or per‑feature files).
  `gofmt -w` any new test file (the rig structs trip alignment).

### Decisions already made — do not relitigate

1. **CopyObject = re‑upload** (stream source bytes → `backend.Upload` → new
   Telegram messages). NOT message reference‑counting (delete is a hard
   per‑message‑ID reap; sharing messages would double‑free). Costs duplicate
   storage; acceptable and consistent with the "incremental, preserve the
   regression path" architecture.
2. **Object metadata = a new side table**, replaced in the *same transaction*
   as `object_chunks` (mirrors that pattern). **No `ALTER objects`.** Legacy
   rows simply have no metadata rows → defaults.
3. **CORS = permissive `*`** by default (the gateway already treats reads as
   public; transport is TLS; SigV4 is in headers/query, not cookies, so
   `Access-Control-Allow-Origin: *` is safe and unlocks Gokapi Level‑3).
4. **Virtual‑hosted = only `<bucket>.<endpointHost>`** derived from
   `cfg.PublicEndpointURL`; path‑style remains the default and the Gokapi path.

---

## C. Work items (dependency‑ordered)

### P7.1 — Bucket subresource probes (routing fix)  ★ start here

**Problem.** `GET /{bucket}?location` (and `?versioning|?acl|?cors|?tagging|
?policy|?lifecycle|?website|?encryption|?object-lock|?notification|?logging|
?replication|?accelerate|?requestPayment|?cors|?uploads|?ownershipControls|
?publicAccessBlock|?analytics|?metrics|?inventory|?intelligent-tiering`)
falls through to `listObjects` and returns a bogus `200 ListBucketResult`.
Breaks the probe phase of rclone/Cyberduck/Veeam/SDKs.

**Do.** In `ServeHTTP`, **before** `case r.Method == http.MethodGet && bucket
!= "":`, add `case r.Method == http.MethodGet && bucket != "" && key == "" &&
isBucketSubresource(q):` → `h.bucketSubresource(ctx, w, bucket, q)`. Add a
helper `isBucketSubresource(q url.Values) bool` (any known subresource key
present) and the handler returning canned responses:

- `?location` → `200` `<LocationConstraint xmlns="…"></LocationConstraint>`
  (empty = us‑east‑1).
- `?versioning` → `200` `<VersioningConfiguration xmlns="…"/>` (unversioned).
- `?acl` → `200` `<AccessControlPolicy><Owner><ID>{AccessKeyID}</ID>
  <DisplayName>{AccessKeyID}</DisplayName></Owner><AccessControlList><Grant>
  <Grantee xsi:type="CanonicalUser">…</Grantee><Permission>FULL_CONTROL
  </Permission></Grant></AccessControlList></AccessControlPolicy>`.
- `?accelerate` → `200` `<AccelerateConfiguration xmlns="…"/>`.
- `?requestPayment` → `200` `<RequestPaymentConfiguration><Payer>BucketOwner
  </Payer></RequestPaymentConfiguration>`.
- `?notification` → `200` `<NotificationConfiguration xmlns="…"/>`.
- `?logging` → `200` `<BucketLoggingStatus xmlns="…"/>`.
- `?cors` → `404 NoSuchCORSConfiguration`.
- `?tagging` → `404 NoSuchTagSet`.
- `?policy` → `404 NoSuchBucketPolicy`.
- `?lifecycle` → `404 NoSuchLifecycleConfiguration`.
- `?website` → `404 NoSuchWebsiteConfiguration`.
- `?encryption` → `404 ServerSideEncryptionConfigurationNotFoundError`.
- `?object-lock` → `404 ObjectLockConfigurationNotFoundError`.
- `?replication` → `404 ReplicationConfigurationNotFoundError`.
- `?ownershipControls` → `404 OwnershipControlsNotFoundError`.
- `?publicAccessBlock` → `404 NoSuchPublicAccessBlockConfiguration`.
- `?analytics|?metrics|?inventory|?intelligent-tiering` → `404` (use
  `NoSuchConfiguration`); first existence check uses `BucketExists` →
  `404 NoSuchBucket` if the bucket is absent (do this first for all).
- `?uploads` (ListMultipartUploads) → implement for real from the existing
  `multipart_uploads` table: add `store.ListMultipartUploads(ctx, bucket)
  ([]MultipartUpload, error)` and render `<ListMultipartUploadsResult>`
  (`Bucket`, `KeyMarker`, `UploadIdMarker`, `MaxUploads=1000`,
  `IsTruncated=false`, `<Upload><Key><UploadId><Initiated>` rows). An empty
  well‑formed result is acceptable if you keep it minimal, but the data is
  cheap to surface.

Bucket‑level **PUT/DELETE** of a subresource (e.g. `PUT /{bucket}?acl`,
`DELETE /{bucket}?tagging`) currently hits createBucket/deleteBucket arms? No —
those require `key==""` with **no** subresource; add guards: only treat
`PUT bucket (no key, no subresource)` as createBucket and
`DELETE bucket (no key, no subresource)` as deleteBucket; route
`PUT/DELETE bucket?<subresource>` to a `200`/`204` no‑op (so `aws s3api
put-bucket-cors` etc. don't 501). Keep this minimal.

**Acceptance.** `aws s3api get-bucket-location/get-bucket-versioning
--bucket send` succeed; `aws s3api get-bucket-tagging` → `NoSuchTagSet`;
rclone/Cyberduck connect cleanly. **Regression:** `GET /send` and
`GET /send?list-type=2` still return `ListBucketResult` (Phase 6 intact),
`GET /send?delimiter=/` still rolls up.

**Tests.** Reuse `signedGet`/`seedBucket`: assert `?location` → 200 +
`<LocationConstraint`, `?tagging` → 404 `NoSuchTagSet`, `?versioning` → 200;
and **Phase‑6 guard**: `GET /send?list-type=2` after seeding still lists keys;
`GET /send` plain still works. Non‑existent bucket + `?location` → 404
`NoSuchBucket`.

---

### P7.2 — DeleteObjects bulk (`POST /{bucket}?delete`)

**Do.** Add `_, hasDelete := q["delete"]`. In `ServeHTTP`, **before** the
multipart POST arms, add `case r.Method == http.MethodPost && bucket != "" &&
key == "" && hasDelete:` → `h.deleteObjects(ctx, w, r, bucket)` (auth
required — POST is never public).

Refactor: extract the per‑object delete core out of `deleteObject`:

```
func (h *Handler) deleteOneObject(ctx context.Context, bucket, key string) error
```

It must reproduce the current `deleteObject` body — load object; if
`ErrNotFound` → return nil (DELETE is idempotent); else read chunks, reap
Telegram messages (chunk rows, or legacy `obj.TelegramMessageID` when no
chunks and `Size>0`), then `store.DeleteObject`. Have the existing
`deleteObject` handler call it (preserve its `204`/error behavior).

`deleteObjects`: parse `<Delete><Quiet>?</Quiet><Object><Key>…</Key>
</Object>…</Delete>` (cap 1000). For each key call `deleteOneObject`; collect
`<Deleted><Key>…</Key></Deleted>` (omit when `Quiet`) and `<Error><Key/>
<Code>InternalError</Code><Message/></Error>` on failure. Always `200`
`<DeleteResult xmlns="…">…</DeleteResult>`.

**Acceptance.** `aws s3 rm s3://send --recursive`, rclone `purge`, restic
`prune`. **Tests.** Seed N keys; POST `?delete` for a subset → 200
`DeleteResult`, deleted keys `GET`→404, others remain; `<Quiet>true</Quiet>`
omits `<Deleted>`; a missing key is reported `Deleted` (idempotent), not
`Error`.

---

### P7.3 — `OPTIONS` preflight + CORS response headers

**Do.** CORS preflight carries **no credentials** — handle `OPTIONS` **before
the auth check** (next to the `/healthz` early return). Add
`func setCORS(w, r)` and call it for **all** responses (top of `ServeHTTP`,
after `/healthz`): `Access-Control-Allow-Origin: *`,
`Access-Control-Allow-Methods: GET, PUT, POST, DELETE, HEAD`,
`Access-Control-Allow-Headers:` echo `Access-Control-Request-Headers` (or
`*`), `Access-Control-Expose-Headers: ETag, Content-Range, Accept-Ranges,
Content-Length, x-amz-request-id`, `Access-Control-Max-Age: 3000`. For
`r.Method == http.MethodOptions`: `setCORS`; `w.WriteHeader(200)`; return
(no body, no auth, no routing).

**Acceptance.** `curl -X OPTIONS` → 200 + `Access-Control-Allow-Origin`;
browser `fetch` PUT works; Gokapi Level‑3 E2E (HANDOFF.md). **Tests.**
`OPTIONS /send/k` with `Origin` + `Access-Control-Request-Method` → 200, ACAO
`*`, ACAM present; a normal `getWith` GET also carries ACAO.

---

### P7.4 — CopyObject + UploadPartCopy

`x-amz-copy-source` header = `/{srcBucket}/{srcKey}` or `{srcBucket}/{srcKey}`
(URL‑encoded; may carry `?versionId=…` — strip and ignore). Add
`parseCopySource(r) (srcBucket, srcKey string, ok bool)` (path‑unescape;
trim leading `/`; split first segment).

**Shared helper.** Add an object‑bytes reader (reuse the Phase‑5 segment
logic). Implement
`func (h *Handler) openObject(ctx, obj metadata.Object, chunks []metadata.Chunk, rng *httpRange) (io.ReadCloser, error)`
that returns a reader over the whole object (or a sub‑range) by chaining
`backend.DownloadRange` over the chunk map (legacy: single `DownloadRange(
obj.TelegramFileID, …)`; empty object: empty reader). Refactor
`streamSegments`/`getObject` to build on it if convenient (optional, keep
behavior identical — Phase‑5 tests must stay green).

**CopyObject.** In `putObject`, if `x-amz-copy-source` present → copy branch:
load src `GetObject`+`GetObjectChunks` (404 `NoSuchKey` if absent), open a
reader via `openObject`, `io.TeeReader` through `md5.New()`, pass to
`backend.Upload(ctx, dstKey, srcContentType, reader)`, `store.PutObject` the
new object (size = copied bytes, ETag = MD5 hex). Respond `200`
`<CopyObjectResult xmlns="…"><LastModified/><ETag>"…"</ETag>
</CopyObjectResult>`. Honor `x-amz-metadata-directive` only once P7.5 lands
(COPY = carry src metadata, REPLACE = take request headers); until then treat
as COPY of `Content-Type`. Reap the superseded destination's old chunks on
overwrite (same pattern as `uploadPart`: read old chunks, store, then
`backend.Delete` them).

**UploadPartCopy.** In `uploadPart`, if `x-amz-copy-source` present → read src
(optionally ranged via `x-amz-copy-source-range: bytes=a-b`, parsed with the
existing `parseByteRange`) through `openObject`, `backend.Upload`, then
`store.PutMultipartPart`. Respond `<CopyPartResult xmlns="…"><ETag>"…"</ETag>
<LastModified/></CopyPartResult>` (note: response is XML, unlike normal
UploadPart which uses the `ETag` header).

**Acceptance.** `aws s3 cp s3://send/a s3://send/b`; rclone server‑side move;
large‑object multipart copy. **Tests.** Put src (multi‑chunk via rig
`chunkSize=4`); CopyObject → dst bytes identical, ETag = `md5hex(src)`;
UploadPartCopy with a `x-amz-copy-source-range` then complete → reassembled
equals the source slice.

---

### P7.5 — Object metadata + conditional requests (schema‑additive)

**Schema.** Add to `store.migrate()` (additive, new table):

```
CREATE TABLE IF NOT EXISTS object_metadata (
  bucket TEXT NOT NULL, key TEXT NOT NULL,
  name TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY (bucket, key, name)
);
```

Store methods: `PutObjectMetadata(tx, bucket, key, kv map[string]string)` and
`GetObjectMetadata(ctx, bucket, key) (map[string]string, error)`; **fold the
delete+insert into the existing `PutObject` / `FinalizeMultipartUpload`
transactions** (next to the `object_chunks` replace) and the delete into
`DeleteObject` / `deleteOneObject`. Keys to persist (lower‑cased): `content-
type` (already on the row — keep there), `content-disposition`,
`content-encoding`, `cache-control`, `expires`, and every `x-amz-meta-*`
verbatim.

**PUT.** Capture those headers in `putObject` (and `createMultipartUpload`
for MPU‑completed objects → carry via the upload row or re‑read on complete)
and persist.

**GET/HEAD.** Echo stored `Content-Disposition`/`Content-Encoding`/
`Cache-Control`/`Expires`/`x-amz-meta-*`. Apply GET query overrides when
present: `response-content-type`, `response-content-disposition`,
`response-content-encoding`, `response-cache-control`, `response-expires`
(override the echoed values; allowed on the public‑read path here).

**Conditional requests** (apply in `getObject`/`headObject`, before streaming;
`If-Range` already handled in Phase 5 — keep it):
- `If-None-Match: "etag"` or `*` matches → `304 Not Modified` (no body).
- `If-Modified-Since` not modified → `304`.
- `If-Match: "etag"` no match → `412 Precondition Failed`.
- `If-Unmodified-Since` modified since → `412`.

**Acceptance.** `aws s3 cp --content-type --content-disposition --metadata
k=v` then `head-object` shows them; `get-object --response-content-
disposition` overrides; conditional GET `If-None-Match` → 304 (CDN/restic
cache). **Tests.** PUT with `Content-Disposition` + `x-amz-meta-foo` →
HEAD/GET echo both; `response-content-type` override; `If-None-Match` ==
current ETag → 304; `If-Match` mismatch → 412. **Regression:** Phase‑5 range
tests and legacy reads stay green.

---

### P7.6 — Virtual‑hosted addressing + `<RequestId>`

**Vhost.** Endpoint host = parsed host of `cfg.PublicEndpointURL` (may be
empty → vhost disabled, path‑style only). Add
`func (h *Handler) splitBucketKey(r *http.Request) (bucket, key string)`:
strip port from `r.Host`; if endpoint host is set and
`host == "<b>." + endpointHost` → `bucket=<b>`, `key = path‑unescaped
r.URL.Path` (no leading bucket segment); else fall back to `parsePath`
(path‑style). Use it in `ServeHTTP` instead of `parsePath`. SigV4 needs **no
change**: `canonicalHeaders` already signs `r.Host`, which the client signed
with the vhost host. **Regression:** when `Host == endpointHost` or
`PublicEndpointURL` is empty → identical to today (Gokapi path‑style intact).

**RequestId.** Add `x-amz-request-id` response header (random hex, reuse the
`newUploadID()` style) on every response, and `<RequestId>` (+ optional
`<Resource>` = path) to the error XML. Modify `errorResponse` +
`writeError`/`writeXML` (or set the header centrally in `ServeHTTP`/
`setCORS`). Some clients (Veeam, SDK retry logging) expect it.

**Acceptance.** An SDK with **default (vhost) addressing** pointed at
`PUBLIC_ENDPOINT_URL` works (`bucket.host/key`); errors carry
`x-amz-request-id` + `<RequestId>`. **Tests.** `NewHandler` with a test cfg
`PublicEndpointURL: "https://example.com"`; request `Host: send.example.com`
+ path `/k` (seed obj `send/k`) → served; path‑style `GET /send/k` still
works; an error body contains `<RequestId>` and the header is set.

---

## D. Done criteria & doc updates

1. Gate (§A) fully green, including `-tags s3live` vet.
2. New unit tests for **every** item, on the existing rig, all passing.
3. Update `S3-COMPAT-PROGRESS.md`:
   - Status line → "Phases 1–7 implemented and unit‑verified".
   - §3: add a "Phase 7 ✅" block summarizing each item.
   - §4: add new test file(s) to the untracked list.
   - §5: add the Phase‑7 test inventory bullet(s).
   - §6: add any new intentional caveats (e.g. copy = re‑upload duplicate
     storage; vhost only `<bucket>.<endpointHost>`; CORS `*`).
   - §7: replace the Phase 7 block with "✅ DONE (unit‑verified)" + the live
     acceptance still owed; nothing left → note the project is feature‑complete
     pending the live pass.
   - §8: add `object_metadata` + new store methods to the file/role map.
4. Leave `S3-COMPAT-PLAN.md` as‑is (historical roadmap).
5. **Do not commit** unless the user asks. If asked, the natural message is a
   single checkpoint of Phases 1–7 (note `.env` stays untracked).

## E. Suggested order within a session

P7.1 → gate → P7.2 → gate → P7.3 → gate → P7.4 (add `openObject` first) →
gate → P7.5 → gate → P7.6 → gate → doc updates (§D) → final gate.
Each "gate" is the four §A commands; fix before proceeding. If context runs
low, stop at a gate boundary — every item is independently complete and the
docs in §D tell the next session exactly where to resume.
