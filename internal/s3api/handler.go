package s3api

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
	"telegram-s3/internal/reader"
	"telegram-s3/internal/storage"
)

type Handler struct {
	cfg     config.Config
	store   *metadata.Store
	backend storage.Backend
	logger  *slog.Logger
	// endpointHost is the lower-cased host of cfg.PublicEndpointURL (no port),
	// or "" when unset. Non-empty enables virtual-hosted addressing for
	// <bucket>.<endpointHost>; path-style (the Gokapi path) is always honored.
	endpointHost string
}

func NewHandler(cfg config.Config, store *metadata.Store, backend storage.Backend, logger *slog.Logger) *Handler {
	endpointHost := ""
	if cfg.PublicEndpointURL != "" {
		if u, err := url.Parse(cfg.PublicEndpointURL); err == nil {
			endpointHost = strings.ToLower(u.Hostname())
		}
	}
	return &Handler{cfg: cfg, store: store, backend: backend, logger: logger, endpointHost: endpointHost}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// CORS (P7.3). A preflight carries no credentials, so OPTIONS is answered
	// before the auth gate and without routing. setCORS runs for every other
	// response too — the gateway already treats reads as public and SigV4
	// travels in headers/query (not cookies), so `*` is safe and unlocks
	// browser fetch / Gokapi Level-3 E2E (HANDOFF.md).
	setCORS(w, r)
	// x-amz-request-id on every response (P7.6). Set centrally so writeError
	// can echo the same value into <RequestId> without threading it through
	// every handler. Some clients (Veeam, SDK retry logging) expect it.
	w.Header().Set("x-amz-request-id", newRequestID())
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	bucket, key := h.splitBucketKey(r)
	publicObjectRead := (r.Method == http.MethodGet || r.Method == http.MethodHead) && bucket != "" && key != ""
	if !publicObjectRead && !h.authorized(r) {
		h.writeError(w, http.StatusForbidden, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.")
		return
	}

	ctx := r.Context()
	q := r.URL.Query()
	_, hasUploads := q["uploads"]
	_, hasDelete := q["delete"]
	uploadID := q.Get("uploadId")

	switch {
	case r.Method == http.MethodGet && bucket == "":
		h.listBuckets(ctx, w)

	// Bucket subresource probes (P7.1). GET returns canned config XML / clean
	// 404s instead of falling through to listObjects (which would answer a
	// ?location/?versioning/... probe with a bogus ListBucketResult and break
	// rclone/Cyberduck/SDK connect); PUT/DELETE of a subresource is a no-op so
	// put-bucket-cors etc. don't 501. Must precede createBucket/deleteBucket
	// (key=="" matches them otherwise) and the GET listObjects catch-all. POST
	// is excluded so POST /{bucket}?delete (P7.2) is unaffected.
	case bucket != "" && key == "" && r.Method != http.MethodPost && isBucketSubresource(q):
		h.bucketSubresource(ctx, w, r, bucket, q)

	case r.Method == http.MethodPut && bucket != "" && key == "":
		h.createBucket(ctx, w, bucket)
	case r.Method == http.MethodHead && bucket != "" && key == "":
		h.headBucket(ctx, w, bucket)
	case r.Method == http.MethodDelete && bucket != "" && key == "":
		h.deleteBucket(ctx, w, bucket)

	// Bulk DeleteObjects (P7.2): POST /{bucket}?delete. Must precede the
	// multipart POST arms; POST is never public so auth already applied above.
	case r.Method == http.MethodPost && bucket != "" && key == "" && hasDelete:
		h.deleteObjects(ctx, w, r, bucket)

	// Multipart upload (Phase 4) — must precede the generic object verbs.
	case r.Method == http.MethodPost && bucket != "" && key != "" && hasUploads:
		h.createMultipartUpload(ctx, w, r, bucket, key)
	case r.Method == http.MethodPut && bucket != "" && key != "" && uploadID != "" && q.Get("partNumber") != "":
		h.uploadPart(ctx, w, r, bucket, key, uploadID, q.Get("partNumber"))
	case r.Method == http.MethodPost && bucket != "" && key != "" && uploadID != "":
		h.completeMultipartUpload(ctx, w, r, bucket, key, uploadID)
	case r.Method == http.MethodDelete && bucket != "" && key != "" && uploadID != "":
		h.abortMultipartUpload(ctx, w, bucket, key, uploadID)
	case r.Method == http.MethodGet && bucket != "" && key != "" && uploadID != "":
		h.listParts(ctx, w, bucket, key, uploadID)

	case r.Method == http.MethodPut && bucket != "" && key != "":
		h.putObject(ctx, w, r, bucket, key)
	case r.Method == http.MethodGet && bucket != "" && key != "":
		h.getObject(ctx, w, r, bucket, key)
	case r.Method == http.MethodHead && bucket != "" && key != "":
		h.headObject(ctx, w, r, bucket, key)
	case r.Method == http.MethodDelete && bucket != "" && key != "":
		h.deleteObject(ctx, w, bucket, key)
	case r.Method == http.MethodGet && bucket != "":
		h.listObjects(ctx, w, r, bucket)
	default:
		h.writeError(w, http.StatusNotImplemented, "NotImplemented", "This S3 operation is not implemented yet.")
	}
}

// setCORS writes permissive CORS headers on every response. Allow-Headers
// echoes the preflight's requested headers (falling back to `*`) so a signed
// browser PUT — whose Authorization/x-amz-* headers the browser lists in
// Access-Control-Request-Headers — is permitted.
func setCORS(w http.ResponseWriter, r *http.Request) {
	hdr := w.Header()
	hdr.Set("Access-Control-Allow-Origin", "*")
	hdr.Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD")
	reqHeaders := r.Header.Get("Access-Control-Request-Headers")
	if reqHeaders == "" {
		reqHeaders = "*"
	}
	hdr.Set("Access-Control-Allow-Headers", reqHeaders)
	hdr.Set("Access-Control-Expose-Headers", "ETag, Content-Range, Accept-Ranges, Content-Length, x-amz-request-id")
	hdr.Set("Access-Control-Max-Age", "3000")
}

func (h *Handler) createBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	if err := h.store.CreateBucket(ctx, bucket); err != nil {
		if strings.Contains(err.Error(), "constraint") {
			h.writeError(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "Bucket already exists.")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	page, err := h.store.ListObjectsPage(ctx, metadata.ListParams{Bucket: bucket, MaxKeys: 1})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if len(page.Objects) > 0 {
		h.writeError(w, http.StatusConflict, "BucketNotEmpty", "The bucket you tried to delete is not empty.")
		return
	}
	if err := h.store.DeleteBucket(ctx, bucket); err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) headBucket(ctx context.Context, w http.ResponseWriter, bucket string) {
	exists, err := h.store.BucketExists(ctx, bucket)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) listBuckets(ctx context.Context, w http.ResponseWriter) {
	buckets, err := h.store.ListBuckets(ctx)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	response := listAllMyBucketsResult{Owner: owner{ID: h.cfg.AccessKeyID, DisplayName: h.cfg.AccessKeyID}}
	for _, b := range buckets {
		response.Buckets.Bucket = append(response.Buckets.Bucket, bucketResult{Name: b.Name, CreationDate: b.CreatedAt.Format(time.RFC3339)})
	}
	h.writeXML(w, http.StatusOK, response)
}

func (h *Handler) putObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	exists, err := h.store.BucketExists(ctx, bucket)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	if !exists {
		h.writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
		return
	}

	// CopyObject (P7.4): PUT with x-amz-copy-source is a server-side copy, not
	// an upload — re-stream the source bytes into a fresh set of Telegram
	// messages (decision: copy = re-upload, never message ref-counting).
	if r.Header.Get("X-Amz-Copy-Source") != "" {
		h.copyObject(ctx, w, r, bucket, key)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// decodeUpload strips aws-chunked framing (Phase 1) so only real object
	// bytes reach Telegram; the Gokapi / aws-sdk-go v1 UNSIGNED-PAYLOAD path
	// passes through untouched — the regression guard.
	body, hasher := decodeUpload(r)
	chunks, err := h.backend.Upload(ctx, key, contentType, body)
	if err != nil {
		// Malformed aws-chunked framing (8.3) is a client error, not a
		// backend failure — map to 400 before the 502 fallthrough. The
		// backend may have already pushed some bytes; reap them.
		if errors.Is(err, ErrMalformedChunked) {
			h.deleteChunks(ctx, chunks)
			h.writeError(w, http.StatusBadRequest, "IncompleteBody", "The request body does not match the declared chunk framing.")
			return
		}
		h.writeError(w, http.StatusBadGateway, "TelegramUploadFailed", err.Error())
		return
	}

	size, ok := h.validateDecodedSize(w, r, body.n)
	if !ok {
		h.deleteChunks(ctx, chunks)
		return
	}

	etag := hex.EncodeToString(hasher.Sum(nil))
	obj := metadata.Object{Bucket: bucket, Key: key, Size: size, ETag: etag, ContentType: contentType, Metadata: captureObjectMetadata(r.Header)}
	if len(chunks) > 0 {
		// Keep the legacy columns non-NULL and pointing at the first chunk.
		obj.TelegramFileID = chunks[0].FileID
		obj.TelegramMessageID = chunks[0].MessageID
	}

	// Read the prior version's Telegram references BEFORE PutObject — the
	// txn replaces object_chunks in place, so afterwards the old rows are
	// gone (8.5, closes §6.1). prev is a legacy fallback: a pre-Phase-3
	// row has no object_chunks but a non-zero TelegramMessageID.
	oldChunks, _ := h.store.GetObjectChunks(ctx, bucket, key)
	prev, _ := h.store.GetObject(ctx, bucket, key)

	if err := h.store.PutObject(ctx, obj, toMetaChunks(chunks)); err != nil {
		h.deleteChunks(ctx, chunks)
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	h.reapSupersededChunks(ctx, oldChunks, prev)
	w.Header().Set("ETag", quoteETag(etag))
	w.WriteHeader(http.StatusOK)
}

// reapSupersededChunks best-effort drops the previous version's Telegram
// messages after a successful overwrite. A failure here is logged but never
// 5xx's the now-durable write — the new object is already authoritative.
// Post Phase 3 the chunk map is authoritative for every non-empty object
// (legacy single-message rows are backfilled at migration time), so the
// pre-Phase-3 prev.TelegramMessageID fallback no longer fires.
func (h *Handler) reapSupersededChunks(ctx context.Context, oldChunks []metadata.Chunk, _ metadata.Object) {
	refs := chunkRefs(oldChunks)
	if len(refs) == 0 {
		return
	}
	if derr := h.backend.DeleteBatch(ctx, refs); derr != nil && h.logger != nil {
		h.logger.Warn("reap superseded chunks failed", "count", len(refs), "error", derr)
	}
}

// decodeUpload wraps the request body so the caller reads decoded object
// bytes: aws-chunked framing stripped (Phase 1), MD5-hashed, and counted.
func decodeUpload(r *http.Request) (*countingReader, hash.Hash) {
	var src io.Reader = r.Body
	if isAWSChunked(r) {
		src = newAWSChunkedReader(r.Body)
	}
	hsh := md5.New()
	return &countingReader{r: io.TeeReader(src, hsh)}, hsh
}

// validateDecodedSize enforces X-Amz-Decoded-Content-Length against the bytes
// actually decoded. On mismatch it writes the S3 error and returns ok=false;
// the caller must still reap any chunks it already uploaded.
func (h *Handler) validateDecodedSize(w http.ResponseWriter, r *http.Request, got int64) (int64, bool) {
	declared := r.Header.Get("X-Amz-Decoded-Content-Length")
	if declared == "" {
		return got, true
	}
	want, err := strconv.ParseInt(declared, 10, 64)
	if err != nil || want < 0 {
		h.writeError(w, http.StatusBadRequest, "InvalidArgument", "Invalid X-Amz-Decoded-Content-Length header.")
		return 0, false
	}
	if want != got {
		h.writeError(w, http.StatusBadRequest, "IncompleteBody", "The number of bytes specified by x-amz-decoded-content-length does not match what was received.")
		return 0, false
	}
	return got, true
}

func toMetaChunks(chunks []storage.Chunk) []metadata.Chunk {
	mc := make([]metadata.Chunk, len(chunks))
	for i, c := range chunks {
		mc[i] = metadata.Chunk{Seq: c.Seq, FileID: c.FileID, MessageID: c.MessageID, Size: c.Size, Offset: c.Offset, Transport: c.Transport, BotIndex: c.BotIndex}
	}
	return mc
}

// chunkRefs converts a metadata.Chunk slice into the ChunkRef slice the
// Backend interface consumes. Failed-put cleanup feeds storage.Chunk values
// through toMetaChunks first, so there's a single conversion path.
func chunkRefs(chunks []metadata.Chunk) []storage.ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	refs := make([]storage.ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = storage.ChunkRef{Transport: c.Transport, BotFileID: c.FileID, MessageID: c.MessageID, BotIndex: c.BotIndex}
	}
	return refs
}

// deleteChunks best-effort removes the Telegram messages for chunks that were
// uploaded but whose object was then rejected/failed to persist.
func (h *Handler) deleteChunks(ctx context.Context, chunks []storage.Chunk) {
	refs := chunkRefs(toMetaChunks(chunks))
	if len(refs) == 0 {
		return
	}
	if err := h.backend.DeleteBatch(ctx, refs); err != nil && h.logger != nil {
		h.logger.Warn("cleanup chunks after failed put", "count", len(refs), "error", err)
	}
}

func (h *Handler) getObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := h.store.GetObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	chunks, err := h.store.GetObjectChunks(ctx, bucket, key)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Conditional requests (P7.5) are evaluated before Range (RFC 7232):
	// If-Match/If-Unmodified-Since → 412, If-None-Match/If-Modified-Since →
	// 304. If-Range stays separate (Phase 5, gates the partial decision).
	if st := conditionalStatus(r, obj); st != 0 {
		h.writeConditional(w, st, obj)
		return
	}
	md, err := h.store.GetObjectMetadata(ctx, bucket, key)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Range GET (Phase 5). A syntactically valid single byte-range that is
	// unsatisfiable → 416; an absent/malformed/multi-range header is ignored
	// and the full object served (matches S3 / RFC 7233). If-Range gates the
	// whole decision: a stale validator falls back to the full object.
	useRange := false
	var rng httpRange
	if hdr := r.Header.Get("Range"); hdr != "" && ifRangeAllows(r, obj) {
		resolved, satisfiable, isRange := parseByteRange(hdr, obj.Size)
		switch {
		case isRange && !satisfiable:
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
			h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
			return
		case isRange && satisfiable:
			useRange, rng = true, resolved
		}
	}

	writeHeaders := func() {
		applyObjectHeaders(w, r.URL.Query(), obj, md)
		w.Header().Set("Accept-Ranges", "bytes")
		if useRange {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, obj.Size))
			w.Header().Set("Content-Length", strconv.FormatInt(rng.length(), 10))
			w.WriteHeader(http.StatusPartialContent)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
		w.WriteHeader(http.StatusOK)
	}

	var rngPtr *httpRange
	if useRange {
		rngPtr = &rng
	}
	h.streamObject(ctx, w, obj, chunks, rngPtr, writeHeaders)
}

// planRead converts an object + its chunk map + optional range into the
// (locs, start, end) triple the parallel prefetch reader consumes. Post
// Phase 3 every non-empty object has at least one object_chunks row (the
// migration backfills legacy single-message rows), so the chunked path is
// the only path. Empty objects (size == 0) yield no locs and start == end,
// so the caller short-circuits before constructing a reader.
func (h *Handler) planRead(obj metadata.Object, chunks []metadata.Chunk, rng *httpRange) ([]reader.ChunkLoc, int64, int64) {
	var start, end int64
	if rng != nil {
		start = rng.start
		end = rng.end + 1
	} else {
		end = obj.Size
	}

	if len(chunks) == 0 {
		return nil, start, end
	}
	locs := make([]reader.ChunkLoc, 0, len(chunks))
	for _, c := range chunks {
		locs = append(locs, reader.ChunkLoc{Ref: metaChunkRef(c), Offset: c.Offset, Size: c.Size})
	}
	return locs, start, end
}

// metaChunkRef builds a ChunkRef from a single metadata.Chunk row.
func metaChunkRef(c metadata.Chunk) storage.ChunkRef {
	return storage.ChunkRef{Transport: c.Transport, BotFileID: c.FileID, MessageID: c.MessageID, BotIndex: c.BotIndex}
}

// newPrefetchReader constructs a parallel-prefetch reader over the given
// locs + range. The caller must Prime() before committing any HTTP
// status line so a chunk-0 failure surfaces as 502 (not a truncated 200).
func (h *Handler) newPrefetchReader(ctx context.Context, objSize int64, locs []reader.ChunkLoc, start, end int64) *reader.Reader {
	src := reader.NewBotSource(h.backend, objSize, locs, h.cfg.StreamChunkSize)
	return reader.New(ctx, src, start, end,
		h.cfg.StreamConcurrency, h.cfg.StreamBuffers, h.cfg.ChunkTimeout)
}

// openObject returns a reader over the whole object (rng == nil) or an
// inclusive sub-range. Uses the parallel-prefetch reader (Phase 2) so
// CopyObject / UploadPartCopy fan-out as well. Prime() runs synchronously
// before this returns so a backend failure surfaces to the caller
// (typically about to stream into backend.Upload) without committing
// half-finished bytes. An empty object yields an empty reader.
func (h *Handler) openObject(ctx context.Context, obj metadata.Object, chunks []metadata.Chunk, rng *httpRange) (io.ReadCloser, error) {
	locs, start, end := h.planRead(obj, chunks, rng)
	if start >= end {
		return io.NopCloser(strings.NewReader("")), nil
	}
	pr := h.newPrefetchReader(ctx, obj.Size, locs, start, end)
	if err := pr.Prime(); err != nil && err != io.EOF {
		pr.Close()
		return nil, err
	}
	return pr, nil
}

// streamObject streams the requested range of an object to w using the
// parallel-prefetch reader. The first chunk is Primed before the status
// line is committed so a backend failure becomes HTTP 502 (not a
// truncated 200/206). Once headers have been written a later-chunk
// failure can only abort the connection.
func (h *Handler) streamObject(ctx context.Context, w http.ResponseWriter, obj metadata.Object, chunks []metadata.Chunk, rng *httpRange, writeHeaders func()) {
	locs, start, end := h.planRead(obj, chunks, rng)
	if start >= end {
		writeHeaders()
		return
	}
	pr := h.newPrefetchReader(ctx, obj.Size, locs, start, end)
	defer pr.Close()
	if err := pr.Prime(); err != nil && err != io.EOF {
		h.writeError(w, http.StatusBadGateway, "TelegramDownloadFailed", err.Error())
		return
	}
	writeHeaders()
	_, _ = io.Copy(w, pr)
}

// httpRange is a resolved, satisfiable, inclusive byte range [start,end].
type httpRange struct{ start, end int64 }

func (r httpRange) length() int64 { return r.end - r.start + 1 }

// parseByteRange resolves a single RFC 7233 byte-range against size:
//   - isRange=false                  → absent/malformed/multi-range; serve 200.
//   - isRange=true, satisfiable=false → 416.
//   - isRange=true, satisfiable=true  → 206 over the returned range.
func parseByteRange(s string, size int64) (r httpRange, satisfiable, isRange bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(s, prefix) {
		return r, false, false
	}
	spec := strings.TrimSpace(s[len(prefix):])
	if spec == "" || strings.Contains(spec, ",") { // multi-range unsupported
		return r, false, false
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return r, false, false
	}
	startStr := strings.TrimSpace(spec[:dash])
	endStr := strings.TrimSpace(spec[dash+1:])

	if startStr == "" { // suffix range: bytes=-N (final N bytes)
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return r, false, false
		}
		if size == 0 {
			return r, false, true
		}
		if n > size {
			n = size
		}
		return httpRange{start: size - n, end: size - 1}, true, true
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return r, false, false
	}
	end := size - 1
	if endStr != "" {
		if end, err = strconv.ParseInt(endStr, 10, 64); err != nil || end < start {
			return r, false, false
		}
	}
	if size == 0 || start >= size {
		return r, false, true // unsatisfiable
	}
	if end >= size {
		end = size - 1
	}
	return httpRange{start: start, end: end}, true, true
}

// ifRangeAllows reports whether an If-Range precondition (RFC 7233) permits a
// partial response: a matching ETag, or an unmodified-since HTTP-date. A
// missing header always allows; an unparseable one falls back to the full
// object (the conservative choice).
func ifRangeAllows(r *http.Request, obj metadata.Object) bool {
	ir := strings.TrimSpace(r.Header.Get("If-Range"))
	if ir == "" {
		return true
	}
	if strings.HasPrefix(ir, `"`) || strings.HasPrefix(ir, "W/") {
		return strings.Trim(strings.TrimPrefix(ir, "W/"), `"`) == obj.ETag
	}
	t, err := http.ParseTime(ir)
	if err != nil {
		return false
	}
	return !obj.UpdatedAt.UTC().Truncate(time.Second).After(t)
}

func (h *Handler) headObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := h.store.GetObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if st := conditionalStatus(r, obj); st != 0 {
		h.writeConditional(w, st, obj)
		return
	}
	md, err := h.store.GetObjectMetadata(ctx, bucket, key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	applyObjectHeaders(w, r.URL.Query(), obj, md)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteObject(ctx context.Context, w http.ResponseWriter, bucket, key string) {
	if err := h.deleteOneObject(ctx, bucket, key); err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteOneObject is the per-object delete core shared by the single-object
// DELETE handler and bulk DeleteObjects (P7.2). Hard delete: reclaim the
// Telegram storage, then drop the metadata. A missing object is not an error
// (DELETE is idempotent). A failed message delete is logged but does not block
// metadata removal — the object must still disappear from the namespace.
func (h *Handler) deleteOneObject(ctx context.Context, bucket, key string) error {
	if _, err := h.store.GetObject(ctx, bucket, key); err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return nil
		}
		return err
	}
	chunks, err := h.store.GetObjectChunks(ctx, bucket, key)
	if err != nil {
		return err
	}

	// Post Phase 3 the chunk map is authoritative: legacy single-message
	// rows were backfilled at migration time, so a non-empty object always
	// has at least one chunk to reap. Empty objects skip the batch.
	if refs := chunkRefs(chunks); len(refs) > 0 {
		if derr := h.backend.DeleteBatch(ctx, refs); derr != nil && h.logger != nil {
			h.logger.Warn("delete telegram chunks failed", "key", key, "count", len(refs), "error", derr)
		}
	}

	return h.store.DeleteObject(ctx, bucket, key)
}

// listObjects serves both ListObjects v1 and ListObjectsV2 (list-type=2). The
// store does the delimiter rollup + real pagination; this layer only maps the
// version-specific cursor params (marker vs continuation-token/start-after)
// and renders the version-specific XML, with optional encoding-type=url.
func (h *Handler) listObjects(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	v2 := q.Get("list-type") == "2"
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	urlEncode := q.Get("encoding-type") == "url"
	rawMax, _ := strconv.Atoi(q.Get("max-keys"))
	maxKeys := maxKeysOrDefault(rawMax)

	var after, marker, contToken, startAfter string
	if v2 {
		contToken = q.Get("continuation-token")
		startAfter = q.Get("start-after")
		if contToken != "" {
			key, ok := decodeListToken(contToken)
			if !ok {
				h.writeError(w, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect.")
				return
			}
			after = key
		} else {
			after = startAfter
		}
	} else {
		marker = q.Get("marker")
		after = marker
	}

	page, err := h.store.ListObjectsPage(ctx, metadata.ListParams{
		Bucket: bucket, Prefix: prefix, Delimiter: delimiter, After: after, MaxKeys: maxKeys,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// encoding-type=url URL-encodes the key-ish fields (S3 contract: clients
	// that request it decode them). Reuse the SigV4 AWS UriEncode.
	enc := func(s string) string {
		if urlEncode && s != "" {
			return awsURIEncode(s, true)
		}
		return s
	}
	encType := ""
	if urlEncode {
		encType = "url"
	}

	common := make([]commonPrefix, 0, len(page.CommonPrefixes))
	for _, p := range page.CommonPrefixes {
		common = append(common, commonPrefix{Prefix: enc(p)})
	}
	contents := make([]objectResult, 0, len(page.Objects))
	for _, obj := range page.Objects {
		contents = append(contents, objectResult{
			Key:          enc(obj.Key),
			LastModified: obj.UpdatedAt.UTC().Format(awsListTimeFormat),
			ETag:         quoteETag(obj.ETag),
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	if v2 {
		res := listBucketV2Result{
			XMLNS: s3XMLNS, Name: bucket, Prefix: enc(prefix), Delimiter: enc(delimiter),
			MaxKeys: maxKeys, KeyCount: len(contents) + len(common), IsTruncated: page.IsTruncated,
			ContinuationToken: contToken, StartAfter: enc(startAfter), EncodingType: encType,
			Contents: contents, CommonPrefixes: common,
		}
		if page.IsTruncated {
			res.NextContinuationToken = encodeListToken(page.NextAfter)
		}
		h.writeXML(w, http.StatusOK, res)
		return
	}

	res := listBucketResult{
		XMLNS: s3XMLNS, Name: bucket, Prefix: enc(prefix), Marker: enc(marker),
		Delimiter: enc(delimiter), MaxKeys: maxKeys, IsTruncated: page.IsTruncated,
		EncodingType: encType, Contents: contents, CommonPrefixes: common,
	}
	if page.IsTruncated {
		res.NextMarker = enc(page.NextAfter)
	}
	h.writeXML(w, http.StatusOK, res)
}

// List continuation tokens are opaque to clients; we make them base64 of the
// resume key so a page is a pure function of its token (no server state).
func encodeListToken(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key))
}

func decodeListToken(tok string) (string, bool) {
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return "", false
	}
	return string(b), true
}

func (h *Handler) authorized(r *http.Request) bool {
	if h.cfg.SecretAccessKey == "" || h.cfg.AccessKeyID == "" {
		return false
	}
	if sig := r.URL.Query().Get("X-Amz-Signature"); sig != "" {
		return h.authorizedPresigned(r, sig)
	}
	auth := r.Header.Get("Authorization")
	if auth == "" || !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return false
	}

	fields := parseAuthFields(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "))
	credential := fields["Credential"]
	signedHeaders := fields["SignedHeaders"]
	signature := fields["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return false
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[0] != h.cfg.AccessKeyID {
		return false
	}
	date, region, service := parts[1], parts[2], parts[3]
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return false
	}
	// Reject stale/future-dated signed requests (±15 min), matching AWS SigV4
	// header-auth behavior. Presigned freshness is governed by X-Amz-Expires.
	reqTime, err := time.Parse(awsTimeFormat, amzDate)
	if err != nil {
		return false
	}
	if skew := time.Since(reqTime); skew > 15*time.Minute || skew < -15*time.Minute {
		return false
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonicalRequest := canonicalRequest(r.Method, canonicalURI(r), canonicalQuery(r.URL.Query(), ""), canonicalHeaders(r, signedHeaders), signedHeaders, payloadHash)
	stringToSign := stringToSign(amzDate, date, region, service, canonicalRequest)
	expected := sign(h.cfg.SecretAccessKey, date, region, service, stringToSign)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		h.logger.Warn("sigv4 header-auth mismatch (DEBUG)",
			"method", r.Method,
			"path", r.URL.Path,
			"rawQuery", r.URL.RawQuery,
			"host", r.Host,
			"signedHeaders", signedHeaders,
			"expected", expected,
			"received", signature,
			"canonicalRequest", canonicalRequest,
			"ua", r.Header.Get("User-Agent"),
		)
		return false
	}
	return true
}

func (h *Handler) authorizedPresigned(r *http.Request, provided string) bool {
	q := r.URL.Query()
	credential := q.Get("X-Amz-Credential")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	amzDate := q.Get("X-Amz-Date")
	expires := q.Get("X-Amz-Expires")
	if credential == "" || signedHeaders == "" || amzDate == "" || expires == "" {
		return false
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[0] != h.cfg.AccessKeyID {
		return false
	}
	// Enforce X-Amz-Expires: integer seconds in [1, 604800], and the URL must
	// not be past X-Amz-Date + X-Amz-Expires. Without this, presigned URLs
	// never expire (correctness + security bug, S3-COMPAT-PLAN.md §2.2).
	exp, perr := strconv.Atoi(expires)
	if perr != nil || exp < 1 || exp > 604800 {
		return false
	}
	reqTime, terr := time.Parse(awsTimeFormat, amzDate)
	if terr != nil {
		return false
	}
	if time.Now().UTC().After(reqTime.Add(time.Duration(exp) * time.Second)) {
		return false
	}
	date, region, service := parts[1], parts[2], parts[3]
	payloadHash := "UNSIGNED-PAYLOAD"
	canonicalRequest := canonicalRequest(r.Method, canonicalURI(r), canonicalQuery(q, "X-Amz-Signature"), canonicalHeaders(r, signedHeaders), signedHeaders, payloadHash)
	stringToSign := stringToSign(amzDate, date, region, service, canonicalRequest)
	expected := sign(h.cfg.SecretAccessKey, date, region, service, stringToSign)
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		h.logger.Warn("sigv4 presigned mismatch (DEBUG)",
			"method", r.Method,
			"path", r.URL.Path,
			"signedHeaders", signedHeaders,
			"expected", expected,
			"received", provided,
			"canonicalRequest", canonicalRequest,
			"ua", r.Header.Get("User-Agent"),
		)
		return false
	}
	return true
}

// splitBucketKey resolves the bucket+key from either virtual-hosted
// (<bucket>.<endpointHost>/key) or path-style (/bucket/key) addressing. Vhost
// is only attempted when PublicEndpointURL is configured and the request Host
// is a strict subdomain of it; the bare endpoint host and an unset endpoint
// both fall back to path-style — byte-identical to pre-P7.6 (Gokapi intact).
// SigV4 is unaffected: canonicalHeaders signs r.Host, which the client signed
// with whichever host it addressed.
func (h *Handler) splitBucketKey(r *http.Request) (string, string) {
	if h.endpointHost != "" {
		host := r.Host
		if i := strings.IndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		host = strings.ToLower(host)
		if suffix := "." + h.endpointHost; host != h.endpointHost && strings.HasSuffix(host, suffix) {
			bucket := strings.TrimSuffix(host, suffix)
			key, _ := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/"))
			return bucket, key
		}
	}
	return parsePath(r.URL.Path)
}

func parsePath(path string) (string, string) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	key, _ := url.PathUnescape(parts[1])
	return parts[0], key
}

func parseAuthFields(value string) map[string]string {
	fields := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			fields[kv[0]] = kv[1]
		}
	}
	return fields
}

func canonicalRequest(method, path, query, headers, signedHeaders, payloadHash string) string {
	if path == "" {
		path = "/"
	}
	return strings.Join([]string{method, path, query, headers, signedHeaders, payloadHash}, "\n")
}

const awsTimeFormat = "20060102T150405Z"

// awsListTimeFormat is the ISO8601 millisecond form S3 returns in listings
// (LastModified); s3XMLNS is the namespace strict clients (rclone) require.
const awsListTimeFormat = "2006-01-02T15:04:05.000Z"
const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

const upperHex = "0123456789ABCDEF"

// awsURIEncode implements AWS's SigV4 UriEncode. Go's url.Values.Encode /
// url.URL.EscapedPath do NOT match it (space→'+', different reserved sets),
// which AWS explicitly warns about — use this everywhere a canonical value is
// built. Slash is preserved only in the object key path (encodeSlash=false).
func awsURIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(upperHex[c>>4])
			b.WriteByte(upperHex[c&0x0f])
		}
	}
	return b.String()
}

// canonicalURI is the AWS-encoded request path (the object key's '/' stays
// literal). Derived from the decoded path so a client signing
// UriEncode(key) matches regardless of how the bytes were escaped on the wire.
func canonicalURI(r *http.Request) string {
	if r.URL.Path == "" {
		return "/"
	}
	return awsURIEncode(r.URL.Path, false)
}

// canonicalQuery builds the SigV4 canonical query string: each name and value
// AWS-encoded, an explicit '=' even for empty values, sorted by encoded name
// then encoded value. skip drops a parameter (X-Amz-Signature for presigned).
func canonicalQuery(values url.Values, skip string) string {
	type pair struct{ k, v string }
	var pairs []pair
	for key, vals := range values {
		if key == skip {
			continue
		}
		ek := awsURIEncode(key, true)
		for _, val := range vals {
			pairs = append(pairs, pair{ek, awsURIEncode(val, true)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

func canonicalHeaders(r *http.Request, signedHeaders string) string {
	var builder strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		lower := strings.ToLower(name)
		value := ""
		if lower == "host" {
			value = r.Host
		} else {
			value = r.Header.Get(name)
		}
		builder.WriteString(lower)
		builder.WriteByte(':')
		builder.WriteString(strings.Join(strings.Fields(value), " "))
		builder.WriteByte('\n')
	}
	return builder.String()
}

func stringToSign(amzDate, date, region, service, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s/%s/%s/aws4_request\n%s", amzDate, date, region, service, hex.EncodeToString(sum[:]))
}

func sign(secret, date, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func quoteETag(etag string) string { return `"` + etag + `"` }

func maxKeysOrDefault(maxKeys int) int {
	if maxKeys <= 0 || maxKeys > 1000 {
		return 1000
	}
	return maxKeys
}

func (h *Handler) writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(value)
}

// writeXMLString writes a pre-rendered XML document (used for the canned
// bucket-subresource responses, where struct marshalling of xsi:type / fixed
// namespaces is more trouble than a literal).
func (h *Handler) writeXMLString(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header)
	_, _ = io.WriteString(w, body)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	// RequestId mirrors the x-amz-request-id header (set centrally in
	// ServeHTTP), so the body and header agree without threading the id
	// through every handler.
	h.writeXML(w, status, errorResponse{Code: code, Message: message, RequestID: w.Header().Get("x-amz-request-id")})
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

var _ hash.Hash = md5.New()

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   owner    `xml:"Owner"`
	Buckets buckets  `xml:"Buckets"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type buckets struct {
	Bucket []bucketResult `xml:"Bucket"`
}

type bucketResult struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// listBucketResult is ListObjects v1. listBucketV2Result is ListObjectsV2;
// both serialize to <ListBucketResult> (the version is request-side only).
type listBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	XMLNS          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	EncodingType   string         `xml:"EncodingType,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []objectResult `xml:"Contents"`
	CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
}

type listBucketV2Result struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	XMLNS                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Contents              []objectResult `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type objectResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}
