package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
	"telegram-s3/internal/storage"
)

// fakeBackend is an in-memory storage.Backend that splits like the real one.
type fakeBackend struct {
	mu        sync.Mutex
	chunkSize int
	files     map[string][]byte
	deleted   map[int64]bool
	seq       int64
}

func newFakeBackend(chunkSize int) *fakeBackend {
	return &fakeBackend{chunkSize: chunkSize, files: map[string][]byte{}, deleted: map[int64]bool{}}
}

func (f *fakeBackend) Upload(_ context.Context, _, _ string, body io.Reader) ([]storage.Chunk, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	var chunks []storage.Chunk
	for off, seq := 0, 0; off < len(data); seq++ {
		end := off + f.chunkSize
		if end > len(data) {
			end = len(data)
		}
		piece := append([]byte(nil), data[off:end]...)
		f.mu.Lock()
		f.seq++
		fid := fmt.Sprintf("f%d", f.seq)
		mid := 1000 + f.seq
		f.files[fid] = piece
		f.mu.Unlock()
		chunks = append(chunks, storage.Chunk{Seq: seq, FileID: fid, MessageID: mid, Size: int64(len(piece)), Offset: int64(off)})
		off = end
	}
	return chunks, nil
}

func (f *fakeBackend) get(fileID string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.files[fileID]
	return b, ok
}

func (f *fakeBackend) Download(_ context.Context, fileID string) (io.ReadCloser, error) {
	b, ok := f.get(fileID)
	if !ok {
		return nil, fmt.Errorf("no such file %s", fileID)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeBackend) DownloadRange(_ context.Context, fileID string, offset, length int64) (io.ReadCloser, error) {
	b, ok := f.get(fileID)
	if !ok {
		return nil, fmt.Errorf("no such file %s", fileID)
	}
	if offset > int64(len(b)) {
		offset = int64(len(b))
	}
	b = b[offset:]
	if length > 0 && length < int64(len(b)) {
		b = b[:length]
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeBackend) Delete(_ context.Context, messageID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted[messageID] = true
	delete(f.files, fmt.Sprintf("f%d", messageID-1000))
	return nil
}

// --- test rig ---------------------------------------------------------------

type mpRig struct {
	t  *testing.T
	h  *Handler
	be *fakeBackend
}

func newMPRig(t *testing.T) *mpRig {
	t.Helper()
	return newMPRigWithChunkSize(t, 4) // default: tiny chunks so parts span many messages
}

// newMPRigWithChunkSize lets a test pick a larger backend chunk size so it
// can use realistic part sizes (e.g., the 5 MiB minPartSize for 8.2) without
// paying the fake backend's per-chunk locking cost.
func newMPRigWithChunkSize(t *testing.T, chunkSize int) *mpRig {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "mp.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	be := newFakeBackend(chunkSize)
	cfg := config.Config{AccessKeyID: testAK, SecretAccessKey: testSecret}
	h := NewHandler(cfg, store, be, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &mpRig{t: t, h: h, be: be}
}

// do signs (write verbs) and dispatches a request, returning the recorder.
func (r *mpRig) do(method, target string, body []byte) *httptest.ResponseRecorder {
	r.t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://example.com"+target, rdr)
	if method != http.MethodGet && method != http.MethodHead {
		signHeaderAuth(req, amz(time.Now().UTC()))
	}
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

func TestMultipartUploadHappyPath(t *testing.T) {
	// Part 1 must be >= 5 MiB per AWS rules (8.2). Bump backend chunk size so
	// the test runs in well under a second.
	r := newMPRigWithChunkSize(t, 1<<24)

	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}

	// CreateMultipartUpload
	rec := r.do(http.MethodPost, "/send/big.bin?uploads", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("create mpu: %d %s", rec.Code, rec.Body)
	}
	var init initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &init); err != nil || init.UploadID == "" {
		t.Fatalf("init parse: %v body=%s", err, rec.Body)
	}
	uid := init.UploadID

	part1 := bytes.Repeat([]byte("a"), minPartSize)     // 5 MiB exactly
	part2 := []byte("and here is the second part, end") // 32 bytes (last part is exempt)

	put := func(n int, data []byte) string {
		rec := r.do(http.MethodPut, fmt.Sprintf("/send/big.bin?partNumber=%d&uploadId=%s", n, uid), data)
		if rec.Code != http.StatusOK {
			t.Fatalf("uploadPart %d: %d %s", n, rec.Code, rec.Body)
		}
		et := rec.Header().Get("ETag")
		want := `"` + md5hex(data) + `"`
		if et != want {
			t.Fatalf("part %d ETag = %s, want %s", n, et, want)
		}
		return et
	}
	put(1, part1)
	put(2, part2)

	// ListParts
	rec = r.do(http.MethodGet, "/send/big.bin?uploadId="+uid, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list parts: %d %s", rec.Code, rec.Body)
	}
	var lp listPartsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &lp); err != nil {
		t.Fatalf("list parts parse: %v", err)
	}
	if len(lp.Parts) != 2 || lp.Parts[0].Size != int64(len(part1)) || lp.Parts[1].Size != int64(len(part2)) {
		t.Fatalf("list parts = %+v", lp.Parts)
	}

	// CompleteMultipartUpload
	body := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`,
		md5hex(part1), md5hex(part2))
	rec = r.do(http.MethodPost, "/send/big.bin?uploadId="+uid, []byte(body))
	if rec.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}
	var cr completeMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatalf("complete parse: %v", err)
	}
	wantETag := `"` + multipartETag(part1, part2) + `"`
	if cr.ETag != wantETag {
		t.Fatalf("complete ETag = %s, want %s", cr.ETag, wantETag)
	}

	// GET reassembles the object byte-identically across all part chunks.
	rec = r.do(http.MethodGet, "/send/big.bin", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	full := append(append([]byte(nil), part1...), part2...)
	if !bytes.Equal(rec.Body.Bytes(), full) {
		t.Fatalf("reassembled %q, want %q", rec.Body.Bytes(), full)
	}
	if cl := rec.Header().Get("Content-Length"); cl != fmt.Sprint(len(full)) {
		t.Fatalf("Content-Length = %s, want %d", cl, len(full))
	}
	if rec.Header().Get("ETag") != wantETag {
		t.Fatalf("GET ETag = %s, want %s", rec.Header().Get("ETag"), wantETag)
	}

	// The multipart bookkeeping is gone after completion.
	if rec := r.do(http.MethodGet, "/send/big.bin?uploadId="+uid, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("list parts after complete = %d, want 404", rec.Code)
	}
}

func TestMultipartAbortFreesMessages(t *testing.T) {
	r := newMPRig(t)
	r.do(http.MethodPut, "/send", nil)

	var init initiateMultipartUploadResult
	xml.Unmarshal(r.do(http.MethodPost, "/send/x?uploads", nil).Body.Bytes(), &init)
	uid := init.UploadID
	r.do(http.MethodPut, "/send/x?partNumber=1&uploadId="+uid, []byte("abcdefghij")) // 3 chunks (size 4)

	r.be.mu.Lock()
	live := len(r.be.files)
	r.be.mu.Unlock()
	if live == 0 {
		t.Fatal("expected uploaded chunk files before abort")
	}

	if rec := r.do(http.MethodDelete, "/send/x?uploadId="+uid, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("abort: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	defer r.be.mu.Unlock()
	if len(r.be.files) != 0 {
		t.Fatalf("abort did not free Telegram messages: %d files remain", len(r.be.files))
	}
	if len(r.be.deleted) != live {
		t.Fatalf("abort deleted %d messages, want %d", len(r.be.deleted), live)
	}
}

func TestMultipartErrorCases(t *testing.T) {
	r := newMPRig(t)
	r.do(http.MethodPut, "/send", nil)
	var init initiateMultipartUploadResult
	xml.Unmarshal(r.do(http.MethodPost, "/send/e?uploads", nil).Body.Bytes(), &init)
	uid := init.UploadID
	r.do(http.MethodPut, "/send/e?partNumber=1&uploadId="+uid, []byte("partone"))
	r.do(http.MethodPut, "/send/e?partNumber=2&uploadId="+uid, []byte("parttwo"))

	cases := []struct {
		name, method, target string
		body                 string
		want                 int
	}{
		{"bad uploadId", http.MethodPost, "/send/e?uploadId=deadbeef",
			`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>x</ETag></Part></CompleteMultipartUpload>`, http.StatusNotFound},
		{"partNumber out of range", http.MethodPut, "/send/e?partNumber=0&uploadId=" + uid, "data", http.StatusBadRequest},
		{"wrong etag", http.MethodPost, "/send/e?uploadId=" + uid,
			`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"deadbeef"</ETag></Part></CompleteMultipartUpload>`, http.StatusBadRequest},
		{"non-ascending parts", http.MethodPost, "/send/e?uploadId=" + uid,
			fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>"%s"</ETag></Part><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`,
				md5hex([]byte("parttwo")), md5hex([]byte("partone"))), http.StatusBadRequest},
		{"empty parts list", http.MethodPost, "/send/e?uploadId=" + uid,
			`<CompleteMultipartUpload></CompleteMultipartUpload>`, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := r.do(c.method, c.target, []byte(c.body))
			if rec.Code != c.want {
				t.Fatalf("%s: status %d, want %d (body %s)", c.name, rec.Code, c.want, rec.Body)
			}
		})
	}
}

// 8.2: AWS rejects multipart uploads whose non-last parts are smaller than
// 5 MiB with EntityTooSmall. The last part is exempt; a single-part complete
// is always the last by definition.
func TestMultipartCompleteEntityTooSmall(t *testing.T) {
	bigPart := bytes.Repeat([]byte("A"), minPartSize)      // 5 MiB exactly
	bigPlus := bytes.Repeat([]byte("B"), minPartSize+1024) // > 5 MiB
	tinyPart := []byte("tiny")                             // < 5 MiB

	uploadPart := func(t *testing.T, r *mpRig, uid string, n int, data []byte) string {
		t.Helper()
		rec := r.do(http.MethodPut, fmt.Sprintf("/send/k?partNumber=%d&uploadId=%s", n, uid), data)
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d: %d %s", n, rec.Code, rec.Body)
		}
		return md5hex(data)
	}
	completeBody := func(parts ...[2]string) string { // (partNumber, hex-md5)
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for _, p := range parts {
			fmt.Fprintf(&b, `<Part><PartNumber>%s</PartNumber><ETag>"%s"</ETag></Part>`, p[0], p[1])
		}
		b.WriteString("</CompleteMultipartUpload>")
		return b.String()
	}
	startUpload := func(t *testing.T) (*mpRig, string) {
		t.Helper()
		// Use a backend chunk size near the real 18 MiB so a 5 MiB part is
		// one backend chunk — otherwise the fake backend's per-chunk locking
		// dominates the test runtime.
		r := newMPRigWithChunkSize(t, 1<<24)
		seedBucket(t, r)
		var init initiateMultipartUploadResult
		xml.Unmarshal(r.do(http.MethodPost, "/send/k?uploads", nil).Body.Bytes(), &init)
		return r, init.UploadID
	}

	t.Run("two undersized parts, second is last → 400 on first", func(t *testing.T) {
		r, uid := startUpload(t)
		e1 := uploadPart(t, r, uid, 1, tinyPart)
		e2 := uploadPart(t, r, uid, 2, tinyPart)
		rec := r.do(http.MethodPost, "/send/k?uploadId="+uid,
			[]byte(completeBody([2]string{"1", e1}, [2]string{"2", e2})))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400 (body %s)", rec.Code, rec.Body)
		}
		var er errorResponse
		if err := xml.Unmarshal(rec.Body.Bytes(), &er); err != nil || er.Code != "EntityTooSmall" {
			t.Fatalf("error code = %q (%v), want EntityTooSmall", er.Code, err)
		}
		// Upload row stays so the client can issue a clean abort.
		if rec := r.do(http.MethodGet, "/send/k?uploadId="+uid, nil); rec.Code != http.StatusOK {
			t.Fatalf("listParts after EntityTooSmall: %d, want 200 (upload row missing)", rec.Code)
		}
	})

	t.Run("single tiny part succeeds (last by definition)", func(t *testing.T) {
		r, uid := startUpload(t)
		e1 := uploadPart(t, r, uid, 1, tinyPart)
		rec := r.do(http.MethodPost, "/send/k?uploadId="+uid,
			[]byte(completeBody([2]string{"1", e1})))
		if rec.Code != http.StatusOK {
			t.Fatalf("single-part complete: %d %s", rec.Code, rec.Body)
		}
	})

	t.Run("middle part undersized → 400", func(t *testing.T) {
		r, uid := startUpload(t)
		e1 := uploadPart(t, r, uid, 1, bigPart)
		e2 := uploadPart(t, r, uid, 2, tinyPart)
		e3 := uploadPart(t, r, uid, 3, bigPlus)
		rec := r.do(http.MethodPost, "/send/k?uploadId="+uid,
			[]byte(completeBody([2]string{"1", e1}, [2]string{"2", e2}, [2]string{"3", e3})))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400 (body %s)", rec.Code, rec.Body)
		}
	})

	t.Run("only last part undersized → 200", func(t *testing.T) {
		r, uid := startUpload(t)
		e1 := uploadPart(t, r, uid, 1, bigPart)
		e2 := uploadPart(t, r, uid, 2, bigPlus)
		e3 := uploadPart(t, r, uid, 3, tinyPart)
		rec := r.do(http.MethodPost, "/send/k?uploadId="+uid,
			[]byte(completeBody([2]string{"1", e1}, [2]string{"2", e2}, [2]string{"3", e3})))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d, want 200 (body %s)", rec.Code, rec.Body)
		}
	})
}

// 8.5: PutObject overwriting an existing key must reap the prior version's
// Telegram messages. Otherwise long-running buckets grow unbounded chunks.
func TestPutObjectOverwriteReapsChunks(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	// First PUT: 10 bytes -> 3 chunks (size 4, 4, 2) -> message IDs 1001..1003.
	if rec := r.do(http.MethodPut, "/send/k", []byte("abcdefghij")); rec.Code != http.StatusOK {
		t.Fatalf("put 1: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	firstMsgs := make([]int64, 0, len(r.be.files))
	for fid := range r.be.files {
		var n int64
		fmt.Sscanf(fid, "f%d", &n)
		firstMsgs = append(firstMsgs, 1000+n)
	}
	r.be.mu.Unlock()
	if len(firstMsgs) == 0 {
		t.Fatal("expected backend files after first PUT")
	}

	// Second PUT: different bytes -> different chunks. Old messages must go.
	if rec := r.do(http.MethodPut, "/send/k", []byte("xy")); rec.Code != http.StatusOK {
		t.Fatalf("put 2: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	defer r.be.mu.Unlock()
	for _, mid := range firstMsgs {
		if !r.be.deleted[mid] {
			t.Fatalf("first PUT's message %d was not reaped after overwrite", mid)
		}
	}
}

// 8.5 (legacy): a pre-Phase-3 object has no object_chunks rows but a
// non-zero TelegramMessageID. Reap that one message on overwrite too.
func TestPutObjectOverwriteReapsLegacy(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	// Manually seed a legacy single-message row directly in the store.
	legacyMID := int64(4242)
	legacy := metadata.Object{
		Bucket: "send", Key: "legacy", Size: 7, ETag: "deadbeef",
		ContentType:    "application/octet-stream",
		TelegramFileID: "f4242", TelegramMessageID: legacyMID,
	}
	if err := r.h.store.PutObject(context.Background(), legacy, nil); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	// PUT same key -> reap the legacy message.
	if rec := r.do(http.MethodPut, "/send/legacy", []byte("new")); rec.Code != http.StatusOK {
		t.Fatalf("put overwrite legacy: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	defer r.be.mu.Unlock()
	if !r.be.deleted[legacyMID] {
		t.Fatalf("legacy message %d not reaped on overwrite", legacyMID)
	}
}

// 8.5: a failing backend.Delete on reap must NOT 5xx the (successful) PUT.
func TestPutObjectOverwriteReapErrorTolerated(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	// First PUT, record its message ids.
	if rec := r.do(http.MethodPut, "/send/k", []byte("abcd")); rec.Code != http.StatusOK {
		t.Fatalf("put 1: %d %s", rec.Code, rec.Body)
	}
	// Wrap the backend so the first reap call fails. The PUT must still 200.
	fb := r.be
	r.h.backend = &failingDeleteBackend{Backend: fb, failNext: true}
	if rec := r.do(http.MethodPut, "/send/k", []byte("xyz")); rec.Code != http.StatusOK {
		t.Fatalf("put 2 with failing reap: %d %s, want 200", rec.Code, rec.Body)
	}
}

// 8.5 (multipart): CompleteMultipartUpload overwriting an existing key reaps
// the prior version's chunks (mirrors putObject).
func TestCompleteMultipartOverwriteReapsChunks(t *testing.T) {
	// Need realistic part sizes; bump chunk to keep the test fast.
	r := newMPRigWithChunkSize(t, 1<<24)
	seedBucket(t, r)

	// First write via plain PUT -> messageID 1001 (single backend chunk).
	if rec := r.do(http.MethodPut, "/send/big.bin", []byte("first version")); rec.Code != http.StatusOK {
		t.Fatalf("put 1: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	firstMsgs := make([]int64, 0, len(r.be.files))
	for fid := range r.be.files {
		var n int64
		fmt.Sscanf(fid, "f%d", &n)
		firstMsgs = append(firstMsgs, 1000+n)
	}
	r.be.mu.Unlock()
	if len(firstMsgs) == 0 {
		t.Fatal("expected backend files after first PUT")
	}

	// Overwrite via single-part MPU.
	var init initiateMultipartUploadResult
	xml.Unmarshal(r.do(http.MethodPost, "/send/big.bin?uploads", nil).Body.Bytes(), &init)
	uid := init.UploadID
	part := []byte("second version (single tiny part — last is exempt from 5 MiB rule)")
	pr := r.do(http.MethodPut, fmt.Sprintf("/send/big.bin?partNumber=1&uploadId=%s", uid), part)
	if pr.Code != http.StatusOK {
		t.Fatalf("upload part: %d %s", pr.Code, pr.Body)
	}
	body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, pr.Header().Get("ETag"))
	if rec := r.do(http.MethodPost, "/send/big.bin?uploadId="+uid, []byte(body)); rec.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	defer r.be.mu.Unlock()
	for _, mid := range firstMsgs {
		if !r.be.deleted[mid] {
			t.Fatalf("multipart complete did not reap prior message %d", mid)
		}
	}
}

// failingDeleteBackend wraps a backend and fails the first Delete call so
// 8.5's "best-effort reap" branch can be exercised end-to-end.
type failingDeleteBackend struct {
	storage.Backend
	failNext bool
}

func (f *failingDeleteBackend) Delete(ctx context.Context, mid int64) error {
	if f.failNext {
		f.failNext = false
		return fmt.Errorf("simulated telegram delete failure for %d", mid)
	}
	return f.Backend.Delete(ctx, mid)
}

func md5hex(b []byte) string { s := md5.Sum(b); return hex.EncodeToString(s[:]) }

func multipartETag(parts ...[]byte) string {
	var concat []byte
	for _, p := range parts {
		s := md5.Sum(p)
		concat = append(concat, s[:]...)
	}
	sum := md5.Sum(concat)
	return fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(parts))
}
