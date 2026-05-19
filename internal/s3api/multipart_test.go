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
	store, err := metadata.Open(filepath.Join(t.TempDir(), "mp.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	be := newFakeBackend(4) // tiny chunks: parts span multiple messages
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
	r := newMPRig(t)

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

	part1 := []byte("hello world, this is part one!!")  // 31 bytes
	part2 := []byte("and here is the second part, end") // 32 bytes

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
