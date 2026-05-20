package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// doWithHeaders signs a write verb and attaches extra headers (x-amz-copy-*).
func doWithHeaders(r *mpRig, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://example.com"+target, rdr)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	signHeaderAuth(req, amz(time.Now().UTC()))
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

func TestCopyObject(t *testing.T) {
	r := newMPRig(t)
	data := rangeData() // 50 bytes → many 4-byte chunks in the rig
	putRangeObject(t, r, "src", data)

	rec := doWithHeaders(r, http.MethodPut, "/send/dst", nil,
		map[string]string{"X-Amz-Copy-Source": "/send/src"})
	if rec.Code != http.StatusOK {
		t.Fatalf("copy: %d %s", rec.Code, rec.Body)
	}
	var cr copyObjectResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatalf("parse CopyObjectResult: %v body=%s", err, rec.Body)
	}
	if want := `"` + md5hex(data) + `"`; cr.ETag != want {
		t.Fatalf("copy ETag = %s, want %s", cr.ETag, want)
	}

	got := getWith(r, "/send/dst", nil)
	if got.Code != http.StatusOK || !bytes.Equal(got.Body.Bytes(), data) {
		t.Fatalf("dst bytes mismatch: status %d len %d", got.Code, got.Body.Len())
	}
	if et := got.Header().Get("ETag"); et != `"`+md5hex(data)+`"` {
		t.Fatalf("dst GET ETag = %s", et)
	}
}

func TestCopyObjectMissingSource(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)
	rec := doWithHeaders(r, http.MethodPut, "/send/dst", nil,
		map[string]string{"X-Amz-Copy-Source": "/send/ghost"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	var er errorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &er); err != nil || er.Code != "NoSuchKey" {
		t.Fatalf("error = %q (%v), want NoSuchKey", er.Code, err)
	}
}

// Overwriting an existing destination must reap its superseded Telegram
// messages (the copy = re-upload overwrite-orphan guard).
func TestCopyObjectOverwriteReapsOld(t *testing.T) {
	r := newMPRig(t)
	data := rangeData()
	putRangeObject(t, r, "src", data)
	if rec := r.do(http.MethodPut, "/send/dst", []byte("old destination payload")); rec.Code != http.StatusOK {
		t.Fatalf("seed dst: %d %s", rec.Code, rec.Body)
	}

	r.be.mu.Lock()
	before := len(r.be.deleted)
	r.be.mu.Unlock()

	rec := doWithHeaders(r, http.MethodPut, "/send/dst", nil,
		map[string]string{"X-Amz-Copy-Source": "/send/src"})
	if rec.Code != http.StatusOK {
		t.Fatalf("copy: %d %s", rec.Code, rec.Body)
	}

	r.be.mu.Lock()
	after := len(r.be.deleted)
	r.be.mu.Unlock()
	if after <= before {
		t.Fatalf("expected superseded dst chunks reaped (deleted %d → %d)", before, after)
	}

	got := getWith(r, "/send/dst", nil)
	if got.Code != http.StatusOK || !bytes.Equal(got.Body.Bytes(), data) {
		t.Fatalf("dst after copy mismatch: status %d", got.Code)
	}
}

func TestUploadPartCopy(t *testing.T) {
	// minPartSize-enforcement (8.2) means a non-last part must be >= 5 MiB,
	// so the source object is sized accordingly; chunk size is bumped to keep
	// the fake backend's per-chunk locking out of the test runtime.
	r := newMPRigWithChunkSize(t, 1<<24)
	src := bytes.Repeat([]byte("x"), minPartSize+128) // 5 MiB + tail
	putRangeObject(t, r, "src", src)

	var init initiateMultipartUploadResult
	xml.Unmarshal(r.do(http.MethodPost, "/send/dst?uploads", nil).Body.Bytes(), &init)
	uid := init.UploadID
	if uid == "" {
		t.Fatal("no upload id")
	}

	// Part 1: full source. Part 2: a sub-range of the source.
	rec := doWithHeaders(r, http.MethodPut,
		fmt.Sprintf("/send/dst?partNumber=1&uploadId=%s", uid), nil,
		map[string]string{"X-Amz-Copy-Source": "/send/src"})
	if rec.Code != http.StatusOK {
		t.Fatalf("uploadPartCopy 1: %d %s", rec.Code, rec.Body)
	}
	var cp1 copyPartResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &cp1); err != nil {
		t.Fatalf("parse CopyPartResult: %v body=%s", err, rec.Body)
	}
	if want := `"` + md5hex(src) + `"`; cp1.ETag != want {
		t.Fatalf("part1 ETag = %s, want %s", cp1.ETag, want)
	}

	rec = doWithHeaders(r, http.MethodPut,
		fmt.Sprintf("/send/dst?partNumber=2&uploadId=%s", uid), nil,
		map[string]string{"X-Amz-Copy-Source": "/send/src", "X-Amz-Copy-Source-Range": "bytes=2-5"})
	if rec.Code != http.StatusOK {
		t.Fatalf("uploadPartCopy 2: %d %s", rec.Code, rec.Body)
	}
	var cp2 copyPartResult
	xml.Unmarshal(rec.Body.Bytes(), &cp2)
	slice := src[2:6]
	if want := `"` + md5hex(slice) + `"`; cp2.ETag != want {
		t.Fatalf("part2 ETag = %s, want %s (slice %v)", cp2.ETag, want, slice)
	}

	body := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		cp1.ETag, cp2.ETag)
	if rec := r.do(http.MethodPost, "/send/dst?uploadId="+uid, []byte(body)); rec.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rec.Code, rec.Body)
	}

	got := getWith(r, "/send/dst", nil)
	want := append(append([]byte(nil), src...), slice...)
	if got.Code != http.StatusOK || !bytes.Equal(got.Body.Bytes(), want) {
		t.Fatalf("reassembled = %v, want %v (status %d)", got.Body.Bytes(), want, got.Code)
	}
}
