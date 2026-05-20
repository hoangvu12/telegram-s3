package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// reqWith issues an unsigned request (the object GET/HEAD public-read path)
// with extra headers — used for conditional + response-override assertions.
func reqWith(r *mpRig, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "http://example.com"+target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

func TestObjectMetadataEcho(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	rec := doWithHeaders(r, http.MethodPut, "/send/k", []byte("hello"), map[string]string{
		"Content-Disposition": `attachment; filename="x.txt"`,
		"Content-Encoding":    "gzip",
		"Cache-Control":       "max-age=60",
		"X-Amz-Meta-Foo":      "bar",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}

	for _, m := range []string{http.MethodGet, http.MethodHead} {
		rec := reqWith(r, m, "/send/k", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", m, rec.Code, rec.Body)
		}
		h := rec.Header()
		if got := h.Get("Content-Disposition"); got != `attachment; filename="x.txt"` {
			t.Fatalf("%s Content-Disposition = %q", m, got)
		}
		if got := h.Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("%s Content-Encoding = %q", m, got)
		}
		if got := h.Get("Cache-Control"); got != "max-age=60" {
			t.Fatalf("%s Cache-Control = %q (stored value must override the immutable default)", m, got)
		}
		if got := h.Get("X-Amz-Meta-Foo"); got != "bar" {
			t.Fatalf("%s X-Amz-Meta-Foo = %q", m, got)
		}
	}
}

func TestObjectResponseOverrides(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)
	doWithHeaders(r, http.MethodPut, "/send/k", []byte("hello"), map[string]string{
		"Content-Type":        "text/plain",
		"Content-Disposition": "inline",
	})

	rec := reqWith(r, http.MethodGet,
		"/send/k?response-content-type=application/json&response-content-disposition=attachment&response-cache-control=no-cache", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	h := rec.Header()
	if h.Get("Content-Type") != "application/json" {
		t.Fatalf("response-content-type override failed: %q", h.Get("Content-Type"))
	}
	if h.Get("Content-Disposition") != "attachment" {
		t.Fatalf("response-content-disposition override failed: %q", h.Get("Content-Disposition"))
	}
	if h.Get("Cache-Control") != "no-cache" {
		t.Fatalf("response-cache-control override failed: %q", h.Get("Cache-Control"))
	}
}

func TestConditionalRequests(t *testing.T) {
	r := newMPRig(t)
	data := []byte("hello")
	etag := putRangeObject(t, r, "k", data) // returns quoted etag

	// If-None-Match matching current ETag → 304, no body (GET and HEAD).
	for _, m := range []string{http.MethodGet, http.MethodHead} {
		rec := reqWith(r, m, "/send/k", map[string]string{"If-None-Match": etag})
		if rec.Code != http.StatusNotModified {
			t.Fatalf("%s If-None-Match: %d, want 304", m, rec.Code)
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("%s 304 must have no body", m)
		}
		if rec.Header().Get("ETag") != etag {
			t.Fatalf("%s 304 missing ETag validator", m)
		}
	}

	// If-None-Match: * also → 304.
	if rec := reqWith(r, http.MethodGet, "/send/k", map[string]string{"If-None-Match": "*"}); rec.Code != http.StatusNotModified {
		t.Fatalf("If-None-Match * : %d, want 304", rec.Code)
	}
	// Stale If-None-Match → full 200 body.
	if rec := reqWith(r, http.MethodGet, "/send/k", map[string]string{"If-None-Match": `"deadbeef"`}); rec.Code != http.StatusOK || rec.Body.Len() != len(data) {
		t.Fatalf("stale If-None-Match: %d len %d, want 200/%d", rec.Code, rec.Body.Len(), len(data))
	}
	// If-Match mismatch → 412.
	rec := reqWith(r, http.MethodGet, "/send/k", map[string]string{"If-Match": `"deadbeef"`})
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-Match mismatch: %d, want 412", rec.Code)
	}
	var er errorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &er); err != nil || er.Code != "PreconditionFailed" {
		t.Fatalf("412 error = %q (%v)", er.Code, err)
	}
	// If-Match hit → 200.
	if rec := reqWith(r, http.MethodGet, "/send/k", map[string]string{"If-Match": etag}); rec.Code != http.StatusOK {
		t.Fatalf("If-Match hit: %d, want 200", rec.Code)
	}

	future := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	if rec := reqWith(r, http.MethodGet, "/send/k", map[string]string{"If-Modified-Since": future}); rec.Code != http.StatusNotModified {
		t.Fatalf("If-Modified-Since future: %d, want 304", rec.Code)
	}
	if rec := reqWith(r, http.MethodGet, "/send/k", map[string]string{"If-Unmodified-Since": past}); rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-Unmodified-Since past: %d, want 412", rec.Code)
	}
}

func TestMultipartMetadataCarry(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	rec := doWithHeaders(r, http.MethodPost, "/send/m?uploads", nil, map[string]string{
		"X-Amz-Meta-Owner":    "alice",
		"Content-Disposition": "attachment",
	})
	var init initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &init); err != nil || init.UploadID == "" {
		t.Fatalf("create mpu: %v body=%s", err, rec.Body)
	}
	uid := init.UploadID

	part := []byte("multipart body data")
	pr := r.do(http.MethodPut, fmt.Sprintf("/send/m?partNumber=1&uploadId=%s", uid), part)
	if pr.Code != http.StatusOK {
		t.Fatalf("upload part: %d %s", pr.Code, pr.Body)
	}
	pe := pr.Header().Get("ETag")
	body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, pe)
	if cr := r.do(http.MethodPost, "/send/m?uploadId="+uid, []byte(body)); cr.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", cr.Code, cr.Body)
	}

	hd := reqWith(r, http.MethodHead, "/send/m", nil)
	if hd.Code != http.StatusOK {
		t.Fatalf("head: %d", hd.Code)
	}
	if hd.Header().Get("X-Amz-Meta-Owner") != "alice" || hd.Header().Get("Content-Disposition") != "attachment" {
		t.Fatalf("MPU metadata not carried: %v", hd.Header())
	}
}

// 8.4: x-amz-checksum-* headers are persisted at PUT and echoed verbatim on
// GET/HEAD. The body is NOT re-verified server-side; this is a parity feature.
func TestChecksumHeadersEcho(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	rec := doWithHeaders(r, http.MethodPut, "/send/k", []byte("hello"), map[string]string{
		"x-amz-checksum-crc32":     "AAAAAA==",
		"x-amz-checksum-algorithm": "CRC32",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("put: %d %s", rec.Code, rec.Body)
	}
	for _, m := range []string{http.MethodGet, http.MethodHead} {
		rec := reqWith(r, m, "/send/k", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", m, rec.Code, rec.Body)
		}
		if got := rec.Header().Get("X-Amz-Checksum-Crc32"); got != "AAAAAA==" {
			t.Fatalf("%s X-Amz-Checksum-Crc32 = %q", m, got)
		}
		if got := rec.Header().Get("X-Amz-Checksum-Algorithm"); got != "CRC32" {
			t.Fatalf("%s X-Amz-Checksum-Algorithm = %q", m, got)
		}
	}
}

func TestChecksumHeadersAbsent(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)
	doWithHeaders(r, http.MethodPut, "/send/k", []byte("hi"), nil)
	rec := reqWith(r, http.MethodGet, "/send/k", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d", rec.Code)
	}
	// An object without any checksum headers must not emit empty placeholders.
	for _, k := range []string{
		"X-Amz-Checksum-Crc32", "X-Amz-Checksum-Crc32c",
		"X-Amz-Checksum-Sha1", "X-Amz-Checksum-Sha256",
		"X-Amz-Checksum-Algorithm",
	} {
		if v := rec.Header().Get(k); v != "" {
			t.Fatalf("%s should be absent, got %q", k, v)
		}
	}
}

// 8.4 (multipart): a checksum on CreateMultipartUpload rides
// multipart_upload_metadata onto the finalized object, exactly like
// Content-Disposition / x-amz-meta-* already do.
func TestChecksumHeadersMultipartCarry(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	rec := doWithHeaders(r, http.MethodPost, "/send/m?uploads", nil, map[string]string{
		"x-amz-checksum-sha256":    "abc123=",
		"x-amz-checksum-algorithm": "SHA256",
	})
	var init initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &init); err != nil || init.UploadID == "" {
		t.Fatalf("create mpu: %v body=%s", err, rec.Body)
	}
	uid := init.UploadID

	part := []byte("multipart body data")
	pr := r.do(http.MethodPut, fmt.Sprintf("/send/m?partNumber=1&uploadId=%s", uid), part)
	if pr.Code != http.StatusOK {
		t.Fatalf("upload part: %d %s", pr.Code, pr.Body)
	}
	pe := pr.Header().Get("ETag")
	body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, pe)
	if cr := r.do(http.MethodPost, "/send/m?uploadId="+uid, []byte(body)); cr.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", cr.Code, cr.Body)
	}

	hd := reqWith(r, http.MethodGet, "/send/m", nil)
	if hd.Code != http.StatusOK {
		t.Fatalf("get: %d", hd.Code)
	}
	if hd.Header().Get("X-Amz-Checksum-Sha256") != "abc123=" {
		t.Fatalf("checksum not carried: %v", hd.Header())
	}
	if hd.Header().Get("X-Amz-Checksum-Algorithm") != "SHA256" {
		t.Fatalf("algorithm not carried: %v", hd.Header())
	}
}

// Regression: x-amz-meta-* still works after the checksum capture is added.
func TestChecksumHeadersRegressionAmzMeta(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	doWithHeaders(r, http.MethodPut, "/send/k", []byte("v"), map[string]string{
		"X-Amz-Meta-Foo": "bar",
	})
	hd := reqWith(r, http.MethodHead, "/send/k", nil)
	if hd.Header().Get("X-Amz-Meta-Foo") != "bar" {
		t.Fatalf("x-amz-meta-* lost: %v", hd.Header())
	}
}

func TestCopyMetadataDirective(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)
	doWithHeaders(r, http.MethodPut, "/send/src", []byte("payload"), map[string]string{
		"Content-Type":   "text/plain",
		"X-Amz-Meta-Foo": "fromsrc",
	})

	// Default directive = COPY: source content-type + metadata carried.
	if rec := doWithHeaders(r, http.MethodPut, "/send/cp", nil, map[string]string{
		"X-Amz-Copy-Source": "/send/src",
	}); rec.Code != http.StatusOK {
		t.Fatalf("copy: %d %s", rec.Code, rec.Body)
	}
	hd := reqWith(r, http.MethodHead, "/send/cp", nil)
	if hd.Header().Get("X-Amz-Meta-Foo") != "fromsrc" || hd.Header().Get("Content-Type") != "text/plain" {
		t.Fatalf("COPY directive lost source metadata: %v", hd.Header())
	}

	// REPLACE: request headers win, source metadata discarded.
	if rec := doWithHeaders(r, http.MethodPut, "/send/rp", nil, map[string]string{
		"X-Amz-Copy-Source":        "/send/src",
		"X-Amz-Metadata-Directive": "REPLACE",
		"Content-Type":             "application/json",
		"X-Amz-Meta-Bar":           "fresh",
	}); rec.Code != http.StatusOK {
		t.Fatalf("copy replace: %d %s", rec.Code, rec.Body)
	}
	hd = reqWith(r, http.MethodHead, "/send/rp", nil)
	if hd.Header().Get("X-Amz-Meta-Bar") != "fresh" || hd.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("REPLACE directive not applied: %v", hd.Header())
	}
	if hd.Header().Get("X-Amz-Meta-Foo") != "" {
		t.Fatalf("REPLACE must drop source metadata, got X-Amz-Meta-Foo=%q", hd.Header().Get("X-Amz-Meta-Foo"))
	}
}
