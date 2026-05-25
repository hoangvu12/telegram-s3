package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// rangeData is 50 bytes (data[i] == i) so a fakeBackend with chunkSize 4 splits
// it into 13 chunks — every multi-chunk range below straddles boundaries.
func rangeData() []byte {
	b := make([]byte, 50)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func getWith(r *mpRig, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "http://example.com"+target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

// putRangeObject creates the bucket + a chunked object and returns (data, ETag).
func putRangeObject(t *testing.T, r *mpRig, key string, data []byte) string {
	t.Helper()
	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}
	if rec := r.do(http.MethodPut, "/send/"+key, data); rec.Code != http.StatusOK {
		t.Fatalf("put object: %d %s", rec.Code, rec.Body)
	}
	return `"` + md5hex(data) + `"`
}

func TestRangeFullGetRegression(t *testing.T) {
	r := newMPRig(t)
	data := rangeData()
	etag := putRangeObject(t, r, "obj", data)

	rec := getWith(r, "/send/obj", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("body mismatch: got %d bytes", rec.Body.Len())
	}
	if cl := rec.Header().Get("Content-Length"); cl != fmt.Sprint(len(data)) {
		t.Fatalf("Content-Length = %s, want %d", cl, len(data))
	}
	if ar := rec.Header().Get("Accept-Ranges"); ar != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", ar)
	}
	if rec.Header().Get("ETag") != etag {
		t.Fatalf("ETag = %s, want %s", rec.Header().Get("ETag"), etag)
	}
	if rec.Header().Get("Content-Range") != "" {
		t.Fatalf("unexpected Content-Range on full GET: %q", rec.Header().Get("Content-Range"))
	}
}

func TestRangeSatisfiable(t *testing.T) {
	r := newMPRig(t)
	data := rangeData()
	putRangeObject(t, r, "obj", data)

	cases := []struct {
		name, hdr  string
		start, end int64 // expected resolved range (inclusive)
	}{
		{"mid cross-chunk", "bytes=10-19", 10, 19},
		{"single byte", "bytes=0-0", 0, 0},
		{"open ended", "bytes=45-", 45, 49},
		{"suffix", "bytes=-7", 43, 49},
		{"end past size clamps", "bytes=40-1000", 40, 49},
		{"suffix larger than size", "bytes=-100", 0, 49},
		{"whole via explicit range", "bytes=0-49", 0, 49},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := getWith(r, "/send/obj", map[string]string{"Range": c.hdr})
			if rec.Code != http.StatusPartialContent {
				t.Fatalf("status = %d, want 206 (body %s)", rec.Code, rec.Body)
			}
			wantCR := fmt.Sprintf("bytes %d-%d/%d", c.start, c.end, len(data))
			if cr := rec.Header().Get("Content-Range"); cr != wantCR {
				t.Fatalf("Content-Range = %q, want %q", cr, wantCR)
			}
			wantLen := c.end - c.start + 1
			if cl := rec.Header().Get("Content-Length"); cl != fmt.Sprint(wantLen) {
				t.Fatalf("Content-Length = %s, want %d", cl, wantLen)
			}
			if ar := rec.Header().Get("Accept-Ranges"); ar != "bytes" {
				t.Fatalf("Accept-Ranges = %q, want bytes", ar)
			}
			if got, want := rec.Body.Bytes(), data[c.start:c.end+1]; !bytes.Equal(got, want) {
				t.Fatalf("body = %v, want %v", got, want)
			}
		})
	}
}

func TestRangeUnsatisfiable(t *testing.T) {
	r := newMPRig(t)
	data := rangeData()
	putRangeObject(t, r, "obj", data)

	for _, hdr := range []string{"bytes=50-60", "bytes=50-", "bytes=100-200"} {
		t.Run(hdr, func(t *testing.T) {
			rec := getWith(r, "/send/obj", map[string]string{"Range": hdr})
			if rec.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Fatalf("status = %d, want 416", rec.Code)
			}
			wantCR := fmt.Sprintf("bytes */%d", len(data))
			if cr := rec.Header().Get("Content-Range"); cr != wantCR {
				t.Fatalf("Content-Range = %q, want %q", cr, wantCR)
			}
			if ar := rec.Header().Get("Accept-Ranges"); ar != "bytes" {
				t.Fatalf("Accept-Ranges = %q, want bytes", ar)
			}
			var er errorResponse
			if err := xml.Unmarshal(rec.Body.Bytes(), &er); err != nil || er.Code != "InvalidRange" {
				t.Fatalf("error body = %s (parse err %v), want code InvalidRange", rec.Body, err)
			}
		})
	}
}

// Malformed / unsupported Range headers must be ignored and the full object
// returned with 200 (S3 / RFC 7233 behavior).
func TestRangeIgnoredFallsBackToFull(t *testing.T) {
	r := newMPRig(t)
	data := rangeData()
	putRangeObject(t, r, "obj", data)

	for _, hdr := range []string{"bytes=abc", "bytes=", "bytes=10-5", "bytes=0-1,3-4", "items=0-1", "0-1"} {
		t.Run(hdr, func(t *testing.T) {
			rec := getWith(r, "/send/obj", map[string]string{"Range": hdr})
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (Range %q ignored)", rec.Code, hdr)
			}
			if !bytes.Equal(rec.Body.Bytes(), data) {
				t.Fatalf("body mismatch for ignored Range %q", hdr)
			}
			if cr := rec.Header().Get("Content-Range"); cr != "" {
				t.Fatalf("unexpected Content-Range %q for ignored Range", cr)
			}
		})
	}
}

func TestRangeIfRange(t *testing.T) {
	r := newMPRig(t)
	data := rangeData()
	etag := putRangeObject(t, r, "obj", data)

	// Matching ETag → honored (206).
	rec := getWith(r, "/send/obj", map[string]string{"Range": "bytes=0-9", "If-Range": etag})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("matching If-Range: status = %d, want 206", rec.Code)
	}

	// Stale ETag → ignored, full object (200).
	rec = getWith(r, "/send/obj", map[string]string{"Range": "bytes=0-9", "If-Range": `"deadbeef"`})
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("stale If-Range: status = %d (want 200 + full body)", rec.Code)
	}
}

func TestRangeEmptyObject(t *testing.T) {
	r := newMPRig(t)
	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d", rec.Code)
	}
	if rec := r.do(http.MethodPut, "/send/empty", []byte{}); rec.Code != http.StatusOK {
		t.Fatalf("put empty: %d %s", rec.Code, rec.Body)
	}

	// Full GET of an empty object: 200, no body.
	if rec := getWith(r, "/send/empty", nil); rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("full empty GET: status %d len %d, want 200/0", rec.Code, rec.Body.Len())
	}
	// Any range on a zero-byte object is unsatisfiable.
	rec := getWith(r, "/send/empty", map[string]string{"Range": "bytes=0-0"})
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("range on empty: status %d, want 416", rec.Code)
	}
	if cr := rec.Header().Get("Content-Range"); cr != "bytes */0" {
		t.Fatalf("Content-Range = %q, want bytes */0", cr)
	}
}

