package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Listing is not a public read (no object key), so it must be signed — unlike
// the object GETs that getWith covers.
func signedGet(r *mpRig, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "http://example.com"+target, nil)
	signHeaderAuth(req, amz(time.Now().UTC()))
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

func seedBucket(t *testing.T, r *mpRig, keys ...string) {
	t.Helper()
	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", rec.Code, rec.Body)
	}
	for _, k := range keys {
		if rec := r.do(http.MethodPut, "/send/"+k, []byte("v")); rec.Code != http.StatusOK {
			t.Fatalf("put %s: %d %s", k, rec.Code, rec.Body)
		}
	}
}

func listV1(t *testing.T, r *mpRig, target string) listBucketResult {
	t.Helper()
	rec := signedGet(r, target)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: %d %s", target, rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `xmlns="`+s3XMLNS+`"`) {
		t.Fatalf("missing/incorrect xmlns in %s", rec.Body)
	}
	var res listBucketResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal v1: %v body=%s", err, rec.Body)
	}
	return res
}

func listV2(t *testing.T, r *mpRig, target string) listBucketV2Result {
	t.Helper()
	rec := signedGet(r, target)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: %d %s", target, rec.Code, rec.Body)
	}
	var res listBucketV2Result
	if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal v2: %v body=%s", err, rec.Body)
	}
	return res
}

func keysOf(c []objectResult) []string {
	out := make([]string, len(c))
	for i, o := range c {
		out[i] = o.Key
	}
	return out
}

func prefixesOf(c []commonPrefix) []string {
	out := make([]string, len(c))
	for i, p := range c {
		out[i] = p.Prefix
	}
	return out
}

func eq(t *testing.T, what string, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func TestListV1Basic(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a", "b", "c")

	res := listV1(t, r, "/send")
	if res.Name != "send" || res.MaxKeys != 1000 || res.IsTruncated {
		t.Fatalf("v1 envelope = %+v", res)
	}
	eq(t, "v1 keys", keysOf(res.Contents), []string{"a", "b", "c"})
	if len(res.CommonPrefixes) != 0 {
		t.Fatalf("unexpected CommonPrefixes: %v", prefixesOf(res.CommonPrefixes))
	}
}

func TestListV2Basic(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a", "b", "c")

	res := listV2(t, r, "/send?list-type=2")
	if res.KeyCount != 3 || res.IsTruncated {
		t.Fatalf("v2 envelope = %+v", res)
	}
	eq(t, "v2 keys", keysOf(res.Contents), []string{"a", "b", "c"})
}

func TestListPrefix(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "x/1", "x/2", "y/1")

	res := listV2(t, r, "/send?list-type=2&prefix=x/")
	eq(t, "prefix keys", keysOf(res.Contents), []string{"x/1", "x/2"})
}

func TestListDelimiterRollup(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "p/a", "p/b", "q/c", "top")

	res := listV2(t, r, "/send?list-type=2&delimiter=/")
	eq(t, "delim keys", keysOf(res.Contents), []string{"top"})
	eq(t, "delim prefixes", prefixesOf(res.CommonPrefixes), []string{"p/", "q/"})
	if res.KeyCount != 3 { // 1 key + 2 common prefixes
		t.Fatalf("KeyCount = %d, want 3", res.KeyCount)
	}
}

func TestListPrefixPlusDelimiter(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "d/x/1", "d/x/2", "d/y", "d/z/1")

	res := listV2(t, r, "/send?list-type=2&prefix=d/&delimiter=/")
	eq(t, "keys", keysOf(res.Contents), []string{"d/y"})
	eq(t, "prefixes", prefixesOf(res.CommonPrefixes), []string{"d/x/", "d/z/"})
}

func TestListV2Pagination(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "k1", "k2", "k3", "k4", "k5")

	var got []string
	target := "/send?list-type=2&max-keys=2"
	for page := 1; ; page++ {
		res := listV2(t, r, target)
		got = append(got, keysOf(res.Contents)...)
		if !res.IsTruncated {
			if res.NextContinuationToken != "" {
				t.Fatalf("page %d not truncated but has NextContinuationToken", page)
			}
			break
		}
		if res.NextContinuationToken == "" {
			t.Fatalf("page %d truncated but no NextContinuationToken", page)
		}
		target = "/send?list-type=2&max-keys=2&continuation-token=" + url.QueryEscape(res.NextContinuationToken)
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	eq(t, "paged v2 keys", got, []string{"k1", "k2", "k3", "k4", "k5"})
}

func TestListV1MarkerPagination(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "m1", "m2", "m3", "m4", "m5")

	var got []string
	target := "/send?max-keys=2"
	for page := 1; ; page++ {
		res := listV1(t, r, target)
		got = append(got, keysOf(res.Contents)...)
		if !res.IsTruncated {
			break
		}
		if res.NextMarker == "" {
			t.Fatalf("page %d truncated but no NextMarker", page)
		}
		target = "/send?max-keys=2&marker=" + url.QueryEscape(res.NextMarker)
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	eq(t, "paged v1 keys", got, []string{"m1", "m2", "m3", "m4", "m5"})
}

// Pagination must resume correctly *through* a delimiter rollup: each page
// returns exactly one new common prefix, none duplicated or skipped.
func TestListDelimiterPagination(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a/1", "a/2", "b/1", "c/1")

	var got []string
	target := "/send?list-type=2&delimiter=/&max-keys=1"
	for page := 1; ; page++ {
		res := listV2(t, r, target)
		got = append(got, prefixesOf(res.CommonPrefixes)...)
		if len(res.Contents) != 0 {
			t.Fatalf("page %d: unexpected Contents %v", page, keysOf(res.Contents))
		}
		if !res.IsTruncated {
			break
		}
		target = "/send?list-type=2&delimiter=/&max-keys=1&continuation-token=" + url.QueryEscape(res.NextContinuationToken)
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	eq(t, "paged prefixes", got, []string{"a/", "b/", "c/"})
}

func TestListStartAfterV2(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a", "b", "c", "d")

	res := listV2(t, r, "/send?list-type=2&start-after=b")
	eq(t, "start-after keys", keysOf(res.Contents), []string{"c", "d"})
}

func TestListEncodingTypeURL(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "a+b", "c*d/x")

	// Without encoding-type the raw bytes are returned verbatim.
	plain := listV2(t, r, "/send?list-type=2&delimiter=/")
	eq(t, "plain keys", keysOf(plain.Contents), []string{"a+b"})
	eq(t, "plain prefixes", prefixesOf(plain.CommonPrefixes), []string{"c*d/"})
	if plain.EncodingType != "" {
		t.Fatalf("EncodingType = %q, want empty", plain.EncodingType)
	}

	// With encoding-type=url the AWS UriEncode is applied to key-ish fields.
	enc := listV2(t, r, "/send?list-type=2&delimiter=/&encoding-type=url")
	eq(t, "encoded keys", keysOf(enc.Contents), []string{"a%2Bb"})
	eq(t, "encoded prefixes", prefixesOf(enc.CommonPrefixes), []string{"c%2Ad%2F"})
	if enc.EncodingType != "url" || enc.Delimiter != "%2F" {
		t.Fatalf("encoded envelope: EncodingType=%q Delimiter=%q", enc.EncodingType, enc.Delimiter)
	}
}

func TestListEmptyBucket(t *testing.T) {
	r := newMPRig(t)
	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d", rec.Code)
	}
	v2 := listV2(t, r, "/send?list-type=2")
	if v2.KeyCount != 0 || v2.IsTruncated || len(v2.Contents) != 0 {
		t.Fatalf("empty v2 = %+v", v2)
	}
	v1 := listV1(t, r, "/send")
	if v1.IsTruncated || len(v1.Contents) != 0 {
		t.Fatalf("empty v1 = %+v", v1)
	}
}

// The bucket-emptiness probe still uses the listing path; deletion must stay
// blocked while objects exist and succeed once the namespace is clear.
func TestDeleteBucketEmptinessRegression(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "k")

	if rec := r.do(http.MethodDelete, "/send", nil); rec.Code != http.StatusConflict {
		t.Fatalf("delete non-empty bucket: %d %s, want 409", rec.Code, rec.Body)
	}
	if rec := r.do(http.MethodDelete, "/send/k", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete object: %d %s", rec.Code, rec.Body)
	}
	if rec := r.do(http.MethodDelete, "/send", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete now-empty bucket: %d %s, want 204", rec.Code, rec.Body)
	}
}
