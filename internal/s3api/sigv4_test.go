package s3api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"telegram-s3/internal/config"
)

// ---- AWS UriEncode (S3-COMPAT-PLAN.md §2.2) --------------------------------

func TestAWSURIEncode(t *testing.T) {
	cases := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"abcXYZ0189", true, "abcXYZ0189"},
		{"-_.~", true, "-_.~"}, // unreserved, never encoded
		{" ", true, "%20"},     // space is %20, never '+'
		{"+", true, "%2B"},
		{"=", true, "%3D"},
		{"&", true, "%26"},
		{"%", true, "%25"},
		{":", true, "%3A"},
		{"@", true, "%40"},
		{"/", true, "%2F"}, // encoded for query components
		{"/", false, "/"},  // preserved in the object key path
		{"a/b", false, "a/b"},
		{"a/b", true, "a%2Fb"},
		{"ü", true, "%C3%BC"},    // UTF-8 bytes, uppercase hex
		{"☃", true, "%E2%98%83"}, // multi-byte rune -> per-byte %XX
		// AWS documentation's own UriEncode example:
		{"/documents and settings/", true, "%2Fdocuments%20and%20settings%2F"},
		{"/documents and settings/", false, "/documents%20and%20settings/"},
	}
	for _, c := range cases {
		if got := awsURIEncode(c.in, c.encodeSlash); got != c.want {
			t.Errorf("awsURIEncode(%q, %v) = %q, want %q", c.in, c.encodeSlash, got, c.want)
		}
	}
}

func TestCanonicalQuery(t *testing.T) {
	cases := []struct {
		name   string
		values url.Values
		skip   string
		want   string
	}{
		{"sorted by name", url.Values{"prefix": {"somePrefix"}, "marker": {"someMarker"}, "max-keys": {"20"}}, "",
			"marker=someMarker&max-keys=20&prefix=somePrefix"},
		{"empty value still emits =", url.Values{"acl": {""}}, "", "acl="},
		{"skip X-Amz-Signature", url.Values{"X-Amz-Signature": {"sig"}, "X-Amz-Date": {"d"}}, "X-Amz-Signature",
			"X-Amz-Date=d"},
		{"space in value -> %20", url.Values{"key": {"a b"}}, "", "key=a%20b"},
		{"duplicate name sorted by value", url.Values{"x": {"b", "a"}}, "", "x=a&x=b"},
		{"uppercase sorts before lowercase", url.Values{"b": {"1"}, "A": {"2"}}, "", "A=2&b=1"},
		{"sort uses the encoded name", url.Values{"z": {"1"}, "é": {"2"}}, "", "%C3%A9=2&z=1"},
		{"empty", url.Values{}, "", ""},
	}
	for _, c := range cases {
		if got := canonicalQuery(c.values, c.skip); got != c.want {
			t.Errorf("%s: canonicalQuery = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestCanonicalURI(t *testing.T) {
	cases := []struct{ path, want string }{
		{"/bucket/key.txt", "/bucket/key.txt"},
		{"/bucket/a b.txt", "/bucket/a%20b.txt"}, // the §2.2 space-in-key bug
		{"/bucket/dir/sub/obj", "/bucket/dir/sub/obj"},
		{"/bucket/résumé.pdf", "/bucket/r%C3%A9sum%C3%A9.pdf"},
		{"", "/"},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "http://h", nil)
		r.URL.Path = c.path
		if got := canonicalURI(r); got != c.want {
			t.Errorf("canonicalURI(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

// ---- signing helpers mirroring the handler, for auth/expiry/skew tests -----

const (
	testAK     = "AKIDEXAMPLE"
	testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testRegion = "us-east-1"
)

func testHandler() *Handler {
	return &Handler{cfg: config.Config{AccessKeyID: testAK, SecretAccessKey: testSecret}}
}

func amz(t time.Time) string { return t.UTC().Format(awsTimeFormat) }

// signHeaderAuth signs r with SigV4 header auth using the handler's own
// canonicalization, so a valid signature isolates the date/skew logic.
func signHeaderAuth(r *http.Request, amzDate string) {
	const sh, ph = "host", "UNSIGNED-PAYLOAD"
	date := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", ph)
	cr := canonicalRequest(r.Method, canonicalURI(r),
		canonicalQuery(r.URL.Query(), ""), canonicalHeaders(r, sh), sh, ph)
	sts := stringToSign(amzDate, date, testRegion, "s3", cr)
	sig := sign(testSecret, date, testRegion, "s3", sts)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAK+"/"+date+
		"/"+testRegion+"/s3/aws4_request, SignedHeaders="+sh+", Signature="+sig)
}

// signPresigned attaches a valid X-Amz-Signature for the given date/expires.
func signPresigned(r *http.Request, amzDate, expires string) {
	const sh, ph = "host", "UNSIGNED-PAYLOAD"
	date := amzDate[:8]
	q := r.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", testAK+"/"+date+"/"+testRegion+"/s3/aws4_request")
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", expires)
	q.Set("X-Amz-SignedHeaders", sh)
	r.URL.RawQuery = q.Encode()
	cr := canonicalRequest(r.Method, canonicalURI(r),
		canonicalQuery(r.URL.Query(), "X-Amz-Signature"), canonicalHeaders(r, sh), sh, ph)
	sts := stringToSign(amzDate, date, testRegion, "s3", cr)
	sig := sign(testSecret, date, testRegion, "s3", sts)
	q.Set("X-Amz-Signature", sig)
	r.URL.RawQuery = q.Encode()
}

// ---- presigned X-Amz-Expires enforcement (§2.2) ---------------------------

func TestPresignedExpiry(t *testing.T) {
	h := testHandler()
	now := time.Now().UTC()

	authPresigned := func(amzDate, expires string) bool {
		r := httptest.NewRequest("GET", "http://example.com/bucket/key", nil)
		signPresigned(r, amzDate, expires)
		return h.authorized(r)
	}

	if !authPresigned(amz(now), "300") {
		t.Error("fresh presigned URL (expires=300) should be authorized")
	}
	if authPresigned(amz(now.Add(-1*time.Hour)), "60") {
		t.Error("presigned URL signed 1h ago with expires=60 must be rejected (expired)")
	}
	if authPresigned(amz(now), "0") {
		t.Error("X-Amz-Expires=0 is out of [1,604800], must be rejected")
	}
	if authPresigned(amz(now), "604801") {
		t.Error("X-Amz-Expires=604801 is out of [1,604800], must be rejected")
	}
	if authPresigned(amz(now), "notanumber") {
		t.Error("non-numeric X-Amz-Expires must be rejected")
	}
}

// ---- header-auth clock skew (±15 min) -------------------------------------

func TestHeaderClockSkew(t *testing.T) {
	h := testHandler()
	now := time.Now().UTC()

	authHeader := func(amzDate string) bool {
		r := httptest.NewRequest("PUT", "http://example.com/bucket/key", nil)
		signHeaderAuth(r, amzDate)
		return h.authorized(r)
	}

	if !authHeader(amz(now)) {
		t.Error("freshly-dated signed request should be authorized")
	}
	if authHeader(amz(now.Add(-30 * time.Minute))) {
		t.Error("request dated 30 min ago (validly signed for that date) must be rejected by skew")
	}
	if authHeader(amz(now.Add(30 * time.Minute))) {
		t.Error("request dated 30 min in the future must be rejected by skew")
	}
}

// ---- §2.2 acceptance: a signed request with a space in the key verifies ---

func TestSignedRequestWithSpaceInKey(t *testing.T) {
	h := testHandler()
	r := httptest.NewRequest("PUT", "http://example.com/send/a%20b.txt", nil)
	signHeaderAuth(r, amz(time.Now().UTC()))
	if !h.authorized(r) {
		t.Error("validly signed request with a space in the key must verify (UriEncode fix)")
	}
}
