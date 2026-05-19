//go:build s3live

// Opt-in live Phase 7 acceptance against a running gateway. Reuses liveEnv /
// liveConfig from integration_test.go but signs with the package's own
// canonical functions so query subresources (?location, ?delete) verify —
// the Phase 1 harness signs an empty canonical query and would 403 here.
//
//	TELEGRAM_S3_ENDPOINT=http://localhost:9099 \
//	TELEGRAM_S3_ACCESS_KEY=... TELEGRAM_S3_SECRET_KEY=... TELEGRAM_S3_BUCKET=p7 \
//	go test -tags s3live -run LivePhase7 -v ./internal/s3api/
package s3api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// sign7 builds a SigV4 (header-auth) request using the gateway's own
// canonicalization, so the signature matches regardless of query params.
func (e liveEnv) sign7(t *testing.T, method, rawURL string, headers map[string]string, body []byte) *http.Request {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, e.endpoint+rawURL, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	now := time.Now().UTC()
	amzDate := now.Format(awsTimeFormat)
	date := amzDate[:8]
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	const sh = "host"
	ch := "host:" + strings.Join(strings.Fields(host), " ") + "\n"
	cr := canonicalRequest(method, awsURIEncode(req.URL.Path, false),
		canonicalQuery(req.URL.Query(), ""), ch, sh, "UNSIGNED-PAYLOAD")
	sts := stringToSign(amzDate, date, e.region, "s3", cr)
	sig := sign(e.sk, date, e.region, "s3", sts)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+e.ak+"/"+date+
		"/"+e.region+"/s3/aws4_request, SignedHeaders="+sh+", Signature="+sig)
	return req
}

func (e liveEnv) do(t *testing.T, req *http.Request) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func TestLivePhase7(t *testing.T) {
	e := liveConfig(t)
	e.ensureBucket(t)
	stamp := time.Now().UnixNano()

	t.Run("P7.1 subresource probes", func(t *testing.T) {
		st, _, b := e.do(t, e.sign7(t, http.MethodGet, "/"+e.bucket+"?location", nil, nil))
		if st != 200 || !strings.Contains(string(b), "<LocationConstraint") {
			t.Fatalf("?location: %d %s", st, b)
		}
		st, _, b = e.do(t, e.sign7(t, http.MethodGet, "/"+e.bucket+"?tagging", nil, nil))
		if st != 404 || !strings.Contains(string(b), "NoSuchTagSet") {
			t.Fatalf("?tagging: %d %s", st, b)
		}
		st, _, b = e.do(t, e.sign7(t, http.MethodGet, "/"+e.bucket+"?uploads", nil, nil))
		if st != 200 || !strings.Contains(string(b), "ListMultipartUploadsResult") {
			t.Fatalf("?uploads: %d %s", st, b)
		}
	})

	t.Run("P7.2 DeleteObjects bulk", func(t *testing.T) {
		k1 := fmt.Sprintf("p7-del-a-%d", stamp)
		k2 := fmt.Sprintf("p7-del-b-%d", stamp)
		for _, k := range []string{k1, k2} {
			if st, _, b := e.do(t, e.sign7(t, http.MethodPut, "/"+e.bucket+"/"+k, nil, []byte("x"))); st != 200 {
				t.Fatalf("put %s: %d %s", k, st, b)
			}
		}
		body := fmt.Sprintf(`<Delete><Object><Key>%s</Key></Object><Object><Key>%s</Key></Object></Delete>`, k1, k2)
		st, _, b := e.do(t, e.sign7(t, http.MethodPost, "/"+e.bucket+"?delete", nil, []byte(body)))
		if st != 200 || !strings.Contains(string(b), "<DeleteResult") {
			t.Fatalf("delete: %d %s", st, b)
		}
		if resp, _ := http.Get(e.endpoint + "/" + e.bucket + "/" + k1); resp != nil {
			resp.Body.Close()
			if resp.StatusCode != 404 {
				t.Fatalf("%s should be 404 after bulk delete, got %d", k1, resp.StatusCode)
			}
		}
	})

	t.Run("P7.4 CopyObject", func(t *testing.T) {
		src := fmt.Sprintf("p7-src-%d", stamp)
		dst := fmt.Sprintf("p7-dst-%d", stamp)
		defer e.cleanup(t, src)
		defer e.cleanup(t, dst)
		payload := bytes.Repeat([]byte("copy-live-"), 512) // ~5 KiB
		if st, _, b := e.do(t, e.sign7(t, http.MethodPut, "/"+e.bucket+"/"+src, nil, payload)); st != 200 {
			t.Fatalf("put src: %d %s", st, b)
		}
		st, _, b := e.do(t, e.sign7(t, http.MethodPut, "/"+e.bucket+"/"+dst,
			map[string]string{"X-Amz-Copy-Source": "/" + e.bucket + "/" + src}, nil))
		if st != 200 || !strings.Contains(string(b), "<CopyObjectResult") {
			t.Fatalf("copy: %d %s", st, b)
		}
		e.getAndVerify(t, dst, payload)
	})

	t.Run("P7.5 metadata + conditional", func(t *testing.T) {
		k := fmt.Sprintf("p7-meta-%d", stamp)
		defer e.cleanup(t, k)
		st, _, b := e.do(t, e.sign7(t, http.MethodPut, "/"+e.bucket+"/"+k,
			map[string]string{"X-Amz-Meta-Foo": "bar", "Content-Disposition": "attachment"},
			[]byte("hello-meta")))
		if st != 200 {
			t.Fatalf("put: %d %s", st, b)
		}
		hresp, err := http.Head(e.endpoint + "/" + e.bucket + "/" + k)
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		hresp.Body.Close()
		if hresp.Header.Get("X-Amz-Meta-Foo") != "bar" || hresp.Header.Get("Content-Disposition") != "attachment" {
			t.Fatalf("metadata not echoed: %v", hresp.Header)
		}
		etag := hresp.Header.Get("ETag")
		req, _ := http.NewRequest(http.MethodGet, e.endpoint+"/"+e.bucket+"/"+k, nil)
		req.Header.Set("If-None-Match", etag)
		cresp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("conditional get: %v", err)
		}
		cresp.Body.Close()
		if cresp.StatusCode != http.StatusNotModified {
			t.Fatalf("If-None-Match: status %d, want 304", cresp.StatusCode)
		}
	})

	t.Run("P7.6 vhost + request-id", func(t *testing.T) {
		k := fmt.Sprintf("p7-vh-%d", stamp)
		defer e.cleanup(t, k)
		want := []byte("vhost-bytes")
		if st, _, b := e.do(t, e.sign7(t, http.MethodPut, "/"+e.bucket+"/"+k, nil, want)); st != 200 {
			t.Fatalf("put: %d %s", st, b)
		}
		// Virtual-hosted: Host = <bucket>.<endpointHost>, path is just the key.
		req, _ := http.NewRequest(http.MethodGet, e.endpoint+"/"+k, nil)
		req.Host = e.bucket + ".localhost:9099"
		st, hdr, b := e.do(t, req)
		if st != 200 || !bytes.Equal(b, want) {
			t.Fatalf("vhost GET: %d, body match %v", st, bytes.Equal(b, want))
		}
		if hdr.Get("X-Amz-Request-Id") == "" {
			t.Fatal("missing x-amz-request-id on success")
		}
		// Error body carries <RequestId>.
		eresp, _ := http.Get(e.endpoint + "/" + e.bucket + "/p7-missing-" + fmt.Sprint(stamp))
		eb, _ := io.ReadAll(eresp.Body)
		eresp.Body.Close()
		if eresp.StatusCode != 404 || !strings.Contains(string(eb), "<RequestId>") {
			t.Fatalf("error body: %d %s", eresp.StatusCode, eb)
		}
	})
}
