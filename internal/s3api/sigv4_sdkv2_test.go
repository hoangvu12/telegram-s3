package s3api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestSDKv2StyleListBuckets reproduces the canonical request shape AWS SDK Go v2
// (which rclone v1.74 uses) sends for ListBuckets: GET /?x-id=ListBuckets, with
// SignedHeaders including amz-sdk-invocation-id;amz-sdk-request;host;
// x-amz-content-sha256;x-amz-date. The signature is computed here with stdlib
// HMAC/SHA256 (NOT the handler's helpers), so a passing test confirms the
// handler's verifier is compatible with the SDK v2 canonical shape — and a
// failing test pinpoints the gateway-side incompatibility.
func TestSDKv2StyleListBuckets(t *testing.T) {
	h := testHandler()

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := amzDate[:8]
	region, service := "us-east-1", "s3"
	host := "s3.nguyenvu.dev"
	emptyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	r := httptest.NewRequest("GET", "https://"+host+"/?x-id=ListBuckets", nil)
	r.Host = host
	r.Header.Set("Amz-Sdk-Invocation-Id", "55d98928-e4e8-4b41-9b31-aeb72f6aae5c")
	r.Header.Set("Amz-Sdk-Request", "attempt=1; max=10")
	r.Header.Set("X-Amz-Content-Sha256", emptyHash)
	r.Header.Set("X-Amz-Date", amzDate)

	// Build canonical request with stdlib only — independent of handler helpers.
	signed := []string{
		"amz-sdk-invocation-id",
		"amz-sdk-request",
		"host",
		"x-amz-content-sha256",
		"x-amz-date",
	}
	sort.Strings(signed)
	signedHeaders := strings.Join(signed, ";")

	values := map[string]string{
		"amz-sdk-invocation-id": "55d98928-e4e8-4b41-9b31-aeb72f6aae5c",
		"amz-sdk-request":       "attempt=1; max=10",
		"host":                  host,
		"x-amz-content-sha256":  emptyHash,
		"x-amz-date":            amzDate,
	}
	var hdrBuf strings.Builder
	for _, n := range signed {
		hdrBuf.WriteString(n)
		hdrBuf.WriteByte(':')
		hdrBuf.WriteString(strings.Join(strings.Fields(values[n]), " "))
		hdrBuf.WriteByte('\n')
	}

	canonReq := strings.Join([]string{
		"GET",
		"/",
		"x-id=ListBuckets",
		hdrBuf.String(),
		signedHeaders,
		emptyHash,
	}, "\n")

	stsHash := sha256.Sum256([]byte(canonReq))
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + date + "/" + region + "/" + service + "/aws4_request\n" + hex.EncodeToString(stsHash[:])

	mac := func(key []byte, data string) []byte {
		m := hmac.New(sha256.New, key)
		m.Write([]byte(data))
		return m.Sum(nil)
	}
	kDate := mac([]byte("AWS4"+testSecret), date)
	kRegion := mac(kDate, region)
	kService := mac(kRegion, service)
	kSigning := mac(kService, "aws4_request")
	sig := hex.EncodeToString(mac(kSigning, sts))

	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+testAK+"/"+date+"/"+region+"/"+service+"/aws4_request, "+
			"SignedHeaders="+signedHeaders+", Signature="+sig)

	if !h.authorized(r) {
		t.Errorf("handler rejected a valid SDK-Go-v2-style ListBuckets request\ncanonicalRequest=\n%s\nstringToSign=\n%s", canonReq, sts)
	}
}

// Also ensure the request without amz-sdk-* headers — what the AWS CLI sends —
// still works, so we know the SDK-v2 case is the only differentiator.
func TestSDKv2StyleListBuckets_CLIShape(t *testing.T) {
	h := testHandler()

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := amzDate[:8]
	region, service := "us-east-1", "s3"
	host := "s3.nguyenvu.dev"
	emptyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	r := httptest.NewRequest("GET", "https://"+host+"/", nil)
	r.Host = host
	r.Header.Set("X-Amz-Content-Sha256", emptyHash)
	r.Header.Set("X-Amz-Date", amzDate)

	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	signedHeaders := strings.Join(signed, ";")

	values := map[string]string{
		"host":                 host,
		"x-amz-content-sha256": emptyHash,
		"x-amz-date":           amzDate,
	}
	var hdrBuf strings.Builder
	for _, n := range signed {
		hdrBuf.WriteString(n)
		hdrBuf.WriteByte(':')
		hdrBuf.WriteString(values[n])
		hdrBuf.WriteByte('\n')
	}

	canonReq := strings.Join([]string{"GET", "/", "", hdrBuf.String(), signedHeaders, emptyHash}, "\n")

	stsHash := sha256.Sum256([]byte(canonReq))
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + date + "/" + region + "/" + service + "/aws4_request\n" + hex.EncodeToString(stsHash[:])

	mac := func(key []byte, data string) []byte {
		m := hmac.New(sha256.New, key)
		m.Write([]byte(data))
		return m.Sum(nil)
	}
	kDate := mac([]byte("AWS4"+testSecret), date)
	kRegion := mac(kDate, region)
	kService := mac(kRegion, service)
	kSigning := mac(kService, "aws4_request")
	sig := hex.EncodeToString(mac(kSigning, sts))

	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+testAK+"/"+date+"/"+region+"/"+service+"/aws4_request, "+
			"SignedHeaders="+signedHeaders+", Signature="+sig)

	if !h.authorized(r) {
		t.Errorf("handler rejected a CLI-shaped ListBuckets request — baseline broken")
	}
}
