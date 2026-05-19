//go:build s3live

// Opt-in live round-trip tests for the Phase 1 acceptance gate
// (S3-COMPAT-PLAN.md §Phase 1). Excluded from normal `go test`; run with:
//
//	TELEGRAM_S3_ENDPOINT=http://localhost:9000 \
//	TELEGRAM_S3_ACCESS_KEY=... TELEGRAM_S3_SECRET_KEY=... \
//	TELEGRAM_S3_BUCKET=send \
//	go test -tags s3live -run Live -v ./internal/s3api/
//
// Self-contained: implements the SigV4 client side and the aws-chunked wire
// framing directly, so it pulls in zero third-party code (no AWS SDK, no
// license entanglement) and faithfully reproduces what a modern client sends.
package s3api

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type liveEnv struct {
	endpoint, ak, sk, region, bucket string
}

func liveConfig(t *testing.T) liveEnv {
	t.Helper()
	e := liveEnv{
		endpoint: strings.TrimRight(os.Getenv("TELEGRAM_S3_ENDPOINT"), "/"),
		ak:       os.Getenv("TELEGRAM_S3_ACCESS_KEY"),
		sk:       os.Getenv("TELEGRAM_S3_SECRET_KEY"),
		region:   os.Getenv("TELEGRAM_S3_REGION"),
		bucket:   os.Getenv("TELEGRAM_S3_BUCKET"),
	}
	if e.endpoint == "" || e.ak == "" || e.sk == "" {
		t.Skip("set TELEGRAM_S3_ENDPOINT, TELEGRAM_S3_ACCESS_KEY, TELEGRAM_S3_SECRET_KEY to run live tests")
	}
	if e.region == "" {
		e.region = "us-east-1"
	}
	if e.bucket == "" {
		e.bucket = "send"
	}
	return e
}

func itestSize() int {
	if v := os.Getenv("TELEGRAM_S3_ITEST_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 5 << 20 // 5 MiB, matching the plan's Phase 1 acceptance (./5MB.bin)
}

// --- minimal SigV4 client signer -------------------------------------------

func sigKey(secret, date, region, service string) []byte {
	h := func(k []byte, s string) []byte { m := hmac.New(sha256.New, k); m.Write([]byte(s)); return m.Sum(nil) }
	return h(h(h(h([]byte("AWS4"+secret), date), region), service), "aws4_request")
}

// signedRequest builds and signs a SigV4 request. canonicalHeaders are the
// headers to sign (lowercase name -> value), host always added.
func (e liveEnv) signedRequest(t *testing.T, method, path string, headers map[string]string, payloadHash string, body io.Reader, contentLength int64) *http.Request {
	t.Helper()
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")

	req, err := http.NewRequest(method, e.endpoint+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if contentLength >= 0 {
		req.ContentLength = contentLength
	}
	host := req.URL.Host

	signed := map[string]string{
		"host":                 host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	for k, v := range headers {
		signed[strings.ToLower(k)] = v
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)

	names := make([]string, 0, len(signed))
	for k := range signed {
		names = append(names, k)
	}
	// simple insertion sort (small set), avoids importing sort for clarity
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j-1] > names[j]; j-- {
			names[j-1], names[j] = names[j], names[j-1]
		}
	}
	var ch strings.Builder
	for _, n := range names {
		ch.WriteString(n)
		ch.WriteByte(':')
		ch.WriteString(strings.Join(strings.Fields(signed[n]), " "))
		ch.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")

	canonicalReq := strings.Join([]string{
		method, req.URL.EscapedPath(), "", ch.String(), signedHeaders, payloadHash,
	}, "\n")
	sum := sha256.Sum256([]byte(canonicalReq))
	scope := date + "/" + e.region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	mac := hmac.New(sha256.New, sigKey(e.sk, date, e.region, "s3"))
	mac.Write([]byte(stringToSign))
	sig := hex.EncodeToString(mac.Sum(nil))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		e.ak, scope, signedHeaders, sig))
	return req
}

func (e liveEnv) ensureBucket(t *testing.T) {
	t.Helper()
	req := e.signedRequest(t, http.MethodPut, "/"+e.bucket, nil, "UNSIGNED-PAYLOAD", nil, 0)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	defer resp.Body.Close()
	// 200 = created; 409 BucketAlreadyOwnedByYou = already there. Both fine.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create bucket: status %d: %s", resp.StatusCode, b)
	}
}

// getAndVerify fetches the (public) object and asserts byte-identity + headers.
func (e liveEnv) getAndVerify(t *testing.T, key string, want []byte) {
	t.Helper()
	resp, err := http.Get(e.endpoint + "/" + e.bucket + "/" + key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("get: status %d: %s", resp.StatusCode, b)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d (sha differs)", len(got), len(want))
	}
	if cl := resp.Header.Get("Content-Length"); cl != strconv.Itoa(len(want)) {
		t.Fatalf("Content-Length = %q, want %d", cl, len(want))
	}
	wantETag := `"` + hex.EncodeToString(md5sum(want)) + `"`
	if et := resp.Header.Get("ETag"); et != wantETag {
		t.Fatalf("ETag = %s, want %s (MD5 of decoded content)", et, wantETag)
	}
}

func md5sum(b []byte) []byte { h := md5.Sum(b); return h[:] }

func (e liveEnv) cleanup(t *testing.T, key string) {
	req := e.signedRequest(t, http.MethodDelete, "/"+e.bucket+"/"+key, nil, "UNSIGNED-PAYLOAD", nil, 0)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

// --- the keystone: STREAMING-UNSIGNED-PAYLOAD-TRAILER round-trip ------------

func TestLiveChunkedRoundTrip(t *testing.T) {
	e := liveConfig(t)
	e.ensureBucket(t)

	size := itestSize()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*131 + 7) // deterministic, non-trivial
	}
	key := fmt.Sprintf("phase1-itest-chunked-%d.bin", time.Now().UnixNano())
	defer e.cleanup(t, key)

	// Frame as aws-chunked, 64 KiB chunks, with the CRC32 trailer a real
	// client sends (server ignores it in Phase 1 but a faithful client sends
	// the correct value).
	const chunk = 64 << 10
	var framed bytes.Buffer
	for off := 0; off < len(payload); off += chunk {
		end := off + chunk
		if end > len(payload) {
			end = len(payload)
		}
		d := payload[off:end]
		fmt.Fprintf(&framed, "%x\r\n", len(d))
		framed.Write(d)
		framed.WriteString("\r\n")
	}
	framed.WriteString("0\r\n")
	var crcBE [4]byte
	binary.BigEndian.PutUint32(crcBE[:], crc32.ChecksumIEEE(payload))
	fmt.Fprintf(&framed, "x-amz-checksum-crc32:%s\r\n\r\n", base64.StdEncoding.EncodeToString(crcBE[:]))

	headers := map[string]string{
		"Content-Encoding":             "aws-chunked",
		"X-Amz-Decoded-Content-Length": strconv.Itoa(len(payload)),
		"X-Amz-Trailer":                "x-amz-checksum-crc32",
	}
	req := e.signedRequest(t, http.MethodPut, "/"+e.bucket+"/"+key, headers,
		"STREAMING-UNSIGNED-PAYLOAD-TRAILER", bytes.NewReader(framed.Bytes()), int64(framed.Len()))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put: status %d: %s", resp.StatusCode, body)
	}
	if et := resp.Header.Get("ETag"); et != `"`+hex.EncodeToString(md5sum(payload))+`"` {
		t.Fatalf("PUT ETag = %s, want MD5 of decoded content (de-framing failed)", et)
	}

	e.getAndVerify(t, key, payload)
}

// --- regression guard: UNSIGNED-PAYLOAD (aws-sdk-go v1 / Gokapi path) -------

func TestLiveUnsignedPayloadRoundTrip(t *testing.T) {
	e := liveConfig(t)
	e.ensureBucket(t)

	payload := bytes.Repeat([]byte("gokapi-regression-"), 4096) // ~73 KiB
	key := fmt.Sprintf("phase1-itest-plain-%d.bin", time.Now().UnixNano())
	defer e.cleanup(t, key)

	req := e.signedRequest(t, http.MethodPut, "/"+e.bucket+"/"+key, nil,
		"UNSIGNED-PAYLOAD", bytes.NewReader(payload), int64(len(payload)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put: status %d: %s", resp.StatusCode, body)
	}
	e.getAndVerify(t, key, payload)
}
