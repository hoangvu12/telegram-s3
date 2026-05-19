package s3api

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
)

func newVhostRig(t *testing.T, endpoint string) *mpRig {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "vh.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	be := newFakeBackend(4)
	cfg := config.Config{AccessKeyID: testAK, SecretAccessKey: testSecret, PublicEndpointURL: endpoint}
	h := NewHandler(cfg, store, be, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &mpRig{t: t, h: h, be: be}
}

func getHost(r *mpRig, host, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "http://"+host+target, nil)
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	return rec
}

func TestVirtualHostedAddressing(t *testing.T) {
	r := newVhostRig(t, "https://example.com")
	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket (path-style): %d %s", rec.Code, rec.Body)
	}
	if rec := r.do(http.MethodPut, "/send/k", []byte("hello")); rec.Code != http.StatusOK {
		t.Fatalf("put object (path-style): %d %s", rec.Code, rec.Body)
	}

	// Virtual-hosted: bucket from the <bucket>.<endpointHost> subdomain.
	rec := getHost(r, "send.example.com", "/k")
	if rec.Code != http.StatusOK || rec.Body.String() != "hello" {
		t.Fatalf("vhost GET: %d %q, want 200 \"hello\"", rec.Code, rec.Body)
	}

	// Path-style against the bare endpoint host still works (Gokapi path).
	rec = getHost(r, "example.com", "/send/k")
	if rec.Code != http.StatusOK || rec.Body.String() != "hello" {
		t.Fatalf("path-style GET: %d %q, want 200 \"hello\"", rec.Code, rec.Body)
	}

	// A bucket subdomain + root path resolves to bucket-only (404 key here).
	if rec := getHost(r, "send.example.com", "/missing"); rec.Code != http.StatusNotFound {
		t.Fatalf("vhost missing key: %d, want 404", rec.Code)
	}
}

// With no PublicEndpointURL configured, addressing is always path-style
// regardless of Host — byte-identical to pre-P7.6 (regression guard).
func TestVhostDisabledWhenNoEndpoint(t *testing.T) {
	r := newMPRig(t)
	if rec := r.do(http.MethodPut, "/send", nil); rec.Code != http.StatusOK {
		t.Fatalf("create bucket: %d", rec.Code)
	}
	r.do(http.MethodPut, "/send/k", []byte("hi"))

	// Host looks like a vhost but endpoint is unset → parsed path-style.
	if rec := getHost(r, "send.example.com", "/send/k"); rec.Code != http.StatusOK || rec.Body.String() != "hi" {
		t.Fatalf("path-style with vhost-looking Host: %d %q", rec.Code, rec.Body)
	}
}

func TestRequestIDHeaderAndErrorBody(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r, "k")

	ok := getWith(r, "/send/k", nil)
	if ok.Code != http.StatusOK {
		t.Fatalf("get: %d", ok.Code)
	}
	if ok.Header().Get("x-amz-request-id") == "" {
		t.Fatal("success response missing x-amz-request-id")
	}

	miss := getWith(r, "/send/ghost", nil)
	if miss.Code != http.StatusNotFound {
		t.Fatalf("missing: %d, want 404", miss.Code)
	}
	hdrID := miss.Header().Get("x-amz-request-id")
	if hdrID == "" {
		t.Fatal("error response missing x-amz-request-id header")
	}
	var er errorResponse
	if err := xml.Unmarshal(miss.Body.Bytes(), &er); err != nil {
		t.Fatalf("parse error body: %v", err)
	}
	if er.Code != "NoSuchKey" {
		t.Fatalf("error code = %q", er.Code)
	}
	if er.RequestID == "" || er.RequestID != hdrID {
		t.Fatalf("<RequestId> %q must match x-amz-request-id header %q", er.RequestID, hdrID)
	}
}
