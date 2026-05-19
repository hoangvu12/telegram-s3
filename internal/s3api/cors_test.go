package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCORSPreflight(t *testing.T) {
	r := newMPRig(t)

	req := httptest.NewRequest(http.MethodOptions, "http://example.com/send/k", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "PUT")
	req.Header.Set("Access-Control-Request-Headers", "authorization,x-amz-date")
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("OPTIONS status = %d, want 200 (no auth, no routing)", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("ACAO = %q, want *", got)
	}
	if rec.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatal("missing Access-Control-Allow-Methods")
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "authorization,x-amz-date" {
		t.Fatalf("ACAH = %q, want echoed Access-Control-Request-Headers", got)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("preflight should have no body, got %q", rec.Body)
	}
}

func TestCORSOnNormalGet(t *testing.T) {
	r := newMPRig(t)
	putRangeObject(t, r, "k", []byte("hello"))

	rec := getWith(r, "/send/k", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET: %d %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("ACAO on GET = %q, want *", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(got, "ETag") {
		t.Fatalf("Expose-Headers = %q, want ETag listed", got)
	}
}

// Allow-Headers falls back to * when the preflight does not list any.
func TestCORSAllowHeadersFallback(t *testing.T) {
	r := newMPRig(t)
	req := httptest.NewRequest(http.MethodOptions, "http://example.com/send/k", nil)
	rec := httptest.NewRecorder()
	r.h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "*" {
		t.Fatalf("ACAH fallback = %q, want *", got)
	}
}
