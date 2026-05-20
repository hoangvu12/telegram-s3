package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// 8.6: the janitor's per-tick body aborts uploads older than the TTL and
// leaves fresh uploads alone.
func TestMultipartJanitorAbortsStaleUploads(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	// Two uploads, both with at least one part so the abort path has work.
	stale := startMPUWithPart(t, r, "stale")
	fresh := startMPUWithPart(t, r, "fresh")

	// Backdate `stale`'s created_at to ~10 days ago (test-only setter; see
	// metadata.Store.SetMultipartCreatedAt — production never edits this).
	if err := r.h.store.SetMultipartCreatedAt(context.Background(), stale, time.Now().Add(-10*24*time.Hour)); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	// One sweep with a 7-day TTL: only `stale` is past the cutoff.
	r.h.sweepStaleMultipartUploads(context.Background(), 7*24*time.Hour)

	// Stale upload row is gone; its parts' messages were Deleted.
	if _, err := r.h.store.GetMultipartUpload(context.Background(), stale); err == nil {
		t.Fatalf("stale upload still present after sweep")
	}
	if _, err := r.h.store.GetMultipartUpload(context.Background(), fresh); err != nil {
		t.Fatalf("fresh upload was reaped: %v", err)
	}
	r.be.mu.Lock()
	if len(r.be.deleted) == 0 {
		t.Fatalf("janitor did not call Delete on any backend message")
	}
	r.be.mu.Unlock()
}

// 8.6 regression: HTTP abort still works once the body is shared with the
// janitor (the abortUploadInternal refactor must not change wire behavior).
func TestMultipartJanitorAbortHTTPRegression(t *testing.T) {
	r := newMPRig(t)
	seedBucket(t, r)

	uid := startMPUWithPart(t, r, "k")
	r.be.mu.Lock()
	live := len(r.be.files)
	r.be.mu.Unlock()
	if live == 0 {
		t.Fatal("expected uploaded chunk files before abort")
	}

	if rec := r.do(http.MethodDelete, "/send/k?uploadId="+uid, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("abort: %d %s", rec.Code, rec.Body)
	}
	r.be.mu.Lock()
	defer r.be.mu.Unlock()
	if len(r.be.files) != 0 {
		t.Fatalf("HTTP abort did not free messages: %d files remain", len(r.be.files))
	}
}

// 8.6: interval <= 0 disables the sweep entirely — the goroutine must return
// immediately rather than spin on a ticker.
func TestMultipartJanitorDisabled(t *testing.T) {
	r := newMPRig(t)
	done := make(chan struct{})
	go func() {
		r.h.RunMultipartJanitor(context.Background(), 0, time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunMultipartJanitor(interval=0) should return immediately")
	}
}

// startMPUWithPart creates a multipart upload for the given key and uploads
// one tiny part — enough to give the abort path some Telegram messages to
// reap.
func startMPUWithPart(t *testing.T, r *mpRig, key string) string {
	t.Helper()
	var init initiateMultipartUploadResult
	if err := xml.Unmarshal(r.do(http.MethodPost, "/send/"+key+"?uploads", nil).Body.Bytes(), &init); err != nil || init.UploadID == "" {
		t.Fatalf("create mpu %s: %v", key, err)
	}
	if rec := r.do(http.MethodPut, fmt.Sprintf("/send/%s?partNumber=1&uploadId=%s", key, init.UploadID), []byte("data")); rec.Code != http.StatusOK {
		t.Fatalf("upload part %s: %d %s", key, rec.Code, rec.Body)
	}
	return init.UploadID
}
