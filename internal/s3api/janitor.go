package s3api

import (
	"context"
	"time"
)

// RunMultipartJanitor periodically aborts multipart uploads whose created_at
// is older than ttl, freeing the Telegram messages they hold (8.6, closes
// §6.2). AWS provides this via the AbortIncompleteMultipartUpload lifecycle
// rule; we run it in-process because the gateway is the only writer.
//
// The loop exits when ctx is cancelled (typically the server's shutdown
// context in cmd/telegram-s3). A non-positive interval is a configuration
// signal that the sweep is disabled; the wiring in main.go should not start
// this goroutine in that case.
func (h *Handler) RunMultipartJanitor(ctx context.Context, interval, ttl time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sweepStaleMultipartUploads(ctx, ttl)
		}
	}
}

func (h *Handler) sweepStaleMultipartUploads(ctx context.Context, ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	stale, err := h.store.StaleMultipartUploads(ctx, cutoff)
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("multipart janitor: list failed", "error", err)
		}
		return
	}
	for _, u := range stale {
		if err := h.abortUploadInternal(ctx, u.UploadID); err != nil && h.logger != nil {
			h.logger.Warn("multipart janitor: abort failed", "upload_id", u.UploadID, "bucket", u.Bucket, "key", u.Key, "error", err)
			continue
		}
		if h.logger != nil {
			h.logger.Info("multipart janitor: aborted stale upload", "upload_id", u.UploadID, "bucket", u.Bucket, "key", u.Key, "age", time.Since(u.CreatedAt).Round(time.Second))
		}
	}
}
