package metadata

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreChunksRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// §6.4: WAL must be active.
	var mode string
	if err := s.read.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if strings.ToLower(mode) != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}

	if err := s.CreateBucket(ctx, "send"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// Phase 3: Transport/BotIndex round-trip through the new schema columns.
	// PutObject normalizes empty Transport to "bot" on insert (matching the
	// column default), so the SELECT side returns "bot" even though the
	// caller passed "". Keep the literal explicit so equality holds.
	chunks := []Chunk{
		{Seq: 0, FileID: "f0", MessageID: 10, Size: 18, Offset: 0, Transport: "bot", BotIndex: 0},
		{Seq: 1, FileID: "f1", MessageID: 11, Size: 7, Offset: 18, Transport: "bot", BotIndex: 0},
	}
	obj := Object{Bucket: "send", Key: "a/b.bin", Size: 25, ETag: "etag",
		ContentType: "application/octet-stream", TelegramFileID: "f0", TelegramMessageID: 10}
	if err := s.PutObject(ctx, obj, chunks); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := s.GetObject(ctx, "send", "a/b.bin")
	if err != nil || got.Size != 25 || got.ETag != "etag" {
		t.Fatalf("get object = %+v, %v", got, err)
	}
	gc, err := s.GetObjectChunks(ctx, "send", "a/b.bin")
	if err != nil {
		t.Fatalf("get chunks: %v", err)
	}
	if len(gc) != 2 || gc[0] != chunks[0] || gc[1] != chunks[1] {
		t.Fatalf("chunks = %+v, want %+v (ordered by seq)", gc, chunks)
	}

	// Overwrite must replace the whole chunk map atomically.
	if err := s.PutObject(ctx, Object{Bucket: "send", Key: "a/b.bin", Size: 5, ETag: "e2",
		ContentType: "text/plain", TelegramFileID: "g0", TelegramMessageID: 99},
		[]Chunk{{Seq: 0, FileID: "g0", MessageID: 99, Size: 5, Offset: 0}}); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	gc2, _ := s.GetObjectChunks(ctx, "send", "a/b.bin")
	if len(gc2) != 1 || gc2[0].FileID != "g0" {
		t.Fatalf("after overwrite chunks = %+v, want single g0", gc2)
	}
	if o2, _ := s.GetObject(ctx, "send", "a/b.bin"); o2.Size != 5 {
		t.Fatalf("after overwrite size = %d, want 5", o2.Size)
	}

	// Hard delete removes object row AND chunk rows.
	if err := s.DeleteObject(ctx, "send", "a/b.bin"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetObject(ctx, "send", "a/b.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	if gc3, _ := s.GetObjectChunks(ctx, "send", "a/b.bin"); len(gc3) != 0 {
		t.Fatalf("chunks not hard-deleted: %+v", gc3)
	}

	// Legacy single-message object (pre-Phase-3): no chunk rows.
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := s.write.ExecContext(ctx, `INSERT INTO objects(bucket,key,size,etag,content_type,telegram_file_id,telegram_message_id,created_at,updated_at,deleted_at) VALUES('send','legacy.bin',12,'le','application/octet-stream','legacyfid',77,?,?,NULL)`, ts, ts); err != nil {
		t.Fatalf("insert legacy: %v", err)
	}
	lo, err := s.GetObject(ctx, "send", "legacy.bin")
	if err != nil || lo.TelegramFileID != "legacyfid" || lo.TelegramMessageID != 77 {
		t.Fatalf("legacy object = %+v, %v", lo, err)
	}
	if lc, _ := s.GetObjectChunks(ctx, "send", "legacy.bin"); len(lc) != 0 {
		t.Fatalf("legacy object must have empty chunk list, got %+v", lc)
	}

	// DELETE is idempotent on a missing key.
	if err := s.DeleteObject(ctx, "send", "does-not-exist"); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
}
