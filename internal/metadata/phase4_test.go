package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestPhase4Migration covers the new tg_sessions and
// bot_chunks_pending_delete tables: the migrate() block runs without
// error on a fresh DB, both tables are queryable, and re-Open is a
// no-op (idempotent).
func TestPhase4Migration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p4.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ctx := context.Background()
	if err := s.StoreSession(ctx, "bot:0", []byte("session")); err != nil {
		t.Fatalf("store session: %v", err)
	}
	// pending_delete table is just queryable; empty result is fine.
	if _, err := s.PendingDeletesOlderThan(ctx, time.Now(), 10); err != nil {
		t.Fatalf("pending list: %v", err)
	}
	s.Close()

	// Re-open: migrate must be idempotent (issue #14 in PHASES.md says
	// additive only — re-running CREATE TABLE IF NOT EXISTS is a no-op).
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.LoadSession(ctx, "bot:0")
	if err != nil || string(got) != "session" {
		t.Fatalf("session survived reopen: got=%q err=%v", got, err)
	}
}

// TestSwapBotChunkToMtproto is the load-bearing pass-1 unit test: a
// successful swap (a) flips the chunk row's transport/message_id/
// bot_index/file_id, AND (b) inserts the OLD (message_id, bot_index)
// into bot_chunks_pending_delete. The two writes share a tx — partial
// state would either leave a duplicate bot message forever or break
// reads, both of which are correctness violations.
func TestSwapBotChunkToMtproto(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "swap.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	// Seed one bot-transport chunk via the normal Put path.
	bot := Chunk{Seq: 0, FileID: "old-file", MessageID: 100, Size: 10, Offset: 0, Transport: "bot", BotIndex: 0}
	if err := s.PutObject(ctx, Object{Bucket: "b", Key: "k", Size: 10, ETag: "e", ContentType: "x"}, []Chunk{bot}); err != nil {
		t.Fatalf("put: %v", err)
	}

	// time.Now() includes nanos; the stored RFC3339Nano string will too,
	// so string comparison against "now + 1ns" is unambiguous. Avoid
	// Truncate(time.Second) — that produces a stripped-fractional string
	// whose char-by-char order with a nanosecond-precision string is
	// position-dependent ('Z' (0x5A) vs '.' (0x2E)) and unreliable.
	now := time.Now().UTC()
	if err := s.SwapBotChunkToMtproto(ctx, "b", "k", 0, 100, 0, 999, 2, now); err != nil {
		t.Fatalf("swap: %v", err)
	}

	chunks, err := s.GetObjectChunks(ctx, "b", "k")
	if err != nil || len(chunks) != 1 {
		t.Fatalf("get chunks: %v %+v", err, chunks)
	}
	got := chunks[0]
	if got.Transport != "mtproto" || got.MessageID != 999 || got.BotIndex != 2 || got.FileID != "" {
		t.Fatalf("post-swap chunk = %+v want transport=mtproto msg=999 bot=2 file=''", got)
	}

	// Pending-delete must hold the OLD (100, 0). Reading "older than
	// now+1ns" should include the just-inserted row.
	pend, err := s.PendingDeletesOlderThan(ctx, now.Add(time.Second), 10)
	if err != nil || len(pend) != 1 {
		t.Fatalf("pending: %v %+v", err, pend)
	}
	if pend[0].MessageID != 100 || pend[0].BotIndex != 0 {
		t.Fatalf("pending = %+v want msg=100 bot=0", pend[0])
	}

	// Double-swap must be a no-op (the row is already mtproto). It also
	// must NOT push a second pending row (we'd reap an already-gone
	// bot message and get a 404 from Telegram for nothing).
	if err := s.SwapBotChunkToMtproto(ctx, "b", "k", 0, 100, 0, 1000, 3, now.Add(time.Minute)); err != nil {
		t.Fatalf("re-swap: %v", err)
	}
	chunks, _ = s.GetObjectChunks(ctx, "b", "k")
	if chunks[0].MessageID != 999 {
		t.Fatalf("re-swap should not overwrite mtproto row; got %+v", chunks[0])
	}
	pend, _ = s.PendingDeletesOlderThan(ctx, now.Add(2*time.Minute), 10)
	if len(pend) != 1 {
		t.Fatalf("pending after re-swap = %d rows; want 1 (no duplicate)", len(pend))
	}

	// Drop the pending row and confirm CountBotChunks observes the swap.
	if err := s.DeletePendingDelete(ctx, 100, 0); err != nil {
		t.Fatalf("delete pending: %v", err)
	}
	pend, _ = s.PendingDeletesOlderThan(ctx, now.Add(time.Hour), 10)
	if len(pend) != 0 {
		t.Fatalf("pending after delete = %d rows; want 0", len(pend))
	}
	n, err := s.CountBotChunks(ctx)
	if err != nil || n != 0 {
		t.Fatalf("CountBotChunks = %d, %v; want 0", n, err)
	}
}

// TestListBotChunksOldestFirst confirms the sweeper's pass-1 scan
// orders by parent object's updated_at, so the oldest data drains first.
func TestListBotChunksOldestFirst(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "ord.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatalf("bucket: %v", err)
	}

	// Put three objects with overridden CreatedAt/UpdatedAt to control order.
	// PutObject overwrites UpdatedAt to time.Now() so we tweak the underlying
	// row directly to force a deterministic order in test.
	put := func(key string, msgID int64) {
		if err := s.PutObject(ctx, Object{Bucket: "b", Key: key, Size: 1, ETag: "e", ContentType: "x"},
			[]Chunk{{Seq: 0, FileID: "f", MessageID: msgID, Size: 1, Offset: 0, Transport: "bot", BotIndex: 0}}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	put("z", 1)
	put("a", 2)
	put("m", 3)

	// Force a fixed ordering by overwriting updated_at.
	for i, key := range []string{"a", "m", "z"} {
		ts := time.Date(2020, 1, int(i+1), 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
		if _, err := s.write.Exec(`UPDATE objects SET updated_at = ? WHERE bucket = ? AND key = ?`, ts, "b", key); err != nil {
			t.Fatalf("force order: %v", err)
		}
	}

	got, err := s.ListBotChunksOldestFirst(ctx, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 || got[0].Key != "a" || got[1].Key != "m" || got[2].Key != "z" {
		t.Fatalf("order = %+v; want a, m, z", keysOf(got))
	}
}

// TestBotMigrationSnapshot pins the three drain numbers the sweeper
// will log every tick. Empty-state ergonomics matter as much as the
// populated case — LatestSwap.IsZero() is what tells the log path to
// drop the timestamp fields so an operator parsing logs doesn't see a
// spurious "0001-01-01T00:00:00Z" before any drain has happened.
func TestBotMigrationSnapshot(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// Empty DB → all zeros, LatestSwap is zero-valued.
	snap, err := s.BotMigrationSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot empty: %v", err)
	}
	if snap.BotChunksRemaining != 0 || snap.PendingDeletes != 0 || !snap.LatestSwap.IsZero() {
		t.Fatalf("empty snapshot = %+v; want zeros with IsZero LatestSwap", snap)
	}

	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatalf("bucket: %v", err)
	}
	// Two bot-transport chunks, no swaps yet.
	for i, key := range []string{"a", "b"} {
		if err := s.PutObject(ctx, Object{Bucket: "b", Key: key, Size: 1, ETag: "e", ContentType: "x"},
			[]Chunk{{Seq: 0, FileID: "f", MessageID: int64(100 + i), Size: 1, Offset: 0, Transport: "bot", BotIndex: 0}}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	snap, err = s.BotMigrationSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot pre-swap: %v", err)
	}
	if snap.BotChunksRemaining != 2 || snap.PendingDeletes != 0 || !snap.LatestSwap.IsZero() {
		t.Fatalf("pre-swap snapshot = %+v; want 2 bot chunks, 0 pending, zero LatestSwap", snap)
	}

	// Swap one row — that drops bot count to 1 and pushes the pending row.
	swapTime := time.Date(2026, 5, 25, 17, 30, 0, 0, time.UTC)
	if err := s.SwapBotChunkToMtproto(ctx, "b", "a", 0, 100, 0, 999, 1, swapTime); err != nil {
		t.Fatalf("swap: %v", err)
	}
	snap, err = s.BotMigrationSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot post-swap: %v", err)
	}
	if snap.BotChunksRemaining != 1 {
		t.Errorf("BotChunksRemaining = %d; want 1", snap.BotChunksRemaining)
	}
	if snap.PendingDeletes != 1 {
		t.Errorf("PendingDeletes = %d; want 1", snap.PendingDeletes)
	}
	if !snap.LatestSwap.Equal(swapTime) {
		t.Errorf("LatestSwap = %v; want %v", snap.LatestSwap, swapTime)
	}

	// A second swap with a later timestamp must surface as LatestSwap.
	// This pins MAX semantics: the row count is 2 but only the newer
	// timestamp matters for "is the sweeper alive" debugging.
	laterSwap := swapTime.Add(time.Hour)
	if err := s.SwapBotChunkToMtproto(ctx, "b", "b", 0, 101, 0, 1000, 1, laterSwap); err != nil {
		t.Fatalf("swap 2: %v", err)
	}
	snap, err = s.BotMigrationSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot post-swap2: %v", err)
	}
	if snap.PendingDeletes != 2 {
		t.Errorf("PendingDeletes = %d; want 2", snap.PendingDeletes)
	}
	if !snap.LatestSwap.Equal(laterSwap) {
		t.Errorf("LatestSwap = %v; want %v (MAX of two swaps)", snap.LatestSwap, laterSwap)
	}
}

func keysOf(cs []BotChunkLoc) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Key
	}
	return out
}
