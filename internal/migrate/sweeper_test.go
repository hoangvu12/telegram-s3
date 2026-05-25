package migrate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telegram-s3/internal/metadata"
	"telegram-s3/internal/storage"
)

// memBackend is a Backend implementation backed by an in-memory
// map[messageID]bytes — enough fidelity to exercise the sweeper's
// two-pass logic without standing up real Telegram. It tracks
// every Delete so tests can assert "pass-1 did NOT delete during
// grace" and "pass-2 deleted after grace".
type memBackend struct {
	label string
	mu    sync.Mutex
	store map[int64][]byte
	next  int64 // next message id to assign on Upload
	dels  []int64
	// nextErr: if non-nil, the next call returning err and then clears.
	nextErr error
}

func newMemBackend(label string) *memBackend {
	return &memBackend{label: label, store: map[int64][]byte{}}
}

func (m *memBackend) put(id int64, data []byte) {
	m.mu.Lock()
	m.store[id] = data
	m.mu.Unlock()
}

func (m *memBackend) deleteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.dels)
}

func (m *memBackend) has(id int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.store[id]
	return ok
}

func (m *memBackend) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]storage.Chunk, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.next++
	id := m.next + 1000 // separate id space from bot
	m.store[id] = data
	m.mu.Unlock()
	return []storage.Chunk{{Seq: 0, MessageID: id, Size: int64(len(data)), Offset: 0, Transport: m.label, BotIndex: 0}}, nil
}

func (m *memBackend) Download(ctx context.Context, ref storage.ChunkRef) (io.ReadCloser, error) {
	return m.DownloadRange(ctx, ref, 0, 0)
}

func (m *memBackend) DownloadRange(ctx context.Context, ref storage.ChunkRef, offset, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	data, ok := m.store[ref.MessageID]
	err := m.nextErr
	m.nextErr = nil
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("memBackend: not found")
	}
	if offset >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := int64(len(data))
	if length > 0 && offset+length < end {
		end = offset + length
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), nil
}

func (m *memBackend) Delete(ctx context.Context, ref storage.ChunkRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dels = append(m.dels, ref.MessageID)
	delete(m.store, ref.MessageID)
	return nil
}

func (m *memBackend) DeleteBatch(ctx context.Context, refs []storage.ChunkRef) error {
	for _, r := range refs {
		_ = m.Delete(ctx, r)
	}
	return nil
}

// seedBotChunk inserts one bot-transport chunk into the store via
// the normal PutObject path AND into the memBackend so the sweeper
// can actually fetch it.
func seedBotChunk(t *testing.T, store *metadata.Store, bot *memBackend, bucket, key string, msgID int64, data []byte) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, bucket); err != nil && !errors.Is(err, errBucketExists()) {
		// CreateBucket returns SQLite UNIQUE constraint error on dup; harmless in tests.
		if err.Error() == "constraint failed: UNIQUE constraint failed: buckets.name (2067)" {
			// ignore
		} else if !contains(err.Error(), "UNIQUE constraint") {
			t.Fatalf("bucket: %v", err)
		}
	}
	obj := metadata.Object{Bucket: bucket, Key: key, Size: int64(len(data)), ETag: "e", ContentType: "x"}
	chunks := []metadata.Chunk{{Seq: 0, FileID: "f", MessageID: msgID, Size: int64(len(data)), Offset: 0, Transport: "bot", BotIndex: 0}}
	if err := store.PutObject(ctx, obj, chunks); err != nil {
		t.Fatalf("put: %v", err)
	}
	bot.put(msgID, data)
}

func errBucketExists() error { return errors.New("bucket exists") }
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func newTestSweeper(t *testing.T) (*Sweeper, *metadata.Store, *memBackend, *memBackend, *clock) {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "sw.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	bot := newMemBackend(storage.TransportBot)
	mt := newMemBackend(storage.TransportMTProto)
	clk := &clock{}
	clk.set(time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC))

	sw, err := NewSweeper(Options{
		Store:          store,
		Bot:            bot,
		MTProto:        mt,
		MigrationRate:  100,         // not used directly; runOnce uses the per-tick budget
		BotDeleteGrace: time.Hour,   // production-shaped grace
		Pass2Budget:    100,
		Logger:         slog.New(slog.DiscardHandler),
		Now:            clk.now,
	})
	if err != nil {
		t.Fatalf("NewSweeper: %v", err)
	}
	return sw, store, bot, mt, clk
}

// clock is an injectable time source for the sweeper. tests advance
// it manually to step past the grace window without sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// TestSweeperTwoPassEndToEnd: load-bearing test for PHASES.md
// decision #14. Pass-1 swaps the row + enqueues the bot message but
// does NOT delete it. Pass-2 only deletes after the grace window
// elapses. A reader fetching the chunk map mid-grace must still see
// readable bot bytes.
func TestSweeperTwoPassEndToEnd(t *testing.T) {
	sw, store, bot, mt, clk := newTestSweeper(t)
	ctx := context.Background()

	payload := []byte("the quick brown fox")
	seedBotChunk(t, store, bot, "b", "k1", 42, payload)

	// Manually invoke the migration (skip the rate gate by calling
	// pass1Migrate directly with a generous budget).
	sw.pass1Migrate(ctx, 100)

	// Post pass-1: row is mtproto, pending_delete has the old bot id,
	// bot message STILL EXISTS (the grace window hasn't passed).
	chunks, err := store.GetObjectChunks(ctx, "b", "k1")
	if err != nil || len(chunks) != 1 {
		t.Fatalf("get chunks: %v %+v", err, chunks)
	}
	got := chunks[0]
	if got.Transport != "mtproto" || got.MessageID == 42 {
		t.Fatalf("row not swapped: %+v", got)
	}
	if !bot.has(42) {
		t.Fatal("bot message deleted during grace — violates decision #14")
	}
	if !mt.has(got.MessageID) {
		t.Fatal("mtproto message missing post-swap")
	}
	pend, _ := store.PendingDeletesOlderThan(ctx, clk.now().Add(time.Hour), 10)
	if len(pend) != 1 || pend[0].MessageID != 42 {
		t.Fatalf("pending = %+v want one row with msg 42", pend)
	}

	// Pass-2 within the grace window: nothing reaped.
	sw.pass2Reap(ctx, 100)
	if bot.deleteCount() != 0 {
		t.Fatalf("pass-2 reaped during grace; deletes=%d want 0", bot.deleteCount())
	}
	if !bot.has(42) {
		t.Fatal("bot message gone after in-grace pass-2")
	}

	// Advance past the grace window and run pass-2 again.
	clk.advance(2 * time.Hour)
	sw.pass2Reap(ctx, 100)

	if bot.has(42) {
		t.Fatal("bot message survived post-grace pass-2")
	}
	if bot.deleteCount() != 1 {
		t.Fatalf("pass-2 deletes=%d want 1", bot.deleteCount())
	}
	pend, _ = store.PendingDeletesOlderThan(ctx, clk.now().Add(time.Hour), 10)
	if len(pend) != 0 {
		t.Fatalf("pending_delete not drained: %+v", pend)
	}
}

// TestConcurrentReadDuringSwap: the most important correctness test.
// A reader that grabbed the bot chunk map BEFORE pass-1 runs must
// still successfully read the bot message DURING the grace window.
// This is exactly the regression decision #14 was designed to
// prevent — if the bot delete happened in the same tx as the swap,
// this reader would 404.
func TestConcurrentReadDuringSwap(t *testing.T) {
	sw, store, bot, _, clk := newTestSweeper(t)
	ctx := context.Background()

	payload := []byte("hello world from the bot tier")
	seedBotChunk(t, store, bot, "b", "k2", 99, payload)

	// "Reader" fetches the chunk map BEFORE pass-1 runs.
	preSwap, err := store.GetObjectChunks(ctx, "b", "k2")
	if err != nil || len(preSwap) != 1 || preSwap[0].Transport != "bot" {
		t.Fatalf("pre-swap chunk: %v %+v", err, preSwap)
	}
	preSwapRef := storage.ChunkRef{
		Transport: storage.TransportBot,
		BotFileID: preSwap[0].FileID,
		MessageID: preSwap[0].MessageID,
		BotIndex:  preSwap[0].BotIndex,
	}

	// Sweeper runs pass-1.
	sw.pass1Migrate(ctx, 100)

	// The reader (with its stale 'bot' ref) reads — must still work.
	rc, err := bot.DownloadRange(ctx, preSwapRef, 0, 0)
	if err != nil {
		t.Fatalf("reader DownloadRange during grace: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Fatalf("reader got %q want %q", got, payload)
	}

	// Pass-2 still respects the grace window.
	sw.pass2Reap(ctx, 100)
	if !bot.has(99) {
		t.Fatal("bot msg deleted during grace window")
	}

	// Now advance past grace. Pass-2 reaps; a *new* reader (fetching
	// a fresh chunk map) sees mtproto and stops needing the bot copy.
	clk.advance(2 * time.Hour)
	sw.pass2Reap(ctx, 100)
	if bot.has(99) {
		t.Fatal("bot msg survived post-grace reap")
	}
}

// TestSweeperPass1FailureLeavesRow: a download / upload failure in
// pass-1 must NOT leave the row half-migrated. The row stays 'bot'
// and the next tick retries.
func TestSweeperPass1FailureLeavesRow(t *testing.T) {
	sw, store, bot, mt, _ := newTestSweeper(t)
	ctx := context.Background()

	seedBotChunk(t, store, bot, "b", "k3", 7, []byte("data"))

	// Inject a bot download error.
	bot.mu.Lock()
	bot.nextErr = errors.New("simulated bot 5xx")
	bot.mu.Unlock()

	sw.pass1Migrate(ctx, 100)

	chunks, _ := store.GetObjectChunks(ctx, "b", "k3")
	if chunks[0].Transport != "bot" {
		t.Fatalf("row should still be bot after pass-1 download failure; got %+v", chunks[0])
	}
	if mt.deleteCount() != 0 {
		t.Fatalf("mt got %d deletes (unexpected; no upload happened)", mt.deleteCount())
	}
}

// TestSweeperInlineReap: grace == 0 (tests only) reaps inline.
// Confirms the bypass path stays exercised.
func TestSweeperInlineReap(t *testing.T) {
	store, _ := metadata.Open(filepath.Join(t.TempDir(), "ir.db"))
	t.Cleanup(func() { store.Close() })

	bot := newMemBackend(storage.TransportBot)
	mt := newMemBackend(storage.TransportMTProto)
	clk := &clock{}
	clk.set(time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC))

	sw, _ := NewSweeper(Options{
		Store: store, Bot: bot, MTProto: mt,
		MigrationRate: 100, BotDeleteGrace: 0, Pass2Budget: 100,
		Logger: slog.New(slog.DiscardHandler), Now: clk.now,
	})

	seedBotChunk(t, store, bot, "b", "k4", 13, []byte("xyz"))
	sw.pass1Migrate(context.Background(), 100)

	if bot.has(13) {
		t.Fatal("inline reap should have deleted the bot msg in pass-1")
	}
	atomic.LoadInt64(&bot.next) // silence unused import in some Go versions
}
