package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telegram-s3/internal/storage"
)

// fakeSource emits sequential bytes derived from offset, optionally
// delaying or erroring on specific chunk indices. Aligned reads from
// fakeSource return chunkSize bytes (or fewer at EOF).
type fakeSource struct {
	size      int64
	chunkSize int64

	// Optional knobs:
	delay      func(chunkIdx int) time.Duration // sleep before returning
	failAt     int                               // chunk idx that should error; -1 disables
	failErr    error
	inFlight   atomic.Int32
	maxInFlight atomic.Int32
	calls      atomic.Int64
}

func newFakeSource(size, chunkSize int64) *fakeSource {
	return &fakeSource{size: size, chunkSize: chunkSize, failAt: -1}
}

func (f *fakeSource) ChunkSize(start, end int64) int64 { return f.chunkSize }

func (f *fakeSource) Chunk(ctx context.Context, offset, limit int64) ([]byte, error) {
	f.calls.Add(1)
	now := f.inFlight.Add(1)
	for {
		max := f.maxInFlight.Load()
		if now <= max || f.maxInFlight.CompareAndSwap(max, now) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	idx := int(offset / f.chunkSize)
	if f.delay != nil {
		if d := f.delay(idx); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if f.failAt == idx {
		return nil, f.failErr
	}
	end := offset + limit
	if end > f.size {
		end = f.size
	}
	if end <= offset {
		return nil, nil
	}
	out := make([]byte, end-offset)
	for i := range out {
		out[i] = byte((offset + int64(i)) % 251) // 251 is prime → no aliasing on chunkSize
	}
	return out, nil
}

func wantedBytes(start, end int64) []byte {
	b := make([]byte, end-start)
	for i := range b {
		b[i] = byte((start + int64(i)) % 251)
	}
	return b
}

// TestEmptyRangeImmediateEOF: a [start, start) range yields no data; the
// first Read must return io.EOF without spawning any worker.
func TestEmptyRangeImmediateEOF(t *testing.T) {
	src := newFakeSource(100, 16)
	r := New(context.Background(), src, 10, 10, 4, 4, 0)
	defer r.Close()

	if err := r.Prime(); err != io.EOF {
		t.Fatalf("Prime on empty range = %v, want io.EOF", err)
	}
	n, err := r.Read(make([]byte, 8))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read on empty range = (%d, %v), want (0, EOF)", n, err)
	}
	if src.calls.Load() != 0 {
		t.Fatalf("empty range issued %d Chunk calls; want 0", src.calls.Load())
	}
}

// TestFullObjectInOrder reassembles a full object and asserts byte order.
func TestFullObjectInOrder(t *testing.T) {
	const size, cs = 1000, 64
	src := newFakeSource(size, cs)
	r := New(context.Background(), src, 0, size, 4, 4, 0)
	defer r.Close()
	if err := r.Prime(); err != nil && err != io.EOF {
		t.Fatalf("Prime: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, wantedBytes(0, size)) {
		t.Fatalf("body mismatch (len got=%d want=%d)", len(got), size)
	}
}

// TestUnalignedRange covers leftCut + rightCut: a request strictly inside
// a chunk requires trimming both ends of the only aligned chunk fetched.
func TestUnalignedRange(t *testing.T) {
	const size, cs = 1000, 64
	src := newFakeSource(size, cs)
	// [70, 130): straddles chunks 1 (64-127) and 2 (128-191). leftCut=6,
	// rightCut=192-130=62.
	r := New(context.Background(), src, 70, 130, 4, 4, 0)
	defer r.Close()
	if err := r.Prime(); err != nil && err != io.EOF {
		t.Fatalf("Prime: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, wantedBytes(70, 130)) {
		t.Fatalf("range body mismatch: got %d bytes, want %d", len(got), 60)
	}
}

// TestOutOfOrderCompletionDeliversInOrder pins the load-bearing
// correctness property: even when later chunks finish before earlier
// ones, the reader delivers bytes in offset order.
func TestOutOfOrderCompletionDeliversInOrder(t *testing.T) {
	const size, cs = 1024, 64
	src := newFakeSource(size, cs)
	// Chunk 0 is slowest, then 1, then 2, etc. → workers complete
	// later indices first. Total work bounded so the test stays fast.
	src.delay = func(idx int) time.Duration {
		// idx 0 → 80ms; subsequent → 10ms each. With concurrency=8 the
		// later chunks definitely finish before chunk 0.
		if idx == 0 {
			return 80 * time.Millisecond
		}
		return 10 * time.Millisecond
	}
	r := New(context.Background(), src, 0, size, 8, 8, 0)
	defer r.Close()
	if err := r.Prime(); err != nil && err != io.EOF {
		t.Fatalf("Prime: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, wantedBytes(0, size)) {
		t.Fatalf("ordering invariant violated")
	}
}

// TestConcurrentFetches asserts the prefetch pool actually fans out:
// observed peak in-flight should reach >= 2 for a multi-chunk read.
func TestConcurrentFetches(t *testing.T) {
	const size, cs = 4096, 64
	src := newFakeSource(size, cs)
	// Inject a small delay so several Chunk calls overlap.
	src.delay = func(int) time.Duration { return 5 * time.Millisecond }
	r := New(context.Background(), src, 0, size, 4, 4, 0)
	defer r.Close()
	if err := r.Prime(); err != nil && err != io.EOF {
		t.Fatalf("Prime: %v", err)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if peak := src.maxInFlight.Load(); peak < 2 {
		t.Fatalf("expected >= 2 concurrent Chunk calls; observed peak=%d", peak)
	}
}

// TestFirstChunkFailureSurfacesViaPrime: a worker failure on chunk 0
// must be observable from Prime BEFORE Read returns any byte. The
// handler relies on this to return HTTP 502 without committing 200/206.
func TestFirstChunkFailureSurfacesViaPrime(t *testing.T) {
	const size, cs = 1024, 64
	src := newFakeSource(size, cs)
	src.failAt = 0
	src.failErr = errors.New("synthetic chunk-0 failure")

	r := New(context.Background(), src, 0, size, 4, 4, 0)
	defer r.Close()

	err := r.Prime()
	if err == nil || err == io.EOF {
		t.Fatalf("Prime should surface the chunk-0 error; got %v", err)
	}
	if err.Error() != "synthetic chunk-0 failure" {
		t.Fatalf("Prime returned wrong error: %v", err)
	}
	// A subsequent Read should not deliver any bytes.
	buf := make([]byte, 16)
	n, rerr := r.Read(buf)
	if n != 0 {
		t.Fatalf("Read returned %d bytes after first-chunk failure; want 0", n)
	}
	if rerr == nil {
		t.Fatalf("Read after first-chunk failure should error; got nil")
	}
}

// TestLateChunkFailureAbortsCleanly: a failure on a middle chunk
// surfaces during Read after earlier chunks were consumed; Close then
// releases resources without hanging.
func TestLateChunkFailureAbortsCleanly(t *testing.T) {
	const size, cs = 1024, 64
	src := newFakeSource(size, cs)
	src.failAt = 5
	src.failErr = errors.New("synthetic chunk-5 failure")

	r := New(context.Background(), src, 0, size, 2, 2, 0)
	if err := r.Prime(); err != nil {
		t.Fatalf("Prime should succeed (chunk 0 is fine), got %v", err)
	}

	var consumed int64
	buf := make([]byte, 32)
	for {
		n, err := r.Read(buf)
		consumed += int64(n)
		if err == nil {
			continue
		}
		if errors.Is(err, src.failErr) {
			break
		}
		t.Fatalf("Read errored with unexpected: %v", err)
	}
	// Some prefix of chunks 0..4 should have been delivered (5*64=320
	// bytes); workers may not have delivered all of them by the time
	// the error surfaces, so just sanity-check we got bytes.
	if consumed == 0 {
		t.Fatalf("expected some bytes before late failure; got 0")
	}

	// Close must return quickly even though workers may still be in
	// flight. Pin a deadline.
	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung after late chunk failure")
	}
}

// TestCloseReleasesPendingFetches: a slow source + early Close must not
// leak goroutines (proxied by a deadline on Close).
func TestCloseReleasesPendingFetches(t *testing.T) {
	const size, cs = 4096, 64
	src := newFakeSource(size, cs)
	src.delay = func(int) time.Duration { return 50 * time.Millisecond }
	r := New(context.Background(), src, 0, size, 4, 4, 0)
	if err := r.Prime(); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	// Read just one byte then bail.
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}

	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung with slow source")
	}
}

// TestPrimeIsIdempotent: a second Prime call after success is a no-op.
func TestPrimeIsIdempotent(t *testing.T) {
	const size, cs = 256, 64
	src := newFakeSource(size, cs)
	r := New(context.Background(), src, 0, size, 2, 2, 0)
	defer r.Close()

	if err := r.Prime(); err != nil {
		t.Fatalf("Prime 1: %v", err)
	}
	if err := r.Prime(); err != nil {
		t.Fatalf("Prime 2: %v", err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, wantedBytes(0, size)) {
		t.Fatalf("ReadAll after double-Prime mismatch")
	}
}

// --- BotSource tests --------------------------------------------------------

// chunkBackend is a minimal storage.Backend that serves in-memory blobs
// keyed by BotFileID. Only DownloadRange is exercised by BotSource, so
// the other methods are no-ops.
type chunkBackend struct {
	mu     sync.Mutex
	blobs  map[string][]byte
	calls  atomic.Int64
	failOn string // BotFileID to fail with errFakeFail; "" disables
}

var errFakeFail = errors.New("chunkBackend: synthetic failure")

func (b *chunkBackend) Upload(context.Context, string, string, io.Reader) ([]storage.Chunk, error) {
	return nil, nil
}
func (b *chunkBackend) Download(ctx context.Context, ref storage.ChunkRef) (io.ReadCloser, error) {
	return b.DownloadRange(ctx, ref, 0, 0)
}
func (b *chunkBackend) Delete(context.Context, storage.ChunkRef) error           { return nil }
func (b *chunkBackend) DeleteBatch(context.Context, []storage.ChunkRef) error    { return nil }
func (b *chunkBackend) DownloadRange(_ context.Context, ref storage.ChunkRef, off, length int64) (io.ReadCloser, error) {
	b.calls.Add(1)
	if ref.BotFileID == b.failOn {
		return nil, errFakeFail
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.blobs[ref.BotFileID]
	if !ok {
		return nil, fmt.Errorf("no such blob %s", ref.BotFileID)
	}
	if off >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	data = data[off:]
	if length > 0 && length < int64(len(data)) {
		data = data[:length]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// buildBlob fills a buffer with deterministic bytes matching wantedBytes
// over [start, end).
func buildBlob(start, end int64) []byte { return wantedBytes(start, end) }

// TestBotSourceFullReadStraddlesMessages: a prefetch chunk larger than
// the upload messages should still stitch bytes together correctly.
func TestBotSourceFullReadStraddlesMessages(t *testing.T) {
	const total = 300
	const uploadSize = 80     // upload chunks of 80 bytes
	const prefetchCS = 128    // prefetch reads 128-byte windows

	be := &chunkBackend{blobs: map[string][]byte{}}
	var locs []ChunkLoc
	for off := int64(0); off < total; off += uploadSize {
		end := off + uploadSize
		if end > total {
			end = total
		}
		fid := fmt.Sprintf("file-%d", off)
		be.blobs[fid] = buildBlob(off, end)
		locs = append(locs, ChunkLoc{
			Ref:    storage.ChunkRef{Transport: storage.TransportBot, BotFileID: fid},
			Offset: off,
			Size:   end - off,
		})
	}

	src := NewBotSource(be, total, locs, prefetchCS)
	r := New(context.Background(), src, 0, total, 2, 2, 0)
	defer r.Close()
	if err := r.Prime(); err != nil && err != io.EOF {
		t.Fatalf("Prime: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, buildBlob(0, total)) {
		t.Fatalf("reassembled blob mismatch (len got=%d want=%d)", len(got), total)
	}
}

// TestBotSourceRangeAcrossMessages exercises an unaligned Range that
// covers a partial slice of two adjacent upload messages.
func TestBotSourceRangeAcrossMessages(t *testing.T) {
	const total = 200
	const uploadSize = 50
	be := &chunkBackend{blobs: map[string][]byte{}}
	var locs []ChunkLoc
	for off := int64(0); off < total; off += uploadSize {
		fid := fmt.Sprintf("f-%d", off)
		be.blobs[fid] = buildBlob(off, off+uploadSize)
		locs = append(locs, ChunkLoc{
			Ref:    storage.ChunkRef{Transport: storage.TransportBot, BotFileID: fid},
			Offset: off,
			Size:   uploadSize,
		})
	}

	// Read [40, 110): straddles upload chunks 0 (0-50), 1 (50-100), 2 (100-150).
	src := NewBotSource(be, total, locs, 64)
	r := New(context.Background(), src, 40, 110, 2, 2, 0)
	defer r.Close()
	if err := r.Prime(); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, buildBlob(40, 110)) {
		t.Fatalf("range body mismatch (len got=%d want=%d)", len(got), 70)
	}
}

// TestBotSourceChunkSizeShrinksForTinyRanges: a small Range should not
// over-fetch the configured prefetch size.
func TestBotSourceChunkSizeShrinksForTinyRanges(t *testing.T) {
	be := &chunkBackend{}
	src := NewBotSource(be, 1<<30, nil, 4<<20) // 4 MiB nominal
	if cs := src.ChunkSize(100, 200); cs > 64<<10 {
		t.Fatalf("ChunkSize(span=100) = %d; want <= 64 KiB", cs)
	}
	// Big range: full size returned.
	if cs := src.ChunkSize(0, 1<<30); cs != 4<<20 {
		t.Fatalf("ChunkSize(big span) = %d; want 4 MiB", cs)
	}
}
