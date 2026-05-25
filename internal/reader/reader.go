// Package reader implements a parallel-prefetch io.ReadCloser over a
// transport-agnostic ChunkSource. The chunked Bot HTTP API path opens
// segments strictly sequentially today; this is the load-bearing path
// that lets a single GET fan out across N concurrent fetches and deliver
// bytes in order. The implementation is transport-agnostic by design so
// the MTProto backend in Phase 4 can plug in a different ChunkSource
// without touching this file.
//
// Correctness invariants (the tests pin all of these):
//   - bytes are delivered strictly in order regardless of fetch order.
//   - a first-chunk failure surfaces from New (via Prime) BEFORE any
//     bytes are returned, so a handler can return HTTP 502 without
//     having already committed a 200/206 status line.
//   - a late-chunk failure aborts the stream cleanly: Close releases
//     buffers and stops all in-flight workers.
//   - an empty read range yields immediate io.EOF.
package reader

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ChunkSource is the transport-agnostic interface the parallel reader
// fans out across. Chunk reads exactly limit bytes starting at object-
// space offset (or fewer at EOF); the reader handles leftCut/rightCut
// trimming for unaligned ranges. ChunkSize returns the source's natural
// alignment for a given byte range; it may halve the default for tiny
// ranges to avoid over-fetching. Implementations must be safe for
// concurrent use — the reader calls Chunk from N worker goroutines.
type ChunkSource interface {
	Chunk(ctx context.Context, offset, limit int64) ([]byte, error)
	ChunkSize(start, end int64) int64
}

// Reader is an io.ReadCloser that delivers [start, end) of the underlying
// ChunkSource in order using a bounded concurrent prefetch pool.
type Reader struct {
	src ChunkSource

	chunkSize    int64
	start        int64 // first object-space byte requested (inclusive)
	end          int64 // one past last object-space byte (exclusive)
	alignedStart int64
	nChunks      int

	// results[i] receives the buffer (or error) for aligned chunk i. Each
	// slot has cap 1, so a worker blocks on send until the drainer reads —
	// natural backpressure keeps in-flight memory bounded by concurrency.
	results []chan *buffer

	// out is the ordered delivery channel the drainer writes to and Read
	// consumes. A small buffer (>= 1) lets the drainer stay slightly ahead
	// of a slow consumer.
	out chan *buffer

	cancel    context.CancelFunc
	workersWG sync.WaitGroup
	drainerWG sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once

	cur *buffer // currently-draining buffer (head of out)
	err error   // sticky read-side error
}

// New constructs a Reader over the half-open byte range [start, end) of
// src. concurrency caps the number of in-flight Chunk calls; outBuffers
// is the capacity of the ordered delivery channel (kept small so the
// drainer doesn't sprint too far ahead of the consumer). chunkTimeout
// applies per-Chunk call when > 0.
//
// The construction is non-blocking: Chunk calls fire asynchronously.
// Call Prime to synchronously surface a first-chunk failure before
// writing response headers.
func New(ctx context.Context, src ChunkSource, start, end int64, concurrency, outBuffers int, chunkTimeout time.Duration) *Reader {
	if start < 0 {
		start = 0
	}
	if end < start {
		end = start
	}
	chunkSize := src.ChunkSize(start, end)
	if chunkSize <= 0 {
		chunkSize = 1 << 20 // last-ditch sanity default; ChunkSource impls should never return 0.
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if outBuffers <= 0 {
		outBuffers = 1
	}

	pctx, cancel := context.WithCancel(ctx)
	r := &Reader{
		src:       src,
		chunkSize: chunkSize,
		start:     start,
		end:       end,
		out:       make(chan *buffer, outBuffers),
		cancel:    cancel,
	}

	// Empty range: nothing to fetch. Close the delivery channel so the
	// first Prime/Read returns io.EOF immediately. Close() is still safe
	// (cancel + drain are no-ops).
	if start >= end {
		close(r.out)
		return r
	}

	alignedStart := start - (start % chunkSize)
	alignedEnd := end
	if rem := end % chunkSize; rem != 0 {
		alignedEnd = end + (chunkSize - rem)
	}
	r.alignedStart = alignedStart
	r.nChunks = int((alignedEnd - alignedStart) / chunkSize)
	r.results = make([]chan *buffer, r.nChunks)
	for i := range r.results {
		r.results[i] = make(chan *buffer, 1)
	}

	// Workers race for indices via an atomic counter. Each fetches one
	// aligned chunk and writes the buffer (or error) into results[i],
	// then loops for the next index. The per-index channel keeps order
	// independent of fetch completion order.
	var idx atomic.Int64
	workerCount := concurrency
	if workerCount > r.nChunks {
		workerCount = r.nChunks
	}
	for w := 0; w < workerCount; w++ {
		r.workersWG.Add(1)
		go r.worker(pctx, &idx, alignedStart, chunkTimeout)
	}
	r.drainerWG.Add(1)
	go r.drain(pctx)
	return r
}

func (r *Reader) worker(ctx context.Context, idx *atomic.Int64, alignedStart int64, chunkTimeout time.Duration) {
	defer r.workersWG.Done()
	for {
		i := int(idx.Add(1) - 1)
		if i >= r.nChunks {
			return
		}
		// If the consumer is gone (Close called), don't bother fetching;
		// just publish a cancellation buffer so the drainer (which may
		// also be exiting) doesn't block on this slot indefinitely.
		if ctx.Err() != nil {
			select {
			case r.results[i] <- &buffer{err: ctx.Err()}:
			default:
			}
			return
		}
		off := alignedStart + int64(i)*r.chunkSize
		fetchCtx := ctx
		var cancel context.CancelFunc
		if chunkTimeout > 0 {
			fetchCtx, cancel = context.WithTimeout(ctx, chunkTimeout)
		}
		data, err := r.src.Chunk(fetchCtx, off, r.chunkSize)
		if cancel != nil {
			cancel()
		}
		b := &buffer{buf: data, err: err}
		select {
		case r.results[i] <- b:
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reader) drain(ctx context.Context) {
	defer r.drainerWG.Done()
	defer close(r.out)
	for i := 0; i < r.nChunks; i++ {
		var b *buffer
		select {
		case <-ctx.Done():
			return
		case b = <-r.results[i]:
		}
		if b.err != nil {
			// Deliver the error so Read surfaces it. The drainer exits;
			// remaining workers are cancelled when Close cancels ctx.
			select {
			case r.out <- b:
			case <-ctx.Done():
			}
			return
		}
		// Trim the buffer to the intersection of the aligned chunk's
		// object-space window with the caller's [start, end). This
		// handles leftCut (first chunk) and rightCut (last chunk)
		// uniformly, and naturally collapses to an empty buffer when
		// the source returned a short read at EOF that doesn't reach
		// the requested range.
		chunkOff := r.alignedStart + int64(i)*r.chunkSize
		chunkEnd := chunkOff + int64(len(b.buf))
		lo := chunkOff
		if lo < r.start {
			lo = r.start
		}
		hi := chunkEnd
		if hi > r.end {
			hi = r.end
		}
		if hi <= lo {
			b.buf = nil
		} else {
			b.buf = b.buf[lo-chunkOff : hi-chunkOff]
		}
		select {
		case r.out <- b:
		case <-ctx.Done():
			return
		}
	}
}

// Prime synchronously waits for the first chunk to be ready (or the
// stream to be empty / fail). The first buffer is staged in r.cur so a
// subsequent Read consumes it normally. Returns io.EOF for an empty
// range, the first chunk's error if it failed, or nil on success.
//
// Call Prime before committing any HTTP status line: a fan-out failure
// on chunk 0 must produce 502, not a truncated 200.
func (r *Reader) Prime() error {
	if r.cur != nil {
		return nil
	}
	b, ok := <-r.out
	if !ok {
		// Closed-without-data: either empty stream or already-drained.
		return io.EOF
	}
	if b.err != nil {
		r.err = b.err
		return b.err
	}
	r.cur = b
	return nil
}

// Read drains the prefetched buffers in order. Errors from a worker (a
// per-chunk fetch failure) are surfaced when the corresponding buffer
// would otherwise be returned.
func (r *Reader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	for {
		if r.cur == nil {
			b, ok := <-r.out
			if !ok {
				r.err = io.EOF
				return 0, io.EOF
			}
			if b.err != nil {
				r.err = b.err
				return 0, b.err
			}
			r.cur = b
		}
		if r.cur.isEmpty() {
			r.cur = nil
			continue
		}
		n := copy(p, r.cur.bytes())
		r.cur.advance(n)
		return n, nil
	}
}

// Close cancels in-flight workers and releases buffered chunks. Safe to
// call more than once.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.cancel()
		// Drain the out channel so the drainer goroutine isn't blocked
		// trying to send into it. Workers are cancelled via ctx; they
		// may still publish one final buffer to their results slot
		// before exiting, which the drainer will see (or skip via the
		// ctx.Done branch).
		go func() {
			for range r.out { //nolint:revive // explicit drain
			}
		}()
		r.workersWG.Wait()
		r.drainerWG.Wait()
	})
	return nil
}
