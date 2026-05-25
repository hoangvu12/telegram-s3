package reader

import (
	"context"
	"fmt"
	"io"
	"sort"

	"telegram-s3/internal/storage"
)

// ChunkLoc maps one Telegram message to its object-space byte range. The
// reader translates the prefetch's object-space offset to (ref, localOff)
// pairs by binary-searching this list.
type ChunkLoc struct {
	Ref    storage.ChunkRef
	Offset int64 // object-space offset where this message's first byte sits
	Size   int64
}

// BotSource is a ChunkSource that translates object-space prefetch reads
// into Backend.DownloadRange calls against the underlying Telegram
// messages. One BotSource per object; locs must be sorted by Offset.
//
// A single Chunk call may straddle Telegram message boundaries when the
// prefetch chunkSize doesn't align with the upload chunk size (e.g. a 4
// MiB prefetch over an object whose upload chunks were 3 MiB).
// BotSource issues sequential DownloadRange calls and concatenates the
// results in that case; callers should size their prefetch chunks to fit
// within the upload chunks for best performance.
type BotSource struct {
	backend   storage.Backend
	objSize   int64
	locs      []ChunkLoc
	chunkSize int64 // configured prefetch chunk size (object-space); halved for tiny ranges
}

// NewBotSource constructs a source over the given chunk map. objSize is
// the total object size; locs must be sorted by Offset and cover the
// whole object (object_chunks rows). chunkSize is the prefetch window
// (e.g. 4 MiB); it does not need to align with the upload chunks.
func NewBotSource(backend storage.Backend, objSize int64, locs []ChunkLoc, chunkSize int64) *BotSource {
	return &BotSource{backend: backend, objSize: objSize, locs: locs, chunkSize: chunkSize}
}

// ChunkSize returns the prefetch window, halved for tiny ranges so a
// 100-byte Range request doesn't fetch 4 MiB just to throw most of it
// away. Floor is 64 KiB so even a small read still amortizes the
// per-fetch overhead (getFile + TLS).
func (s *BotSource) ChunkSize(start, end int64) int64 {
	cs := s.chunkSize
	if cs <= 0 {
		cs = 1 << 22 // 4 MiB
	}
	span := end - start
	for cs > 64<<10 && cs > span*2 && span > 0 {
		cs >>= 1
	}
	return cs
}

// Chunk reads [offset, offset+limit) from the underlying messages,
// stitching together multiple DownloadRange calls if the range straddles
// a chunk boundary. A read past objSize returns the available prefix and
// silently truncates (the parallel reader's alignedEnd may overshoot).
func (s *BotSource) Chunk(ctx context.Context, offset, limit int64) ([]byte, error) {
	if offset >= s.objSize {
		return nil, nil
	}
	end := offset + limit
	if end > s.objSize {
		end = s.objSize
	}

	out := make([]byte, 0, end-offset)
	cur := offset
	for cur < end {
		loc, localOff, ok := s.locate(cur)
		if !ok {
			return nil, fmt.Errorf("reader: offset %d outside chunk map", cur)
		}
		readEnd := loc.Offset + loc.Size
		if readEnd > end {
			readEnd = end
		}
		want := readEnd - cur
		rc, err := s.backend.DownloadRange(ctx, loc.Ref, localOff, want)
		if err != nil {
			return nil, err
		}
		n, err := io.Copy(byteSliceWriter{&out}, io.LimitReader(rc, want))
		rc.Close()
		if err != nil {
			return nil, err
		}
		cur += n
		if n == 0 || n < want {
			// Short read at EOF or unexpected truncation: stop without
			// surfacing an error — the caller (drainer) trims via
			// rightCut and the read still respects objSize.
			break
		}
	}
	return out, nil
}

// locate returns the loc containing the given object-space offset along
// with the offset *within* that message's bytes. Binary search keeps
// large chunk maps cheap (a 1 TiB object at 18 MiB chunks has ~57k locs).
func (s *BotSource) locate(offset int64) (ChunkLoc, int64, bool) {
	i := sort.Search(len(s.locs), func(i int) bool {
		return s.locs[i].Offset > offset
	}) - 1
	if i < 0 {
		return ChunkLoc{}, 0, false
	}
	loc := s.locs[i]
	if offset >= loc.Offset+loc.Size {
		return ChunkLoc{}, 0, false
	}
	return loc, offset - loc.Offset, true
}

// byteSliceWriter is a tiny io.Writer that appends to the underlying
// slice. io.Copy through this avoids the ReadAll allocation pattern when
// the chunk is large.
type byteSliceWriter struct{ p *[]byte }

func (w byteSliceWriter) Write(p []byte) (int, error) {
	*w.p = append(*w.p, p...)
	return len(p), nil
}
