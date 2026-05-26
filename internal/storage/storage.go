package storage

import (
	"context"
	"io"
)

// Chunk is one Telegram message holding a contiguous slice of an object.
// An object is the ordered concatenation of its chunks (Seq 0..N).
type Chunk struct {
	Seq       int
	FileID    string
	MessageID int64
	Size      int64
	Offset    int64 // byte offset of this chunk's first byte within the object
	Transport string
	BotIndex  int
}

// Transport identifier persisted on every chunk row. Kept as a column
// + struct field for schema compatibility (additive-only invariant);
// only one value is in use post-migration.
const TransportMTProto = "mtproto"

// ChunkRef is the transport-agnostic locator the Backend uses to fetch
// or delete one Telegram message. MessageID + BotIndex are the
// load-bearing fields under MTProto.
type ChunkRef struct {
	Transport string
	BotFileID string
	MessageID int64
	BotIndex  int
}

type Backend interface {
	// Upload streams body, splitting it into Telegram messages each no larger
	// than the backend's chunk size, and returns the ordered chunk list.
	Upload(ctx context.Context, name, contentType string, body io.Reader) ([]Chunk, error)
	// Download returns a reader over a single chunk's full content.
	Download(ctx context.Context, ref ChunkRef) (io.ReadCloser, error)
	// DownloadRange returns [offset, offset+length) of a single chunk.
	// length <= 0 means "to end of chunk".
	DownloadRange(ctx context.Context, ref ChunkRef, offset, length int64) (io.ReadCloser, error)
	// Delete removes a single Telegram message (hard delete of stored bytes).
	Delete(ctx context.Context, ref ChunkRef) error
	// DeleteBatch removes a batch of messages. MTProto batches at
	// 100/call via ChannelsDeleteMessages. The error returned is the
	// first one encountered; per-ref failures are logged by the backend
	// so the caller can treat this as best-effort cleanup.
	DeleteBatch(ctx context.Context, refs []ChunkRef) error
}
