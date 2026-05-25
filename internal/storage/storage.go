package storage

import (
	"context"
	"io"
)

// Chunk is one Telegram message holding a contiguous slice of an object.
// An object is the ordered concatenation of its chunks (Seq 0..N).
//
// Transport and BotIndex are populated by the backend at upload time
// (Phase 2 introduces the fields; Phase 3 persists them to SQLite and
// makes BotIndex meaningful with the multi-bot pool). Both default to
// the zero value for legacy rows read from a pre-Phase-3 DB; the
// dispatcher treats Transport == "" as "bot" so old rows keep flowing
// through BotStorage.
type Chunk struct {
	Seq       int
	FileID    string
	MessageID int64
	Size      int64
	Offset    int64 // byte offset of this chunk's first byte within the object
	Transport string
	BotIndex  int
}

// Ref builds a ChunkRef from a Chunk, defaulting Transport to "bot" so
// legacy rows resolve correctly. Used at every backend call site so the
// transport selection lives in one place.
func (c Chunk) Ref() ChunkRef {
	transport := c.Transport
	if transport == "" {
		transport = TransportBot
	}
	return ChunkRef{
		Transport: transport,
		BotFileID: c.FileID,
		MessageID: c.MessageID,
		BotIndex:  c.BotIndex,
	}
}

// Transport identifiers used by Chunk.Transport / ChunkRef.Transport.
const (
	TransportBot     = "bot"
	TransportMTProto = "mtproto"
)

// ChunkRef is the transport-agnostic locator the Backend uses to fetch
// or delete one Telegram message. Bot HTTP API path: BotFileID is set
// and identifies the file; MTProto path (Phase 4): MessageID +
// BotIndex are the load-bearing fields and BotFileID is unused. The
// dispatcher in Phase 4 switches on Transport.
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
	// DeleteBatch removes a batch of messages. The Bot HTTP API has no
	// batched delete so the BotStorage impl fan-outs serially; MTProto
	// (Phase 4) batches at 100/call via ChannelsDeleteMessages. The error
	// returned is the first one encountered; per-ref failures are logged
	// by the backend so the caller can treat this as best-effort cleanup.
	DeleteBatch(ctx context.Context, refs []ChunkRef) error
}
