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
}

type Backend interface {
	// Upload streams body, splitting it into Telegram messages each no larger
	// than the backend's chunk size, and returns the ordered chunk list.
	Upload(ctx context.Context, name, contentType string, body io.Reader) ([]Chunk, error)
	// Download returns a reader over a single chunk's full content.
	Download(ctx context.Context, fileID string) (io.ReadCloser, error)
	// DownloadRange returns [offset, offset+length) of a single chunk.
	// Used by Range GET (Phase 5); length <= 0 means "to end of chunk".
	DownloadRange(ctx context.Context, fileID string, offset, length int64) (io.ReadCloser, error)
	// Delete removes a Telegram message (hard delete of stored bytes).
	Delete(ctx context.Context, messageID int64) error
}
