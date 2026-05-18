package storage

import (
	"context"
	"io"
)

type UploadedObject struct {
	FileID    string
	MessageID int64
}

type Backend interface {
	Upload(ctx context.Context, name, contentType string, body io.Reader) (UploadedObject, error)
	Download(ctx context.Context, fileID string) (io.ReadCloser, error)
}
