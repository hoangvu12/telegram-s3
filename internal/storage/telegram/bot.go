package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"telegram-s3/internal/storage"
)

// MaxChunkSize keeps every Telegram message independently downloadable: the
// public Bot API getFile limit is ~20 MB, so each chunk stays under it. A
// self-hosted local Bot API server removes the cap (see config), but 18 MiB
// is a safe default that works on the public API unchanged.
const MaxChunkSize = 18 << 20

type BotStorage struct {
	token     string
	chatID    string
	baseURL   string
	chunkSize int // == MaxChunkSize in production; overridable in tests
	client    *http.Client
	logger    *slog.Logger
}

func NewBotStorage(token, chatID, baseURL string, logger *slog.Logger) *BotStorage {
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}
	return &BotStorage{
		token:     token,
		chatID:    chatID,
		baseURL:   strings.TrimRight(baseURL, "/"),
		chunkSize: MaxChunkSize,
		client:    &http.Client{Timeout: 0},
		logger:    logger,
	}
}

// Upload reads body in chunkSize windows, sending each as its own Telegram
// document, and returns the ordered chunk list. Peak memory is one chunk and
// grows only to the chunk's *actual* size: a 1-byte object costs a few bytes,
// not chunkSize. The previous make([]byte, chunkSize) allocated 18 MiB per
// call regardless of payload size, so ~10 concurrent small PUTs (the aws-cli
// default for `s3 cp --recursive`) pinned ~180 MiB of buffers and the
// container was OOM-killed mid-bulk-upload (the proxy then 502s). Whether the
// body is short/truncated is the caller's concern: we store whatever bytes we
// receive and let putObject validate the total against
// X-Amz-Decoded-Content-Length.
func (b *BotStorage) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]storage.Chunk, error) {
	var chunks []storage.Chunk
	var offset int64
	scratch := make([]byte, 64<<10) // small fixed read buffer, reused every chunk
	var chunk bytes.Buffer          // grows to the chunk's real size, reused
	for seq := 0; ; seq++ {
		chunk.Reset()
		// io.Copy maps a source EOF to err==nil, so a non-nil rerr is a real
		// read/framing error (e.g. malformed aws-chunked input); a short read
		// (n < chunkSize) means the body is exhausted.
		n, rerr := io.CopyBuffer(&chunk, io.LimitReader(body, int64(b.chunkSize)), scratch)
		if n > 0 {
			ch, err := b.sendChunk(ctx, name, contentType, seq, chunk.Bytes())
			if err != nil {
				b.cleanup(ctx, chunks)
				return nil, err
			}
			ch.Seq = seq
			ch.Size = n
			ch.Offset = offset
			chunks = append(chunks, ch)
			offset += n
		}
		if rerr != nil {
			b.cleanup(ctx, chunks) // malformed framing etc.
			return nil, rerr
		}
		if n < int64(b.chunkSize) {
			break // body exhausted (short body is caught upstream)
		}
	}
	return chunks, nil
}

func (b *BotStorage) cleanup(ctx context.Context, chunks []storage.Chunk) {
	for _, c := range chunks {
		if err := b.Delete(ctx, c.MessageID); err != nil && b.logger != nil {
			b.logger.Warn("cleanup orphaned chunk failed", "message_id", c.MessageID, "error", err)
		}
	}
}

// errRetryable wraps a transient sendDocument failure. retryAfter > 0 is the
// delay Telegram explicitly asked for (HTTP 429 / parameters.retry_after);
// retryAfter == 0 means "back off and retry" (network blip / Telegram 5xx).
type errRetryable struct {
	err        error
	retryAfter time.Duration
}

func (e *errRetryable) Error() string { return e.err.Error() }
func (e *errRetryable) Unwrap() error { return e.err }

// sendChunk sends one chunk, retrying transient failures. Telegram enforces
// flood limits (HTTP 429 with parameters.retry_after) and occasionally 5xxs;
// without this a burst of uploads surfaced those as hard 502s to the client.
// data is already in memory, so each attempt simply rebuilds the multipart
// body. Bounded attempts + ctx cancellation keep a wedged backend from
// hanging the request forever.
func (b *BotStorage) sendChunk(ctx context.Context, name, contentType string, seq int, data []byte) (storage.Chunk, error) {
	const maxAttempts = 5
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ch, err := b.sendChunkOnce(ctx, name, contentType, seq, data)
		if err == nil {
			return ch, nil
		}
		lastErr = err
		var re *errRetryable
		if !errors.As(err, &re) || attempt == maxAttempts {
			return storage.Chunk{}, err
		}
		wait := re.retryAfter
		if wait <= 0 { // transient without an explicit hint: exponential backoff
			wait = time.Duration(200*(1<<(attempt-1))) * time.Millisecond
		}
		if wait > 60*time.Second {
			wait = 60 * time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return storage.Chunk{}, ctx.Err()
		case <-timer.C:
		}
	}
	return storage.Chunk{}, lastErr
}

// sendChunkOnce streams data (already in memory) as a multipart sendDocument
// via an io.Pipe, so the request body is not copied into a second buffer. A
// transient failure is returned wrapped in *errRetryable.
func (b *BotStorage) sendChunkOnce(ctx context.Context, name, contentType string, seq int, data []byte) (storage.Chunk, error) {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		var werr error
		defer func() { pw.CloseWithError(werr) }()
		if werr = mw.WriteField("chat_id", b.chatID); werr != nil {
			return
		}
		part, err := mw.CreateFormFile("document", chunkFilename(name, seq))
		if err != nil {
			werr = err
			return
		}
		if _, werr = part.Write(data); werr != nil {
			return
		}
		werr = mw.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.apiURL("sendDocument"), pr)
	if err != nil {
		return storage.Chunk{}, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := b.client.Do(req)
	if err != nil {
		// Transport-level failure (connection reset, timeout): worth a retry.
		return storage.Chunk{}, &errRetryable{err: err}
	}
	defer resp.Body.Close()

	var result sendDocumentResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return storage.Chunk{}, err
	}
	if resp.StatusCode >= 300 || !result.OK {
		sendErr := fmt.Errorf("telegram sendDocument failed: %s", result.Description)
		// 429 (flood control) carries parameters.retry_after; 5xx is a
		// transient Telegram-side error. Both are retryable; a 4xx other
		// than 429 (bad request) is permanent.
		if resp.StatusCode == http.StatusTooManyRequests || result.ErrorCode == http.StatusTooManyRequests || result.Parameters.RetryAfter > 0 {
			ra := time.Duration(result.Parameters.RetryAfter) * time.Second
			if ra <= 0 {
				ra = time.Second
			}
			return storage.Chunk{}, &errRetryable{err: sendErr, retryAfter: ra}
		}
		if resp.StatusCode >= 500 {
			return storage.Chunk{}, &errRetryable{err: sendErr}
		}
		return storage.Chunk{}, sendErr
	}
	if result.Result.Document.FileID == "" {
		return storage.Chunk{}, fmt.Errorf("telegram response did not include document file_id")
	}
	return storage.Chunk{FileID: result.Result.Document.FileID, MessageID: result.Result.MessageID}, nil
}

func (b *BotStorage) Download(ctx context.Context, fileID string) (io.ReadCloser, error) {
	return b.DownloadRange(ctx, fileID, 0, 0)
}

// DownloadRange resolves fileID via getFile then fetches the file, optionally
// requesting only [offset, offset+length). Telegram's file CDN honors HTTP
// Range, which Phase 5 (Range GET) relies on.
func (b *BotStorage) DownloadRange(ctx context.Context, fileID string, offset, length int64) (io.ReadCloser, error) {
	reqBody := strings.NewReader("file_id=" + url.QueryEscape(fileID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.apiURL("getFile"), reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result getFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 || !result.OK {
		return nil, fmt.Errorf("telegram getFile failed: %s", result.Description)
	}

	downloadReq, err := http.NewRequestWithContext(ctx, http.MethodGet, b.fileURL(result.Result.FilePath), nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 || length > 0 {
		if length > 0 {
			downloadReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		} else {
			downloadReq.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
	}
	downloadResp, err := b.client.Do(downloadReq)
	if err != nil {
		return nil, err
	}
	if downloadResp.StatusCode >= 300 {
		downloadResp.Body.Close()
		return nil, fmt.Errorf("telegram file download failed: %s", downloadResp.Status)
	}
	return downloadResp.Body, nil
}

func (b *BotStorage) Delete(ctx context.Context, messageID int64) error {
	form := url.Values{"chat_id": {b.chatID}, "message_id": {strconv.FormatInt(messageID, 10)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.apiURL("deleteMessage"), strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result baseResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if resp.StatusCode >= 300 || !result.OK {
		return fmt.Errorf("telegram deleteMessage failed: %s", result.Description)
	}
	return nil
}

func (b *BotStorage) apiURL(method string) string {
	return fmt.Sprintf("%s/bot%s/%s", b.baseURL, b.token, method)
}

func (b *BotStorage) fileURL(path string) string {
	return fmt.Sprintf("%s/file/bot%s/%s", b.baseURL, b.token, path)
}

func chunkFilename(name string, seq int) string {
	base := safeFilename(name)
	if seq == 0 {
		return base
	}
	return fmt.Sprintf("%s.part%d", base, seq)
}

func safeFilename(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\\", "/"))
	if name == "" || strings.HasSuffix(name, "/") {
		return fmt.Sprintf("object-%d.bin", time.Now().UnixNano())
	}
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}

type baseResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
}

type sendDocumentResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
	Result struct {
		MessageID int64 `json:"message_id"`
		Document  struct {
			FileID string `json:"file_id"`
		} `json:"document"`
	} `json:"result"`
}

type getFileResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	Result      struct {
		FilePath string `json:"file_path"`
	} `json:"result"`
}
