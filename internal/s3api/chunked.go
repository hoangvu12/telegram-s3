package s3api

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// ErrMalformedChunked marks any failure to de-frame an `aws-chunked` body
// (8.3, closes §6.4). The caller wraps it in a 400 IncompleteBody so a
// malformed frame surfaces as a client error rather than a TelegramUploadFailed
// 502 (the Telegram backend's Upload error simply propagates this sentinel).
var ErrMalformedChunked = errors.New("aws-chunked: malformed body")

// isAWSChunked reports whether the request body is framed in the S3
// `aws-chunked` content-encoding (a streaming upload). Both the unsigned and
// signed streaming modes set one of these. A plain UNSIGNED-PAYLOAD body
// (the aws-sdk-go v1 / Gokapi path) sets neither and must be left untouched.
func isAWSChunked(r *http.Request) bool {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") {
		return true
	}
	return strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-")
}

// awsChunkedReader de-frames an `aws-chunked` request body, yielding only the
// real object bytes. It handles both the unsigned-trailer framing
// (`<hexsize>\r\n<data>\r\n`) and the signed framing
// (`<hexsize>;chunk-signature=<sig>\r\n<data>\r\n`) by ignoring any chunk
// extension after the size. Per-chunk signatures are NOT verified (Phase 1
// scope per S3-COMPAT-PLAN.md §3.2); the trailing checksum/signature block
// after the final 0-sized chunk is left unread.
type awsChunkedReader struct {
	br        *bufio.Reader
	remaining int64
	done      bool
}

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{br: bufio.NewReader(r)}
}

func (c *awsChunkedReader) Read(p []byte) (int, error) {
	if c.done {
		return 0, io.EOF
	}
	if c.remaining == 0 {
		size, err := c.readChunkSize()
		if err != nil {
			return 0, err
		}
		if size == 0 {
			// Final chunk. The trailer block (if any) is intentionally
			// left unread; net/http drains the rest of the body.
			c.done = true
			return 0, io.EOF
		}
		c.remaining = size
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.br.Read(p)
	c.remaining -= int64(n)
	if err == io.EOF {
		// Stream ended before the terminating 0-sized chunk.
		err = fmt.Errorf("%w: truncated chunk body", ErrMalformedChunked)
	}
	if c.remaining == 0 && err == nil {
		err = c.consumeCRLF()
	}
	return n, err
}

// readChunkSize reads a `<hexsize>[;ext...]\r\n` line and returns the size.
func (c *awsChunkedReader) readChunkSize() (int64, error) {
	line, err := c.readLine()
	if err != nil {
		return 0, err
	}
	if i := strings.IndexByte(line, ';'); i >= 0 {
		line = line[:i]
	}
	size, err := strconv.ParseInt(strings.TrimSpace(line), 16, 64)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("%w: invalid chunk size", ErrMalformedChunked)
	}
	return size, nil
}

func (c *awsChunkedReader) readLine() (string, error) {
	// AWS chunk headers are tiny (hex size + optional ";chunk-signature=" +
	// 64 hex chars), far under bufio's default 4 KiB buffer. ReadSlice keeps
	// a malformed/malicious header without a newline from growing unbounded.
	line, err := c.br.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			return "", fmt.Errorf("%w: chunk header too long", ErrMalformedChunked)
		}
		if err == io.EOF {
			err = fmt.Errorf("%w: truncated chunk header", ErrMalformedChunked)
		}
		return "", err
	}
	return strings.TrimRight(string(line), "\r\n"), nil
}

func (c *awsChunkedReader) consumeCRLF() error {
	var buf [2]byte
	if _, err := io.ReadFull(c.br, buf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return fmt.Errorf("%w: missing chunk terminator", ErrMalformedChunked)
		}
		return err
	}
	if buf[0] != '\r' || buf[1] != '\n' {
		return fmt.Errorf("%w: malformed chunk terminator", ErrMalformedChunked)
	}
	return nil
}

// countingReader counts the bytes read through it. Used to derive the true
// decoded object size (ground truth) and to validate it against the
// client-declared X-Amz-Decoded-Content-Length.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
