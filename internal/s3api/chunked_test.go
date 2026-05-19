package s3api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---- framing helpers -------------------------------------------------------

func signedChunk(data []byte, sig string) string {
	return fmt.Sprintf("%x;chunk-signature=%s\r\n%s\r\n", len(data), sig, data)
}

func unsignedChunk(data []byte) string {
	return fmt.Sprintf("%x\r\n%s\r\n", len(data), data)
}

// readAllWithBuffer drains r using a fixed-size buffer so partial-chunk Read
// paths (buffer smaller than a chunk, chunk boundary mid-buffer) are exercised.
func readAllWithBuffer(t *testing.T, r io.Reader, bufSize int) []byte {
	t.Helper()
	var out bytes.Buffer
	buf := make([]byte, bufSize)
	for {
		n, err := r.Read(buf)
		out.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
	}
	return out.Bytes()
}

// ---- isAWSChunked detection truth table ------------------------------------

func TestIsAWSChunked(t *testing.T) {
	cases := []struct {
		name            string
		contentEncoding string
		contentSHA256   string
		want            bool
	}{
		{"plain unsigned payload (Gokapi path)", "", "UNSIGNED-PAYLOAD", false},
		{"plain sha256 hex", "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", false},
		{"no headers", "", "", false},
		{"streaming unsigned trailer", "", "STREAMING-UNSIGNED-PAYLOAD-TRAILER", true},
		{"streaming signed", "", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", true},
		{"streaming signed trailer", "", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", true},
		{"content-encoding aws-chunked", "aws-chunked", "UNSIGNED-PAYLOAD", true},
		{"content-encoding aws-chunked,gzip", "aws-chunked,gzip", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "/b/k", nil)
			if c.contentEncoding != "" {
				r.Header.Set("Content-Encoding", c.contentEncoding)
			}
			if c.contentSHA256 != "" {
				r.Header.Set("X-Amz-Content-Sha256", c.contentSHA256)
			}
			if got := isAWSChunked(r); got != c.want {
				t.Fatalf("isAWSChunked = %v, want %v", got, c.want)
			}
		})
	}
}

// ---- AWS official signed PUT vector (S3-COMPAT-PLAN.md §3.2) ----------------
//
// AWS "Transferring payload in multiple chunks" example: 66560 bytes of 'a',
// framed as 65536 + 1024 + 0 with the documented seed/chunk signatures. The
// de-framer must ignore ;chunk-signature= and yield exactly the 66560 bytes.
func TestAWSChunkedReader_OfficialSignedVector(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 66560)
	const (
		sig1 = "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
		sig2 = "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
		sig3 = "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
	)
	body := signedChunk(payload[:65536], sig1) +
		signedChunk(payload[65536:], sig2) +
		fmt.Sprintf("0;chunk-signature=%s\r\n\r\n", sig3)

	for _, bufSize := range []int{1, 7, 4096, 70000} {
		got := readAllWithBuffer(t, newAWSChunkedReader(strings.NewReader(body)), bufSize)
		if !bytes.Equal(got, payload) {
			t.Fatalf("bufSize=%d: decoded %d bytes, want %d (equal=%v)",
				bufSize, len(got), len(payload), bytes.Equal(got, payload))
		}
	}
}

// ---- STREAMING-UNSIGNED-PAYLOAD-TRAILER (the common modern-client mode) -----
//
// Same layout but no ;chunk-signature=, followed by a CRC32 trailer block
// (seed 106e2a8a... per §3.2). The de-framer must stop at the 0-chunk and
// return exactly the payload, leaving the trailer unread.
func TestAWSChunkedReader_UnsignedTrailerVector(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 66560)
	body := unsignedChunk(payload[:65536]) +
		unsignedChunk(payload[65536:]) +
		"0\r\n" +
		"x-amz-checksum-crc32:dCXkrg==\r\n" + // arbitrary trailer; must be ignored
		"\r\n"

	for _, bufSize := range []int{1, 13, 4096, 70000} {
		got := readAllWithBuffer(t, newAWSChunkedReader(strings.NewReader(body)), bufSize)
		if !bytes.Equal(got, payload) {
			t.Fatalf("bufSize=%d: decoded %d bytes, want %d", bufSize, len(got), len(payload))
		}
	}
}

// ---- signed trailer mode: ;chunk-signature= AND a trailer signature --------

func TestAWSChunkedReader_SignedTrailerVector(t *testing.T) {
	payload := []byte("hello world, telegram-s3")
	body := signedChunk(payload, "deadbeef") +
		"0;chunk-signature=cafebabe\r\n" +
		"x-amz-checksum-crc32:dCXkrg==\r\n" +
		"x-amz-trailer-signature:0123456789abcdef\r\n" +
		"\r\n"

	got := readAllWithBuffer(t, newAWSChunkedReader(strings.NewReader(body)), 4096)
	if !bytes.Equal(got, payload) {
		t.Fatalf("decoded %q, want %q", got, payload)
	}
}

// ---- many small chunks, default buffer -------------------------------------

func TestAWSChunkedReader_ManySmallChunks(t *testing.T) {
	var want bytes.Buffer
	var body strings.Builder
	for i := 0; i < 500; i++ {
		piece := []byte(fmt.Sprintf("chunk-%03d-data;", i))
		want.Write(piece)
		body.WriteString(unsignedChunk(piece))
	}
	body.WriteString("0\r\n\r\n")

	got := readAllWithBuffer(t, newAWSChunkedReader(strings.NewReader(body.String())), 64)
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("decoded %d bytes, want %d", len(got), want.Len())
	}
}

// ---- post-EOF Read is stable -----------------------------------------------

func TestAWSChunkedReader_EOFStable(t *testing.T) {
	r := newAWSChunkedReader(strings.NewReader(unsignedChunk([]byte("xy")) + "0\r\n\r\n"))
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	for i := 0; i < 3; i++ {
		n, err := r.Read(make([]byte, 8))
		if n != 0 || err != io.EOF {
			t.Fatalf("post-EOF Read = (%d, %v), want (0, EOF)", n, err)
		}
	}
}

// ---- corruption must be rejected, never silently accepted ------------------

func TestAWSChunkedReader_Errors(t *testing.T) {
	cases := []struct {
		name string
		body string
		want error // errors.Is target, or nil to just require non-nil
	}{
		{"data shorter than declared size", "64\r\n" + strings.Repeat("a", 50), io.ErrUnexpectedEOF},
		{"missing terminating 0-chunk", unsignedChunk([]byte("abc")), io.ErrUnexpectedEOF},
		{"truncated header (no newline)", "10", io.ErrUnexpectedEOF},
		{"missing CRLF after data", "3\r\nabc", io.ErrUnexpectedEOF},
		{"malformed chunk terminator", "3\r\nabcXX0\r\n\r\n", nil},
		{"invalid hex chunk size", "zz\r\nabc\r\n0\r\n\r\n", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := io.ReadAll(newAWSChunkedReader(strings.NewReader(c.body)))
			if err == nil {
				t.Fatalf("expected an error, got nil (silent corruption!)")
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("error = %v, want errors.Is %v", err, c.want)
			}
		})
	}
}

// ---- countingReader --------------------------------------------------------

func TestCountingReader(t *testing.T) {
	src := bytes.Repeat([]byte("z"), 12345)
	c := &countingReader{r: bytes.NewReader(src)}
	n, err := io.Copy(io.Discard, c)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if n != 12345 || c.n != 12345 {
		t.Fatalf("counted %d (io.Copy %d), want 12345", c.n, n)
	}
}

// End-to-end: a decoded stream's byte count (the value stored as object Size)
// matches X-Amz-Decoded-Content-Length, the property putObject enforces.
func TestChunkedDecodeSizeMatchesDecodedContentLength(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 66560)
	body := unsignedChunk(payload[:65536]) + unsignedChunk(payload[65536:]) + "0\r\n\r\n"

	c := &countingReader{r: newAWSChunkedReader(strings.NewReader(body))}
	if _, err := io.Copy(io.Discard, c); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if c.n != int64(len(payload)) {
		t.Fatalf("decoded size = %d, want %d (X-Amz-Decoded-Content-Length)", c.n, len(payload))
	}
}
