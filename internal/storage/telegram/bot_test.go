package telegram

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTelegram mocks the Bot API endpoints bot.go uses.
type fakeTelegram struct {
	mu       sync.Mutex
	files    map[string][]byte // file_id -> bytes
	msgFile  map[int64]string  // message_id -> file_id
	deleted  map[int64]bool
	seq      int64
	failFrom int // if >0, sendDocument fails once seq index >= failFrom (0-based count)
	sent     int
	attempts int // total sendDocument calls incl. flooded ones
	floodN   int  // answer the next floodN calls with HTTP 429 (flood control)
	floodRA  int  // retry_after seconds to advertise in the 429 (0 => omit hint)
	permFail bool // answer every call with a permanent HTTP 400 (not retryable)
}

func newFakeTelegram() *fakeTelegram {
	return &fakeTelegram{files: map[string][]byte{}, msgFile: map[int64]string{}, deleted: map[int64]bool{}}
}

func (f *fakeTelegram) server(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/botTOKEN/sendDocument", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			t.Errorf("sendDocument parse: %v", err)
		}
		file, _, err := r.FormFile("document")
		if err != nil {
			t.Errorf("sendDocument FormFile: %v", err)
		}
		data, _ := io.ReadAll(file)
		f.mu.Lock()
		defer f.mu.Unlock()
		f.attempts++
		if f.permFail {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`))
			return
		}
		if f.floodN > 0 {
			f.floodN--
			w.WriteHeader(http.StatusTooManyRequests)
			if f.floodRA > 0 {
				fmt.Fprintf(w, `{"ok":false,"error_code":429,"description":"Too Many Requests: retry after %d","parameters":{"retry_after":%d}}`, f.floodRA, f.floodRA)
			} else {
				w.Write([]byte(`{"ok":false,"error_code":429,"description":"Too Many Requests"}`))
			}
			return
		}
		idx := f.sent
		f.sent++
		if f.failFrom > 0 && idx >= f.failFrom {
			w.Write([]byte(`{"ok":false,"description":"simulated failure"}`))
			return
		}
		f.seq++
		fileID := fmt.Sprintf("file-%d", f.seq)
		msgID := 1000 + f.seq
		f.files[fileID] = data
		f.msgFile[msgID] = fileID
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"document":{"file_id":%q}}}`, msgID, fileID)
	})
	mux.HandleFunc("/botTOKEN/getFile", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		fileID := r.FormValue("file_id")
		fmt.Fprintf(w, `{"ok":true,"result":{"file_path":%q}}`, "stored/"+fileID)
	})
	mux.HandleFunc("/file/botTOKEN/stored/", func(w http.ResponseWriter, r *http.Request) {
		fileID := strings.TrimPrefix(r.URL.Path, "/file/botTOKEN/stored/")
		f.mu.Lock()
		data, ok := f.files[fileID]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		http.ServeContent(w, r, fileID, time.Time{}, bytes.NewReader(data)) // honors Range
	})
	mux.HandleFunc("/botTOKEN/deleteMessage", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		id, _ := strconv.ParseInt(r.FormValue("message_id"), 10, 64)
		f.mu.Lock()
		f.deleted[id] = true
		f.mu.Unlock()
		w.Write([]byte(`{"ok":true}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestBot(t *testing.T, f *fakeTelegram, chunkSize int) *BotStorage {
	srv := f.server(t)
	b := NewBotStorage("TOKEN", "chat123", srv.URL, nil)
	b.chunkSize = chunkSize
	return b
}

func TestUploadSplitsReassemblesDeletes(t *testing.T) {
	ctx := context.Background()
	f := newFakeTelegram()
	b := newTestBot(t, f, 8)

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes -> 8 + 8 + 4
	chunks, err := b.Upload(ctx, "dir/obj.bin", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}
	wantSizes := []int64{8, 8, 4}
	wantOffsets := []int64{0, 8, 16}
	for i, c := range chunks {
		if c.Seq != i || c.Size != wantSizes[i] || c.Offset != wantOffsets[i] {
			t.Fatalf("chunk %d = %+v, want seq=%d size=%d offset=%d", i, c, i, wantSizes[i], wantOffsets[i])
		}
		if c.FileID == "" || c.MessageID == 0 {
			t.Fatalf("chunk %d missing FileID/MessageID: %+v", i, c)
		}
	}

	// Reassemble via Download in order == original bytes.
	var got bytes.Buffer
	for _, c := range chunks {
		rc, err := b.Download(ctx, c.FileID)
		if err != nil {
			t.Fatalf("download %s: %v", c.FileID, err)
		}
		io.Copy(&got, rc)
		rc.Close()
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("reassembled %q, want %q", got.Bytes(), payload)
	}

	// Ranged read within the first chunk (bytes [2,5)).
	rc, err := b.DownloadRange(ctx, chunks[0].FileID, 2, 3)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	rb, _ := io.ReadAll(rc)
	rc.Close()
	if string(rb) != "234" {
		t.Fatalf("range read = %q, want %q", rb, "234")
	}

	// Delete reaches deleteMessage for each chunk's message.
	for _, c := range chunks {
		if err := b.Delete(ctx, c.MessageID); err != nil {
			t.Fatalf("delete %d: %v", c.MessageID, err)
		}
		if !f.deleted[c.MessageID] {
			t.Fatalf("message %d not deleted on server", c.MessageID)
		}
	}
}

func TestUploadEmptyBodyNoChunks(t *testing.T) {
	f := newFakeTelegram()
	b := newTestBot(t, f, 8)
	chunks, err := b.Upload(context.Background(), "empty", "application/octet-stream", strings.NewReader(""))
	if err != nil {
		t.Fatalf("upload empty: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("empty body produced %d chunks, want 0", len(chunks))
	}
	if f.sent != 0 {
		t.Fatalf("sendDocument called %d times for empty body, want 0", f.sent)
	}
}

func TestUploadCleansUpOnMidStreamFailure(t *testing.T) {
	f := newFakeTelegram()
	f.failFrom = 1 // first chunk succeeds, second fails
	b := newTestBot(t, f, 8)

	_, err := b.Upload(context.Background(), "obj", "application/octet-stream",
		bytes.NewReader([]byte("0123456789ABCDEFGHIJ")))
	if err == nil {
		t.Fatal("expected upload error when a chunk send fails")
	}
	// The successfully-sent first chunk (message 1001) must be reaped.
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.deleted[1001] {
		t.Fatalf("orphaned first chunk not cleaned up; deleted=%v", f.deleted)
	}
}

// TestUploadRetriesFloodControlThenSucceeds: a burst that trips Telegram's
// flood control (HTTP 429) must be retried, not surfaced as a hard failure.
func TestUploadRetriesFloodControlThenSucceeds(t *testing.T) {
	f := newFakeTelegram()
	f.floodN = 2 // first two sendDocument calls 429, third succeeds
	b := newTestBot(t, f, 8)

	chunks, err := b.Upload(context.Background(), "obj", "application/octet-stream",
		bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("upload should survive flood control, got: %v", err)
	}
	if len(chunks) != 1 || chunks[0].Size != 5 {
		t.Fatalf("chunks = %+v, want one 5-byte chunk", chunks)
	}
	if f.attempts != 3 || f.sent != 1 {
		t.Fatalf("attempts=%d sent=%d, want 3 attempts and 1 stored", f.attempts, f.sent)
	}
}

// TestUploadDoesNotRetryPermanentError: a non-transient Telegram error (4xx
// other than 429) must fail fast, not burn the retry budget.
func TestUploadDoesNotRetryPermanentError(t *testing.T) {
	f := newFakeTelegram()
	f.permFail = true
	b := newTestBot(t, f, 8)

	start := time.Now()
	_, err := b.Upload(context.Background(), "obj", "application/octet-stream",
		bytes.NewReader([]byte("hello")))
	if err == nil {
		t.Fatal("expected a permanent error")
	}
	if f.attempts != 1 {
		t.Fatalf("permanent error retried: attempts=%d, want 1", f.attempts)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("permanent error should not back off; took %s", elapsed)
	}
}

// TestUploadRetryRespectsContextCancel: a wedged backend (perpetual 429) must
// not hang the request — a cancelled context aborts the retry loop promptly.
func TestUploadRetryRespectsContextCancel(t *testing.T) {
	f := newFakeTelegram()
	f.floodN = 1000 // never recovers
	b := newTestBot(t, f, 8)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := b.Upload(ctx, "obj", "application/octet-stream", bytes.NewReader([]byte("hello")))
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("retry loop ignored ctx cancel; took %s", elapsed)
	}
}

// TestUploadSmallObjectDoesNotBufferChunkSize is the OOM regression guard: a
// tiny object with a production-sized chunk window must allocate ~its size,
// not chunkSize. The old make([]byte, chunkSize) allocated chunkSize per call
// regardless of payload and OOM-killed the container under concurrent PUTs.
func TestUploadSmallObjectDoesNotBufferChunkSize(t *testing.T) {
	f := newFakeTelegram()
	b := newTestBot(t, f, 32<<20) // 32 MiB window, like production's 18 MiB

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	chunks, err := b.Upload(context.Background(), "tiny", "application/octet-stream",
		bytes.NewReader([]byte("x")))
	runtime.ReadMemStats(&after)

	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(chunks) != 1 || chunks[0].Size != 1 {
		t.Fatalf("chunks = %+v, want one 1-byte chunk", chunks)
	}
	const limit = 4 << 20 // generous; old code would allocate >= 32 MiB here
	if grew := after.TotalAlloc - before.TotalAlloc; grew > limit {
		t.Fatalf("Upload of 1 byte allocated %d bytes (> %d) — chunkSize-sized buffer regressed", grew, limit)
	}
}

func TestUploadExactMultipleOfChunkSize(t *testing.T) {
	f := newFakeTelegram()
	b := newTestBot(t, f, 8)
	chunks, err := b.Upload(context.Background(), "obj", "application/octet-stream",
		bytes.NewReader(bytes.Repeat([]byte("x"), 16)))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(chunks) != 2 || chunks[0].Size != 8 || chunks[1].Size != 8 {
		t.Fatalf("16 bytes / 8 = %+v, want two 8-byte chunks (no trailing empty message)", chunks)
	}
}
