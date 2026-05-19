package telegram

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
