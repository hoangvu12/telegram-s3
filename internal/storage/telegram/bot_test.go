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

	"telegram-s3/internal/cache"
	"telegram-s3/internal/storage"
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
	// Phase 1 instrumentation:
	getFileCalls int  // count of /getFile resolves
	fileGetCalls int  // count of /file/... CDN reads
	cdn404Next   int  // answer the next N CDN GETs with 404 (path-stale simulation)
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
		f.mu.Lock()
		f.getFileCalls++
		f.mu.Unlock()
		fmt.Fprintf(w, `{"ok":true,"result":{"file_path":%q}}`, "stored/"+fileID)
	})
	mux.HandleFunc("/file/botTOKEN/stored/", func(w http.ResponseWriter, r *http.Request) {
		fileID := strings.TrimPrefix(r.URL.Path, "/file/botTOKEN/stored/")
		f.mu.Lock()
		f.fileGetCalls++
		drop404 := f.cdn404Next > 0
		if drop404 {
			f.cdn404Next--
		}
		data, ok := f.files[fileID]
		f.mu.Unlock()
		if drop404 || !ok {
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
		rc, err := b.Download(ctx, c.Ref())
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
	rc, err := b.DownloadRange(ctx, chunks[0].Ref(), 2, 3)
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
		if err := b.Delete(ctx, c.Ref()); err != nil {
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

// newTestBotWithCache wires the Phase 1 file_path cache into a test
// BotStorage. The default-NewBotStorage path uses nil pathCache, so the
// existing tests still exercise the no-cache fallback.
func newTestBotWithCache(t *testing.T, f *fakeTelegram, chunkSize int, c *cache.Cache[string, string]) *BotStorage {
	srv := f.server(t)
	b := NewBotStorageWithOptions([]string{"TOKEN"}, "chat123", srv.URL, 0, 0, c, nil)
	b.chunkSize = chunkSize
	return b
}

// TestDownloadCachesFilePath: with the path cache wired, repeated
// DownloadRange calls for the same file_id must call getFile exactly once.
// Without the cache, this would be N round-trips per chunk read.
func TestDownloadCachesFilePath(t *testing.T) {
	ctx := context.Background()
	f := newFakeTelegram()
	pc := cache.New[string, string](time.Minute, 0)
	defer pc.Close()
	b := newTestBotWithCache(t, f, 8, pc)

	chunks, err := b.Upload(ctx, "obj", "application/octet-stream",
		bytes.NewReader([]byte("hello world")))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(chunks))
	}

	const reads = 5
	for i := 0; i < reads; i++ {
		rc, err := b.DownloadRange(ctx, chunks[0].Ref(), 0, 0)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getFileCalls != 1 {
		t.Fatalf("getFile called %d times across %d reads; want 1", f.getFileCalls, reads)
	}
	if f.fileGetCalls != reads {
		t.Fatalf("file CDN reads = %d; want %d", f.fileGetCalls, reads)
	}
}

// TestDownloadInvalidatesOnCDN404: a cached file_path that has expired on
// Telegram's side returns 404 from the file CDN. We must invalidate the
// cache entry, re-resolve via getFile, and retry once.
func TestDownloadInvalidatesOnCDN404(t *testing.T) {
	ctx := context.Background()
	f := newFakeTelegram()
	pc := cache.New[string, string](time.Minute, 0)
	defer pc.Close()
	b := newTestBotWithCache(t, f, 8, pc)

	chunks, err := b.Upload(ctx, "obj", "application/octet-stream",
		bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	// Warm the cache so a stale path exists to invalidate.
	rc, err := b.DownloadRange(ctx, chunks[0].Ref(), 0, 0)
	if err != nil {
		t.Fatalf("warm: %v", err)
	}
	rc.Close()

	// Next CDN read 404s once. The retry through getFile should succeed.
	f.mu.Lock()
	f.cdn404Next = 1
	gfBefore := f.getFileCalls
	f.mu.Unlock()

	rc, err = b.DownloadRange(ctx, chunks[0].Ref(), 0, 0)
	if err != nil {
		t.Fatalf("expected one-shot retry to succeed, got: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("retry returned %q, want %q", got, "hello")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if delta := f.getFileCalls - gfBefore; delta != 1 {
		t.Fatalf("expected exactly 1 re-resolve, got %d", delta)
	}
}

// multiBotFake is a token-agnostic version of fakeTelegram used for Phase 3
// pool tests. Routes are dispatched by parsing the {token} segment of
// /bot{token}/{method}, so any number of tokens can hit the same server
// concurrently and the test can verify which one handled each call.
type multiBotFake struct {
	mu sync.Mutex
	// uploadsByToken counts sendDocument hits per token — the test
	// assertion confirms round-robin spread the upload load.
	uploadsByToken map[string]int
	// fileByID records every stored file_id → which token sent it. Used to
	// assert that download routes back through the same token on read.
	fileByID  map[string]string
	seq       int64
	getFileBy map[string]int // file_id -> count of getFile resolves
}

func newMultiBotFake() *multiBotFake {
	return &multiBotFake{
		uploadsByToken: map[string]int{},
		fileByID:       map[string]string{},
		getFileBy:      map[string]int{},
	}
}

// parseBotPath extracts (token, method) from /bot{token}/{method} or
// /file/bot{token}/{path...}. Returns "" token if the path doesn't match.
func parseBotPath(path string) (token, rest string, isFile bool) {
	if strings.HasPrefix(path, "/file/bot") {
		tail := strings.TrimPrefix(path, "/file/bot")
		if i := strings.IndexByte(tail, '/'); i >= 0 {
			return tail[:i], tail[i+1:], true
		}
		return "", "", true
	}
	if strings.HasPrefix(path, "/bot") {
		tail := strings.TrimPrefix(path, "/bot")
		if i := strings.IndexByte(tail, '/'); i >= 0 {
			return tail[:i], tail[i+1:], false
		}
	}
	return "", "", false
}

func (f *multiBotFake) server(t *testing.T) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, rest, isFile := parseBotPath(r.URL.Path)
		if token == "" {
			http.NotFound(w, r)
			return
		}
		switch {
		case isFile:
			fileID := strings.TrimPrefix(rest, "stored/")
			f.mu.Lock()
			storingToken := f.fileByID[fileID]
			f.mu.Unlock()
			// Reads must come back through the SAME token that uploaded —
			// Bot API file_id is bot-bound. The fake enforces that contract.
			if storingToken != token {
				http.Error(w, "file_id not bound to this bot", http.StatusForbidden)
				return
			}
			http.ServeContent(w, r, fileID, time.Time{}, bytes.NewReader([]byte(fileID)))
		case rest == "sendDocument":
			if err := r.ParseMultipartForm(64 << 20); err != nil {
				t.Errorf("sendDocument parse: %v", err)
			}
			f.mu.Lock()
			f.uploadsByToken[token]++
			f.seq++
			fileID := fmt.Sprintf("file-%d", f.seq)
			msgID := 1000 + f.seq
			f.fileByID[fileID] = token
			f.mu.Unlock()
			fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"document":{"file_id":%q}}}`, msgID, fileID)
		case rest == "getFile":
			r.ParseForm()
			fileID := r.FormValue("file_id")
			f.mu.Lock()
			f.getFileBy[fileID]++
			f.mu.Unlock()
			fmt.Fprintf(w, `{"ok":true,"result":{"file_path":%q}}`, "stored/"+fileID)
		case rest == "deleteMessage":
			w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// TestUploadRoundRobinsAcrossTokens: with 2 tokens, six PUTs (one chunk
// each) tag chunks with alternating BotIndex values, and every chunk is
// readable through its assigned bot. This is the Phase 3 end-to-end
// acceptance hint from PHASES.md: bot_index = [0,1,0,1,0,1].
func TestUploadRoundRobinsAcrossTokens(t *testing.T) {
	ctx := context.Background()
	f := newMultiBotFake()
	srv := f.server(t)

	tokens := []string{"TOKA", "TOKB"}
	b := NewBotStorageWithOptions(tokens, "chat", srv.URL, 0, 0, nil, nil)
	b.chunkSize = 1 << 20 // one chunk per object for clarity

	wantSeq := []int{0, 1, 0, 1, 0, 1}
	gotSeq := make([]int, 0, len(wantSeq))
	for i := 0; i < len(wantSeq); i++ {
		chunks, err := b.Upload(ctx, fmt.Sprintf("obj%d", i), "application/octet-stream", bytes.NewReader([]byte("x")))
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
		if len(chunks) != 1 {
			t.Fatalf("upload %d: %d chunks, want 1", i, len(chunks))
		}
		gotSeq = append(gotSeq, chunks[0].BotIndex)
		// Read-back through the right bot must succeed (file_id is bound).
		rc, err := b.DownloadRange(ctx, chunks[0].Ref(), 0, 0)
		if err != nil {
			t.Fatalf("read back %d: %v", i, err)
		}
		rc.Close()
	}
	for i, want := range wantSeq {
		if gotSeq[i] != want {
			t.Fatalf("bot_index sequence = %v, want %v", gotSeq, wantSeq)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploadsByToken["TOKA"] != 3 || f.uploadsByToken["TOKB"] != 3 {
		t.Fatalf("upload distribution = %v, want 3 per token", f.uploadsByToken)
	}
}

// TestDownloadFailsForOutOfRangeBotIndex: a row written on a larger pool
// then read on a smaller pool returns a clean error (the BotPool.At
// safety net) rather than panicking on a slice-index out of bounds.
func TestDownloadFailsForOutOfRangeBotIndex(t *testing.T) {
	f := newMultiBotFake()
	srv := f.server(t)
	b := NewBotStorageWithOptions([]string{"TOKA"}, "chat", srv.URL, 0, 0, nil, nil)
	_, err := b.DownloadRange(context.Background(), storageRefForBot(2, "file-1", 1001), 0, 0)
	if err == nil {
		t.Fatal("expected error for bot_index beyond pool size")
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("error = %v, want one mentioning 'out of range'", err)
	}
}

// storageRefForBot constructs a ChunkRef for the multi-bot fake without
// dragging the s3api package's helpers into the storage tests.
func storageRefForBot(botIndex int, fileID string, messageID int64) storage.ChunkRef {
	return storage.ChunkRef{
		Transport: storage.TransportBot,
		BotFileID: fileID,
		MessageID: messageID,
		BotIndex:  botIndex,
	}
}

// TestDownloadNoCache exercises the nil-cache path (the bare NewBotStorage
// constructor). Repeated reads should each call getFile — the no-cache
// behavior must remain unchanged from Phase 0.
func TestDownloadNoCache(t *testing.T) {
	ctx := context.Background()
	f := newFakeTelegram()
	b := newTestBot(t, f, 8)

	chunks, err := b.Upload(ctx, "obj", "application/octet-stream", bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	const reads = 3
	for i := 0; i < reads; i++ {
		rc, err := b.Download(ctx, chunks[0].Ref())
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getFileCalls != reads {
		t.Fatalf("no-cache getFile calls = %d, want %d", f.getFileCalls, reads)
	}
}
