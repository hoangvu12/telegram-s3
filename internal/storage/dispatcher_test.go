package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// fakeBackend records every call it receives so the dispatcher tests
// can assert routing without standing up real BotStorage / MTProto.
// Each instance is "labelled" so error messages distinguish bot vs
// mtproto routing in test failures.
type fakeBackend struct {
	label string

	mu sync.Mutex
	// Counts let tests assert "the bot saw the bot refs" etc.
	uploads      int
	downloads    int
	downloadRng  int
	deletes      int
	deleteBatch  int
	lastBatchLen int
	lastTxs      []string // transports seen across the last DeleteBatch call

	// Optional injection: if non-nil, Upload returns these and skips
	// the default behavior. Lets tests assert "upload went to MTProto"
	// by giving each backend a unique chunk.
	uploadResult []Chunk
}

func (f *fakeBackend) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]Chunk, error) {
	f.mu.Lock()
	f.uploads++
	f.mu.Unlock()
	if f.uploadResult != nil {
		return f.uploadResult, nil
	}
	return []Chunk{{Seq: 0, Transport: f.label}}, nil
}
func (f *fakeBackend) Download(ctx context.Context, ref ChunkRef) (io.ReadCloser, error) {
	f.mu.Lock()
	f.downloads++
	f.mu.Unlock()
	return io.NopCloser(bytes.NewReader([]byte(f.label))), nil
}
func (f *fakeBackend) DownloadRange(ctx context.Context, ref ChunkRef, offset, length int64) (io.ReadCloser, error) {
	f.mu.Lock()
	f.downloadRng++
	f.mu.Unlock()
	return io.NopCloser(bytes.NewReader([]byte(f.label))), nil
}
func (f *fakeBackend) Delete(ctx context.Context, ref ChunkRef) error {
	f.mu.Lock()
	f.deletes++
	f.mu.Unlock()
	return nil
}
func (f *fakeBackend) DeleteBatch(ctx context.Context, refs []ChunkRef) error {
	f.mu.Lock()
	f.deleteBatch++
	f.lastBatchLen = len(refs)
	f.lastTxs = nil
	for _, r := range refs {
		f.lastTxs = append(f.lastTxs, r.Transport)
	}
	f.mu.Unlock()
	return nil
}

func TestDispatcherUploadRouting(t *testing.T) {
	cases := []struct {
		mode    TransportMode
		wantBot bool
	}{
		{TransportModeBot, true},
		{TransportModeDual, false},
		{TransportModeMTProto, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.mode), func(t *testing.T) {
			bot := &fakeBackend{label: "bot"}
			mt := &fakeBackend{label: "mtproto"}
			d, err := NewDispatcher(tc.mode, bot, mt)
			if err != nil {
				t.Fatalf("dispatcher: %v", err)
			}
			if _, err := d.Upload(context.Background(), "k", "ct", bytes.NewReader(nil)); err != nil {
				t.Fatalf("upload: %v", err)
			}
			if tc.wantBot && bot.uploads != 1 {
				t.Fatalf("mode=%s: bot uploads=%d want 1", tc.mode, bot.uploads)
			}
			if !tc.wantBot && mt.uploads != 1 {
				t.Fatalf("mode=%s: mtproto uploads=%d want 1", tc.mode, mt.uploads)
			}
		})
	}
}

func TestDispatcherReadRoutesByTransport(t *testing.T) {
	bot := &fakeBackend{label: "bot"}
	mt := &fakeBackend{label: "mtproto"}
	d, err := NewDispatcher(TransportModeDual, bot, mt)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Empty transport (legacy row before Phase 3 backfill columns existed)
	// → bot, per Chunk.Ref's normalization contract.
	if _, err := d.DownloadRange(ctx, ChunkRef{Transport: ""}, 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := d.DownloadRange(ctx, ChunkRef{Transport: "bot"}, 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := d.DownloadRange(ctx, ChunkRef{Transport: "mtproto"}, 0, 10); err != nil {
		t.Fatal(err)
	}

	if bot.downloadRng != 2 {
		t.Errorf("bot downloads=%d want 2", bot.downloadRng)
	}
	if mt.downloadRng != 1 {
		t.Errorf("mt downloads=%d want 1", mt.downloadRng)
	}
}

func TestDispatcherDeleteBatchGroupsByTransport(t *testing.T) {
	bot := &fakeBackend{label: "bot"}
	mt := &fakeBackend{label: "mtproto"}
	d, _ := NewDispatcher(TransportModeDual, bot, mt)

	refs := []ChunkRef{
		{Transport: "bot", MessageID: 1},
		{Transport: "mtproto", MessageID: 2},
		{Transport: "", MessageID: 3}, // empty → bot
		{Transport: "mtproto", MessageID: 4},
		{Transport: "bot", MessageID: 5},
	}
	if err := d.DeleteBatch(context.Background(), refs); err != nil {
		t.Fatal(err)
	}
	if bot.deleteBatch != 1 || bot.lastBatchLen != 3 {
		t.Errorf("bot batches=%d last=%d want 1 batches of 3", bot.deleteBatch, bot.lastBatchLen)
	}
	if mt.deleteBatch != 1 || mt.lastBatchLen != 2 {
		t.Errorf("mt batches=%d last=%d want 1 batches of 2", mt.deleteBatch, mt.lastBatchLen)
	}
}

func TestDispatcherUnknownTransportRejected(t *testing.T) {
	bot := &fakeBackend{label: "bot"}
	d, _ := NewDispatcher(TransportModeBot, bot, nil)
	_, err := d.DownloadRange(context.Background(), ChunkRef{Transport: "garbage"}, 0, 1)
	if err == nil || !strings.Contains(err.Error(), "unknown transport") {
		t.Fatalf("got %v want unknown-transport error", err)
	}
}

func TestDispatcherBotOnlyModeRejectsMtprotoRef(t *testing.T) {
	// A mode=bot deploy that somehow has an mtproto chunk row (operator
	// reverted the binary without flipping rows back) must surface a
	// clear error rather than silently 404. This is the "rollback past
	// any mtproto chunks" warning in PHASES.md.
	bot := &fakeBackend{label: "bot"}
	d, _ := NewDispatcher(TransportModeBot, bot, nil)
	_, err := d.DownloadRange(context.Background(), ChunkRef{Transport: "mtproto"}, 0, 1)
	if err == nil {
		t.Fatal("want error on mtproto ref in bot-only mode")
	}
}

func TestNewDispatcherValidation(t *testing.T) {
	bot := &fakeBackend{label: "bot"}
	if _, err := NewDispatcher(TransportModeBot, nil, nil); err == nil {
		t.Error("want error for nil bot backend")
	}
	if _, err := NewDispatcher(TransportModeDual, bot, nil); err == nil {
		t.Error("want error for dual without mtproto")
	}
	if _, err := NewDispatcher(TransportModeMTProto, bot, nil); err == nil {
		t.Error("want error for mtproto without mtproto backend")
	}
	if _, err := NewDispatcher("garbage", bot, nil); err == nil {
		t.Error("want error for unknown mode")
	}
}

// Compile-time guard: *Dispatcher satisfies Backend so handlers can
// take it transparently.
var _ Backend = (*Dispatcher)(nil)

// errBackend variant for completeness — surfaces a backend error
// through the dispatcher unchanged.
type errBackend struct{ err error }

func (e errBackend) Upload(context.Context, string, string, io.Reader) ([]Chunk, error) {
	return nil, e.err
}
func (e errBackend) Download(context.Context, ChunkRef) (io.ReadCloser, error)              { return nil, e.err }
func (e errBackend) DownloadRange(context.Context, ChunkRef, int64, int64) (io.ReadCloser, error) {
	return nil, e.err
}
func (e errBackend) Delete(context.Context, ChunkRef) error             { return e.err }
func (e errBackend) DeleteBatch(context.Context, []ChunkRef) error      { return e.err }

func TestDispatcherPropagatesBackendError(t *testing.T) {
	want := errors.New("backend boom")
	d, _ := NewDispatcher(TransportModeDual, errBackend{err: want}, &fakeBackend{label: "mt"})
	_, err := d.Download(context.Background(), ChunkRef{Transport: "bot"})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want %v", err, want)
	}
}
