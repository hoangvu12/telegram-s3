package mtproto

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gotd/td/session"

	"telegram-s3/internal/metadata"
)

func TestSessionStorage(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	s := NewSessionStorage(store, "bot:0")

	// First load = not found, translated to gotd's sentinel so
	// session.Loader knows to do auth.importBotAuthorization.
	if _, err := s.LoadSession(ctx); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("first LoadSession err=%v want session.ErrNotFound", err)
	}

	// Round-trip.
	want := []byte(`{"version":1}`)
	if err := s.StoreSession(ctx, want); err != nil {
		t.Fatalf("StoreSession: %v", err)
	}
	got, err := s.LoadSession(ctx)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q want %q", got, want)
	}

	// Replace-on-conflict: re-Store same key with new bytes overwrites.
	want2 := []byte(`{"version":2}`)
	if err := s.StoreSession(ctx, want2); err != nil {
		t.Fatalf("re-StoreSession: %v", err)
	}
	got, _ = s.LoadSession(ctx)
	if string(got) != string(want2) {
		t.Fatalf("after replace got %q want %q", got, want2)
	}

	// Keys are isolated: bot:1 still sees ErrNotFound.
	s1 := NewSessionStorage(store, "bot:1")
	if _, err := s1.LoadSession(ctx); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("bot:1 LoadSession err=%v want ErrNotFound", err)
	}
}

// TestSessionStorageConcurrent ensures concurrent StoreSession calls
// don't corrupt the row — gotd may call StoreSession from multiple
// goroutines as the client renews salts / updates auth state.
func TestSessionStorageConcurrent(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	s := NewSessionStorage(store, "bot:c")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.StoreSession(ctx, []byte("payload"))
		}()
	}
	wg.Wait()

	got, err := s.LoadSession(ctx)
	if err != nil {
		t.Fatalf("LoadSession after concurrent writes: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("got %q want %q", got, "payload")
	}
}

func openTestStore(t *testing.T) *metadata.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
