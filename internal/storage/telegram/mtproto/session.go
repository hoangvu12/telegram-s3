package mtproto

import (
	"context"
	"errors"

	"github.com/gotd/td/session"

	"telegram-s3/internal/metadata"
)

// sessionStore is the per-bot session.Storage backed by the gateway's
// metadata.Store. Each bot in the pool gets its own key (typically
// "bot:<index>"), so a session blob never leaks across bots — gotd
// would happily reuse a wrong-bot session and authenticate as the
// wrong identity, then drop the new bot when the first signal arrives.
//
// All gotd needs is two methods. The only subtlety: gotd asks for
// session.ErrNotFound (not just "any error") on a first-boot miss, so
// the adapter translates metadata.ErrNotFound here. Anything else is a
// real I/O failure and propagates.
type sessionStore struct {
	store *metadata.Store
	key   string
}

// NewSessionStorage builds a per-bot session.Storage. key must be
// unique per bot — the boot wiring uses "bot:<index>" so a pool
// resize doesn't shuffle session blobs between bots.
func NewSessionStorage(store *metadata.Store, key string) session.Storage {
	return &sessionStore{store: store, key: key}
}

func (s *sessionStore) LoadSession(ctx context.Context) ([]byte, error) {
	data, err := s.store.LoadSession(ctx, s.key)
	if errors.Is(err, metadata.ErrNotFound) {
		return nil, session.ErrNotFound
	}
	return data, err
}

func (s *sessionStore) StoreSession(ctx context.Context, data []byte) error {
	return s.store.StoreSession(ctx, s.key, data)
}
