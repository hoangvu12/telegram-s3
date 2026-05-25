// Package cache provides a small generic TTL cache used across the gateway.
//
// The shape is deliberately transport-agnostic so Phase 4 can reuse the same
// type for `(messageID, botIndex) → *tg.InputDocumentFileLocation` once the
// MTProto backend lands. Phase 1's only user is the Bot HTTP API path cache
// keyed by file_id → file_path. No Redis; sync.Map + per-entry expiresAt +
// lazy eviction on Get + a background sweeper.
package cache

import (
	"sync"
	"time"
)

// Cache is a concurrent map with per-entry expiry. The zero value is not
// usable — construct with New.
type Cache[K comparable, V any] struct {
	items      sync.Map // map[K]*entry[V]
	defaultTTL time.Duration
	sweep      time.Duration
	stop       chan struct{}
	once       sync.Once
}

type entry[V any] struct {
	value     V
	expiresAt time.Time
}

// New constructs a Cache with the given default TTL and starts the background
// sweeper. sweepInterval defaults to max(defaultTTL/4, 1 minute) when
// non-positive; tests can pass a smaller interval to exercise the sweep.
func New[K comparable, V any](defaultTTL, sweepInterval time.Duration) *Cache[K, V] {
	if sweepInterval <= 0 {
		sweepInterval = defaultTTL / 4
		if sweepInterval < time.Minute {
			sweepInterval = time.Minute
		}
	}
	c := &Cache[K, V]{
		defaultTTL: defaultTTL,
		sweep:      sweepInterval,
		stop:       make(chan struct{}),
	}
	go c.run()
	return c
}

// Get returns the value for key and whether it was present and unexpired.
// Expired entries are deleted lazily so callers see a miss without waiting
// for the sweep tick.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V
	v, ok := c.items.Load(key)
	if !ok {
		return zero, false
	}
	e := v.(*entry[V])
	if time.Now().After(e.expiresAt) {
		// CompareAndDelete avoids racing with a concurrent Set that replaced
		// the entry between our Load and the eviction.
		c.items.CompareAndDelete(key, v)
		return zero, false
	}
	return e.value, true
}

// Set stores value under key with the given TTL. A non-positive ttl falls
// back to the cache's default.
func (c *Cache[K, V]) Set(key K, value V, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	c.items.Store(key, &entry[V]{value: value, expiresAt: time.Now().Add(ttl)})
}

// Delete removes key from the cache. Safe to call for missing keys.
func (c *Cache[K, V]) Delete(key K) {
	c.items.Delete(key)
}

// Len returns the current number of entries, including any that have expired
// but not yet been swept. Primarily for tests / metrics.
func (c *Cache[K, V]) Len() int {
	n := 0
	c.items.Range(func(_, _ any) bool { n++; return true })
	return n
}

// Close stops the background sweeper. Idempotent.
func (c *Cache[K, V]) Close() {
	c.once.Do(func() { close(c.stop) })
}

func (c *Cache[K, V]) run() {
	t := time.NewTicker(c.sweep)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			now := time.Now()
			c.items.Range(func(k, v any) bool {
				e := v.(*entry[V])
				if now.After(e.expiresAt) {
					c.items.CompareAndDelete(k, v)
				}
				return true
			})
		}
	}
}
