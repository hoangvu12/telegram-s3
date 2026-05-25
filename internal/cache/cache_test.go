package cache

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSetGetDelete(t *testing.T) {
	c := New[string, string](time.Minute, 0)
	defer c.Close()

	if _, ok := c.Get("missing"); ok {
		t.Fatal("Get on empty cache returned ok=true")
	}

	c.Set("a", "1", 0) // 0 → default TTL
	if v, ok := c.Get("a"); !ok || v != "1" {
		t.Fatalf("Get('a') = (%q, %v), want (\"1\", true)", v, ok)
	}

	c.Set("a", "2", 0)
	if v, _ := c.Get("a"); v != "2" {
		t.Fatalf("Set should overwrite; got %q want \"2\"", v)
	}

	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("Delete did not remove entry")
	}
}

// TTL expiry is observable via Get (lazy eviction) without waiting for the
// sweep tick. This is the load-bearing correctness path — the sweeper is
// only there to bound memory for keys that are never read again.
func TestLazyExpiry(t *testing.T) {
	c := New[string, int](time.Minute, 0)
	defer c.Close()

	c.Set("k", 7, 20*time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("entry missing immediately after Set")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("entry still readable after TTL")
	}
}

// The background sweeper should drop expired entries even when no one reads
// them. Use a tight sweep interval so the test stays fast.
func TestBackgroundSweep(t *testing.T) {
	c := New[string, int](20*time.Millisecond, 10*time.Millisecond)
	defer c.Close()

	for i := 0; i < 10; i++ {
		c.Set(strconv.Itoa(i), i, 20*time.Millisecond)
	}
	if c.Len() != 10 {
		t.Fatalf("Len after inserts = %d, want 10", c.Len())
	}
	// Wait for the sweep tick after TTL expiry. Two sweep periods + TTL is a
	// generous bound that keeps the test reliable on slow CI.
	time.Sleep(80 * time.Millisecond)
	if n := c.Len(); n != 0 {
		t.Fatalf("Len after sweep = %d, want 0", n)
	}
}

// Concurrent Get/Set must not race. Run under `go test -race` to enforce.
func TestConcurrentAccess(t *testing.T) {
	c := New[int, int](time.Minute, 0)
	defer c.Close()

	const workers, ops = 8, 200
	var wg sync.WaitGroup
	var hits atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := (id*ops + i) % 50
				c.Set(k, id*1000+i, 0)
				if _, ok := c.Get(k); ok {
					hits.Add(1)
				}
				if i%5 == 0 {
					c.Delete(k)
				}
			}
		}(w)
	}
	wg.Wait()
	// Just sanity-check that Get found *something* through the churn; the
	// race detector is the real assertion here.
	if hits.Load() == 0 {
		t.Fatal("no Get hits across concurrent workload")
	}
}

// Close must be idempotent and must stop the sweeper goroutine without
// leaking. We don't have a clean way to assert goroutine exit from inside
// the package, so we settle for "Close twice does not panic".
func TestCloseIdempotent(t *testing.T) {
	c := New[string, string](time.Minute, 0)
	c.Close()
	c.Close()
}
