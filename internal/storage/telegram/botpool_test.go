package telegram

import "testing"

// TestBotPoolRoundRobin: Pick advances the per-op counter and wraps.
// With 3 bots, ten consecutive stream picks must land on [0,1,2,0,1,2,...].
func TestBotPoolRoundRobin(t *testing.T) {
	p := NewBotPool([]string{"a", "b", "c"}, 0)

	got := make([]int, 9)
	for i := 0; i < 9; i++ {
		idx, _ := p.Pick(BotOpStream)
		got[i] = idx
	}
	want := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stream pick %d = %d, want %d (full sequence %v)", i, got[i], want[i], got)
		}
	}
}

// TestBotPoolSeparateCountersPerOp: an upload burst must not shift the
// stream counter (and vice versa) — teldrive's two-counter model exists
// so a long-running upload sequence doesn't desynchronize range-read
// affinity for an unrelated stream of GETs.
func TestBotPoolSeparateCountersPerOp(t *testing.T) {
	p := NewBotPool([]string{"a", "b"}, 0)

	for i := 0; i < 5; i++ {
		p.Pick(BotOpUpload)
	}
	// Stream counter is still at 0; next stream pick is index 0.
	if idx, _ := p.Pick(BotOpStream); idx != 0 {
		t.Fatalf("first stream after 5 uploads = %d, want 0 (separate counters)", idx)
	}
	if idx, _ := p.Pick(BotOpStream); idx != 1 {
		t.Fatalf("second stream = %d, want 1", idx)
	}
}

// TestBotPoolAtOutOfRange: rows persisted on a deploy with a larger pool
// must not panic when read on a smaller pool — At returns nil and the
// caller surfaces a clean error (the dispatcher's safety net).
func TestBotPoolAtOutOfRange(t *testing.T) {
	p := NewBotPool([]string{"a"}, 0)
	if _, c := p.At(0); c == nil {
		t.Fatal("At(0) returned nil for a valid index")
	}
	if _, c := p.At(1); c != nil {
		t.Fatalf("At(1) returned %v, want nil (pool size 1)", c)
	}
	if _, c := p.At(-1); c != nil {
		t.Fatalf("At(-1) returned %v, want nil", c)
	}
}

// TestBotPoolPickReturnsCorrectClient: the returned *botClient must match
// the index's slot — important because Upload tags Chunk.BotIndex with
// the picked index and the read path uses that index to re-resolve. If
// the two diverge, downloads route to the wrong token.
func TestBotPoolPickReturnsCorrectClient(t *testing.T) {
	tokens := []string{"t0", "t1", "t2"}
	p := NewBotPool(tokens, 0)
	seen := map[string]bool{}
	for i := 0; i < len(tokens); i++ {
		idx, c := p.Pick(BotOpUpload)
		if c.token != tokens[idx] {
			t.Fatalf("Pick returned idx=%d with token %q, want %q", idx, c.token, tokens[idx])
		}
		seen[c.token] = true
	}
	if len(seen) != len(tokens) {
		t.Fatalf("round-robin covered %d distinct tokens, want %d (saw %v)", len(seen), len(tokens), seen)
	}
}
