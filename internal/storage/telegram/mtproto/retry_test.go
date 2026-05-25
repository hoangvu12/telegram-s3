package mtproto

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// scriptedInvoker is a deterministic stand-in for gotd's tg.Invoker. Each
// call advances a counter and returns the next scripted error (or nil).
// Anything past the end of script returns the final value forever —
// mirrors the steady-state behavior of a real server.
type scriptedInvoker struct {
	calls  int
	script []error
}

func (f *scriptedInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	idx := f.calls
	f.calls++
	if idx < len(f.script) {
		return f.script[idx]
	}
	if len(f.script) == 0 {
		return nil
	}
	return f.script[len(f.script)-1]
}

// rpcErr builds a *tgerr.Error matching the shape gotd returns from
// real RPC failures. Type is what tgerr.Is matches against.
func rpcErr(code int, t string) *tgerr.Error {
	return &tgerr.Error{Code: code, Message: t, Type: t}
}

// TestRetry_PassesThroughOnSuccess pins the no-op path: a single Invoke
// returning nil must NOT retry, NOT mutate the call count beyond 1.
// A retry loop that re-invokes on success would double the RPC budget
// for every working call — quietly catastrophic against Telegram's
// per-bot rate caps.
func TestRetry_PassesThroughOnSuccess(t *testing.T) {
	inv := &scriptedInvoker{script: []error{nil}}
	mw := newRetry(5).Handle(inv)
	if err := mw(context.Background(), nil, nil); err != nil {
		t.Fatalf("err=%v", err)
	}
	if inv.calls != 1 {
		t.Errorf("calls=%d, want 1", inv.calls)
	}
}

// TestRetry_RecoversAfterTransient covers the load-bearing case: a
// Timedout-then-success sequence must succeed on attempt 2.
func TestRetry_RecoversAfterTransient(t *testing.T) {
	inv := &scriptedInvoker{script: []error{rpcErr(500, "Timedout"), nil}}
	mw := newRetry(5).Handle(inv)
	if err := mw(context.Background(), nil, nil); err != nil {
		t.Fatalf("err=%v", err)
	}
	if inv.calls != 2 {
		t.Errorf("calls=%d, want 2", inv.calls)
	}
}

// TestRetry_PassesNonMatchImmediately ensures CHANNEL_INVALID (a fatal
// access error not in retryableTypes) is surfaced after exactly one
// invocation. Spinning here would waste pool slots on a doomed call
// and delay the round-robin fallback in download.go.
func TestRetry_PassesNonMatchImmediately(t *testing.T) {
	inv := &scriptedInvoker{script: []error{rpcErr(400, "CHANNEL_INVALID")}}
	mw := newRetry(5).Handle(inv)
	err := mw(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("want error")
	}
	if !tgerr.Is(err, "CHANNEL_INVALID") {
		t.Errorf("err = %v; want CHANNEL_INVALID propagated", err)
	}
	if inv.calls != 1 {
		t.Errorf("calls=%d, want 1", inv.calls)
	}
}

// TestRetry_ExhaustsMaxAttempts confirms the loop terminates with the
// last error wrapped after r.max attempts on a never-recovering
// transient. Without termination, a permanently-broken Telegram DC
// would hang every caller forever.
func TestRetry_ExhaustsMaxAttempts(t *testing.T) {
	inv := &scriptedInvoker{script: []error{rpcErr(500, "RPC_CALL_FAIL")}}
	mw := newRetry(3).Handle(inv)
	err := mw(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("want error after exhaustion")
	}
	if inv.calls != 3 {
		t.Errorf("calls=%d, want 3", inv.calls)
	}
	var te *tgerr.Error
	if !errors.As(err, &te) {
		t.Errorf("exhaustion error should wrap the underlying tgerr; got %v", err)
	}
}

// TestRetry_RetryableTypesCoverage spot-checks each curated type is
// actually treated as retryable. Catches accidental removal during
// future edits — these strings came from upstream production pain and
// dropping one re-introduces a known bad path.
func TestRetry_RetryableTypesCoverage(t *testing.T) {
	for _, typ := range retryableTypes {
		inv := &scriptedInvoker{script: []error{rpcErr(500, typ), nil}}
		mw := newRetry(5).Handle(inv)
		if err := mw(context.Background(), nil, nil); err != nil {
			t.Errorf("%s: err=%v (should have retried then succeeded)", typ, err)
		}
		if inv.calls != 2 {
			t.Errorf("%s: calls=%d, want 2", typ, inv.calls)
		}
	}
}

// TestRecovery_PassesThroughOnSuccess ensures the backoff wrapper is
// a no-op on success — like the retry middleware, double-invoking would
// silently waste RPC budget on every healthy call.
func TestRecovery_PassesThroughOnSuccess(t *testing.T) {
	inv := &scriptedInvoker{script: []error{nil}}
	mw := newRecovery(context.Background(), instantBackoff).Handle(inv)
	if err := mw(context.Background(), nil, nil); err != nil {
		t.Fatalf("err=%v", err)
	}
	if inv.calls != 1 {
		t.Errorf("calls=%d, want 1", inv.calls)
	}
}

// TestRecovery_PassesTGErrorImmediately confirms RPC-typed errors fall
// through to the layer below (retry / caller) without burning the
// recovery budget. This is the boundary we drew explicitly: recovery
// only masks transport failures.
func TestRecovery_PassesTGErrorImmediately(t *testing.T) {
	inv := &scriptedInvoker{script: []error{rpcErr(400, "CHANNEL_PRIVATE")}}
	mw := newRecovery(context.Background(), instantBackoff).Handle(inv)
	err := mw(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("want error")
	}
	if !tgerr.Is(err, "CHANNEL_PRIVATE") {
		t.Errorf("err = %v; want CHANNEL_PRIVATE propagated", err)
	}
	if inv.calls != 1 {
		t.Errorf("calls=%d, want 1", inv.calls)
	}
}

// TestRecovery_RetriesNonTGError covers the value: a bare transport
// error ("connection reset") triggers backoff retries until the script
// hands back a success.
func TestRecovery_RetriesNonTGError(t *testing.T) {
	inv := &scriptedInvoker{script: []error{errors.New("connection reset"), nil}}
	mw := newRecovery(context.Background(), instantBackoff).Handle(inv)
	if err := mw(context.Background(), nil, nil); err != nil {
		t.Fatalf("err=%v", err)
	}
	if inv.calls != 2 {
		t.Errorf("calls=%d, want 2", inv.calls)
	}
}

// TestRecovery_CancelStopsRetries gates shutdown correctness: once the
// run context is canceled, the backoff loop must NOT keep retrying
// forever. A bot teardown that hangs on a wedged transport would leak
// goroutines on every restart.
func TestRecovery_CancelStopsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inv := &scriptedInvoker{script: []error{errors.New("transport down")}}
	mw := newRecovery(ctx, instantBackoff).Handle(inv)
	err := mw(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("want error after cancel")
	}
}

// instantBackoff yields an exponential schedule whose intervals are
// essentially zero so retry tests don't burn wall time. ZeroBackOff
// would skip the elapsed-time gate; this preserves the API shape while
// being effectively instant.
func instantBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Microsecond
	b.MaxInterval = time.Millisecond
	b.MaxElapsedTime = 50 * time.Millisecond
	return b
}

// TestInspectDocumentSize verifies the verifyUploadedSize helper's
// shape-checker. teldrive's bug class (5b4faaa) was sendMedia
// succeeding but the doc being zero-sized or missing; this function
// is what catches that before we persist the chunk.
func TestInspectDocumentSize(t *testing.T) {
	t.Run("happy path returns size", func(t *testing.T) {
		msg := &tg.Message{Media: &tg.MessageMediaDocument{
			Document: &tg.Document{ID: 1, AccessHash: 2, FileReference: []byte{1}, Size: 1024},
		}}
		size, ok := inspectDocumentSize(msg)
		if !ok || size != 1024 {
			t.Errorf("got size=%d ok=%v; want 1024 true", size, ok)
		}
	})
	t.Run("empty message returns false", func(t *testing.T) {
		_, ok := inspectDocumentSize(&tg.MessageEmpty{ID: 1})
		if ok {
			t.Error("empty message should not yield a size")
		}
	})
	t.Run("non-document media returns false", func(t *testing.T) {
		msg := &tg.Message{Media: &tg.MessageMediaPhoto{}}
		_, ok := inspectDocumentSize(msg)
		if ok {
			t.Error("photo media should not yield a doc size")
		}
	})
	t.Run("empty document returns false", func(t *testing.T) {
		msg := &tg.Message{Media: &tg.MessageMediaDocument{
			Document: &tg.DocumentEmpty{ID: 1},
		}}
		_, ok := inspectDocumentSize(msg)
		if ok {
			t.Error("empty document should not yield a size")
		}
	})
	t.Run("zero-size document is reported (caller decides)", func(t *testing.T) {
		// The verify path compares against expected; a 0-byte document
		// for a non-zero upload IS a failure, but this helper just
		// reports the size and lets the caller decide. Pinning that
		// boundary so future edits don't push the comparison into
		// inspectDocumentSize itself.
		msg := &tg.Message{Media: &tg.MessageMediaDocument{
			Document: &tg.Document{ID: 1, AccessHash: 2, FileReference: []byte{1}, Size: 0},
		}}
		size, ok := inspectDocumentSize(msg)
		if !ok || size != 0 {
			t.Errorf("got size=%d ok=%v; want 0 true", size, ok)
		}
	})
}
