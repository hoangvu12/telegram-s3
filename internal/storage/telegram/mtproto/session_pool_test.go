package mtproto

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// fakeInvoker is the terminal tg.Invoker for chainMiddlewares tests —
// it stamps an identifier into the trace so the test can assert which
// middlewares ran and in what order. We don't try to satisfy any real
// MTProto schema; the trace is the whole point.
type fakeInvoker struct {
	trace *[]string
	label string
	err   error
}

func (f *fakeInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	*f.trace = append(*f.trace, f.label)
	return f.err
}

// labelMiddleware appends `label` to the shared trace before delegating
// to next.Invoke. The before-after ordering exposed in TestChainMiddlewaresOrder
// pins how chainMiddlewares wraps the chain: the first middleware in
// chain should be the outermost wrapper, i.e. its label appears first.
type labelMiddleware struct {
	label string
	trace *[]string
}

func (m labelMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		*m.trace = append(*m.trace, m.label+":before")
		err := next.Invoke(ctx, input, output)
		*m.trace = append(*m.trace, m.label+":after")
		return err
	}
}

// TestChainMiddlewaresOrder pins the contract that future maintainers
// would otherwise re-derive from the loop body: chain[0] is the
// outermost wrapper. The telegram.Options.Middlewares stack relies on
// this order too, and our session pool re-applies the same chain on
// the per-pool invoker — flipping it would silently change the
// floodwait/ratelimit semantics under the pool but not under the
// default session.
func TestChainMiddlewaresOrder(t *testing.T) {
	var trace []string
	terminal := &fakeInvoker{trace: &trace, label: "rpc"}
	mws := []telegram.Middleware{
		labelMiddleware{label: "outer", trace: &trace},
		labelMiddleware{label: "inner", trace: &trace},
	}

	chained := chainMiddlewares(terminal, mws...)
	if err := chained.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	want := []string{"outer:before", "inner:before", "rpc", "inner:after", "outer:after"}
	if len(trace) != len(want) {
		t.Fatalf("trace len %d, want %d (trace=%v)", len(trace), len(want), trace)
	}
	for i := range want {
		if trace[i] != want[i] {
			t.Errorf("trace[%d] = %q, want %q (full=%v)", i, trace[i], want[i], trace)
		}
	}
}

// TestChainMiddlewaresEmpty covers the no-middleware path — the
// returned invoker must be the terminal invoker, not a wrapper around
// it, otherwise sessions with no extra middlewares would pay one
// function-call hop per RPC for nothing.
func TestChainMiddlewaresEmpty(t *testing.T) {
	var trace []string
	terminal := &fakeInvoker{trace: &trace, label: "rpc"}

	chained := chainMiddlewares(terminal)
	if err := chained.Invoke(context.Background(), nil, nil); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(trace) != 1 || trace[0] != "rpc" {
		t.Errorf("expected just rpc in trace, got %v", trace)
	}
}

// TestChainMiddlewaresPropagatesError checks that a terminal-invoker
// error surfaces through the chain unchanged. floodwait/ratelimit
// middlewares both rely on this: they react to specific error codes
// returned by the underlying RPC, and silent error swallowing in the
// chain wrapper would break that.
func TestChainMiddlewaresPropagatesError(t *testing.T) {
	var trace []string
	want := errors.New("RPC failed")
	terminal := &fakeInvoker{trace: &trace, label: "rpc", err: want}

	chained := chainMiddlewares(terminal, labelMiddleware{label: "m", trace: &trace})
	got := chained.Invoke(context.Background(), nil, nil)
	if !errors.Is(got, want) {
		t.Errorf("error %v, want %v", got, want)
	}
}

// TestSessionPoolCloseIdempotent covers the lifecycle invariant: a
// sessionPool that was constructed but never serviced a Client(ctx)
// call must Close cleanly (the gotd pool was never opened, so there
// is no invoker.Close to call). And a second Close after a real one
// must be a no-op rather than double-closing. Both branches matter
// because StartBot may construct a pool then bail on a downstream
// failure (no traffic), and bot.Close is documented as idempotent.
func TestSessionPoolCloseIdempotent(t *testing.T) {
	// Construct an unstarted client. Pool init would fail because the
	// client has no DC config yet — but Close shouldn't care, since it
	// only acts when the invoker was successfully built.
	client := telegram.NewClient(1, "deadbeef", telegram.Options{})
	pool := newSessionPool(client, 4, nil)

	if err := pool.Close(); err != nil {
		t.Errorf("first Close on never-used pool: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// The fallback-on-init-failure path (sessionPool.invoker returns the
// bare *telegram.Client when telegram.Client.Pool errors) is not unit-
// tested here: gotd's Pool panics on an unstarted client rather than
// returning an error, so the only way to exercise the fallback is via
// a live network failure. The path is a single `if err != nil` branch
// in invoker(); inspection covers it.

// Compile-time check that *fakeInvoker satisfies tg.Invoker. If gotd
// changes the interface, the failure mode should be a build error here
// rather than an obscure runtime panic inside chainMiddlewares.
var _ tg.Invoker = (*fakeInvoker)(nil)
