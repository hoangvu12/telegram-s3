package mtproto

import (
	"context"
	"log/slog"
	"sync"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// sessionPool wraps gotd's per-DC connection pool so a single bot can
// issue many MTProto RPCs in parallel without serializing them through
// one connection. Lifted from teldrive's internal/pool/pool.go (which
// in turn credits iyear/tdl) — adapted to slog and made private to
// this package. The interface stays small because only download/upload
// need it; delete is single-shot.
//
// Lifecycle: NewSessionPool registers the telegram.Client + middleware
// chain but does NOT open any extra connections. The underlying gotd
// Pool is opened lazily on the first Client(ctx) call. Close releases
// whatever was opened (no-op if nothing ever opened).
//
// Why lazy: telegram.Client.Pool(size) needs the client past auth (it
// reads Config().ThisDC). The bot's StartBot path blocks on ready
// before exposing the pool, so by the time any download/upload calls
// Client(ctx), auth is done — but lazy init keeps tests / tooling
// safe even if someone constructs a sessionPool against a not-yet-running
// client.
type sessionPool struct {
	api         *telegram.Client
	size        int64
	mu          sync.Mutex
	middlewares []telegram.Middleware
	invoke      tg.Invoker
	close       func() error
	logger      *slog.Logger
}

// newSessionPool registers a multi-session invoker on top of the given
// telegram.Client. size is the gotd pool capacity (1 reverts to the
// pre-pool single-session behavior). middlewares are applied in order
// on every RPC the returned *tg.Client issues — pass the same
// floodwait/ratelimit stack the underlying client was built with so
// the per-session invoker doesn't bypass them.
func newSessionPool(c *telegram.Client, size int64, logger *slog.Logger, middlewares ...telegram.Middleware) *sessionPool {
	if logger == nil {
		logger = slog.Default()
	}
	if size < 1 {
		size = 1
	}
	return &sessionPool{
		api:         c,
		size:        size,
		middlewares: middlewares,
		logger:      logger,
	}
}

// current returns the DC the underlying client is bound to. Must only
// be called after the client's Run goroutine has authenticated — before
// that, Config().ThisDC is zero. The pool's lazy init guards against
// that case by lifting this call to first Client(ctx) use, by which
// time StartBot has gated the bot as ready.
func (p *sessionPool) current() int {
	return p.api.Config().ThisDC
}

// Client returns a *tg.Client whose RPCs flow through the multiplexed
// invoker chain. Safe for concurrent use. The same underlying invoker
// backs every returned client — gotd internally fans calls across the
// session pool — so callers don't need to cache or rotate the result.
func (p *sessionPool) Client(ctx context.Context) *tg.Client {
	return tg.NewClient(p.invoker(ctx))
}

// invoker lazy-initializes the gotd pool on first call. On failure it
// falls back to the bare *telegram.Client (which itself implements
// tg.Invoker) so the bot stays usable at single-session throughput
// instead of degrading to a hard error — a partial-pool boot is more
// useful than a dead bot.
func (p *sessionPool) invoker(ctx context.Context) tg.Invoker {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.invoke != nil {
		return p.invoke
	}

	invoker, err := p.api.Pool(p.size)
	if err != nil {
		p.logger.Error("mtproto session pool init failed; falling back to single session", "error", err, "size", p.size)
		return p.api
	}

	p.close = invoker.Close
	p.invoke = chainMiddlewares(invoker, p.middlewares...)
	return p.invoke
}

// Close releases the underlying gotd pool if it was opened. Idempotent
// — a sessionPool that was constructed but never serviced a Client
// call cleans up to a no-op.
func (p *sessionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.close != nil {
		err := p.close()
		p.close = nil
		return err
	}
	return nil
}

// chainMiddlewares wraps invoker with the given middleware chain so the
// first middleware in chain is the outermost wrapper (its Handle runs
// first on each RPC). Identical semantics to telegram.Options.Middlewares
// applied at client construction — we re-apply it here because the
// per-session invoker returned by telegram.Client.Pool bypasses the
// client's own middleware stack.
func chainMiddlewares(invoker tg.Invoker, chain ...telegram.Middleware) tg.Invoker {
	if len(chain) == 0 {
		return invoker
	}
	for i := len(chain) - 1; i >= 0; i-- {
		invoker = chain[i].Handle(invoker)
	}
	return invoker
}
