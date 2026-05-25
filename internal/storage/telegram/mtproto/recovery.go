package mtproto

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// recoveryMiddleware wraps non-tg.Error failures (typically transport-
// level network drops or context-not-yet-Done write errors during a
// reconnect) with an exponential backoff. tgerr.Error responses are
// passed through unmodified — those carry semantic information (e.g.
// FILE_REFERENCE_EXPIRED, CHANNEL_INVALID) that callers handle directly,
// and retrying them blindly would either be redundant (the retry
// middleware below already filters on Type) or actively wrong
// (CHANNEL_PRIVATE will fail every attempt).
//
// Lifted from teldrive's internal/recovery/recovery.go with the same
// rationale: the gotd reconnect path produces brief windows where the
// next RPC fails with a non-RPC error; sleeping a few hundred ms and
// retrying is usually enough.
type recoveryMiddleware struct {
	ctx     context.Context
	backoff func() backoff.BackOff
}

// newRecovery returns a recovery middleware tied to ctx. When ctx is
// canceled (bot shutdown), shouldRecover returns false so in-flight
// retries unwind instead of looping forever against a dead connection.
// The backoff is rebuilt per RPC so two simultaneous calls don't share
// a single elapsed-time budget.
func newRecovery(ctx context.Context, newBO func() backoff.BackOff) telegram.Middleware {
	return &recoveryMiddleware{ctx: ctx, backoff: newBO}
}

func (r *recoveryMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		return backoff.RetryNotify(func() error {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			if !r.shouldRecover(err) {
				return backoff.Permanent(err)
			}
			return err
		}, r.backoff(), nil)
	}
}

// shouldRecover returns true for non-tg.Error failures while the bot's
// run-context is still live. A tg.Error has structured Type/Code fields
// and should bubble up to the retry middleware (which decides per Type)
// or to the caller (for fatal access errors). A bare network/transport
// error is the only thing this middleware tries to mask.
func (r *recoveryMiddleware) shouldRecover(err error) bool {
	select {
	case <-r.ctx.Done():
		return false
	default:
	}
	_, isTGErr := tgerr.As(err)
	return !isTGErr
}

// newRecoveryBackoff returns the exponential-backoff schedule the
// recovery middleware uses. Mirrors teldrive: gentle 1.1 multiplier so
// 10 retries stay under ~5s, capped at 10s/step, and a configurable
// total elapsed-time ceiling. The single-bot recovery budget should be
// well under any external request deadline so a stuck recovery doesn't
// silently extend client wait times.
func newRecoveryBackoff(total time.Duration) backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.Multiplier = 1.1
	b.MaxElapsedTime = total
	b.MaxInterval = 10 * time.Second
	return b
}
