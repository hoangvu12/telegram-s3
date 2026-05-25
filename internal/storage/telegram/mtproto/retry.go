package mtproto

import (
	"context"
	"fmt"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// retryableTypes are the tgerr.Type values that signal a transient
// server-side fault — retrying the same call usually succeeds. Curated
// from teldrive's internal/retry/retry.go (which collected them from
// production pain). FLOOD_WAIT is intentionally absent: the floodwait
// middleware below us already absorbs those, and a retry budget would
// just amplify rate-limit pressure.
var retryableTypes = []string{
	"Timedout",
	"No workers running",
	"RPC_CALL_FAIL",
	"RPC_MCGET_FAIL",
	"WORKER_BUSY_TOO_LONG_RETRY",
	"memory limit exit",
	"connection dead",
	"engine was closed",
	"STORAGE_CHOOSE_VOLUME_FAILED",
}

type retryMiddleware struct {
	max   int
	types []string
}

// newRetry returns a telegram.Middleware that retries up to `max`
// times on tg errors whose Type matches retryableTypes (plus any extra
// caller-supplied types). Non-matching errors pass through immediately
// so the recovery / round-robin layers above can handle them.
func newRetry(max int, extra ...string) telegram.Middleware {
	return retryMiddleware{
		max:   max,
		types: append(extra, retryableTypes...),
	}
}

func (r retryMiddleware) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		var lastErr error
		for attempt := 0; attempt < r.max; attempt++ {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}
			if !tgerr.Is(err, r.types...) {
				return err
			}
			lastErr = err
		}
		return fmt.Errorf("retry: exhausted %d attempts: %w", r.max, lastErr)
	}
}
