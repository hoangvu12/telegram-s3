package mtproto

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// TestExtractMessageID covers every Updates variant the upload path
// can plausibly receive. A successful sendMedia must yield exactly
// one message ID; failing here would mean an uploaded chunk has no
// addressable identity in object_chunks (corruption — the bytes are
// in Telegram but we can never look them up).
func TestExtractMessageID(t *testing.T) {
	cases := []struct {
		name string
		in   tg.UpdatesClass
		want int
	}{
		{
			name: "UpdateShortSentMessage (the common bot-sent path)",
			in:   &tg.UpdateShortSentMessage{ID: 42},
			want: 42,
		},
		{
			name: "Updates with UpdateNewChannelMessage",
			in: &tg.Updates{Updates: []tg.UpdateClass{
				&tg.UpdateNewChannelMessage{Message: &tg.Message{ID: 7}},
			}},
			want: 7,
		},
		{
			name: "Updates with UpdateMessageID fallback",
			in: &tg.Updates{Updates: []tg.UpdateClass{
				&tg.UpdateMessageID{ID: 99, RandomID: 12345},
			}},
			want: 99,
		},
		{
			name: "UpdatesCombined branch",
			in: &tg.UpdatesCombined{Updates: []tg.UpdateClass{
				&tg.UpdateNewMessage{Message: &tg.Message{ID: 555}},
			}},
			want: 555,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractMessageID(tc.in)
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if got != tc.want {
				t.Errorf("got %d want %d", got, tc.want)
			}
		})
	}

	// Negative: an empty Updates list is a server bug (or a network
	// response we don't recognize) — must surface as a clean error,
	// not a phantom message id 0.
	if _, err := extractMessageID(&tg.Updates{Updates: nil}); err == nil {
		t.Errorf("want error for empty Updates")
	}
}

// TestIsMembershipError pins the set of errors that mean "every bot
// in the pool will fail the same way" so the round-robin fallback
// short-circuits. A wrong call here would either (a) waste pool slots
// retrying a doomed call or (b) silently swallow a transient error
// the pool could have recovered.
func TestIsMembershipError(t *testing.T) {
	for _, code := range []string{"CHANNEL_PRIVATE", "CHAT_FORBIDDEN", "CHANNEL_INVALID"} {
		err := &tgerr.Error{Code: 400, Message: code, Type: code}
		if !isMembershipError(err) {
			t.Errorf("%q should be fatal", code)
		}
		// Wrapped variants must also classify as fatal — the download
		// path wraps errors with fmt.Errorf("...: %w", err).
		wrapped := fmt.Errorf("bot 0 fetch loc: %w", err)
		if !isMembershipError(wrapped) {
			t.Errorf("wrapped %q should be fatal", code)
		}
	}

	// Transient errors do NOT classify as fatal; the fallback loop
	// should retry them on another bot.
	transient := []error{
		errors.New("connection reset"),
		&tgerr.Error{Code: 500, Message: "INTERNAL", Type: "INTERNAL"},
		&tgerr.Error{Code: 420, Message: "FLOOD_WAIT_5", Type: "FLOOD_WAIT_5"},
	}
	for _, err := range transient {
		if isMembershipError(err) {
			t.Errorf("%v should be transient (recoverable via fallback)", err)
		}
	}

	// nil → not fatal (defensive against bare callsite check).
	if isMembershipError(nil) {
		t.Error("nil should not be fatal")
	}
}
