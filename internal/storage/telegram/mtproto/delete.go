package mtproto

import (
	"context"
	"fmt"

	"github.com/gotd/td/tg"

	"telegram-s3/internal/storage"
	parent "telegram-s3/internal/storage/telegram"
)

// deleteBatchSize is the upper bound Telegram accepts in a single
// channels.deleteMessages call. Per teldrive's tgc/helpers.go and
// MTProto docs: 100. Larger batches return INPUT_TOO_LONG.
const deleteBatchSize = 100

// Delete removes one MTProto message. Implemented as a single-element
// DeleteBatch so the batched path is the only one with API logic.
func (s *Storage) Delete(ctx context.Context, ref storage.ChunkRef) error {
	return s.DeleteBatch(ctx, []storage.ChunkRef{ref})
}

// DeleteBatch groups refs by bot (any bot in the same channel can
// delete any message, but routing through the writer matches the
// BotStorage contract and keeps "which bot deleted what" debuggable)
// and issues channels.deleteMessages batched at 100 IDs/call.
//
// A non-mtproto ref is a programming bug — the dispatcher should
// have routed it to BotStorage. We surface as an error so a stray
// ref doesn't get silently dropped.
//
// Returns the first error encountered; subsequent failures are
// logged. The Bot HTTP API contract documented on BotStorage's
// DeleteBatch is: "best-effort cleanup, surface the first error,
// keep going." MTProto's batched path follows the same contract so
// callers (multipart abort, reap-superseded-chunks) don't need to
// switch on transport.
func (s *Storage) DeleteBatch(ctx context.Context, refs []storage.ChunkRef) error {
	if len(refs) == 0 {
		return nil
	}
	type botBatch struct {
		bot *MTProtoBot
		ids []int
	}
	byBot := make(map[int]*botBatch, s.pool.Len())

	var firstErr error
	for _, ref := range refs {
		if ref.Transport != storage.TransportMTProto {
			err := fmt.Errorf("mtproto: DeleteBatch got non-mtproto ref (transport=%q)", ref.Transport)
			if firstErr == nil {
				firstErr = err
			}
			if s.logger != nil {
				s.logger.Warn("mtproto delete skipped (wrong transport)", "msg_id", ref.MessageID, "transport", ref.Transport)
			}
			continue
		}
		bot := s.pool.At(ref.BotIndex)
		if bot == nil {
			// Pool shrunk — route to any pool member. Under MTProto delete
			// is bot-agnostic (channel admin permission, not message
			// ownership), so any in-pool bot suffices.
			_, bot = s.pool.Pick(parent.BotOpStream) // op irrelevant for deletes; reuse stream counter
		}
		b := byBot[bot.Index()]
		if b == nil {
			b = &botBatch{bot: bot, ids: make([]int, 0, deleteBatchSize)}
			byBot[bot.Index()] = b
		}
		b.ids = append(b.ids, int(ref.MessageID))
	}

	for _, b := range byBot {
		if err := s.deleteForBot(ctx, b.bot, b.ids); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if s.logger != nil {
				s.logger.Warn("mtproto delete batch failed",
					"bot_index", b.bot.Index(), "count", len(b.ids), "error", err)
			}
		}
	}
	return firstErr
}

// deleteForBot slices the bot's id list into 100-ID windows and
// fires one channels.deleteMessages per window. Telegram's response
// is *MessagesAffectedMessages which we ignore — a 0-count return
// just means the message was already gone (idempotent delete).
func (s *Storage) deleteForBot(ctx context.Context, bot *MTProtoBot, ids []int) error {
	api := bot.API()
	ch := bot.Channel()
	inputCh := &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}

	var firstErr error
	for start := 0; start < len(ids); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		_, err := api.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
			Channel: inputCh,
			ID:      ids[start:end],
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
