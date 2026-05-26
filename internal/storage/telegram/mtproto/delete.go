package mtproto

import (
	"context"

	"github.com/gotd/td/tg"

	"telegram-s3/internal/storage"
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

// DeleteBatch groups refs by bot (routing through the writer keeps
// "which bot deleted what" debuggable) and issues
// channels.deleteMessages batched at 100 IDs/call. Returns the first
// error encountered; subsequent failures are logged. Callers
// (multipart abort, reap-superseded-chunks) treat this as best-effort
// cleanup.
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
		bot := s.pool.At(ref.BotIndex)
		if bot == nil {
			// Pool shrunk — route to any pool member. Under MTProto delete
			// is bot-agnostic (channel admin permission, not message
			// ownership), so any in-pool bot suffices.
			_, bot = s.pool.Pick(BotOpStream) // op irrelevant for deletes; reuse stream counter
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
