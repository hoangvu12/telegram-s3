package mtproto

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"

	"telegram-s3/internal/storage"
	parent "telegram-s3/internal/storage/telegram"
)

// Upload chunks the body and sends each chunk as its own Telegram
// document via MTProto. Mirrors BotStorage.Upload in shape — same
// chunk size, same per-chunk bot rotation, same offset/seq bookkeeping
// — so the chunk map shape is invariant across transports and the
// dispatcher / reader / sweeper don't need transport-specific logic.
//
// Each chunk is uploaded with uploader.NewUploader at 512 KiB part
// size (the only MTProto-compatible part size that fills the 2 GiB
// per-document envelope) and `s.uploadThreads` parallel parts.
// Telegram's flood/rate middleware is wired into the *tg.Client at
// pool construction time, so retries on FLOOD_WAIT happen for free.
func (s *Storage) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]storage.Chunk, error) {
	var chunks []storage.Chunk
	var offset int64
	scratch := make([]byte, 64<<10) // io.CopyBuffer reuse, like BotStorage
	var buf bytes.Buffer

	for seq := 0; ; seq++ {
		buf.Reset()
		n, rerr := io.CopyBuffer(&buf, io.LimitReader(body, int64(s.chunkSize)), scratch)
		if n > 0 {
			botIdx, bot := s.pool.Pick(parent.BotOpUpload)
			ch, err := s.uploadChunk(ctx, bot, name, contentType, seq, buf.Bytes(), n)
			if err != nil {
				s.cleanup(ctx, chunks)
				return nil, err
			}
			ch.Seq = seq
			ch.Size = n
			ch.Offset = offset
			ch.BotIndex = botIdx
			ch.Transport = storage.TransportMTProto
			chunks = append(chunks, ch)
			offset += n
		}
		if rerr != nil {
			s.cleanup(ctx, chunks)
			return nil, rerr
		}
		if n < int64(s.chunkSize) {
			break // body exhausted
		}
	}
	return chunks, nil
}

// uploadChunk uploads one chunk's bytes via MTProto and returns the
// resulting chunk's MessageID. FileID is intentionally left empty —
// MTProto chunk identity is (MessageID, BotIndex), and persisting a
// bot-bound file_id alongside the mtproto message would be a footgun
// for the read path (it'd look usable but the bot transport rejects
// "no file_id" rows, and the mtproto transport ignores file_id).
func (s *Storage) uploadChunk(ctx context.Context, bot *MTProtoBot, name, contentType string, seq int, data []byte, total int64) (storage.Chunk, error) {
	// Session, not API: uploader.WithThreads fans `s.uploadThreads`
	// concurrent saveFilePart calls that would otherwise serialize
	// through gotd's one default session per bot. sendMedia on the
	// same session is fine — it's a single follow-up RPC.
	api := bot.Session(ctx)

	u := uploader.NewUploader(api).WithThreads(s.uploadThreads).WithPartSize(uploadPartSize)
	inputFile, err := u.Upload(ctx, uploader.NewUpload(chunkFilename(name, seq), bytes.NewReader(data), total))
	if err != nil {
		return storage.Chunk{}, fmt.Errorf("mtproto upload (bot %d seq %d): %w", bot.Index(), seq, err)
	}

	sender := message.NewSender(api)
	ch := bot.Channel()
	peer := &tg.InputPeerChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}

	doc := message.UploadedDocument(inputFile).Filename(chunkFilename(name, seq)).ForceFile(true)
	if contentType != "" {
		doc = doc.MIME(contentType)
	}

	upd, err := sender.To(peer).Media(ctx, doc)
	if err != nil {
		return storage.Chunk{}, fmt.Errorf("mtproto sendMedia (bot %d seq %d): %w", bot.Index(), seq, err)
	}

	msgID, err := extractMessageID(upd)
	if err != nil {
		return storage.Chunk{}, fmt.Errorf("mtproto extract message id (bot %d seq %d): %w", bot.Index(), seq, err)
	}

	// Read back the message we just sent and confirm the server-side
	// document size matches what we streamed. teldrive learned (commit
	// 5b4faaa, Feb 2025) that sendMedia can return ok while the
	// resulting document is short or zero-sized — surfacing only later
	// as a corrupt GET. The send bucket cannot tolerate that, so we
	// catch it at upload time and bounce. verifyUploadedSize deletes
	// the bad message itself; the chunk just returns the error.
	if err := s.verifyUploadedSize(ctx, bot, msgID, total); err != nil {
		return storage.Chunk{}, fmt.Errorf("mtproto verify (bot %d seq %d): %w", bot.Index(), seq, err)
	}
	return storage.Chunk{MessageID: int64(msgID)}, nil
}

// verifyUploadedSize round-trips through channels.getMessages on the
// freshly-uploaded message and confirms its document size equals the
// expected bytes. On any mismatch (zero doc, missing media, wrong size)
// the bot message is best-effort deleted so a corrupt body doesn't
// linger in the channel as a zombie. The verify failure surfaces to
// uploadChunk's caller, which triggers the standard cleanup path for
// earlier chunks in the same Upload.
func (s *Storage) verifyUploadedSize(ctx context.Context, bot *MTProtoBot, msgID int, expected int64) error {
	api := bot.API()
	ch := bot.Channel()
	res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
	})
	if err != nil {
		return fmt.Errorf("channels.getMessages msg %d: %w", msgID, err)
	}
	modified, ok := res.AsModified()
	if !ok {
		s.deleteByMessageID(ctx, bot, msgID)
		return fmt.Errorf("msg %d: notModified response on fresh verify", msgID)
	}
	msgs := modified.GetMessages()
	if len(msgs) == 0 {
		s.deleteByMessageID(ctx, bot, msgID)
		return fmt.Errorf("msg %d: not found", msgID)
	}
	size, ok := inspectDocumentSize(msgs[0])
	if !ok {
		s.deleteByMessageID(ctx, bot, msgID)
		return fmt.Errorf("msg %d: no document in message", msgID)
	}
	if size != expected {
		s.deleteByMessageID(ctx, bot, msgID)
		return fmt.Errorf("msg %d: size mismatch got=%d want=%d", msgID, size, expected)
	}
	return nil
}

// inspectDocumentSize returns (doc.Size, true) when msg carries a
// non-empty document. Any other shape returns false — the verify path
// treats those as upload failures rather than trying to interpret them.
// Split out as a pure function so the size-mismatch / shape tests
// don't need a *tg.Client mock.
func inspectDocumentSize(msg tg.MessageClass) (int64, bool) {
	res, ok := msg.AsNotEmpty()
	if !ok {
		return 0, false
	}
	m, ok := res.(*tg.Message)
	if !ok {
		return 0, false
	}
	media, ok := m.Media.(*tg.MessageMediaDocument)
	if !ok || media == nil {
		return 0, false
	}
	doc, ok := media.Document.AsNotEmpty()
	if !ok {
		return 0, false
	}
	return doc.Size, true
}

// deleteByMessageID best-effort removes a single message via the
// passed bot's API. Verify-fail cleanup — failures here are logged
// and swallowed since the verify error is the load-bearing one.
func (s *Storage) deleteByMessageID(ctx context.Context, bot *MTProtoBot, msgID int) {
	api := bot.API()
	ch := bot.Channel()
	if _, err := api.ChannelsDeleteMessages(ctx, &tg.ChannelsDeleteMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
		ID:      []int{msgID},
	}); err != nil && s.logger != nil {
		s.logger.Warn("mtproto verify-fail cleanup delete failed",
			"bot", bot.Index(), "msg", msgID, "error", err)
	}
}

// cleanup best-effort deletes chunks already uploaded when a later
// chunk fails — same contract as BotStorage.cleanup. Per-chunk
// failures are logged; the original Upload error is what surfaces.
func (s *Storage) cleanup(ctx context.Context, chunks []storage.Chunk) {
	if len(chunks) == 0 {
		return
	}
	refs := make([]storage.ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = storage.ChunkRef{
			Transport: storage.TransportMTProto,
			MessageID: c.MessageID,
			BotIndex:  c.BotIndex,
		}
	}
	if err := s.DeleteBatch(ctx, refs); err != nil && s.logger != nil {
		s.logger.Warn("mtproto upload cleanup failed", "count", len(refs), "error", err)
	}
}

// extractMessageID walks the Updates response from sendMedia to find
// the message ID Telegram assigned to the freshly-sent document. The
// response shape varies by chat type (channel vs user), so cover the
// three common variants. Returning an error on miss is correct — a
// successful send without a message ID would mean we lose the chunk
// (no addressable identity to write into object_chunks.message_id).
func extractMessageID(upd tg.UpdatesClass) (int, error) {
	switch u := upd.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID, nil
	case *tg.Updates:
		if id, ok := scanForMessageID(u.Updates); ok {
			return id, nil
		}
	case *tg.UpdatesCombined:
		if id, ok := scanForMessageID(u.Updates); ok {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no message id in updates %T", upd)
}

func scanForMessageID(events []tg.UpdateClass) (int, bool) {
	for _, ev := range events {
		switch e := ev.(type) {
		case *tg.UpdateNewChannelMessage:
			if m, ok := e.Message.(*tg.Message); ok {
				return m.ID, true
			}
		case *tg.UpdateNewMessage:
			if m, ok := e.Message.(*tg.Message); ok {
				return m.ID, true
			}
		case *tg.UpdateMessageID:
			// Sent before the *NewMessage event in some responses; it
			// carries the assigned ID but not the rest of the message.
			// Prefer the New* update if both are present, but fall
			// back to this for completeness.
			return e.ID, true
		}
	}
	return 0, false
}
