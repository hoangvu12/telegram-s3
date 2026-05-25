package mtproto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"telegram-s3/internal/storage"
	parent "telegram-s3/internal/storage/telegram"
)

// mtprotoDownloadChunk is the maximum bytes per UploadGetFile call.
// Telegram requires Limit to be a power-of-2 between 4 KiB and 1 MiB
// and Offset to be a multiple of Limit. 1 MiB is the largest value
// the server accepts; smaller values multiply RPC count for a given
// download size, so use the max.
const mtprotoDownloadChunk = 1 << 20

// Download returns a ReadCloser over the full chunk's bytes. This is
// the "no range" path BotStorage exposes for parity, used by the
// migration sweeper's pass-1 (it pulls the whole chunk via the BOT
// transport then re-uploads via MTProto; the reciprocal path isn't
// exercised in production, but the dispatcher's conformance tests
// expect both backends to implement it).
func (s *Storage) Download(ctx context.Context, ref storage.ChunkRef) (io.ReadCloser, error) {
	return s.DownloadRange(ctx, ref, 0, 0)
}

// DownloadRange returns [offset, offset+length) of the chunk's bytes.
// length <= 0 means "to end of chunk" (matching BotStorage). The
// returned ReadCloser buffers the whole response in memory — the
// parallel-prefetch reader's bounded chunk size keeps that buffer
// small (typically 1 MiB × ChunkSource step).
//
// Lookup is bot-affinity-first (ref.BotIndex), falling back to
// round-robin across the rest of the pool on transient errors:
// MTProto message IDs are bot-agnostic so any pool member can resolve
// a foreign bot's message. CHANNEL_PRIVATE / CHAT_FORBIDDEN are
// fatal — every bot would fail the same way — and surface
// immediately without burning more pool slots.
func (s *Storage) DownloadRange(ctx context.Context, ref storage.ChunkRef, offset, length int64) (io.ReadCloser, error) {
	if ref.Transport != storage.TransportMTProto {
		return nil, fmt.Errorf("mtproto: ref transport %q, want %q", ref.Transport, storage.TransportMTProto)
	}
	if offset < 0 {
		return nil, fmt.Errorf("mtproto: negative offset %d", offset)
	}
	end := int64(math.MaxInt64)
	if length > 0 {
		end = offset + length
	}

	primary := s.pool.At(ref.BotIndex)
	if primary == nil {
		// Pool shrunk underneath a row written on a larger deploy. Fall
		// back to round-robin — under MTProto any bot can resolve the
		// message (unlike Bot API file_ids which are bot-bound).
		_, primary = s.pool.Pick(parent.BotOpStream)
	}

	rc, err := s.downloadViaBot(ctx, primary, ref, offset, end)
	if err == nil {
		return rc, nil
	}
	if isMembershipError(err) {
		return nil, err
	}
	// Round-robin over every other bot, in order. A successful read
	// returns immediately; an exhausted pool surfaces the most recent
	// error wrapped with context for observability.
	lastErr := err
	for i := 0; i < s.pool.Len(); i++ {
		bot := s.pool.At(i)
		if bot == primary {
			continue
		}
		rc, err2 := s.downloadViaBot(ctx, bot, ref, offset, end)
		if err2 == nil {
			s.logger.Debug("mtproto download fallback succeeded",
				"primary_bot", primary.Index(), "fallback_bot", bot.Index(),
				"msg_id", ref.MessageID, "primary_err", lastErr)
			return rc, nil
		}
		if isMembershipError(err2) {
			return nil, err2
		}
		lastErr = err2
	}
	return nil, fmt.Errorf("mtproto download msg %d: all bots failed: %w", ref.MessageID, lastErr)
}

// downloadViaBot fetches the requested byte range from one specific
// bot. The FILE_REFERENCE_EXPIRED dance happens here so a 30-minute
// stale cache entry doesn't bubble all the way up — refresh the
// location and retry once before giving up.
func (s *Storage) downloadViaBot(ctx context.Context, bot *MTProtoBot, ref storage.ChunkRef, offset, end int64) (io.ReadCloser, error) {
	loc, err := s.resolveLocation(ctx, bot, ref.MessageID)
	if err != nil {
		return nil, err
	}

	data, err := s.fetchRange(ctx, bot, loc, offset, end)
	if err == nil {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	// FILE_REFERENCE_EXPIRED / _INVALID: the cached InputDocumentFileLocation
	// carries a server-side opaque ref that goes stale (~30m, undocumented).
	// Invalidate, re-resolve, retry once. Anything else is permanent for
	// this attempt — surface to the caller's fallback loop.
	if !tgerr.Is(err, tg.ErrFileReferenceExpired, tg.ErrFileReferenceInvalid) {
		return nil, err
	}
	s.locCache.Delete(docLocationKey{MessageID: ref.MessageID, BotIndex: bot.Index()})
	loc, err = s.resolveLocation(ctx, bot, ref.MessageID)
	if err != nil {
		return nil, fmt.Errorf("refresh location after FILE_REFERENCE_EXPIRED: %w", err)
	}
	data, err = s.fetchRange(ctx, bot, loc, offset, end)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// fetchRange issues 1+ UploadGetFile calls covering [offset, end) and
// returns the concatenated trimmed bytes. The MTProto request must
// align Offset to a multiple of Limit (a power-of-2 between 4 KiB and
// 1 MiB); the caller-supplied offset is arbitrary, so we align down,
// read aligned chunks, and trim the head + tail at the boundaries.
//
// Short reads from the server signal EOF (the file is smaller than
// the requested window). Stop early in that case — overshooting the
// chunk size with extra zero-padded reads is a Telegram quirk we
// would inherit from a naive impl.
func (s *Storage) fetchRange(ctx context.Context, bot *MTProtoBot, loc *tg.InputDocumentFileLocation, offset, end int64) ([]byte, error) {
	api := bot.API()
	alignedStart := offset - (offset % mtprotoDownloadChunk)

	var buf bytes.Buffer
	cur := alignedStart
	for cur < end {
		res, err := api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: loc,
			Offset:   cur,
			Limit:    mtprotoDownloadChunk,
			Precise:  true,
		})
		if err != nil {
			return nil, err
		}
		file, ok := res.(*tg.UploadFile)
		if !ok {
			// CDN redirect path is rare for bot-uploaded content (CDN serves
			// popular public-channel content); production failures here
			// would tell us to implement the redirect follow. Until then,
			// fail loudly so the operator knows to investigate.
			return nil, fmt.Errorf("mtproto: unexpected UploadGetFile response %T (CDN redirect not implemented)", res)
		}
		if len(file.Bytes) == 0 {
			break // EOF
		}
		buf.Write(file.Bytes)
		cur += int64(len(file.Bytes))
		if len(file.Bytes) < mtprotoDownloadChunk {
			break // short read = EOF before end
		}
	}

	// Trim head (alignedStart..offset) and tail (offset+length..).
	full := buf.Bytes()
	leftCut := int(offset - alignedStart)
	if leftCut >= len(full) {
		return nil, nil
	}
	full = full[leftCut:]
	if end < math.MaxInt64 {
		want := end - offset
		if int64(len(full)) > want {
			full = full[:want]
		}
	}
	return full, nil
}

// resolveLocation memoizes the *tg.InputDocumentFileLocation for the
// (messageID, botIndex) key. Cache misses call channels.GetMessages,
// extract the document, and convert it to a download location.
// file_reference inside the location decays after ~30m so the cache
// TTL matches.
func (s *Storage) resolveLocation(ctx context.Context, bot *MTProtoBot, messageID int64) (*tg.InputDocumentFileLocation, error) {
	key := docLocationKey{MessageID: messageID, BotIndex: bot.Index()}
	if loc, ok := s.locCache.Get(key); ok {
		return loc, nil
	}
	loc, err := s.fetchLocation(ctx, bot, messageID)
	if err != nil {
		return nil, err
	}
	s.locCache.Set(key, loc, 0)
	return loc, nil
}

func (s *Storage) fetchLocation(ctx context.Context, bot *MTProtoBot, messageID int64) (*tg.InputDocumentFileLocation, error) {
	api := bot.API()
	ch := bot.Channel()
	res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: int(messageID)}},
	})
	if err != nil {
		return nil, fmt.Errorf("channels.getMessages bot %d msg %d: %w", bot.Index(), messageID, err)
	}
	// MessagesMessagesClass has variants — `messagesNotModified` carries no
	// messages, only a count. AsModified narrows to the variants that do.
	// A NotModified response on a fresh fetch would mean Telegram thinks
	// we have a cached copy, which we don't, so surface it as an error.
	modified, ok := res.AsModified()
	if !ok {
		return nil, fmt.Errorf("channels.getMessages returned notModified for msg %d", messageID)
	}
	for _, m := range modified.GetMessages() {
		msg, ok := m.(*tg.Message)
		if !ok || int64(msg.ID) != messageID {
			continue
		}
		media, ok := msg.Media.(*tg.MessageMediaDocument)
		if !ok {
			return nil, fmt.Errorf("message %d has no document media (got %T)", messageID, msg.Media)
		}
		doc, ok := media.Document.AsNotEmpty()
		if !ok {
			return nil, fmt.Errorf("message %d document is empty", messageID)
		}
		return doc.AsInputDocumentFileLocation(), nil
	}
	return nil, fmt.Errorf("message %d not in response", messageID)
}

// isMembershipError flags the access errors that mean "every bot will
// fail this call" — typically because the channel is no longer
// accessible to *any* bot in the pool (kicked, channel deleted,
// privacy flipped). Round-robin'ing through the rest of the pool
// would just burn RPCs for the same result.
func isMembershipError(err error) bool {
	if err == nil {
		return false
	}
	if tgerr.Is(err, "CHANNEL_PRIVATE", "CHAT_FORBIDDEN", "CHANNEL_INVALID") {
		return true
	}
	// Some path errors wrap tgerr.Error via fmt.Errorf("...: %w"); errors.Is
	// chain-walks correctly through %w but tgerr.Is checks the message field
	// directly, so wrap once more to handle nested cases defensively.
	var te *tgerr.Error
	return errors.As(err, &te) && (te.Type == "CHANNEL_PRIVATE" || te.Type == "CHAT_FORBIDDEN" || te.Type == "CHANNEL_INVALID")
}
