package mtproto

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gotd/td/tg"

	"telegram-s3/internal/cache"
)

// uploadPartSize is the MTProto wire-chunk size for uploads. Telegram
// caps a single document at 4000 parts × 512 KiB = 2 GiB. Smaller parts
// raise the part count and trip the per-document ceiling; larger parts
// (1 MiB) are rejected by the server. 512 KiB is the only safe value
// that fills the 2 GiB envelope — do not change without verifying
// the per-document cap matches Telegram's current limit.
const uploadPartSize = 512 * 1024

// docLocationKey identifies one cached *tg.InputDocumentFileLocation.
// (MessageID, BotIndex) is the chunk identity; message_ids are
// bot-agnostic so any pool member can resolve a foreign bot's message
// (the BotIndex here is the writer hint, not a lookup constraint).
type docLocationKey struct {
	MessageID int64
	BotIndex  int
}

// Storage is the MTProto Backend. The locCache turns repeated reads
// of a single chunk into one channels.GetMessages plus N UploadGetFile
// calls — without it the file_reference resolve cost dominates the
// hot path.
type Storage struct {
	pool          *Pool
	chunkSize     int
	uploadThreads int
	logger        *slog.Logger
	// locCache memoizes (messageID, botIndex) → *tg.InputDocumentFileLocation.
	// file_reference inside InputDocumentFileLocation expires periodically
	// (Telegram doesn't document the exact lifetime; observed at ~30m).
	// The cache TTL matches that; download.go invalidates the entry on
	// FILE_REFERENCE_EXPIRED and re-resolves once.
	locCache *cache.Cache[docLocationKey, *tg.InputDocumentFileLocation]
}

// Options bundles MTProto Storage construction. The chat/channel
// resolution is per-bot (each bot has its own access hash), so the
// channel pointer lives on MTProtoBot, not here. ChunkSize defaults
// to 18 MiB so chunked objects stay the same shape across the
// pre/post-migration boundary and the prefetch reader's window stays
// meaningful.
type Options struct {
	Pool          *Pool
	ChunkSize     int // bytes; defaults to 18 MiB
	UploadThreads int // parallel parts per upload; defaults to 8
	LocationTTL   time.Duration
	Logger        *slog.Logger
}

// NewStorage wires the Storage. The pool must already be started
// (every bot in it must be past StartBot, otherwise an Upload call
// races the auth handshake). Construction is cheap — call it once
// at boot and share across the handler / sweeper.
func NewStorage(opts Options) (*Storage, error) {
	if opts.Pool == nil || opts.Pool.Len() == 0 {
		return nil, fmt.Errorf("mtproto: NewStorage requires a non-empty pool")
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 18 << 20
	}
	if opts.UploadThreads <= 0 {
		opts.UploadThreads = 8
	}
	if opts.LocationTTL <= 0 {
		opts.LocationTTL = 30 * time.Minute
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Storage{
		pool:          opts.Pool,
		chunkSize:     opts.ChunkSize,
		uploadThreads: opts.UploadThreads,
		logger:        logger,
		locCache:      cache.New[docLocationKey, *tg.InputDocumentFileLocation](opts.LocationTTL, 0),
	}, nil
}

// Close releases the cache sweeper. Does NOT close the pool — the
// caller owns the bots and is responsible for shutting them down.
func (s *Storage) Close() {
	if s.locCache != nil {
		s.locCache.Close()
	}
}

// Pool exposes the underlying bot pool for callers that need to
// coordinate with bot affinity.
func (s *Storage) Pool() *Pool { return s.pool }

// chunkFilename produces the per-chunk Telegram filename: object key
// for seq 0, "<key>.partN" for subsequent chunks. Telegram preserves
// the filename on download, useful for debugging via the client UI.
func chunkFilename(name string, seq int) string {
	base := safeFilename(name)
	if seq == 0 {
		return base
	}
	return fmt.Sprintf("%s.part%d", base, seq)
}

func safeFilename(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\\", "/"))
	if name == "" || strings.HasSuffix(name, "/") {
		return fmt.Sprintf("object-%d.bin", time.Now().UnixNano())
	}
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}
