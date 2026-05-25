package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr      string
	DatabasePath    string
	AccessKeyID     string
	SecretAccessKey string
	// TelegramBotTokens is the active multi-bot pool (Phase 3). New uploads
	// round-robin across it; each chunk's bot_index is persisted so reads
	// resolve through the same bot under the Bot HTTP API's file_id-bound
	// model. Parsed from TELEGRAM_BOT_TOKENS (comma-separated). Required.
	TelegramBotTokens []string
	// TelegramLegacyBotToken was the single-bot Phase-0..2 token (env
	// TELEGRAM_BOT_TOKEN). It is preserved for two reasons:
	//   * deploy rollback: if the binary is reverted before the Phase 4
	//     migration drains, this value must still resolve legacy chunks.
	//   * fallback: if TELEGRAM_BOT_TOKENS is unset, treat the legacy token
	//     as a one-element pool so a partial deploy still boots.
	// The migration backfills legacy single-message rows with bot_index=0,
	// which is also TELEGRAM_BOT_TOKENS[0] — operators are expected to put
	// the legacy token first in the new list.
	TelegramLegacyBotToken string
	TelegramChatID         string
	// TelegramAPIBaseURL defaults to the public Bot API (50 MB send / 20 MB
	// getFile limits). Point it at a self-hosted local Bot API server to raise
	// the ceiling to ~2000 MB / unbounded downloads (S3-COMPAT-PLAN.md §3.4).
	TelegramAPIBaseURL string
	TempDir            string
	PublicEndpointURL  string
	// MultipartTTL is how long an in-progress multipart upload may sit
	// untouched before the janitor aborts it (P8.6). MultipartSweepInterval
	// is how often the janitor runs; <= 0 disables the sweep entirely.
	MultipartTTL           time.Duration
	MultipartSweepInterval time.Duration
	// HTTPMaxIdleConnsPerHost bounds the keepalive pool the Telegram client
	// reuses across chunk requests. Go's default of 2 silently throttles
	// concurrent uploads/downloads — every chunk past the second pays a TLS
	// handshake. 32 keeps the pool large enough for the stream-prefetch
	// fan-out introduced in Phase 2 without bloating memory.
	HTTPMaxIdleConnsPerHost int
	// SQLiteReaderConns bounds the read-only *sql.DB. WAL allows many
	// concurrent readers and one writer; we open a separate single-conn
	// writer pool, so this knob only governs the SELECT side.
	SQLiteReaderConns int
	// LocationCacheTTL is how long a resolved Bot API file_path is cached
	// under its file_id. Telegram's docs say a file_path is valid for at
	// least an hour; 30m is a safe default that survives the longest reads
	// we issue (range fetches over chunked objects). Phase 1.
	LocationCacheTTL time.Duration
	// TelegramMaxChunkSize is the upload chunk window. 18 MiB stays under
	// the public Bot API 20 MB getFile cap; users running a self-hosted
	// `tdlib/telegram-bot-api` server can raise this to ~1.9 GiB. Phase 1.
	TelegramMaxChunkSize int64
	// StreamConcurrency / StreamBuffers / ChunkTimeout govern the
	// parallel-prefetch reader on the object GET path (Phase 2).
	// StreamConcurrency caps the number of in-flight chunk fetches per
	// stream; StreamBuffers is the ordered delivery channel's capacity;
	// ChunkTimeout bounds each individual fetch. StreamChunkSize is the
	// reader's prefetch window in object-space bytes — independent of
	// the upload chunk size (typically larger, so one fetch can span
	// multiple Telegram messages).
	StreamConcurrency int
	StreamBuffers     int
	ChunkTimeout      time.Duration
	StreamChunkSize   int64
	// Phase 4 — MTProto backend + dispatcher knobs. Default
	// TelegramTransport="bot" is a deploy-time no-op: only BotStorage
	// runs and the dispatcher routes everything to it. Flip to "dual"
	// to start writing new chunks via MTProto while old chunks keep
	// reading via the Bot API (and the sweeper drains them in the
	// background). Flip to "mtproto" only after the sweeper has
	// drained every transport='bot' row.
	TelegramTransport     string
	TelegramAppID         int    // from my.telegram.org; required when transport != "bot"
	TelegramAppHash       string // from my.telegram.org; required when transport != "bot"
	TelegramPoolSize      int    // gotd pool.Pool size per bot (session multiplexing)
	TelegramUploadThreads int    // uploader.WithThreads per upload
	// MigrationRate is the sweeper's pass-1 throughput cap in rows/day.
	// 0 disables the sweeper entirely (the dispatcher still routes new
	// uploads via MTProto in dual mode, but no migration happens).
	MigrationRate int
	// MigrationWorkers caps pass-1 concurrency within a single tick. Each
	// worker holds one in-flight (bot download + mtproto upload) pair, so
	// e.g. 4 workers lets 4 chunks migrate at once instead of serialized.
	// Default 4, clamped to [1, 32]. Tune up only if profiling says the
	// MTProto session pool / bot HTTP API can absorb more parallelism
	// without tripping FLOOD_WAIT.
	MigrationWorkers int
	// BotDeleteGrace is how long a migrated bot message lingers in
	// bot_chunks_pending_delete before the sweeper's pass-2 reaps it.
	// See PHASES.md design decision #14: this window prevents a reader
	// that fetched the chunk map just before the swap from 404'ing on a
	// permanent delete. Minimum 1m; 0 forces immediate delete (test-only).
	BotDeleteGrace time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:              getenv("LISTEN_ADDR", ":9000"),
		DatabasePath:            getenv("DATABASE_PATH", "telegram-s3.db"),
		AccessKeyID:             os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:         os.Getenv("S3_SECRET_ACCESS_KEY"),
		TelegramBotTokens:       parseTokenList(os.Getenv("TELEGRAM_BOT_TOKENS")),
		TelegramLegacyBotToken:  os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:          os.Getenv("TELEGRAM_CHAT_ID"),
		TelegramAPIBaseURL:      getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
		TempDir:                 getenv("TEMP_DIR", os.TempDir()),
		PublicEndpointURL:       os.Getenv("PUBLIC_ENDPOINT_URL"),
		MultipartTTL:            getDuration("MULTIPART_TTL", 7*24*time.Hour),
		MultipartSweepInterval:  getDuration("MULTIPART_SWEEP_INTERVAL", time.Hour),
		HTTPMaxIdleConnsPerHost: getInt("HTTP_MAX_IDLE_CONNS_PER_HOST", 32),
		SQLiteReaderConns:       getInt("SQLITE_READER_CONNS", 8),
		LocationCacheTTL:        getDuration("LOCATION_CACHE_TTL", 30*time.Minute),
		TelegramMaxChunkSize:    getBytes("TELEGRAM_MAX_CHUNK_SIZE", 18<<20),
		StreamConcurrency:       getInt("STREAM_CONCURRENCY", 4),
		StreamBuffers:           getInt("STREAM_BUFFERS", 8),
		ChunkTimeout:            getDuration("CHUNK_TIMEOUT", 30*time.Second),
		StreamChunkSize:         getBytes("STREAM_CHUNK_SIZE", 4<<20),
		TelegramTransport:       strings.ToLower(getenv("TELEGRAM_TRANSPORT", "bot")),
		TelegramAppID:           getInt("TELEGRAM_APP_ID", 0),
		TelegramAppHash:         os.Getenv("TELEGRAM_APP_HASH"),
		TelegramPoolSize:        getInt("TELEGRAM_POOL_SIZE", 4),
		TelegramUploadThreads:   getInt("TELEGRAM_UPLOAD_THREADS", 8),
		MigrationRate:           getIntNonNeg("MIGRATION_RATE", 100),
		MigrationWorkers:        getInt("MIGRATION_WORKERS", 4),
		BotDeleteGrace:          getDuration("BOT_DELETE_GRACE", time.Hour),
	}

	// Soft-fallback to the pre-Phase-3 single-token env so a partial deploy
	// (binary updated, env not yet) still boots in single-bot mode. The
	// "production deploy note" in PHASES.md still asks operators to set
	// the plural; this is a safety net, not a recommended steady state.
	if len(cfg.TelegramBotTokens) == 0 && cfg.TelegramLegacyBotToken != "" {
		cfg.TelegramBotTokens = []string{cfg.TelegramLegacyBotToken}
	}

	if cfg.AccessKeyID == "" {
		return Config{}, errors.New("S3_ACCESS_KEY_ID is required")
	}
	if cfg.SecretAccessKey == "" {
		return Config{}, errors.New("S3_SECRET_ACCESS_KEY is required")
	}
	if len(cfg.TelegramBotTokens) == 0 {
		return Config{}, errors.New("TELEGRAM_BOT_TOKENS is required (comma-separated list; or set TELEGRAM_BOT_TOKEN for single-bot mode)")
	}
	if cfg.TelegramChatID == "" {
		return Config{}, errors.New("TELEGRAM_CHAT_ID is required")
	}

	// Phase 4 validation. The transport flag must be one of the three known
	// values — a typo silently routing to "bot" would mask a misconfigured
	// migration. MTProto-using transports need app credentials from
	// my.telegram.org; "bot" leaves them optional so the legacy single-
	// transport deploy boots without them.
	switch cfg.TelegramTransport {
	case "bot", "dual", "mtproto":
	default:
		return Config{}, fmt.Errorf("TELEGRAM_TRANSPORT=%q not recognized (want bot, dual, or mtproto)", cfg.TelegramTransport)
	}
	if cfg.TelegramTransport != "bot" {
		if cfg.TelegramAppID <= 0 {
			return Config{}, errors.New("TELEGRAM_APP_ID is required when TELEGRAM_TRANSPORT != bot (register an app at my.telegram.org)")
		}
		if cfg.TelegramAppHash == "" {
			return Config{}, errors.New("TELEGRAM_APP_HASH is required when TELEGRAM_TRANSPORT != bot")
		}
	}
	// Clamp the grace window to a safe production minimum so a typo like
	// `BOT_DELETE_GRACE=10s` doesn't quietly enable the read-during-swap
	// race. The explicit 0 value bypasses the clamp because tests rely on
	// it to drive the immediate-delete path.
	if cfg.BotDeleteGrace > 0 && cfg.BotDeleteGrace < time.Minute {
		cfg.BotDeleteGrace = time.Minute
	}

	return cfg, nil
}

// parseTokenList splits a comma-separated TELEGRAM_BOT_TOKENS value,
// trimming whitespace around each entry and dropping empties (so a
// trailing comma or stray spaces don't produce empty tokens that would
// confuse the round-robin).
func parseTokenList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	tokens := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			tokens = append(tokens, t)
		}
	}
	return tokens
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// getIntNonNeg is getInt's permissive cousin: it accepts 0 as a real value
// (the "disable" sentinel for knobs like MIGRATION_RATE). A negative value
// is still treated as invalid and falls back.
func getIntNonNeg(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}

// getBytes parses a byte-count env var. Accepts plain integers ("1900000000")
// and common suffixes (KB/MB/GB decimal, KiB/MiB/GiB binary). Invalid values
// silently fall back; we keep go-humanize an indirect dep rather than promote
// it just for this.
func getBytes(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	if n, err := parseBytes(value); err == nil && n > 0 {
		return n
	}
	return fallback
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	// Split numeric prefix from unit suffix.
	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numStr := s[:i]
	unit := strings.TrimSpace(strings.ToLower(s[i:]))
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, err
	}
	var mult float64 = 1
	switch unit {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = 1000
	case "m", "mb":
		mult = 1000 * 1000
	case "g", "gb":
		mult = 1000 * 1000 * 1000
	case "kib":
		mult = 1024
	case "mib":
		mult = 1024 * 1024
	case "gib":
		mult = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unrecognized byte unit %q", unit)
	}
	return int64(num * mult), nil
}
