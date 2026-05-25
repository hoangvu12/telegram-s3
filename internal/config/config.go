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
	ListenAddr       string
	DatabasePath     string
	AccessKeyID      string
	SecretAccessKey  string
	TelegramBotToken string
	TelegramChatID   string
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
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:             getenv("LISTEN_ADDR", ":9000"),
		DatabasePath:           getenv("DATABASE_PATH", "telegram-s3.db"),
		AccessKeyID:            os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		TelegramBotToken:       os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:         os.Getenv("TELEGRAM_CHAT_ID"),
		TelegramAPIBaseURL:     getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
		TempDir:                getenv("TEMP_DIR", os.TempDir()),
		PublicEndpointURL:      os.Getenv("PUBLIC_ENDPOINT_URL"),
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
	}

	if cfg.AccessKeyID == "" {
		return Config{}, errors.New("S3_ACCESS_KEY_ID is required")
	}
	if cfg.SecretAccessKey == "" {
		return Config{}, errors.New("S3_SECRET_ACCESS_KEY is required")
	}
	if cfg.TelegramBotToken == "" {
		return Config{}, errors.New("TELEGRAM_BOT_TOKEN is required")
	}
	if cfg.TelegramChatID == "" {
		return Config{}, errors.New("TELEGRAM_CHAT_ID is required")
	}

	return cfg, nil
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
