package config

import (
	"errors"
	"os"
	"strconv"
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
