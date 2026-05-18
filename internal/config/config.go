package config

import (
	"errors"
	"os"
)

type Config struct {
	ListenAddr        string
	DatabasePath      string
	AccessKeyID       string
	SecretAccessKey   string
	TelegramBotToken  string
	TelegramChatID    string
	TempDir           string
	PublicEndpointURL string
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:        getenv("LISTEN_ADDR", ":9000"),
		DatabasePath:      getenv("DATABASE_PATH", "telegram-s3.db"),
		AccessKeyID:       os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:   os.Getenv("S3_SECRET_ACCESS_KEY"),
		TelegramBotToken:  os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:    os.Getenv("TELEGRAM_CHAT_ID"),
		TempDir:           getenv("TEMP_DIR", os.TempDir()),
		PublicEndpointURL: os.Getenv("PUBLIC_ENDPOINT_URL"),
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
