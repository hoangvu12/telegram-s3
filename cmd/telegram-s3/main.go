package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"telegram-s3/internal/cache"
	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
	"telegram-s3/internal/s3api"
	"telegram-s3/internal/storage/telegram"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	store, err := metadata.OpenWithOptions(cfg.DatabasePath, cfg.SQLiteReaderConns)
	if err != nil {
		logger.Error("open database", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Bot API file_path cache: getFile becomes a per-fileID one-shot
	// instead of per-chunk-read. The cache is reusable by shape in Phase 4
	// (different K/V); the type lives in internal/cache.
	pathCache := cache.New[string, string](cfg.LocationCacheTTL, 0)
	defer pathCache.Close()

	backend := telegram.NewBotStorageWithOptions(
		cfg.TelegramBotTokens,
		cfg.TelegramChatID,
		cfg.TelegramAPIBaseURL,
		cfg.HTTPMaxIdleConnsPerHost,
		int(cfg.TelegramMaxChunkSize),
		pathCache,
		logger,
	)
	handler := s3api.NewHandler(cfg, store, backend, logger)

	// Abandoned-multipart janitor (P8.6). Skipped if the sweep is disabled
	// (interval <= 0). Stops with the server on SIGINT/SIGTERM.
	janitorCtx, cancelJanitor := context.WithCancel(context.Background())
	defer cancelJanitor()
	if cfg.MultipartSweepInterval > 0 {
		go handler.RunMultipartJanitor(janitorCtx, cfg.MultipartSweepInterval, cfg.MultipartTTL)
	}

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("server listening", "addr", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	cancelJanitor()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("shutdown failed", "error", err)
		os.Exit(1)
	}
}
