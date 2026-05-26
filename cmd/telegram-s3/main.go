package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
	"telegram-s3/internal/s3api"
	"telegram-s3/internal/storage/telegram/mtproto"
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

	channelID, err := mtproto.ParseChannelID(cfg.TelegramChatID)
	if err != nil {
		logger.Error("parse channel id", "error", err)
		os.Exit(1)
	}
	mtPool, err := startMTProtoPool(context.Background(), cfg, store, channelID, logger)
	if err != nil {
		logger.Error("start mtproto pool", "error", err)
		os.Exit(1)
	}
	mtStorage, err := mtproto.NewStorage(mtproto.Options{
		Pool:          mtPool,
		ChunkSize:     int(cfg.TelegramMaxChunkSize),
		UploadThreads: cfg.TelegramUploadThreads,
		Logger:        logger,
	})
	if err != nil {
		mtPool.Close()
		logger.Error("init mtproto storage", "error", err)
		os.Exit(1)
	}
	defer mtStorage.Close()

	handler := s3api.NewHandler(cfg, store, mtStorage, logger)

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
		logger.Info("server listening",
			"addr", cfg.ListenAddr,
			"bots", len(cfg.TelegramBotTokens),
		)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	}
	mtPool.Close()
}

// startMTProtoPool brings up every bot in parallel. Each StartBot
// blocks on its own handshake, so wrapping them in goroutines lets a
// 4-bot pool boot in ~handshake-time rather than 4× handshake-time.
// A single bot failure fails the whole boot — partial pools would
// silently degrade write throughput.
//
// Initial auth is staggered (200ms × index) per PHASES.md decision:
// concurrent auth.importBotAuthorization from the same IP trips
// flood control. The stagger is small enough that 4 bots still boot
// in under a second.
func startMTProtoPool(ctx context.Context, cfg config.Config, store *metadata.Store, channelID int64, logger *slog.Logger) (*mtproto.Pool, error) {
	bots := make([]*mtproto.MTProtoBot, len(cfg.TelegramBotTokens))
	errs := make([]error, len(cfg.TelegramBotTokens))
	var wg sync.WaitGroup
	for i, token := range cfg.TelegramBotTokens {
		wg.Add(1)
		go func(i int, token string) {
			defer wg.Done()
			b, err := mtproto.StartBot(ctx, mtproto.BotOptions{
				Index:     i,
				Token:     token,
				AppID:     cfg.TelegramAppID,
				AppHash:   cfg.TelegramAppHash,
				ChannelID: channelID,
				Sessions:  store,
				Logger:    logger,
				AuthDelay: time.Duration(i) * 200 * time.Millisecond,
				PoolSize:  cfg.TelegramPoolSize,
			})
			bots[i] = b
			errs[i] = err
		}(i, token)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			// Close any bots that succeeded so we don't leak goroutines.
			for j, b := range bots {
				if j != i && b != nil {
					b.Close()
				}
			}
			return nil, err
		}
	}
	return mtproto.NewPool(bots)
}
