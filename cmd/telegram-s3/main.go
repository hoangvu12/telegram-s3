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

	"telegram-s3/internal/cache"
	"telegram-s3/internal/config"
	"telegram-s3/internal/metadata"
	"telegram-s3/internal/migrate"
	"telegram-s3/internal/s3api"
	"telegram-s3/internal/storage"
	"telegram-s3/internal/storage/telegram"
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

	// Bot API file_path cache: getFile becomes a per-fileID one-shot
	// instead of per-chunk-read. Generic by shape so Phase 4 reuses
	// the type for (messageID, botIndex) → InputDocumentFileLocation.
	pathCache := cache.New[string, string](cfg.LocationCacheTTL, 0)
	defer pathCache.Close()

	botBackend := telegram.NewBotStorageWithOptions(
		cfg.TelegramBotTokens,
		cfg.TelegramChatID,
		cfg.TelegramAPIBaseURL,
		cfg.HTTPMaxIdleConnsPerHost,
		int(cfg.TelegramMaxChunkSize),
		pathCache,
		logger,
	)

	// Phase 4 — optionally bring up MTProto. Mode "bot" (the default)
	// keeps the binary's behavior identical to pre-Phase-4: no gotd
	// boot, no extra connections, no app creds required. Modes "dual"
	// and "mtproto" require app credentials + a successful handshake
	// for every bot in the pool before serving.
	var (
		mtStorage   *mtproto.Storage
		mtPool      *mtproto.Pool
		sweeper     *migrate.Sweeper
		dispatcher  *storage.Dispatcher
		mtprotoMode = storage.TransportMode(cfg.TelegramTransport)
	)

	if mtprotoMode != storage.TransportModeBot {
		channelID, err := mtproto.ParseChannelID(cfg.TelegramChatID)
		if err != nil {
			logger.Error("parse channel id for mtproto", "error", err)
			os.Exit(1)
		}
		mtPool, err = startMTProtoPool(context.Background(), cfg, store, channelID, logger)
		if err != nil {
			logger.Error("start mtproto pool", "error", err)
			os.Exit(1)
		}
		mtStorage, err = mtproto.NewStorage(mtproto.Options{
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
	}

	dispatcher, err = storage.NewDispatcher(mtprotoMode, botBackend, mtStorage)
	if err != nil {
		if mtPool != nil {
			mtPool.Close()
		}
		logger.Error("init dispatcher", "error", err)
		os.Exit(1)
	}

	handler := s3api.NewHandler(cfg, store, dispatcher, logger)

	// Abandoned-multipart janitor (P8.6). Skipped if the sweep is disabled
	// (interval <= 0). Stops with the server on SIGINT/SIGTERM.
	janitorCtx, cancelJanitor := context.WithCancel(context.Background())
	defer cancelJanitor()
	if cfg.MultipartSweepInterval > 0 {
		go handler.RunMultipartJanitor(janitorCtx, cfg.MultipartSweepInterval, cfg.MultipartTTL)
	}

	// Phase 4 sweeper — dual mode runs the background migration that
	// drains transport='bot' rows. mtproto-only mode keeps the sweeper
	// alive too: any pending_delete rows that pass-1 wrote in a prior
	// dual-mode deploy still need to drain through pass-2.
	if mtStorage != nil {
		sw, err := migrate.NewSweeper(migrate.Options{
			Store:          store,
			Bot:            botBackend,
			MTProto:        mtStorage,
			MigrationRate:  pickMigrationRate(cfg),
			BotDeleteGrace: cfg.BotDeleteGrace,
			Logger:         logger,
		})
		if err != nil {
			logger.Error("init sweeper", "error", err)
			os.Exit(1)
		}
		sweeper = sw
		go sweeper.Run(janitorCtx)
	}

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("server listening",
			"addr", cfg.ListenAddr,
			"transport", cfg.TelegramTransport,
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
	if mtPool != nil {
		mtPool.Close()
	}
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

// pickMigrationRate gives the sweeper its rate budget. The user may
// set 0 to disable pass-1 (e.g. during a deploy where they want to
// pause migration to investigate a failure pattern), in which case
// pass-2 still runs to drain any pending_delete rows pass-1 wrote
// before the pause.
func pickMigrationRate(cfg config.Config) int {
	if cfg.MigrationRate < 0 {
		return 0
	}
	return cfg.MigrationRate
}
