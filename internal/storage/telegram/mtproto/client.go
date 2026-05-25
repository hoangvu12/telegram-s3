package mtproto

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"

	"telegram-s3/internal/metadata"
	parent "telegram-s3/internal/storage/telegram"
)

// supergroupPrefix is the Bot-API encoding for channel chat IDs:
// -100 followed by the underlying channelID. MTProto methods (the
// channels.* namespace) want the raw channelID, so the gateway has
// to peel the prefix once at boot. Basic groups (-G) and DMs (+U)
// are not supported transports.
const supergroupPrefix = -1_000_000_000_000

// ParseChannelID converts a Bot-API style chat_id like "-1001234567890"
// to the raw channel_id (1234567890) that channels.* requires. A
// non-supergroup chat ID is rejected up front — the gateway has only
// ever been deployed against a supergroup channel and silently
// routing a basic-group ID would surface as an opaque CHANNEL_INVALID
// later.
func ParseChannelID(chatID string) (int64, error) {
	n, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse TELEGRAM_CHAT_ID %q: %w", chatID, err)
	}
	if n >= supergroupPrefix { // not strictly < the prefix
		return 0, fmt.Errorf("TELEGRAM_CHAT_ID %d is not a supergroup/channel (need -100...)", n)
	}
	return -n + supergroupPrefix, nil
}

// BotOptions wires one MTProto bot into StartBot. All bots in the
// pool share AppID/AppHash (the app identity from my.telegram.org);
// each bot has its own Token (from @BotFather). The Sessions store
// is the gateway's SQLite metadata.Store — gotd uses it through the
// session.Storage adapter built by NewSessionStorage.
type BotOptions struct {
	Index     int    // 0-based; matches BotIndex written to chunks (so writes/reads target the same bot)
	Token     string // bot token from @BotFather
	AppID     int
	AppHash   string
	ChannelID int64 // raw channel ID, derived via ParseChannelID
	Sessions  *metadata.Store
	Logger    *slog.Logger
	// AuthDelay staggers the initial auth.importBotAuthorization call
	// across bots on cold boot. Telegram throttles concurrent bot
	// auths from the same IP; 200ms × index is empirically safe.
	AuthDelay time.Duration
	// PoolSize sets the gotd per-DC session-pool capacity for the bot's
	// hot-path RPCs (download fetchRange, upload uploadChunk). 0 falls
	// back to 1 (single-session, pre-pool behavior). The pool is shared
	// per bot — N bots × PoolSize sessions total. Sized too high will
	// trip Telegram's per-key connection caps; 4 is the empirical
	// sweet spot for parallel-prefetch reads on a 1 GiB object.
	PoolSize int
}

// MTProtoBot is one authenticated gotd client + its cached channel
// resolver. The Run goroutine stays alive until Close. Concurrent
// callers may use the *tg.Client returned by API() freely; gotd
// multiplexes requests over its internal connection pool.
//
// sessions wraps an additional gotd Pool that multiplexes a single
// bot's RPCs across many MTProto sessions — the parallel-prefetch
// reader's STREAM_CONCURRENCY would otherwise serialize through
// gotd's one default session. Built once during StartBot after auth;
// hot-path callers go through Session(ctx) instead of API().
type MTProtoBot struct {
	index    int
	api      *tg.Client
	client   *telegram.Client
	channel  *tg.InputChannel
	sessions *sessionPool
	cancel   context.CancelFunc
	done     chan struct{}
	runErr   atomic.Pointer[error]
	logger   *slog.Logger
}

// Index returns the bot's pool index — the same value that gets
// persisted in Chunk.BotIndex when this bot uploads a chunk.
func (b *MTProtoBot) Index() int { return b.index }

// API exposes the raw *tg.Client tied to gotd's single default
// session. Safe for concurrent use, but RPCs serialize through one
// MTProto session. Prefer Session(ctx) for hot-path calls (parallel
// range reads / parallel chunk uploads); keep API() for one-shot
// calls (delete) and tests that pre-date the session pool.
func (b *MTProtoBot) API() *tg.Client { return b.api }

// Session returns a *tg.Client whose RPCs flow through the bot's
// session pool, multiplexing up to PoolSize concurrent invocations
// across distinct MTProto sessions. Falls back to API() if the pool
// wasn't built (PoolSize <= 1 or a pool-init failure logged during
// StartBot). Safe for concurrent use; callers don't need to cache
// the result.
func (b *MTProtoBot) Session(ctx context.Context) *tg.Client {
	if b.sessions == nil {
		return b.api
	}
	return b.sessions.Client(ctx)
}

// Channel returns the cached *tg.InputChannel resolved at boot.
// Embedding the access hash in the cached value keeps the hot path
// (Upload / DownloadRange / Delete) free of per-call resolves.
func (b *MTProtoBot) Channel() *tg.InputChannel { return b.channel }

// Err returns the run-goroutine's exit error if it has died, nil
// otherwise. Long-running loops (sweeper, handler) should check this
// before treating a per-call failure as user error — a dead bot needs
// reboot, not retry.
func (b *MTProtoBot) Err() error {
	if e := b.runErr.Load(); e != nil {
		return *e
	}
	return nil
}

// Close cancels the run goroutine and waits for it to exit. The
// session pool's connections are released first so the gotd Run loop
// doesn't tear them down mid-RPC. Idempotent.
func (b *MTProtoBot) Close() {
	if b.sessions != nil {
		if err := b.sessions.Close(); err != nil && b.logger != nil {
			b.logger.Warn("mtproto session pool close failed", "index", b.index, "error", err)
		}
	}
	if b.cancel != nil {
		b.cancel()
	}
	if b.done != nil {
		<-b.done
	}
}

// StartBot brings up a single MTProto bot: builds the client, runs
// the auth + channel-resolve handshake in a background goroutine, and
// blocks until either the handshake succeeds (returning a ready bot)
// or fails (returning the error). The bot's API() is safe to use as
// soon as StartBot returns nil.
//
// The run goroutine outlives StartBot — it keeps the MTProto
// connection pool alive until ctx (or Close) cancels it. A late
// connection drop sets runErr; the next API call will see whatever
// gotd surfaces (typically a reconnect-in-progress error).
func StartBot(ctx context.Context, opts BotOptions) (*MTProtoBot, error) {
	if opts.Sessions == nil {
		return nil, errors.New("mtproto: BotOptions.Sessions is nil")
	}
	if opts.Token == "" {
		return nil, fmt.Errorf("mtproto: bot %d token is empty", opts.Index)
	}
	if opts.AppID <= 0 || opts.AppHash == "" {
		return nil, fmt.Errorf("mtproto: bot %d missing AppID/AppHash", opts.Index)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	sessionKey := fmt.Sprintf("bot:%d", opts.Index)
	storage := NewSessionStorage(opts.Sessions, sessionKey)

	// FLOOD_WAIT — the SimpleWaiter variant is stateless (no .Run
	// loop), so the bot's run goroutine stays as simple as the docs'
	// "auth + wait" pattern. Rate limit per teldrive: 5 calls per
	// 100ms burst. Both apply on top of every RPC the *tg.Client
	// issues, so the upload/download paths get them for free.
	mws := []telegram.Middleware{
		floodwait.NewSimpleWaiter(),
		ratelimit.New(rate.Every(100*time.Millisecond), 5),
	}

	client := telegram.NewClient(opts.AppID, opts.AppHash, telegram.Options{
		Logger:         NewZapLogger(opts.Logger.With("component", "mtproto", "bot", opts.Index)),
		SessionStorage: storage,
		Middlewares:    mws,
	})

	bot := &MTProtoBot{
		index:  opts.Index,
		client: client,
		done:   make(chan struct{}),
		logger: opts.Logger,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	bot.cancel = cancel

	// ready signals StartBot the handshake finished (success or fail).
	// The run goroutine sends one value: nil on success (caller proceeds),
	// or the error on failure (caller returns it). The channel is then
	// closed so an accidental second send doesn't block the goroutine.
	ready := make(chan error, 1)

	// readyOnce gates the ready send so handshake errors and steady-state
	// errors don't both try to publish. StartBot consumes ready exactly
	// once; subsequent errors flow into runErr.
	var readyOnce sync.Once

	go func() {
		defer close(bot.done)

		// Stagger initial auth — Telegram penalizes concurrent
		// auth.importBotAuthorization from the same IP. Skip for
		// AuthDelay == 0 (single-bot deploys / tests).
		if opts.AuthDelay > 0 {
			select {
			case <-runCtx.Done():
				readyOnce.Do(func() { ready <- runCtx.Err() })
				return
			case <-time.After(opts.AuthDelay):
			}
		}

		err := client.Run(runCtx, func(ctx context.Context) error {
			if _, authErr := client.Auth().Bot(ctx, opts.Token); authErr != nil {
				return fmt.Errorf("bot %d auth: %w", opts.Index, authErr)
			}
			api := client.API()
			bot.api = api

			ch, resolveErr := resolveChannel(ctx, api, opts.ChannelID)
			if resolveErr != nil {
				return fmt.Errorf("bot %d resolve channel: %w", opts.Index, resolveErr)
			}
			bot.channel = ch

			// Auth done — the client is past Run handshake, so
			// Config().ThisDC is valid and the session pool can be
			// built. Construction itself doesn't open connections
			// (gotd lazy-opens on first Client(ctx) call); we just
			// register the size + middleware chain here so the
			// readers/uploaders can see a non-nil bot.sessions
			// immediately after ready.
			poolSize := opts.PoolSize
			if poolSize < 1 {
				poolSize = 1
			}
			bot.sessions = newSessionPool(client, int64(poolSize), opts.Logger.With("component", "mtproto-pool", "bot", opts.Index), mws...)

			opts.Logger.Info("mtproto bot ready",
				"index", opts.Index,
				"channel_id", opts.ChannelID,
				"access_hash", ch.AccessHash,
				"pool_size", poolSize)
			readyOnce.Do(func() { ready <- nil })

			// Block until the parent cancels — that keeps the
			// MTProto connection pool alive across requests.
			<-ctx.Done()
			return ctx.Err()
		})

		// If we got here before ready was published, bubble up
		// the boot error rather than swallowing it as runErr —
		// StartBot waits on ready and would otherwise deadlock.
		readyOnce.Do(func() { ready <- err })
		if err != nil && !errors.Is(err, context.Canceled) {
			e := err
			bot.runErr.Store(&e)
			opts.Logger.Error("mtproto bot run exited", "index", opts.Index, "error", err)
		}
	}()

	// Wait for handshake. If the caller's ctx fires first (e.g. boot
	// timeout), cancel the bot and surface that error.
	select {
	case err := <-ready:
		if err != nil {
			bot.Close()
			return nil, err
		}
		return bot, nil
	case <-ctx.Done():
		bot.Close()
		return nil, ctx.Err()
	}
}

// resolveChannel asks the bot's API for the channel's current access
// hash. Bots are allowed to call channels.getChannels with
// AccessHash=0 for channels they're a member of — the server validates
// by membership and returns the real hash. If the bot isn't a member,
// the call fails with CHANNEL_INVALID / CHANNEL_PRIVATE.
//
// We cache the resulting *tg.InputChannel once per bot at boot. The
// access hash is stable for the bot's lifetime in the channel (it
// changes only when the bot is kicked + re-added), so a single resolve
// covers every Upload/Download/Delete the bot will ever do.
func resolveChannel(ctx context.Context, api *tg.Client, channelID int64) (*tg.InputChannel, error) {
	res, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: channelID, AccessHash: 0},
	})
	if err != nil {
		return nil, err
	}
	for _, c := range res.GetChats() {
		if ch, ok := c.(*tg.Channel); ok && ch.ID == channelID {
			return &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}, nil
		}
	}
	return nil, fmt.Errorf("channel %d not in bot's reachable set", channelID)
}

// --- Pool ------------------------------------------------------------------

// Pool round-robins MTProto bots the same way parent.BotPool does for
// Bot API tokens — two independent counters (stream/upload) so a burst
// of one op type doesn't starve the other. Pick's returned index is
// what gets persisted in Chunk.BotIndex; later reads target the same
// bot via At so the dispatcher can pin reads to the writer (MTProto's
// access hashes are bot-agnostic, but pinning keeps debugging sane and
// matches the BotStorage contract).
type Pool struct {
	bots      []*MTProtoBot
	streamIdx atomic.Int64
	uploadIdx atomic.Int64
}

// NewPool wraps the given bots without taking ownership of their
// lifecycles — the caller is responsible for calling each bot's
// Close. Empty pools are rejected (Pick would divide by zero).
func NewPool(bots []*MTProtoBot) (*Pool, error) {
	if len(bots) == 0 {
		return nil, errors.New("mtproto: empty bot pool")
	}
	return &Pool{bots: bots}, nil
}

// Pick advances the appropriate counter and returns the next bot
// + its index. Mirror of parent.BotPool.Pick.
func (p *Pool) Pick(op parent.BotOp) (int, *MTProtoBot) {
	var next int64
	switch op {
	case parent.BotOpUpload:
		next = p.uploadIdx.Add(1) - 1
	default:
		next = p.streamIdx.Add(1) - 1
	}
	i := int(next % int64(len(p.bots)))
	return i, p.bots[i]
}

// At returns the bot at index i, or nil for out-of-range. The
// download path uses this to pin a read to the writer; a missing
// bot is the dispatcher's signal to fall back to round-robin (under
// MTProto the message ID is bot-agnostic so any pool member can serve).
func (p *Pool) At(i int) *MTProtoBot {
	if i < 0 || i >= len(p.bots) {
		return nil
	}
	return p.bots[i]
}

// Len returns the bot count.
func (p *Pool) Len() int { return len(p.bots) }

// Close shuts down every bot in the pool concurrently. Safe to call
// multiple times — each bot's Close is idempotent.
func (p *Pool) Close() {
	var wg sync.WaitGroup
	for _, b := range p.bots {
		wg.Add(1)
		go func(b *MTProtoBot) { defer wg.Done(); b.Close() }(b)
	}
	wg.Wait()
}
