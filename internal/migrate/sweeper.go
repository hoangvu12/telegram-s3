// Package migrate runs the opportunistic background re-upload that
// drains transport='bot' chunks to MTProto during Phase 4's "dual"
// transport mode. Two passes: pass-1 reads a bot chunk + re-uploads
// via MTProto + atomically swaps the chunk row to mtproto + enqueues
// the old (message_id, bot_index) in bot_chunks_pending_delete;
// pass-2 reaps pending_delete rows after cfg.BotDeleteGrace. The
// split exists because a reader that fetched the chunk map just
// before the swap holds a stale 'bot' ref by value — deleting the
// bot message immediately would 404 it permanently. See PHASES.md
// design decision #14.
package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"telegram-s3/internal/metadata"
	"telegram-s3/internal/storage"
)

// Sweeper migrates Bot HTTP API chunks to MTProto in the background.
// One Sweeper per gateway; Run blocks until ctx is cancelled. The
// runtime cost is bounded by cfg.MigrationRate (pass-1 rows/day) +
// the pending_delete drain rate (pass-2 runs once per cycle and
// catches any rows past the grace window).
type Sweeper struct {
	store *metadata.Store
	bot   storage.Backend
	mt    storage.Backend
	// rate is the pass-1 budget in rows per day. The interval between
	// pass-1 ticks is max(15m, 24h/rate) so a slow rate still issues
	// regular pings (useful for observability) without hammering the
	// DB. 0 disables pass-1 entirely.
	rate int
	// grace is how long a pending_delete row sits before pass-2 reaps
	// it. 0 (test-only) reaps inline.
	grace time.Duration
	// pass2Budget caps the per-cycle pass-2 work — a backlog still
	// drains over multiple ticks rather than spiking RPC count.
	pass2Budget int
	logger      *slog.Logger
	now         func() time.Time // injectable clock for tests
}

// Options is the Sweeper constructor input. MigrationRate / Grace
// come straight from config; Store / Bot / MTProto are the same
// instances the handler already holds. The MT backend is required —
// without it pass-1 has nowhere to upload to.
type Options struct {
	Store          *metadata.Store
	Bot            storage.Backend
	MTProto        storage.Backend
	MigrationRate  int           // rows/day; 0 disables pass-1
	BotDeleteGrace time.Duration // pass-2 lag; 0 reaps inline (tests only)
	Pass2Budget    int           // per-cycle reap cap; defaults to 4× pass-1 cap (catches backlog faster than pass-1 fills it)
	Logger         *slog.Logger
	Now            func() time.Time // optional; defaults to time.Now
}

// NewSweeper validates options and constructs a Sweeper. Returns
// nil sweeper + nil error when MigrationRate == 0 AND no pending
// rows exist — saves the operator from running an idle goroutine
// when migration is disabled. In all other cases a Sweeper is
// returned (pass-2 still needs to run for any backlog that pass-1
// produced in a previous deploy).
func NewSweeper(opts Options) (*Sweeper, error) {
	if opts.Store == nil {
		return nil, errors.New("migrate: Store is required")
	}
	if opts.Bot == nil || opts.MTProto == nil {
		return nil, errors.New("migrate: Bot and MTProto backends are required")
	}
	pass2 := opts.Pass2Budget
	if pass2 <= 0 {
		pass2 = max(4*opts.MigrationRate/24, 50) // catch backlogs faster than pass-1 fills
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Sweeper{
		store:       opts.Store,
		bot:         opts.Bot,
		mt:          opts.MTProto,
		rate:        opts.MigrationRate,
		grace:       opts.BotDeleteGrace,
		pass2Budget: pass2,
		logger:      logger,
		now:         now,
	}, nil
}

// Run loops on a ticker until ctx is cancelled. Each tick: pass-1
// migrates up to `rate/24` rows (one hour's worth), then pass-2
// reaps pending_delete rows older than the grace window. Errors
// inside a pass are logged and don't abort the loop — a single bad
// chunk shouldn't stop the migration.
//
// A drain snapshot is logged at startup and once per tick so an
// operator can read remaining-bot-chunks / pending-deletes / latest-
// swap progress directly from `journalctl` (or Easypanel's log
// tail) without needing container-shell access to the DB. The
// startup line in particular makes every restart a free measurement.
func (s *Sweeper) Run(ctx context.Context) {
	if s == nil {
		return
	}
	interval := s.tickInterval()
	t := time.NewTicker(interval)
	defer t.Stop()
	s.logger.Info("migration sweeper started", "rate_per_day", s.rate, "grace", s.grace, "tick_interval", interval)
	s.logDrainSnapshot(ctx)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("migration sweeper exiting")
			return
		case <-t.C:
			s.runOnce(ctx)
			s.logDrainSnapshot(ctx)
		}
	}
}

// logDrainSnapshot fetches a BotMigrationSnapshot and emits it at INFO.
// Errors are logged at WARN and don't propagate — a transient SQLite
// read failure shouldn't taint the sweeper's tick. The "since_latest_swap"
// field is derived here rather than in the store so the value reflects
// the same clock the rest of the sweeper uses (s.now); test code that
// freezes the clock sees consistent timestamps.
func (s *Sweeper) logDrainSnapshot(ctx context.Context) {
	snap, err := s.store.BotMigrationSnapshot(ctx)
	if err != nil {
		s.logger.Warn("drain snapshot failed", "error", err)
		return
	}
	if snap.LatestSwap.IsZero() {
		s.logger.Info("drain snapshot",
			"bot_chunks_remaining", snap.BotChunksRemaining,
			"pending_deletes", snap.PendingDeletes)
		return
	}
	s.logger.Info("drain snapshot",
		"bot_chunks_remaining", snap.BotChunksRemaining,
		"pending_deletes", snap.PendingDeletes,
		"latest_swap", snap.LatestSwap.UTC().Format(time.RFC3339),
		"since_latest_swap", s.now().UTC().Sub(snap.LatestSwap.UTC()).Round(time.Second))
}

// tickInterval is 1 hour by default — that converts `rate` into an
// "rows per tick" budget directly. A 0 rate still ticks (for pass-2
// drain) but at a slower 1h cadence so the sweeper is observable
// in logs.
func (s *Sweeper) tickInterval() time.Duration {
	if s.rate <= 0 {
		return time.Hour
	}
	d := 24 * time.Hour / time.Duration(s.rate)
	if d < time.Minute {
		d = time.Minute
	}
	if d > time.Hour {
		d = time.Hour
	}
	return d
}

// runOnce executes one tick of both passes. Public-via-package so
// tests can step the sweeper deterministically without sleeping
// through real durations.
func (s *Sweeper) runOnce(ctx context.Context) {
	if s.rate > 0 {
		s.pass1Migrate(ctx, s.perTickBudget())
	}
	s.pass2Reap(ctx, s.pass2Budget)
}

func (s *Sweeper) perTickBudget() int {
	if s.rate <= 0 {
		return 0
	}
	// rate per day → per tick. We use ceil-ish so a tiny rate (e.g. 12/day
	// with 2h ticks) doesn't round to 0 rows/tick.
	per := int(time.Duration(s.rate) * s.tickInterval() / (24 * time.Hour))
	if per < 1 {
		per = 1
	}
	return per
}

// pass1Migrate downloads the oldest N bot chunks, re-uploads each
// through MTProto, and atomically swaps the row + enqueues the old
// bot message for pass-2 reap. A single chunk failure is logged and
// skipped — the next tick will pick it up again.
func (s *Sweeper) pass1Migrate(ctx context.Context, limit int) {
	if limit <= 0 {
		return
	}
	chunks, err := s.store.ListBotChunksOldestFirst(ctx, limit)
	if err != nil {
		s.logger.Error("migrate pass-1 list failed", "error", err)
		return
	}
	if len(chunks) == 0 {
		return
	}
	for _, c := range chunks {
		if ctx.Err() != nil {
			return
		}
		if err := s.migrateOne(ctx, c); err != nil {
			s.logger.Warn("migrate pass-1 chunk failed",
				"bucket", c.Bucket, "key", c.Key, "seq", c.PartSeq, "error", err)
		}
	}
}

// migrateOne is the per-chunk pass-1 sequence:
//  1. Read full chunk bytes via the Bot backend.
//  2. Upload through MTProto (always produces exactly one chunk: the
//     MTProto upload chunk size is >= bot chunk size, so a single
//     bot chunk → single mtproto document).
//  3. Atomic swap (row UPDATE + pending_delete INSERT in one tx).
//
// If pass-1 succeeds but the bot delete (pass-2 path) fails later,
// the duplicate bot message persists harmlessly. If pass-1 swaps the
// row but never enqueues — impossible, both writes share a tx — the
// bot message would leak. Don't tear this apart.
func (s *Sweeper) migrateOne(ctx context.Context, c metadata.BotChunkLoc) error {
	ref := storage.ChunkRef{
		Transport: storage.TransportBot,
		BotFileID: c.FileID,
		MessageID: c.MessageID,
		BotIndex:  c.BotIndex,
	}
	rc, err := s.bot.DownloadRange(ctx, ref, 0, 0)
	if err != nil {
		return fmt.Errorf("bot download: %w", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("read chunk body: %w", err)
	}
	// Re-upload via MTProto. Use a traceable synthetic filename so a
	// Telegram-client browser of the channel can connect the message
	// back to its bucket/key — object_chunks doesn't persist the
	// original content-type or filename, so we synthesize one.
	name := migratedName(c)
	newChunks, err := s.mt.Upload(ctx, name, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("mtproto upload: %w", err)
	}
	if len(newChunks) != 1 {
		// The bot chunk is <= mtproto upload chunk size, so a single chunk
		// in must produce a single chunk out. More chunks would mean we
		// either need a multi-row swap (currently unimplemented) or our
		// chunk-size invariant is broken. Surface so we notice.
		s.cleanupNewChunks(ctx, newChunks)
		return errors.New("mtproto upload produced unexpected chunk count")
	}
	nc := newChunks[0]
	if err := s.store.SwapBotChunkToMtproto(ctx, c.Bucket, c.Key, c.PartSeq,
		c.MessageID, c.BotIndex, nc.MessageID, nc.BotIndex, s.now().UTC()); err != nil {
		// Swap failed — clean up the just-uploaded MTProto message so we
		// don't leak it. The bot chunk is still authoritative for reads.
		s.cleanupNewChunks(ctx, newChunks)
		return fmt.Errorf("swap row: %w", err)
	}
	s.logger.Info("migrated chunk to mtproto",
		"bucket", c.Bucket, "key", c.Key, "seq", c.PartSeq,
		"old_msg", c.MessageID, "old_bot", c.BotIndex,
		"new_msg", nc.MessageID, "new_bot", nc.BotIndex,
		"size", c.Size)

	// Inline-reap path: grace == 0 (tests only) skips pass-2 and deletes
	// the bot message now. Production deploys always set grace > 0.
	if s.grace == 0 {
		if err := s.reapOne(ctx, c.MessageID, c.BotIndex); err != nil {
			s.logger.Warn("inline reap failed", "msg", c.MessageID, "bot", c.BotIndex, "error", err)
		}
	}
	return nil
}

// pass2Reap drains pending_delete rows whose swapped_at is older
// than the grace window. Each successful bot delete removes the
// pending row; failures leave it for the next tick.
func (s *Sweeper) pass2Reap(ctx context.Context, limit int) {
	if limit <= 0 {
		return
	}
	before := s.now().UTC().Add(-s.grace)
	rows, err := s.store.PendingDeletesOlderThan(ctx, before, limit)
	if err != nil {
		s.logger.Error("migrate pass-2 list failed", "error", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	for _, r := range rows {
		if ctx.Err() != nil {
			return
		}
		if err := s.reapOne(ctx, r.MessageID, r.BotIndex); err != nil {
			s.logger.Warn("migrate pass-2 reap failed",
				"msg", r.MessageID, "bot", r.BotIndex, "error", err)
		}
	}
}

// reapOne deletes the legacy bot-API-uploaded message via MTProto and
// drops the pending_delete row. The TransportMTProto tag means "use
// the MTProto delete path"; the underlying message ID was assigned
// when Bot API uploaded the chunk, but message IDs are channel-scoped
// (not transport-scoped) so MTProto can target it.
//
// Operational note (2026-05-26 production incident): Telegram refuses
// to delete sufficiently-old bot-authored messages over MTProto with
// MESSAGE_DELETE_FORBIDDEN, even when the bot has the can_delete_messages
// admin right server-side (verified via channels.getParticipant). The
// Bot HTTP API has the same constraint and surfaces it as
// "Bad Request: message can't be deleted" past 48h. Telegram exposes
// no admin override that bypasses this for bot identities. Production
// runs with BOT_DELETE_GRACE=999h to make pass-2 effectively dormant
// — legacy bot messages linger in the channel as inaccessible-by-S3
// zombies, which is harmless (chunk map points to the new mtproto
// transport, reads work). reapOne stays MTProto-routed so when grace
// is small enough that this CAN succeed (e.g., during fresh deploys
// where pass-1 just swapped) the path works without code change.
//
// Idempotent: a message that's already gone returns 0 affected and
// no error.
func (s *Sweeper) reapOne(ctx context.Context, msgID int64, botIndex int) error {
	if err := s.mt.Delete(ctx, storage.ChunkRef{
		Transport: storage.TransportMTProto,
		MessageID: msgID,
		BotIndex:  botIndex,
	}); err != nil {
		return fmt.Errorf("mtproto delete: %w", err)
	}
	if err := s.store.DeletePendingDelete(ctx, msgID, botIndex); err != nil {
		return fmt.Errorf("drop pending row: %w", err)
	}
	return nil
}

func (s *Sweeper) cleanupNewChunks(ctx context.Context, chunks []storage.Chunk) {
	if len(chunks) == 0 {
		return
	}
	refs := make([]storage.ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = storage.ChunkRef{
			Transport: storage.TransportMTProto,
			MessageID: c.MessageID,
			BotIndex:  c.BotIndex,
		}
	}
	if err := s.mt.DeleteBatch(ctx, refs); err != nil {
		s.logger.Warn("post-failure mtproto cleanup failed", "count", len(refs), "error", err)
	}
}

// migratedName builds a human-readable filename for the re-uploaded
// chunk so a Telegram-client browser of the channel can tell what
// the message belongs to. Slashes are replaced with underscores
// because Telegram's UI doesn't render paths.
func migratedName(c metadata.BotChunkLoc) string {
	safe := c.Bucket + "_" + c.Key
	// Strip path separators that would look odd in the file panel.
	out := make([]byte, 0, len(safe))
	for i := 0; i < len(safe); i++ {
		ch := safe[i]
		if ch == '/' || ch == '\\' {
			ch = '_'
		}
		out = append(out, ch)
	}
	if c.PartSeq > 0 {
		return string(out) + ".part" + strconv.Itoa(c.PartSeq)
	}
	return string(out)
}
