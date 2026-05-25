package storage

import (
	"context"
	"fmt"
	"io"
)

// TransportMode is the boot-time policy that controls which transport
// new uploads go through. Existing chunks always read through the
// transport their row declares; this only governs writes (Upload).
//
//   - "bot":     new uploads via Bot HTTP API. Default. MTProto backend
//                is unused (and may be nil) — pure pre-Phase-4 behavior.
//   - "dual":    new uploads via MTProto, but reads/deletes route per
//                chunk's transport (so old 'bot' rows keep working).
//                The migration sweeper drains 'bot' rows in the
//                background.
//   - "mtproto": new uploads via MTProto. Only safe AFTER every
//                transport='bot' row has been migrated; otherwise the
//                Bot HTTP API backend is still needed for legacy reads
//                (the dispatcher keeps routing reads by transport, so
//                a stray 'bot' row reads correctly as long as the bot
//                backend stays wired).
type TransportMode string

const (
	TransportModeBot     TransportMode = "bot"
	TransportModeDual    TransportMode = "dual"
	TransportModeMTProto TransportMode = "mtproto"
)

// Dispatcher routes Backend calls to one of two sub-backends based on
// the ChunkRef's Transport field. It is the *only* Backend the
// handler sees in Phase 4 mode — the existing s3api layer is
// transport-unaware.
//
// Read paths (Download, DownloadRange, Delete, DeleteBatch): route
// per ref. A nil sub-backend for a referenced transport returns an
// error rather than silently dropping; the most common cause would
// be a misconfigured deploy (mtproto chunk rows present but MTProto
// not wired).
//
// Write path (Upload): always goes through the Mode-selected backend.
// "bot" → Bot. "dual" or "mtproto" → MTProto. The Mode is set at
// boot from cfg.TelegramTransport and never changes per-request.
type Dispatcher struct {
	Mode    TransportMode
	Bot     Backend // always set; Phase 0..3 fallback path
	MTProto Backend // nil when Mode == "bot"
}

// NewDispatcher constructs a routing Backend. mtproto may be nil when
// mode == "bot" — the dispatcher will refuse any 'mtproto' ref in
// that case, which is the right failure mode for a deploy that
// somehow ended up with mtproto rows but reverted to the bot-only
// binary.
func NewDispatcher(mode TransportMode, bot, mtproto Backend) (*Dispatcher, error) {
	if bot == nil {
		return nil, fmt.Errorf("storage: dispatcher needs a Bot backend even in mode=%q", mode)
	}
	switch mode {
	case TransportModeBot:
		// mtproto optional
	case TransportModeDual, TransportModeMTProto:
		if mtproto == nil {
			return nil, fmt.Errorf("storage: dispatcher mode=%q requires an MTProto backend", mode)
		}
	default:
		return nil, fmt.Errorf("storage: dispatcher unknown mode %q", mode)
	}
	return &Dispatcher{Mode: mode, Bot: bot, MTProto: mtproto}, nil
}

// Upload routes the new chunk to the Mode-selected backend. The
// chunks returned have their Transport field set by the backend; the
// dispatcher does no rewriting here (a misbehaving backend that
// returns the wrong transport would surface as routing errors later,
// which is preferable to silent dispatcher fixups).
func (d *Dispatcher) Upload(ctx context.Context, name, contentType string, body io.Reader) ([]Chunk, error) {
	if d.Mode == TransportModeBot {
		return d.Bot.Upload(ctx, name, contentType, body)
	}
	return d.MTProto.Upload(ctx, name, contentType, body)
}

// Download routes by ref.Transport. Empty transport (legacy rows from
// a pre-Phase-3 DB that somehow escaped the backfill) is treated as
// 'bot' to match Chunk.Ref()'s normalization.
func (d *Dispatcher) Download(ctx context.Context, ref ChunkRef) (io.ReadCloser, error) {
	b, err := d.pick(ref.Transport)
	if err != nil {
		return nil, err
	}
	return b.Download(ctx, ref)
}

// DownloadRange routes by ref.Transport — same rules as Download.
func (d *Dispatcher) DownloadRange(ctx context.Context, ref ChunkRef, offset, length int64) (io.ReadCloser, error) {
	b, err := d.pick(ref.Transport)
	if err != nil {
		return nil, err
	}
	return b.DownloadRange(ctx, ref, offset, length)
}

// Delete routes by ref.Transport.
func (d *Dispatcher) Delete(ctx context.Context, ref ChunkRef) error {
	b, err := d.pick(ref.Transport)
	if err != nil {
		return err
	}
	return b.Delete(ctx, ref)
}

// DeleteBatch groups refs by transport and forwards each subgroup to
// its sub-backend. Returns the first error encountered, mirroring
// the per-backend DeleteBatch contract.
//
// The grouping preserves per-backend batching (MTProto's
// channels.deleteMessages at 100/call) — a mixed-transport ref list
// would otherwise trigger 1 RPC per ref via the single-ref Delete
// fallback, which is what Phase 2's DeleteBatch was meant to avoid.
func (d *Dispatcher) DeleteBatch(ctx context.Context, refs []ChunkRef) error {
	if len(refs) == 0 {
		return nil
	}
	var botRefs, mtRefs []ChunkRef
	for _, r := range refs {
		t := r.Transport
		if t == "" {
			t = TransportBot
		}
		switch t {
		case TransportBot:
			botRefs = append(botRefs, r)
		case TransportMTProto:
			mtRefs = append(mtRefs, r)
		default:
			return fmt.Errorf("dispatcher: unknown transport %q in DeleteBatch ref", t)
		}
	}
	var firstErr error
	if len(botRefs) > 0 {
		if err := d.Bot.DeleteBatch(ctx, botRefs); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if len(mtRefs) > 0 {
		if d.MTProto == nil {
			err := fmt.Errorf("dispatcher: mode=%q cannot delete mtproto refs (no MTProto backend wired)", d.Mode)
			if firstErr == nil {
				firstErr = err
			}
		} else if err := d.MTProto.DeleteBatch(ctx, mtRefs); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (d *Dispatcher) pick(transport string) (Backend, error) {
	if transport == "" || transport == TransportBot {
		return d.Bot, nil
	}
	if transport == TransportMTProto {
		if d.MTProto == nil {
			return nil, fmt.Errorf("dispatcher: mode=%q has no MTProto backend wired", d.Mode)
		}
		return d.MTProto, nil
	}
	return nil, fmt.Errorf("dispatcher: unknown transport %q", transport)
}
