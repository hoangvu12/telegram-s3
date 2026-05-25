package telegram

import (
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// legacyBotIndex marks every chunk persisted before Phase 3 — backfilled at
// migration time as a one-row chunk pointing at TELEGRAM_BOT_TOKENS[0]. The
// Phase 4 migration sweeper walks transport='bot' rows in age order, so the
// oldest (legacy) ones drain first; until then they keep reading through
// whichever token sits at index 0.
const legacyBotIndex = 0

// BotOp selects which round-robin counter Pick advances. Two counters (per
// teldrive's model) prevent an upload burst from starving range reads.
type BotOp string

const (
	BotOpStream BotOp = "stream"
	BotOpUpload BotOp = "upload"
)

// botClient is one Telegram bot token + the *http.Client used to talk to
// the Bot HTTP API on its behalf. Each bot gets its own client so the
// keepalive pool is sized per-token (Telegram is a single host, but
// concurrent operations through different tokens shouldn't contend for the
// same idle-conn slots).
type botClient struct {
	token  string
	client *http.Client
}

// BotPool round-robins a fixed slice of bots across read/write operations.
// The two counters are independent atomics so per-op fairness holds even
// under bursts (a stream of 10 GETs followed by 10 PUTs returns indices
// [0,1,0,1,...] in each sequence rather than skewing).
type BotPool struct {
	bots      []*botClient
	streamIdx atomic.Int64
	uploadIdx atomic.Int64
}

// NewBotPool builds the pool from a token slice. idlePerHost tunes each
// bot's keepalive pool — Go's default of 2 throttles concurrent fan-out, so
// 32 (Phase 0 default) keeps a multi-bot prefetch fan-out from paying TLS
// handshakes on every chunk. tokens must be non-empty; the caller (config
// load) validates that.
func NewBotPool(tokens []string, idlePerHost int) *BotPool {
	if idlePerHost <= 0 {
		idlePerHost = 32
	}
	bots := make([]*botClient, 0, len(tokens))
	for _, tok := range tokens {
		bots = append(bots, &botClient{token: tok, client: newBotHTTPClient(idlePerHost)})
	}
	return &BotPool{bots: bots}
}

// newBotHTTPClient mirrors the Phase 0 tuned transport: large keepalive
// pool, no client-wide timeout (per-request deadlines flow through ctx).
func newBotHTTPClient(idlePerHost int) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          idlePerHost * 4,
		MaxIdleConnsPerHost:   idlePerHost,
		MaxConnsPerHost:       idlePerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 0}
}

// Pick returns the next bot for op. The returned index is what gets
// persisted in object_chunks.bot_index so the same bot resolves the chunk
// on read; the *botClient is the actual client to use for this request.
func (p *BotPool) Pick(op BotOp) (int, *botClient) {
	var next int64
	switch op {
	case BotOpUpload:
		next = p.uploadIdx.Add(1) - 1
	default:
		next = p.streamIdx.Add(1) - 1
	}
	n := int64(len(p.bots))
	// Modulo a positive count is always non-negative; cast safely.
	i := int(next % n)
	return i, p.bots[i]
}

// At returns the bot at index i, or (i, nil) when i is out of range. The
// nil case is the dispatcher's signal to fall back to round-robin: a row
// stored with bot_index=K on a deploy with K+1 bots becomes unreadable on
// a deploy that shrunk the pool, and silently picking a different bot is
// safer than panicking on a slice-index out of bounds.
func (p *BotPool) At(i int) (int, *botClient) {
	if i < 0 || i >= len(p.bots) {
		return i, nil
	}
	return i, p.bots[i]
}

// Len is the bot count. Callers use it to bound the fallback round-robin
// (try every bot exactly once on download failure).
func (p *BotPool) Len() int { return len(p.bots) }
