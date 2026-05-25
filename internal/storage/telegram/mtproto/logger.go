// Package mtproto implements the Telegram MTProto Backend (Phase 4). It
// runs alongside BotStorage during the dual-transport migration window
// and eventually replaces it for all new uploads. The package is split
// across logger.go (zap→slog bridge so gotd logs land in the gateway's
// slog handler), session.go (SQLite-backed session.Storage), client.go
// (one MTProtoBot per token, lifecycle + cached *tg.InputChannel),
// upload.go / download.go / delete.go (Backend impls).
package mtproto

import (
	"context"
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewZapLogger builds a *zap.Logger that emits through the supplied
// slog.Logger. gotd's API insists on *zap.Logger throughout, but the
// rest of the gateway is slog-native — this adapter keeps the boot
// log handler the single source of truth for level / formatting.
//
// A nil slog logger produces a zap.NewNop (silent) so tests that
// don't care about logs stay quiet without ceremony.
func NewZapLogger(s *slog.Logger) *zap.Logger {
	if s == nil {
		return zap.NewNop()
	}
	return zap.New(&slogCore{
		LevelEnabler: zapcore.DebugLevel, // slog enforces its own level filter
		logger:       s,
	})
}

// slogCore routes every zap entry through slog. Levels map directly
// (zap.Debug→slog.Debug, etc.) and zap fields become structured args
// — a 1:1 translation so a "missed file_reference refresh" gotd log
// is searchable by the same key/value in the gateway's logs.
type slogCore struct {
	zapcore.LevelEnabler
	logger *slog.Logger
	fields []zapcore.Field
}

func (c *slogCore) With(fields []zapcore.Field) zapcore.Core {
	return &slogCore{
		LevelEnabler: c.LevelEnabler,
		logger:       c.logger,
		fields:       append(append([]zapcore.Field(nil), c.fields...), fields...),
	}
}

func (c *slogCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *slogCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	all := append(append([]zapcore.Field(nil), c.fields...), fields...)
	attrs := make([]any, 0, len(all)*2)
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range all {
		f.AddTo(enc)
	}
	for k, v := range enc.Fields {
		attrs = append(attrs, k, v)
	}
	c.logger.Log(context.Background(), zapLevelToSlog(e.Level), e.Message, attrs...)
	return nil
}

func (c *slogCore) Sync() error { return nil }

func zapLevelToSlog(l zapcore.Level) slog.Level {
	switch {
	case l <= zapcore.DebugLevel:
		return slog.LevelDebug
	case l == zapcore.InfoLevel:
		return slog.LevelInfo
	case l == zapcore.WarnLevel:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}
