package config

import (
	"strings"
	"testing"
	"time"
)

// TestLoadPhase4Validation covers the Phase 4 transport / app-credential /
// grace-window rules added to Load(). Earlier phases' config has been
// implicitly covered by the higher-level test suites for years; this test
// just guards the new failure modes so a typo in the transport flag or a
// missing app credential fails fast at boot instead of mid-flight.
func TestLoadPhase4Validation(t *testing.T) {
	base := map[string]string{
		"S3_ACCESS_KEY_ID":     "k",
		"S3_SECRET_ACCESS_KEY": "s",
		"TELEGRAM_BOT_TOKENS":  "abc",
		"TELEGRAM_CHAT_ID":     "-100",
	}

	cases := []struct {
		name    string
		env     map[string]string
		wantErr string // substring; "" means must succeed
		check   func(t *testing.T, c Config)
	}{
		{
			name: "default transport bot needs no app creds",
			env:  map[string]string{},
			check: func(t *testing.T, c Config) {
				if c.TelegramTransport != "bot" {
					t.Fatalf("transport=%q want bot", c.TelegramTransport)
				}
				if c.MigrationRate != 100 {
					t.Fatalf("migration_rate=%d want 100", c.MigrationRate)
				}
				if c.BotDeleteGrace != time.Hour {
					t.Fatalf("grace=%v want 1h", c.BotDeleteGrace)
				}
			},
		},
		{
			name:    "unknown transport rejected",
			env:     map[string]string{"TELEGRAM_TRANSPORT": "BOTH"},
			wantErr: "TELEGRAM_TRANSPORT",
		},
		{
			name:    "dual without app id rejected",
			env:     map[string]string{"TELEGRAM_TRANSPORT": "dual"},
			wantErr: "TELEGRAM_APP_ID",
		},
		{
			name: "dual without app hash rejected",
			env: map[string]string{
				"TELEGRAM_TRANSPORT": "dual",
				"TELEGRAM_APP_ID":    "42",
			},
			wantErr: "TELEGRAM_APP_HASH",
		},
		{
			name: "dual with creds parses",
			env: map[string]string{
				"TELEGRAM_TRANSPORT": "dual",
				"TELEGRAM_APP_ID":    "42",
				"TELEGRAM_APP_HASH":  "deadbeef",
			},
			check: func(t *testing.T, c Config) {
				if c.TelegramTransport != "dual" || c.TelegramAppID != 42 || c.TelegramAppHash != "deadbeef" {
					t.Fatalf("dual creds not parsed: %+v", c)
				}
			},
		},
		{
			name: "MIGRATION_RATE=0 disables sweeper (not rejected)",
			env: map[string]string{
				"TELEGRAM_TRANSPORT": "dual",
				"TELEGRAM_APP_ID":    "42",
				"TELEGRAM_APP_HASH":  "x",
				"MIGRATION_RATE":     "0",
			},
			check: func(t *testing.T, c Config) {
				if c.MigrationRate != 0 {
					t.Fatalf("migration_rate=%d want 0", c.MigrationRate)
				}
			},
		},
		{
			name: "BOT_DELETE_GRACE below 1m clamps to 1m",
			env: map[string]string{
				"TELEGRAM_TRANSPORT": "dual",
				"TELEGRAM_APP_ID":    "42",
				"TELEGRAM_APP_HASH":  "x",
				"BOT_DELETE_GRACE":   "10s",
			},
			check: func(t *testing.T, c Config) {
				if c.BotDeleteGrace != time.Minute {
					t.Fatalf("grace=%v want 1m (clamp)", c.BotDeleteGrace)
				}
			},
		},
		{
			name: "BOT_DELETE_GRACE=0 preserved (test-only path)",
			env: map[string]string{
				"TELEGRAM_TRANSPORT": "dual",
				"TELEGRAM_APP_ID":    "42",
				"TELEGRAM_APP_HASH":  "x",
				"BOT_DELETE_GRACE":   "0s",
			},
			check: func(t *testing.T, c Config) {
				if c.BotDeleteGrace != 0 {
					t.Fatalf("grace=%v want 0 (explicit test bypass)", c.BotDeleteGrace)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range base {
				t.Setenv(k, v)
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			c, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if tc.check != nil {
				tc.check(t, c)
			}
		})
	}
}
