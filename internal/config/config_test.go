package config

import (
	"strings"
	"testing"
)

func TestLoadRequiredFields(t *testing.T) {
	base := map[string]string{
		"S3_ACCESS_KEY_ID":     "k",
		"S3_SECRET_ACCESS_KEY": "s",
		"TELEGRAM_BOT_TOKENS":  "abc",
		"TELEGRAM_CHAT_ID":     "-100",
		"TELEGRAM_APP_ID":      "42",
		"TELEGRAM_APP_HASH":    "deadbeef",
	}

	cases := []struct {
		name    string
		env     map[string]string
		wantErr string // substring; "" means must succeed
		check   func(t *testing.T, c Config)
	}{
		{
			name: "all required env present",
			env:  map[string]string{},
			check: func(t *testing.T, c Config) {
				if c.TelegramAppID != 42 || c.TelegramAppHash != "deadbeef" {
					t.Fatalf("app creds not parsed: %+v", c)
				}
				if len(c.TelegramBotTokens) != 1 || c.TelegramBotTokens[0] != "abc" {
					t.Fatalf("tokens not parsed: %+v", c.TelegramBotTokens)
				}
			},
		},
		{
			name:    "missing TELEGRAM_BOT_TOKENS rejected",
			env:     map[string]string{"TELEGRAM_BOT_TOKENS": ""},
			wantErr: "TELEGRAM_BOT_TOKENS",
		},
		{
			name:    "missing TELEGRAM_CHAT_ID rejected",
			env:     map[string]string{"TELEGRAM_CHAT_ID": ""},
			wantErr: "TELEGRAM_CHAT_ID",
		},
		{
			name:    "missing TELEGRAM_APP_ID rejected",
			env:     map[string]string{"TELEGRAM_APP_ID": ""},
			wantErr: "TELEGRAM_APP_ID",
		},
		{
			name:    "missing TELEGRAM_APP_HASH rejected",
			env:     map[string]string{"TELEGRAM_APP_HASH": ""},
			wantErr: "TELEGRAM_APP_HASH",
		},
		{
			name: "comma-separated tokens parsed",
			env:  map[string]string{"TELEGRAM_BOT_TOKENS": "a, b ,c"},
			check: func(t *testing.T, c Config) {
				if len(c.TelegramBotTokens) != 3 ||
					c.TelegramBotTokens[0] != "a" ||
					c.TelegramBotTokens[1] != "b" ||
					c.TelegramBotTokens[2] != "c" {
					t.Fatalf("tokens=%v want [a b c]", c.TelegramBotTokens)
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
