package mtproto

import "testing"

func TestParseChannelID(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"-1001234567890", 1234567890, false},
		{"-1009999999999", 9999999999, false},        // 10-digit channel id (close to current ceiling)
		{"-100", 0, true},                            // exactly the prefix is rejected (chat_id must be more negative)
		{"-12345", 0, true},                          // basic group
		{"123", 0, true},                             // user DM
		{"not-a-number", 0, true},
	}
	for _, c := range cases {
		got, err := ParseChannelID(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseChannelID(%q) want err, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseChannelID(%q) err=%v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseChannelID(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
