package retry

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentially(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
	}
	for _, c := range cases {
		got := Backoff(2, c.attempts)
		if got != c.want {
			t.Errorf("Backoff(2, %d) = %s, want %s", c.attempts, got, c.want)
		}
	}
}
