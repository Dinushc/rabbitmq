package client

import (
	"testing"
	"time"
)

func TestBackoffPolicyNext(t *testing.T) {
	t.Parallel()

	b := newBackoff(100*time.Millisecond, time.Second, 2, 0)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 100 * time.Millisecond},
		{attempt: 2, want: 200 * time.Millisecond},
		{attempt: 3, want: 400 * time.Millisecond},
		{attempt: 4, want: 800 * time.Millisecond},
		{attempt: 5, want: time.Second},
		{attempt: 100, want: time.Second},
	}

	for _, tt := range tests {
		if got := b.next(tt.attempt); got != tt.want {
			t.Fatalf("next(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffPolicyJitterBounds(t *testing.T) {
	t.Parallel()

	b := newBackoff(100*time.Millisecond, 10*time.Second, 2, 0.5)
	for i := 0; i < 100; i++ {
		got := b.next(3)
		if got < 200*time.Millisecond || got > 600*time.Millisecond {
			t.Fatalf("jittered backoff = %s, want within [200ms, 600ms]", got)
		}
	}
}
