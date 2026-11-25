package client

import (
	"math"
	"math/rand"
	"time"
)

type backoffPolicy struct {
	initial time.Duration
	max     time.Duration
	factor  float64
	jitter  float64
}

func newBackoff(initial, max time.Duration, factor, jitter float64) *backoffPolicy {
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	if max < initial {
		max = initial
	}
	if factor < 1 {
		factor = 1
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return &backoffPolicy{
		initial: initial,
		max:     max,
		factor:  factor,
		jitter:  jitter,
	}
}

func (b *backoffPolicy) next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := float64(b.initial) * math.Pow(b.factor, float64(attempt-1))
	if d > float64(b.max) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(b.max)
	}

	if b.jitter > 0 {
		delta := (rand.Float64()*2 - 1) * b.jitter
		d *= 1 + delta
	}

	if d > float64(b.max) {
		d = float64(b.max)
	}
	if d < float64(time.Millisecond) {
		d = float64(time.Millisecond)
	}

	return time.Duration(d)
}
