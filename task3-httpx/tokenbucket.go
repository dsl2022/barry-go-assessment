package httpx

import (
	"context"
	"sync"
	"time"
)

// TokenBucket is a classic token-bucket rate limiter, implemented here (rather
// than pulling in golang.org/x/time/rate) so the mechanism is explicit and
// dependency-free — tokens refill continuously at a fixed rate up to a burst
// cap, and each allowed request consumes one. A production system might swap in
// x/time/rate behind the same Limiter interface; nothing else would change.
//
// It serves both halves of this package: the client's rate-limit decorator
// (via Wait, which blocks for a token) and the server's per-client limiter
// (via Allow, which is non-blocking and yields a 429 when empty).
type TokenBucket struct {
	mu           sync.Mutex
	tokens       float64
	max          float64
	refillPerSec float64
	last         time.Time
	now          func() time.Time
}

// TokenBucketOption configures a bucket.
type TokenBucketOption func(*TokenBucket)

// WithBucketClock injects the clock (tests use a manual clock for determinism).
func WithBucketClock(now func() time.Time) TokenBucketOption {
	return func(b *TokenBucket) { b.now = now }
}

// NewTokenBucket creates a bucket that refills ratePerSec tokens/second up to a
// burst capacity, starting full.
func NewTokenBucket(ratePerSec float64, burst int, opts ...TokenBucketOption) *TokenBucket {
	b := &TokenBucket{
		tokens:       float64(burst),
		max:          float64(burst),
		refillPerSec: ratePerSec,
		now:          time.Now,
	}
	for _, o := range opts {
		o(b)
	}
	b.last = b.now()
	return b
}

// refill adds tokens for elapsed time since the last update. Caller holds mu.
func (b *TokenBucket) refill() {
	t := b.now()
	elapsed := t.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.last = t
	b.tokens += elapsed * b.refillPerSec
	if b.tokens > b.max {
		b.tokens = b.max
	}
}

// Allow reports whether a request may proceed right now, consuming one token if
// so. Non-blocking — used by the server middleware.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Wait blocks until a token is available or the context is cancelled, consuming
// one token on success. Used by the client decorator so callers are throttled
// rather than rejected. It sleeps for the computed time-to-next-token instead of
// busy-looping.
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		b.refill()
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		needed := 1 - b.tokens
		wait := time.Duration(needed / b.refillPerSec * float64(time.Second))
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
