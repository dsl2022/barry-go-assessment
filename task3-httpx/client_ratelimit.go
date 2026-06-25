package httpx

import (
	"context"
	"net/http"
)

// Limiter is the throttling contract the rate-limit decorator needs. Depending
// on an interface (not *TokenBucket directly) keeps the decorator testable in
// isolation: a test injects a fake Limiter that records calls or returns an
// error, with no real time involved.
type Limiter interface {
	Wait(ctx context.Context) error
}

// RateLimitDoer throttles outbound requests: before each call it waits for a
// token from the Limiter, so a burst of callers is smoothed to the configured
// rate rather than rejected. It respects the request's context, so a cancelled
// request stops waiting immediately.
type RateLimitDoer struct {
	next    HttpDoer
	limiter Limiter
}

// NewRateLimitDoer wraps next with the given limiter.
func NewRateLimitDoer(next HttpDoer, limiter Limiter) *RateLimitDoer {
	return &RateLimitDoer{next: next, limiter: limiter}
}

// Do implements HttpDoer.
func (d *RateLimitDoer) Do(req *http.Request) (*http.Response, error) {
	if err := d.limiter.Wait(req.Context()); err != nil {
		return nil, err
	}
	return d.next.Do(req)
}

// RateLimit returns a Decorator backed by a token bucket of ratePerSec/burst.
func RateLimit(ratePerSec float64, burst int) Decorator {
	limiter := NewTokenBucket(ratePerSec, burst)
	return func(next HttpDoer) HttpDoer { return NewRateLimitDoer(next, limiter) }
}
