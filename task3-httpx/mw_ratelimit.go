package httpx

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// PerClientRateLimiter applies an independent token bucket per client, so one
// noisy client can't exhaust everyone else's budget. Clients are identified by a
// key function (default: remote IP). When a client's bucket is empty the request
// is rejected with 429 — non-blocking, because a server should shed load, not
// queue it (the opposite choice from the client decorator, which waits).
type PerClientRateLimiter struct {
	ratePerSec float64
	burst      int
	keyFn      func(*http.Request) string
	now        func() time.Time

	mu      sync.Mutex
	buckets map[string]*TokenBucket
}

// RateLimitOption configures the limiter.
type RateLimitOption func(*PerClientRateLimiter)

// WithClientKey overrides how a client is identified (e.g. by API key/header).
func WithClientKey(fn func(*http.Request) string) RateLimitOption {
	return func(l *PerClientRateLimiter) { l.keyFn = fn }
}

// WithRateLimitClock injects the clock used by the per-client buckets (tests).
func WithRateLimitClock(now func() time.Time) RateLimitOption {
	return func(l *PerClientRateLimiter) { l.now = now }
}

// NewPerClientRateLimiter builds the limiter; use Middleware() to mount it.
func NewPerClientRateLimiter(ratePerSec float64, burst int, opts ...RateLimitOption) *PerClientRateLimiter {
	l := &PerClientRateLimiter{
		ratePerSec: ratePerSec,
		burst:      burst,
		keyFn:      clientIP,
		now:        time.Now,
		buckets:    make(map[string]*TokenBucket),
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// bucketFor returns (creating if needed) the bucket for a client key.
func (l *PerClientRateLimiter) bucketFor(key string) *TokenBucket {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		b = NewTokenBucket(l.ratePerSec, l.burst, WithBucketClock(l.now))
		l.buckets[key] = b
	}
	return b
}

// Middleware returns the rate-limiting Middleware.
func (l *PerClientRateLimiter) Middleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !l.bucketFor(l.keyFn(r)).Allow() {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// PerClientRateLimit is a convenience constructor returning the Middleware with
// default (per-IP) keying.
func PerClientRateLimit(ratePerSec float64, burst int) Middleware {
	return NewPerClientRateLimiter(ratePerSec, burst).Middleware()
}

// clientIP extracts the client IP from RemoteAddr (host:port), falling back to
// the raw value if it isn't in that form.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
