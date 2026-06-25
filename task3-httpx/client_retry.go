package httpx

import (
	"context"
	"math/rand/v2"
	"net/http"
	"time"
)

// RetryDoer retries transient failures with exponential backoff and optional
// full jitter. "Transient" = a transport error or a 5xx response; 4xx are
// client errors and are not retried (retrying them just wastes calls). The
// sleeper and RNG are injectable so tests are instant and deterministic, and the
// backoff wait honors the request context so a cancelled request stops retrying.
type RetryDoer struct {
	next        HttpDoer
	opts        RetryOptions
	sleep       func(ctx context.Context, d time.Duration) error
	rnd         func() float64
	shouldRetry func(*http.Response, error) bool
}

// RetryOptions configures retry behavior.
type RetryOptions struct {
	MaxAttempts int           // total attempts including the first
	BaseDelay   time.Duration // delay before the 2nd attempt; doubles each retry
	MaxDelay    time.Duration // cap on pre-jitter delay; 0 = uncapped
	Jitter      bool          // full jitter to de-correlate concurrent clients
}

// RetryDoerOption customizes a RetryDoer (used by tests to inject sleeper/RNG).
type RetryDoerOption func(*RetryDoer)

// WithRetrySleeper overrides how backoff waits.
func WithRetrySleeper(s func(ctx context.Context, d time.Duration) error) RetryDoerOption {
	return func(d *RetryDoer) { d.sleep = s }
}

// WithRetryRand overrides the jitter RNG.
func WithRetryRand(r func() float64) RetryDoerOption {
	return func(d *RetryDoer) { d.rnd = r }
}

// NewRetryDoer wraps next with retry behavior.
func NewRetryDoer(next HttpDoer, opts RetryOptions, o ...RetryDoerOption) *RetryDoer {
	d := &RetryDoer{
		next:        next,
		opts:        opts,
		sleep:       sleepCtx,
		rnd:         rand.Float64,
		shouldRetry: defaultShouldRetry,
	}
	for _, opt := range o {
		opt(d)
	}
	return d
}

// Do implements HttpDoer.
func (d *RetryDoer) Do(req *http.Request) (*http.Response, error) {
	max := d.opts.MaxAttempts
	if max < 1 {
		max = 1
	}
	var resp *http.Response
	var err error
	for attempt := 1; attempt <= max; attempt++ {
		if attempt > 1 {
			if serr := d.sleep(req.Context(), d.backoff(attempt)); serr != nil {
				return nil, serr
			}
		}
		resp, err = d.next.Do(req)
		if !d.shouldRetry(resp, err) {
			return resp, err
		}
		// We'll retry: close the response body we're discarding to avoid a leak.
		if resp != nil {
			resp.Body.Close()
		}
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
	}
	return resp, err
}

// backoff computes the delay before a 1-based attempt: base * 2^(attempt-2),
// capped, optionally full-jittered.
func (d *RetryDoer) backoff(attempt int) time.Duration {
	if attempt <= 1 || d.opts.BaseDelay <= 0 {
		return 0
	}
	delay := d.opts.BaseDelay
	for i := 0; i < attempt-2; i++ {
		delay *= 2
		if d.opts.MaxDelay > 0 && delay >= d.opts.MaxDelay {
			delay = d.opts.MaxDelay
			break
		}
	}
	if d.opts.MaxDelay > 0 && delay > d.opts.MaxDelay {
		delay = d.opts.MaxDelay
	}
	if d.opts.Jitter {
		delay = time.Duration(float64(delay) * d.rnd())
	}
	return delay
}

// defaultShouldRetry retries transport errors and 5xx responses only.
func defaultShouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	return resp != nil && resp.StatusCode >= 500
}

// sleepCtx waits for d or context cancellation.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Retry returns a Decorator with the given options.
func Retry(opts RetryOptions) Decorator {
	return func(next HttpDoer) HttpDoer { return NewRetryDoer(next, opts) }
}
