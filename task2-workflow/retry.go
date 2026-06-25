package workflow

import "time"

// RetryPolicy configures per-job retry with exponential backoff and optional
// jitter. The zero value means "try once, no retry", so a job without an
// explicit policy behaves sensibly.
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first; <=1 means no retry
	BaseDelay   time.Duration // delay before the 2nd attempt; doubles each retry
	MaxDelay    time.Duration // cap on the (pre-jitter) delay; 0 means uncapped
	Jitter      bool          // apply full jitter to avoid thundering-herd retries
}

// attempts returns the effective number of attempts (at least 1).
func (p RetryPolicy) attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// backoff returns the delay to wait *before* the given 1-based attempt. Attempt
// 1 has no delay. Attempt n waits BaseDelay * 2^(n-2), capped at MaxDelay, then
// (optionally) full-jittered to a random point in [0, delay] using rnd.
//
// rnd is injected (rather than calling math/rand directly) so tests can make
// backoff deterministic — the same reason Task 1 injects its clock.
func (p RetryPolicy) backoff(attempt int, rnd func() float64) time.Duration {
	if attempt <= 1 || p.BaseDelay <= 0 {
		return 0
	}
	d := p.BaseDelay
	for i := 0; i < attempt-2; i++ {
		d *= 2
		if p.MaxDelay > 0 && d >= p.MaxDelay {
			d = p.MaxDelay
			break
		}
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	if p.Jitter {
		d = time.Duration(float64(d) * rnd())
	}
	return d
}
