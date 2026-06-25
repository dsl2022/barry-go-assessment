package httpx

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// manualClock is a controllable clock for deterministic time-based tests.
type manualClock struct{ t time.Time }

func (c *manualClock) now() time.Time          { return c.t }
func (c *manualClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func getReq() *http.Request { return httptest.NewRequest(http.MethodGet, "http://svc/x", nil) }

// noSleep is an injected sleeper that doesn't actually wait but honors ctx.
func noSleep(ctx context.Context, d time.Duration) error { return ctx.Err() }

// --- token bucket -----------------------------------------------------------

func TestTokenBucket_AllowAndRefill(t *testing.T) {
	clk := &manualClock{t: time.Unix(0, 0)}
	b := NewTokenBucket(2 /*per sec*/, 2 /*burst*/, WithBucketClock(clk.now))

	if !b.Allow() || !b.Allow() {
		t.Fatal("first two requests should be allowed (burst=2)")
	}
	if b.Allow() {
		t.Fatal("third immediate request should be denied (bucket empty)")
	}
	clk.advance(time.Second) // refills 2 tokens
	if !b.Allow() || !b.Allow() {
		t.Fatal("after 1s, two more should be allowed")
	}
}

// --- logging ----------------------------------------------------------------

func TestLoggingDoer_LogsStatus(t *testing.T) {
	var buf bytes.Buffer
	d := NewLoggingDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		return newResp(200, "ok"), nil
	}), &buf)

	if _, err := d.Do(getReq()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), "status=200") || !strings.Contains(buf.String(), "method=GET") {
		t.Errorf("log line missing fields: %q", buf.String())
	}
}

// --- rate limit (isolated via fake Limiter) ---------------------------------

type fakeLimiter struct {
	calls int
	err   error
}

func (f *fakeLimiter) Wait(ctx context.Context) error { f.calls++; return f.err }

func TestRateLimitDoer_WaitsBeforeCall(t *testing.T) {
	lim := &fakeLimiter{}
	called := false
	d := NewRateLimitDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		called = true
		return newResp(200, ""), nil
	}), lim)

	if _, err := d.Do(getReq()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lim.calls != 1 || !called {
		t.Errorf("limiter should be consulted once before the call (calls=%d called=%v)", lim.calls, called)
	}
}

func TestRateLimitDoer_BlocksOnLimiterError(t *testing.T) {
	lim := &fakeLimiter{err: context.Canceled}
	called := false
	d := NewRateLimitDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		called = true
		return newResp(200, ""), nil
	}), lim)

	if _, err := d.Do(getReq()); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected limiter error, got %v", err)
	}
	if called {
		t.Error("next must NOT be called when the limiter denies")
	}
}

// --- cache ------------------------------------------------------------------

func TestCacheDoer_HitMissAndTTL(t *testing.T) {
	clk := &manualClock{t: time.Unix(0, 0)}
	calls := 0
	d := NewCacheDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return newResp(200, "payload"), nil
	}), 10*time.Second, WithCacheClock(clk.now))

	// 1st: miss -> underlying called.
	r1, _ := d.Do(getReq())
	b1, _ := io.ReadAll(r1.Body)
	if calls != 1 || string(b1) != "payload" {
		t.Fatalf("first call: calls=%d body=%q", calls, b1)
	}
	// 2nd within TTL: hit -> underlying NOT called again, body still readable.
	r2, _ := d.Do(getReq())
	b2, _ := io.ReadAll(r2.Body)
	if calls != 1 || string(b2) != "payload" || r2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("second call should be a cache hit: calls=%d body=%q xcache=%q", calls, b2, r2.Header.Get("X-Cache"))
	}
	// After TTL: miss again.
	clk.advance(11 * time.Second)
	if _, err := d.Do(getReq()); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if calls != 2 {
		t.Errorf("after TTL expiry underlying should be called again, calls=%d", calls)
	}
}

func TestCacheDoer_DoesNotCacheNonGETorNon2xx(t *testing.T) {
	calls := 0
	d := NewCacheDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return newResp(500, "err"), nil
	}), time.Minute)

	d.Do(getReq()) // 500 not cached
	d.Do(getReq())
	if calls != 2 {
		t.Errorf("non-2xx must not be cached, calls=%d", calls)
	}
}

// --- retry ------------------------------------------------------------------

func TestRetryDoer_RetriesThenSucceeds(t *testing.T) {
	attempt := 0
	d := NewRetryDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		attempt++
		if attempt < 3 {
			return newResp(503, "busy"), nil
		}
		return newResp(200, "ok"), nil
	}), RetryOptions{MaxAttempts: 5, BaseDelay: time.Millisecond}, WithRetrySleeper(noSleep))

	resp, err := d.Do(getReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 || attempt != 3 {
		t.Errorf("want status 200 after 3 attempts, got status=%d attempts=%d", resp.StatusCode, attempt)
	}
}

func TestRetryDoer_DoesNotRetry4xx(t *testing.T) {
	attempt := 0
	d := NewRetryDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		attempt++
		return newResp(404, "nope"), nil
	}), RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}, WithRetrySleeper(noSleep))

	resp, _ := d.Do(getReq())
	if resp.StatusCode != 404 || attempt != 1 {
		t.Errorf("4xx must not be retried, got status=%d attempts=%d", resp.StatusCode, attempt)
	}
}

func TestRetryDoer_ExhaustsOnPersistentError(t *testing.T) {
	attempt := 0
	d := NewRetryDoer(DoerFunc(func(r *http.Request) (*http.Response, error) {
		attempt++
		return nil, errors.New("conn refused")
	}), RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}, WithRetrySleeper(noSleep))

	if _, err := d.Do(getReq()); err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if attempt != 3 {
		t.Errorf("want 3 attempts, got %d", attempt)
	}
}

// --- composition ------------------------------------------------------------

func TestChain_ComposedBehavior(t *testing.T) {
	// Compose all four. A cache hit on the 2nd call must prevent the base from
	// being hit again — proving the layers nest and cooperate.
	var log bytes.Buffer
	baseCalls := 0
	base := DoerFunc(func(r *http.Request) (*http.Response, error) {
		baseCalls++
		return newResp(200, "data"), nil
	})

	client := Chain(base,
		Logging(&log),
		Cache(time.Minute),
		RateLimit(1000, 1000), // high enough to never block in the test
		Retry(RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}),
	)

	for i := 0; i < 2; i++ {
		resp, err := client.Do(getReq())
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		io.ReadAll(resp.Body)
	}
	if baseCalls != 1 {
		t.Errorf("base should be called once (2nd served from cache), got %d", baseCalls)
	}
	if strings.Count(log.String(), "status=200") != 2 {
		t.Errorf("both calls should be logged, log=%q", log.String())
	}
}
