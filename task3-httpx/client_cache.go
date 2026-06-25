package httpx

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"
)

// CacheDoer caches successful GET responses for a TTL. The subtlety it handles
// (and a common bug it avoids): an http.Response.Body is a one-shot stream, so
// to serve a cached response more than once we must buffer the bytes on the way
// in and hand out a fresh io.ReadCloser over those bytes on every hit.
//
// Only idempotent GETs with 2xx status are cached — caching a POST or an error
// response would be incorrect. The clock is injectable so TTL expiry is testable
// without sleeping.
type CacheDoer struct {
	next HttpDoer
	ttl  time.Duration
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	status  int
	header  http.Header
	body    []byte
	expires time.Time
}

// CacheOption configures the cache.
type CacheOption func(*CacheDoer)

// WithCacheClock injects the clock (tests).
func WithCacheClock(now func() time.Time) CacheOption {
	return func(c *CacheDoer) { c.now = now }
}

// NewCacheDoer wraps next with a TTL response cache.
func NewCacheDoer(next HttpDoer, ttl time.Duration, opts ...CacheOption) *CacheDoer {
	c := &CacheDoer{next: next, ttl: ttl, now: time.Now, entries: make(map[string]cacheEntry)}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Do implements HttpDoer.
func (c *CacheDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return c.next.Do(req) // only GETs are safe to cache
	}
	key := req.URL.String()

	if e, ok := c.lookup(key); ok {
		return c.respond(e, req), nil // cache hit
	}

	resp, err := c.next.Do(req)
	if err != nil {
		return resp, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, nil // don't cache non-2xx
	}

	// Buffer the body so we can both return it now and replay it later.
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	e := cacheEntry{status: resp.StatusCode, header: resp.Header.Clone(), body: body, expires: c.now().Add(c.ttl)}
	c.store(key, e)

	resp.Body = io.NopCloser(bytes.NewReader(body))
	return resp, nil
}

func (c *CacheDoer) lookup(key string) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || !c.now().Before(e.expires) {
		return cacheEntry{}, false
	}
	return e, true
}

func (c *CacheDoer) store(key string, e cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = e
}

// respond builds a fresh *http.Response from a cache entry, with a new body
// reader each time and an X-Cache: HIT marker for observability.
func (c *CacheDoer) respond(e cacheEntry, req *http.Request) *http.Response {
	h := e.header.Clone()
	h.Set("X-Cache", "HIT")
	return &http.Response{
		StatusCode: e.status,
		Header:     h,
		Body:       io.NopCloser(bytes.NewReader(e.body)),
		Request:    req,
	}
}

// Cache returns a Decorator with the given TTL.
func Cache(ttl time.Duration) Decorator {
	return func(next HttpDoer) HttpDoer { return NewCacheDoer(next, ttl) }
}
