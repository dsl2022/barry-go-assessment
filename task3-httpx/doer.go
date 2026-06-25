// Package httpx provides composable cross-cutting concerns for HTTP, split into
// two halves that mirror each other:
//
//   - Client (outbound): decorators implementing a common HttpDoer interface —
//     logging, TTL caching, token-bucket rate limiting, retry with backoff.
//   - Server (inbound): a middleware chain (func(http.Handler) http.Handler) —
//     request ID, structured logging, per-client rate limiting, auth.
//
// Each concern is a separate, independently testable unit; behaviors are added
// by composition, never by interleaving logic into one function.
package httpx

import "net/http"

// HttpDoer is the single method shared by the real *http.Client and every client
// decorator. Because *http.Client already satisfies it, the real client is just
// the innermost layer; each decorator wraps an HttpDoer and is itself an
// HttpDoer, so they nest arbitrarily.
type HttpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// DoerFunc adapts a function to HttpDoer — used as the mock "layer below" in
// tests, so each decorator can be tested in isolation against a canned response.
type DoerFunc func(req *http.Request) (*http.Response, error)

// Do implements HttpDoer.
func (f DoerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

// Decorator wraps an HttpDoer to add one cross-cutting concern.
type Decorator func(HttpDoer) HttpDoer

// Chain composes decorators around a base doer. The FIRST decorator is the
// outermost layer (closest to the caller); the last wraps base directly. So
//
//	Chain(client, Logging(...), Cache(...), RateLimit(...), Retry(...))
//
// produces logging( cache( ratelimit( retry( client ) ) ) ): every call is
// logged; cache hits short-circuit before consuming rate-limit tokens; retries
// happen closest to the wire. Making order explicit (and reversible) is the
// whole point of composition over a hand-written wrapper.
func Chain(base HttpDoer, decorators ...Decorator) HttpDoer {
	for i := len(decorators) - 1; i >= 0; i-- {
		base = decorators[i](base)
	}
	return base
}
