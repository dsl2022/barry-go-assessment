package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// ctxKey is an unexported context-key type so values set here can't collide with
// keys from other packages (the standard context-key idiom).
type ctxKey int

const requestIDKey ctxKey = iota

// RequestIDHeader is the canonical header used to carry the request ID in and out.
const RequestIDHeader = "X-Request-ID"

// RequestID injects a request ID into the context and echoes it on the response.
// If the incoming request already has one (e.g. from an upstream proxy), it's
// reused so the ID is stable across a call graph; otherwise a new one is minted.
// Placed first in the stack so every downstream middleware/handler can read it.
func RequestID() Middleware {
	return RequestIDWithGenerator(newRequestID)
}

// RequestIDWithGenerator is RequestID with an injectable generator (tests want a
// deterministic ID).
func RequestIDWithGenerator(gen func() string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(RequestIDHeader)
			if id == "" {
				id = gen()
			}
			w.Header().Set(RequestIDHeader, id)
			ctx := context.WithValue(r.Context(), requestIDKey, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequestIDFromContext returns the request ID stored by the RequestID middleware.
func RequestIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(requestIDKey).(string)
	return id, ok
}

// newRequestID returns a random 128-bit hex ID.
func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
