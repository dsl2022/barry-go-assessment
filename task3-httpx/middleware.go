package httpx

import "net/http"

// Middleware is the idiomatic Go server-side decorator: it wraps an http.Handler
// and returns a new one. Every concern below is a Middleware, so they compose
// uniformly and each is testable by wrapping a fake handler.
type Middleware func(http.Handler) http.Handler

// MiddlewareStack is the composition mechanism: a builder that assembles a chain
// in a readable, order-explicit way (no hand-written nested wrappers). The FIRST
// middleware added is the OUTERMOST layer — so it runs first on the way in and
// last on the way out, which is why request-ID goes first (everything below can
// see the ID) and auth goes last (reject before the handler, after logging has
// recorded the attempt).
type MiddlewareStack struct {
	mws []Middleware
}

// NewStack starts a stack with an optional initial set of middlewares.
func NewStack(mws ...Middleware) *MiddlewareStack {
	return &MiddlewareStack{mws: mws}
}

// Use appends a middleware (chainable).
func (s *MiddlewareStack) Use(mw Middleware) *MiddlewareStack {
	s.mws = append(s.mws, mw)
	return s
}

// Then wraps the final handler with the whole stack, preserving add order
// (first added = outermost).
func (s *MiddlewareStack) Then(h http.Handler) http.Handler {
	for i := len(s.mws) - 1; i >= 0; i-- {
		h = s.mws[i](h)
	}
	return h
}

// statusRecorder wraps http.ResponseWriter to capture the status code (and bytes
// written) so the logging middleware can report them. Handlers that never call
// WriteHeader implicitly return 200, which this records on first Write.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}
