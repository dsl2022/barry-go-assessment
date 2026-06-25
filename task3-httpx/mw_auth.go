package httpx

import (
	"context"
	"net/http"
	"strings"
)

const principalKey ctxKey = iota + 100

// Authenticator validates a bearer token and returns the principal (user/service
// identity) it represents. Injecting this function keeps the auth middleware
// agnostic to *how* tokens are validated (static map, JWT, introspection call) —
// the middleware only owns the HTTP concern (extract header, 401 on failure,
// stash principal); the policy lives in the injected function.
type Authenticator func(token string) (principal string, ok bool)

// Auth rejects requests without a valid "Authorization: Bearer <token>" header
// (401), and on success stores the principal in the context for handlers to use.
// Placed late in the stack so logging still records rejected attempts.
func Auth(authenticate Authenticator) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearerToken(r)
			if !ok {
				http.Error(w, "missing bearer token", http.StatusUnauthorized)
				return
			}
			principal, ok := authenticate(token)
			if !ok {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), principalKey, principal)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// PrincipalFromContext returns the authenticated principal set by Auth.
func PrincipalFromContext(ctx context.Context) (string, bool) {
	p, ok := ctx.Value(principalKey).(string)
	return p, ok
}

// bearerToken extracts the token from an Authorization header.
func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return h[len(prefix):], true
}
