package httpx

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// okHandler is the fake "layer below" for server middleware tests.
func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}

func TestRequestID_GeneratesAndExposes(t *testing.T) {
	var gotInHandler string
	h := RequestIDWithGenerator(func() string { return "fixed-id" })(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotInHandler, _ = RequestIDFromContext(r.Context())
			w.WriteHeader(200)
		}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if gotInHandler != "fixed-id" {
		t.Errorf("handler context id = %q, want fixed-id", gotInHandler)
	}
	if rec.Header().Get(RequestIDHeader) != "fixed-id" {
		t.Errorf("response header id = %q, want fixed-id", rec.Header().Get(RequestIDHeader))
	}
}

func TestRequestID_ReusesIncoming(t *testing.T) {
	h := RequestID()(okHandler())
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(RequestIDHeader, "upstream-123")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get(RequestIDHeader) != "upstream-123" {
		t.Errorf("should reuse incoming id, got %q", rec.Header().Get(RequestIDHeader))
	}
}

func TestRequestLogging_RecordsStatusAndID(t *testing.T) {
	var buf bytes.Buffer
	// Stack request-id then logging so the log line carries the id.
	h := NewStack(RequestIDWithGenerator(func() string { return "rid" }), RequestLogging(&buf)).
		Then(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", http.StatusForbidden)
		}))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/secret", nil))

	log := buf.String()
	for _, want := range []string{"request_id=rid", "status=403", "path=/secret", "method=GET"} {
		if !strings.Contains(log, want) {
			t.Errorf("log missing %q in %q", want, log)
		}
	}
}

func TestPerClientRateLimit_BurstThen429(t *testing.T) {
	// burst=2, and a frozen clock so no tokens refill mid-test.
	clk := &manualClock{}
	mw := NewPerClientRateLimiter(1, 2, WithRateLimitClock(clk.now)).Middleware()
	h := mw(okHandler())

	codes := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "10.0.0.1:5000"
		h.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != http.StatusTooManyRequests {
		t.Errorf("burst=2 then deny expected [200 200 429], got %v", codes)
	}
}

func TestPerClientRateLimit_IndependentClients(t *testing.T) {
	clk := &manualClock{}
	h := NewPerClientRateLimiter(1, 1, WithRateLimitClock(clk.now)).Middleware()(okHandler())

	call := func(ip string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = ip + ":1"
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	// Client A exhausts its single token; client B is unaffected.
	if call("1.1.1.1") != 200 || call("1.1.1.1") != http.StatusTooManyRequests {
		t.Error("client A should get 200 then 429")
	}
	if call("2.2.2.2") != 200 {
		t.Error("client B must be independent of A")
	}
}

func TestAuth_MissingInvalidValid(t *testing.T) {
	auth := Auth(func(token string) (string, bool) {
		return "alice", token == "good"
	})
	var principal string
	h := auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, _ = PrincipalFromContext(r.Context())
		w.WriteHeader(200)
	}))

	do := func(authHeader string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if do("") != http.StatusUnauthorized {
		t.Error("missing token should be 401")
	}
	if do("Bearer bad") != http.StatusUnauthorized {
		t.Error("invalid token should be 401")
	}
	if do("Bearer good") != 200 || principal != "alice" {
		t.Errorf("valid token should be 200 with principal alice, got principal=%q", principal)
	}
}

func TestStack_OrderAndShortCircuit(t *testing.T) {
	// Full inbound stack. An unauthenticated request must be rejected by auth
	// BUT still be logged (logging is outside auth) and carry a request id.
	var buf bytes.Buffer
	handlerCalled := false
	stack := NewStack(
		RequestIDWithGenerator(func() string { return "rid" }),
		RequestLogging(&buf),
		PerClientRateLimit(100, 100),
		Auth(func(t string) (string, bool) { return "svc", t == "ok" }),
	).Then(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(200)
	}))

	rec := httptest.NewRecorder()
	stack.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil)) // no auth header

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated request should be 401, got %d", rec.Code)
	}
	if handlerCalled {
		t.Error("handler must not run when auth rejects")
	}
	if !strings.Contains(buf.String(), "status=401") || !strings.Contains(buf.String(), "request_id=rid") {
		t.Errorf("rejected request should still be logged with id: %q", buf.String())
	}
}
