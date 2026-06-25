package httpx

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestEndToEnd wires the whole picture: an inbound server middleware stack ->
// a handler -> an outbound client decorator chain -> a fake (flaky) downstream.
// It proves the cross-cutting behaviors are observable end-to-end in logs and
// call counters: auth gating, client-side retry, and client-side caching.
func TestEndToEnd(t *testing.T) {
	// Fake downstream: fails once with 503, then serves "payload". Counts hits.
	var downstreamHits int32
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&downstreamHits, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("payload"))
	}))
	defer downstream.Close()

	// Outbound client chain: cache -> retry -> logging -> real client.
	// Order matters: a cache hit short-circuits before retry/logging; logging is
	// innermost (closest to the wire) so it records EVERY attempt, including the
	// 503 that retry then recovers from. Put logging outermost instead and it
	// would only see retry's final 200 — the kind of thing composition order
	// quietly decides.
	var clientLog bytes.Buffer
	client := Chain(http.DefaultClient,
		Cache(time.Minute),
		Retry(RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}),
		Logging(&clientLog),
	)

	// Handler calls downstream through the client chain.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, downstream.URL, nil)
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		w.Write(b)
	})

	// Inbound server stack.
	var serverLog bytes.Buffer
	front := httptest.NewServer(NewStack(
		RequestID(),
		RequestLogging(&serverLog),
		PerClientRateLimit(1000, 1000), // generous; rate limiting tested separately
		Auth(func(tok string) (string, bool) { return "svc", tok == "secret" }),
	).Then(handler))
	defer front.Close()

	call := func(token string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, front.URL, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		return resp
	}

	// 1) No token -> rejected by auth, downstream never touched.
	if r := call(""); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-auth: want 401, got %d", r.StatusCode)
	}
	if got := atomic.LoadInt32(&downstreamHits); got != 0 {
		t.Fatalf("downstream should not be hit on 401, hits=%d", got)
	}

	// 2) Valid token -> 200; client retried the 503, so downstream saw 2 hits.
	r2 := call("secret")
	b2, _ := io.ReadAll(r2.Body)
	r2.Body.Close()
	if r2.StatusCode != 200 || string(b2) != "payload" {
		t.Fatalf("auth call: status=%d body=%q", r2.StatusCode, b2)
	}
	if got := atomic.LoadInt32(&downstreamHits); got != 2 {
		t.Fatalf("retry should yield 2 downstream hits (503 then 200), got %d", got)
	}
	if r2.Header.Get(RequestIDHeader) == "" {
		t.Error("response should carry a request id")
	}

	// 3) Repeat -> served from the client cache; downstream hit count unchanged.
	r3 := call("secret")
	io.ReadAll(r3.Body)
	r3.Body.Close()
	if got := atomic.LoadInt32(&downstreamHits); got != 2 {
		t.Errorf("cache should prevent a new downstream hit, got %d", got)
	}

	// State transitions are visible in the logs.
	if !strings.Contains(serverLog.String(), "status=401") || !strings.Contains(serverLog.String(), "status=200") {
		t.Errorf("server log should show 401 then 200: %q", serverLog.String())
	}
	if !strings.Contains(clientLog.String(), "status=503") || !strings.Contains(clientLog.String(), "status=200") {
		t.Errorf("client log should show the 503 retry then 200: %q", clientLog.String())
	}
}

// TestEndToEnd_RateLimit proves the server sheds load with 429 once a client's
// burst is spent.
func TestEndToEnd_RateLimit(t *testing.T) {
	front := httptest.NewServer(NewStack(
		PerClientRateLimit(1, 2), // burst 2, ~1/s refill
	).Then(okHandler()))
	defer front.Close()

	var codes []int
	for i := 0; i < 4; i++ {
		resp, err := http.Get(front.URL)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		codes = append(codes, resp.StatusCode)
	}
	// First two within burst succeed; later ones (no time to refill) are 429.
	got429 := false
	for _, c := range codes[2:] {
		if c == http.StatusTooManyRequests {
			got429 = true
		}
	}
	if codes[0] != 200 || codes[1] != 200 || !got429 {
		t.Errorf("expected 200,200,then 429(s); got %v", codes)
	}
}
