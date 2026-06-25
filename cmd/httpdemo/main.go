// Command httpdemo wires the full Task 3 picture and prints what each layer
// does: an inbound server middleware stack -> a handler -> an outbound client
// decorator chain -> a fake (flaky) downstream service.
//
//	go run ./cmd/httpdemo
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/2015rpro/fantasy-assessment/task3-httpx"
)

func main() {
	// Fake downstream: 503 on the first hit, then "payload". Counts its hits so
	// we can show retry (hit count goes up) and caching (it doesn't).
	var hits int32
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&hits, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("payload"))
	}))
	defer downstream.Close()

	// Outbound client chain (cache -> retry -> logging -> wire).
	client := httpx.Chain(http.DefaultClient,
		httpx.Cache(30*time.Second),
		httpx.Retry(httpx.RetryOptions{MaxAttempts: 3, BaseDelay: 5 * time.Millisecond}),
		httpx.Logging(os.Stdout),
	)

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

	// Inbound server stack (request id -> logging -> rate limit -> auth).
	front := httptest.NewServer(httpx.NewStack(
		httpx.RequestID(),
		httpx.RequestLogging(os.Stdout),
		httpx.PerClientRateLimit(5, 3), // burst 3: calls 1-3 pass; the burst section then 429s
		httpx.Auth(func(tok string) (string, bool) { return "demo-svc", tok == "secret" }),
	).Then(handler))
	defer front.Close()

	call := func(label, token string) {
		req, _ := http.NewRequest(http.MethodGet, front.URL, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf(">> %-22s status=%d body=%q reqID=%s\n\n", label, resp.StatusCode, string(body), resp.Header.Get(httpx.RequestIDHeader))
	}

	fmt.Println("=== 1) no token (auth rejects, downstream never called) ===")
	call("unauthenticated", "")

	fmt.Println("=== 2) valid token (client retries 503 -> 200) ===")
	call("authenticated", "secret")

	fmt.Println("=== 3) valid token again (served from client cache) ===")
	call("authenticated-cached", "secret")

	fmt.Println("=== 4) burst to trip the server rate limiter ===")
	call("burst-1", "secret")
	call("burst-2", "secret")

	fmt.Printf("downstream was hit %d time(s) total (retry added 1; cache prevented more)\n", atomic.LoadInt32(&hits))
}
