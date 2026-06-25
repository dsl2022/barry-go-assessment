package httpx

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// RequestLogging emits one structured line per request: method, path, status,
// response size, elapsed time, and the request ID (so logs correlate with the
// X-Request-ID injected upstream). It relies on statusRecorder to observe the
// status the handler actually wrote.
func RequestLogging(out io.Writer) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			id, _ := RequestIDFromContext(r.Context())
			fmt.Fprintf(out, "server request_id=%s method=%s path=%s status=%d bytes=%d elapsed=%s\n",
				id, r.Method, r.URL.Path, rec.status, rec.bytes, time.Since(start))
		})
	}
}
