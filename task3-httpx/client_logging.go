package httpx

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// LoggingDoer logs one line per request with method, URL, resulting status (or
// error), and elapsed time. It's the simplest decorator and a clear template for
// the pattern: do something, delegate to next, do something with the result.
type LoggingDoer struct {
	next HttpDoer
	out  io.Writer
	now  func() time.Time
}

// NewLoggingDoer wraps next, writing log lines to out.
func NewLoggingDoer(next HttpDoer, out io.Writer) *LoggingDoer {
	return &LoggingDoer{next: next, out: out, now: time.Now}
}

// Do implements HttpDoer.
func (d *LoggingDoer) Do(req *http.Request) (*http.Response, error) {
	start := d.now()
	resp, err := d.next.Do(req)
	elapsed := d.now().Sub(start)
	if err != nil {
		fmt.Fprintf(d.out, "client method=%s url=%s error=%q elapsed=%s\n", req.Method, req.URL, err.Error(), elapsed)
		return resp, err
	}
	fmt.Fprintf(d.out, "client method=%s url=%s status=%d elapsed=%s\n", req.Method, req.URL, resp.StatusCode, elapsed)
	return resp, nil
}

// Logging returns a Decorator for use with Chain.
func Logging(out io.Writer) Decorator {
	return func(next HttpDoer) HttpDoer { return NewLoggingDoer(next, out) }
}
