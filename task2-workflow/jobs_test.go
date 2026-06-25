package workflow

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// fakeDoer is an HTTPDoer test double returning a canned response.
type fakeDoer struct {
	status int
	body   string
	err    error
}

func (f fakeDoer) Do(req *http.Request) (*http.Response, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &http.Response{
		StatusCode: f.status,
		Body:       io.NopCloser(strings.NewReader(f.body)),
	}, nil
}

// fakeMailer records sends instead of contacting a server.
type fakeMailer struct {
	mu   sync.Mutex
	sent []string
}

func (m *fakeMailer) Send(ctx context.Context, to, subject, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, to+":"+subject)
	return nil
}

func TestHTTPCallJob(t *testing.T) {
	ec := newExecutionContext(NewResultStore(), nil)

	ok := &HTTPCallJob{Client: fakeDoer{status: 200, body: "hello"}, Method: "GET", URL: "http://x"}
	out, err := ok.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m := out.(map[string]any); m["status"] != 200 || m["body"] != "hello" {
		t.Errorf("unexpected output: %v", m)
	}

	bad := &HTTPCallJob{Client: fakeDoer{status: 500, body: "err"}, Method: "GET", URL: "http://x"}
	if _, err := bad.Execute(context.Background(), ec); err == nil {
		t.Error("status 500 should be an error")
	}
}

func TestEmailSendJob(t *testing.T) {
	mailer := &fakeMailer{}
	job := &EmailSendJob{Mailer: mailer, To: "a@b.com", Subject: "hi", Body: "yo"}
	if _, err := job.Execute(context.Background(), newExecutionContext(NewResultStore(), nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mailer.sent) != 1 || mailer.sent[0] != "a@b.com:hi" {
		t.Errorf("email not recorded: %v", mailer.sent)
	}
}

func TestTransformJob_ReadsSourceOutput(t *testing.T) {
	store := NewResultStore()
	store.Set(Result{JobID: "src", State: JobSucceeded, Output: 21})
	ec := newExecutionContext(store, nil)

	job := &TransformJob{Source: "src", Fn: func(in any) (any, error) { return in.(int) * 2, nil }}
	out, err := job.Execute(context.Background(), ec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != 42 {
		t.Errorf("transform output = %v, want 42", out)
	}
}

func TestSubWorkflowJob_Composite(t *testing.T) {
	eng := testEngine()

	// Inner workflow doubles an input.
	inner := NewWorkflow("inner").
		Add("double", &TransformJob{Input: "n", Fn: func(in any) (any, error) { return in.(int) * 2, nil }})

	// Outer workflow embeds the inner workflow as a single job step.
	outer := NewWorkflow("outer").
		Add("sub", &SubWorkflowJob{Engine: eng, Workflow: inner, Inputs: map[string]any{"n": 5}})

	res, err := eng.Run(context.Background(), outer)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.State != WorkflowSucceeded {
		t.Fatalf("outer state = %s, want succeeded", res.State)
	}
	// The sub job's output is the inner RunResult; check the inner job's output.
	subRes := res.Results["sub"].Output.(*RunResult)
	if subRes.Results["double"].Output != 10 {
		t.Errorf("inner double output = %v, want 10", subRes.Results["double"].Output)
	}
}
