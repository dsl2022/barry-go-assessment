package workflow

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// This file holds the three concrete job types the brief asks for (HTTP call,
// email send, data transform) plus the sub-workflow composite. Each is a plain
// Job — the engine has no idea they exist, which is the point: new types plug in
// without engine changes.

// HTTPDoer is the minimal contract HTTPCallJob needs. Depending on this
// interface (not *http.Client) is what makes the job unit-testable: a test
// injects a fake that returns canned responses, with no network involved.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// HTTPCallJob performs an HTTP request and returns {status, body}. A response
// status >= 400 is treated as an error so the engine's retry/failure machinery
// applies to flaky endpoints.
type HTTPCallJob struct {
	Client HTTPDoer
	Method string
	URL    string
}

// Execute implements Job.
func (j *HTTPCallJob) Execute(ctx context.Context, ec *ExecutionContext) (any, error) {
	req, err := http.NewRequestWithContext(ctx, j.Method, j.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := j.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	out := map[string]any{"status": resp.StatusCode, "body": string(body)}
	if resp.StatusCode >= 400 {
		return out, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return out, nil
}

// Mailer is the minimal contract EmailSendJob needs — injected so tests use a
// fake that records sends instead of contacting a mail server.
type Mailer interface {
	Send(ctx context.Context, to, subject, body string) error
}

// EmailSendJob sends one email via the injected Mailer.
type EmailSendJob struct {
	Mailer  Mailer
	To      string
	Subject string
	Body    string
}

// Execute implements Job.
func (j *EmailSendJob) Execute(ctx context.Context, ec *ExecutionContext) (any, error) {
	if err := j.Mailer.Send(ctx, j.To, j.Subject, j.Body); err != nil {
		return nil, fmt.Errorf("send email: %w", err)
	}
	return map[string]any{"to": j.To, "sent": true}, nil
}

// TransformJob applies a pure function to the output of a source job (or to a
// workflow input when Source is empty). It's the job type that demonstrates data
// flowing along the graph: it reads an upstream result and produces a new value
// for downstream jobs.
type TransformJob struct {
	Source string // job ID whose output to transform; "" => use InputKey
	Input  string // workflow input key, used when Source == ""
	Fn     func(in any) (any, error)
}

// Execute implements Job.
func (j *TransformJob) Execute(ctx context.Context, ec *ExecutionContext) (any, error) {
	var in any
	var ok bool
	if j.Source != "" {
		in, ok = ec.Output(j.Source)
		if !ok {
			return nil, fmt.Errorf("transform: no output from source job %q", j.Source)
		}
	} else {
		in, _ = ec.Input(j.Input)
	}
	return j.Fn(in)
}

// SubWorkflowJob embeds an entire workflow as a single job step (the composite
// pattern: a Workflow exposed through the Job interface). It runs on the same
// Engine — which is reentrant precisely so this works — and shares the engine's
// event bus, so sub-workflow events surface alongside the parent's. If the
// embedded workflow doesn't succeed, the step fails, and the parent's normal
// failure-isolation/retry rules then apply to it.
type SubWorkflowJob struct {
	Engine   *Engine
	Workflow *Workflow
	Inputs   map[string]any
}

// Execute implements Job.
func (j *SubWorkflowJob) Execute(ctx context.Context, ec *ExecutionContext) (any, error) {
	res, err := j.Engine.Run(ctx, j.Workflow, WithInputs(j.Inputs))
	if err != nil {
		return nil, fmt.Errorf("sub-workflow %q: %w", j.Workflow.Name, err)
	}
	if res.State != WorkflowSucceeded {
		return res, fmt.Errorf("sub-workflow %q ended in state %s", j.Workflow.Name, res.State)
	}
	return res, nil
}
