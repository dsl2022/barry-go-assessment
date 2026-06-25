package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// testEngine builds an engine whose backoff costs no wall-clock time and whose
// jitter is deterministic, so retry tests are fast and stable.
func testEngine(opts ...EngineOption) *Engine {
	base := []EngineOption{
		WithSleeper(func(ctx context.Context, d time.Duration) error { return ctx.Err() }),
		WithRand(func() float64 { return 1.0 }),
	}
	return NewEngine(append(base, opts...)...)
}

// recorder captures the order in which jobs executed (thread-safe, since
// branches run concurrently).
type recorder struct {
	mu  sync.Mutex
	ran []string
}

func (r *recorder) record(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ran = append(r.ran, id)
}

func (r *recorder) ran_(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, x := range r.ran {
		if x == id {
			return true
		}
	}
	return false
}

func recordJob(rec *recorder, id string, out any) Job {
	return JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
		rec.record(id)
		return out, nil
	})
}

func TestEngine_LinearDependencies(t *testing.T) {
	rec := &recorder{}
	wf := NewWorkflow("linear").
		Add("a", recordJob(rec, "a", 1)).
		Add("b", recordJob(rec, "b", 2), DependsOn("a")).
		Add("c", recordJob(rec, "c", 3), DependsOn("b"))

	res, err := testEngine().Run(context.Background(), wf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.State != WorkflowSucceeded {
		t.Fatalf("state = %s, want succeeded", res.State)
	}
	// a must precede b must precede c.
	order := rec.ran
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("execution order = %v, want [a b c]", order)
	}
}

func TestEngine_FailureIsolation(t *testing.T) {
	// Graph:  a -> b (a fails)      x -> y (independent, must still run)
	rec := &recorder{}
	failA := JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
		rec.record("a")
		return nil, errors.New("boom")
	})
	wf := NewWorkflow("isolation").
		Add("a", failA).
		Add("b", recordJob(rec, "b", nil), DependsOn("a")).
		Add("x", recordJob(rec, "x", nil)).
		Add("y", recordJob(rec, "y", nil), DependsOn("x"))

	res, err := testEngine().Run(context.Background(), wf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.State != WorkflowFailed {
		t.Fatalf("state = %s, want failed (a failed)", res.State)
	}
	if res.Results["a"].State != JobFailed {
		t.Errorf("a should be Failed, got %s", res.Results["a"].State)
	}
	// b depends on a failed job -> Skipped, never ran.
	if res.Results["b"].State != JobSkipped || rec.ran_("b") {
		t.Errorf("b should be Skipped and not run, got state=%s ran=%v", res.Results["b"].State, rec.ran_("b"))
	}
	// The unrelated x->y branch must complete despite a's failure.
	if res.Results["x"].State != JobSucceeded || res.Results["y"].State != JobSucceeded {
		t.Errorf("independent branch must succeed: x=%s y=%s", res.Results["x"].State, res.Results["y"].State)
	}
}

func TestEngine_ConditionalRouting(t *testing.T) {
	// "deploy" fails; an "alert" job conditioned on deploy FAILING must run,
	// while a "notify-success" job conditioned on success must be skipped.
	rec := &recorder{}
	deploy := JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
		rec.record("deploy")
		return nil, errors.New("deploy failed")
	})
	wf := NewWorkflow("conditional").
		Add("deploy", deploy).
		Add("alert", recordJob(rec, "alert", nil), DependsOn("deploy"), When(OnState("deploy", JobFailed))).
		Add("notify", recordJob(rec, "notify", nil), DependsOn("deploy"), When(OnState("deploy", JobSucceeded)))

	res, err := testEngine().Run(context.Background(), wf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Results["alert"].State != JobSucceeded || !rec.ran_("alert") {
		t.Errorf("alert should run on deploy failure, got %s", res.Results["alert"].State)
	}
	if res.Results["notify"].State != JobSkipped || rec.ran_("notify") {
		t.Errorf("notify should be skipped, got %s ran=%v", res.Results["notify"].State, rec.ran_("notify"))
	}
}

func TestEngine_RetryThenSucceed(t *testing.T) {
	attempts := 0
	flaky := JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("transient")
		}
		return "ok", nil
	})
	metrics := NewMetricsSubscriber()
	bus := NewEventBus(metrics)
	wf := NewWorkflow("retry").
		Add("flaky", flaky, WithRetry(RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond}))

	res, err := testEngine(WithEventBus(bus)).Run(context.Background(), wf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Results["flaky"].State != JobSucceeded {
		t.Fatalf("state = %s, want succeeded", res.Results["flaky"].State)
	}
	if got := res.Results["flaky"].Attempts; got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	// Two failures preceded success => two retry events.
	if got := metrics.Count(EventJobRetrying); got != 2 {
		t.Errorf("retry events = %d, want 2", got)
	}
}

func TestEngine_RetryExhausted(t *testing.T) {
	always := JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
		return nil, errors.New("nope")
	})
	wf := NewWorkflow("exhaust").
		Add("j", always, WithRetry(RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond}))

	res, _ := testEngine().Run(context.Background(), wf)
	if res.Results["j"].State != JobFailed {
		t.Fatalf("state = %s, want failed", res.Results["j"].State)
	}
	if got := res.Results["j"].Attempts; got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestEngine_GracefulShutdown(t *testing.T) {
	// A job blocks until the context is cancelled; the engine must report it as
	// Cancelled (not Failed) and the dependent must be Cancelled too.
	started := make(chan struct{})
	blocker := JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
		close(started)
		<-ctx.Done() // wait for cancellation
		return nil, ctx.Err()
	})
	wf := NewWorkflow("shutdown").
		Add("block", blocker).
		Add("after", JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) { return nil, nil }), DependsOn("block"))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	res, err := testEngine().Run(ctx, wf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.State != WorkflowCancelled {
		t.Fatalf("workflow state = %s, want cancelled", res.State)
	}
	if res.Results["block"].State != JobCancelled {
		t.Errorf("block state = %s, want cancelled", res.Results["block"].State)
	}
	if res.Results["after"].State != JobCancelled {
		t.Errorf("after state = %s, want cancelled", res.Results["after"].State)
	}
}

func TestWorkflow_ValidateCycle(t *testing.T) {
	wf := NewWorkflow("cyclic").
		Add("a", JobFunc(func(c context.Context, e *ExecutionContext) (any, error) { return nil, nil }), DependsOn("b")).
		Add("b", JobFunc(func(c context.Context, e *ExecutionContext) (any, error) { return nil, nil }), DependsOn("a"))

	if _, err := testEngine().Run(context.Background(), wf); err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestWorkflow_ValidateUnknownDepAndDuplicate(t *testing.T) {
	noop := JobFunc(func(c context.Context, e *ExecutionContext) (any, error) { return nil, nil })

	unknown := NewWorkflow("unknown").Add("a", noop, DependsOn("ghost"))
	if err := unknown.Validate(); err == nil {
		t.Error("expected unknown-dependency error")
	}

	dup := NewWorkflow("dup").Add("a", noop).Add("a", noop)
	if err := dup.Validate(); err == nil {
		t.Error("expected duplicate-id error")
	}
}
