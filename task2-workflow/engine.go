package workflow

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// Engine runs workflows. It holds only configuration (event bus + injectable
// clock/RNG/sleeper) and no per-run mutable state, so a single Engine is safe to
// reuse and — importantly — reentrant: a SubWorkflowJob can call Run on the same
// Engine to execute an embedded workflow.
type Engine struct {
	bus   *EventBus
	now   func() time.Time
	rnd   func() float64
	sleep func(ctx context.Context, d time.Duration) error
}

// EngineOption configures an Engine.
type EngineOption func(*Engine)

// WithEventBus attaches an event bus; without one, events are silently dropped.
func WithEventBus(b *EventBus) EngineOption { return func(e *Engine) { e.bus = b } }

// WithClock overrides the event timestamp clock (tests).
func WithClock(now func() time.Time) EngineOption { return func(e *Engine) { e.now = now } }

// WithRand overrides the jitter RNG (tests want determinism).
func WithRand(rnd func() float64) EngineOption { return func(e *Engine) { e.rnd = rnd } }

// WithSleeper overrides how backoff waits — tests inject a no-op sleeper so
// retries don't cost wall-clock time while still honoring cancellation.
func WithSleeper(s func(ctx context.Context, d time.Duration) error) EngineOption {
	return func(e *Engine) { e.sleep = s }
}

// NewEngine builds an Engine with production defaults.
func NewEngine(opts ...EngineOption) *Engine {
	e := &Engine{
		now:   time.Now,
		rnd:   rand.Float64,
		sleep: defaultSleep,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// RunResult is the outcome of a workflow run: the final workflow state and every
// job's result.
type RunResult struct {
	State   WorkflowState
	Results map[string]Result
}

// RunOption configures a single run.
type RunOption func(*runConfig)

type runConfig struct {
	inputs map[string]any
}

// WithInputs supplies workflow-level inputs readable by jobs/conditions.
func WithInputs(inputs map[string]any) RunOption {
	return func(c *runConfig) { c.inputs = inputs }
}

// Run executes a workflow to completion (or cancellation) and reports the
// outcome via RunResult. It returns an error ONLY when the workflow can't start
// (a validation failure) — a job failing is a normal outcome reflected in
// RunResult.State/Results, not a Run error. That keeps "a failed job doesn't
// halt the run" the default behavior rather than something callers must opt into.
func (e *Engine) Run(ctx context.Context, wf *Workflow, opts ...RunOption) (*RunResult, error) {
	if err := wf.Validate(); err != nil {
		return nil, err
	}
	var cfg runConfig
	for _, o := range opts {
		o(&cfg)
	}

	store := NewResultStore()
	ec := newExecutionContext(store, cfg.inputs)

	wfMachine := newWorkflowMachine()
	_ = wfMachine.To(WorkflowRunning)
	e.publish(Event{Type: EventWorkflowStarted, Workflow: wf.Name, WorkflowState: WorkflowRunning})

	// One done-channel per node, closed when the node reaches a terminal state.
	// Dependents block on these, which is how ordering and failure isolation are
	// enforced without a central scheduler loop.
	done := make(map[string]chan struct{}, len(wf.order))
	for _, id := range wf.order {
		done[id] = make(chan struct{})
	}

	var wg sync.WaitGroup
	for _, node := range wf.Nodes() {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			defer close(done[n.ID]) // signal dependents only after our result is stored
			e.runNode(ctx, wf, n, ec, store, done)
		}(node)
	}
	wg.Wait()

	final := e.finalState(ctx, store)
	_ = wfMachine.To(final)
	e.publish(Event{Type: EventWorkflowCompleted, Workflow: wf.Name, WorkflowState: final})

	return &RunResult{State: final, Results: store.Snapshot()}, nil
}

// runNode is the lifecycle of a single node: wait for dependencies, decide
// whether to run (condition), then run with retries — recording its result and
// emitting events at each transition.
func (e *Engine) runNode(ctx context.Context, wf *Workflow, n *Node, ec *ExecutionContext, store *ResultStore, done map[string]chan struct{}) {
	m := newJobMachine()

	// 1. Wait for every dependency to finish, or bail out on cancellation.
	for _, dep := range n.DependsOn {
		select {
		case <-done[dep]:
		case <-ctx.Done():
			e.cancel(wf, n, m, store)
			return
		}
	}
	if ctx.Err() != nil {
		e.cancel(wf, n, m, store)
		return
	}

	// 2. Decide whether to run. Skipped (condition unmet / upstream not
	//    succeeded) is distinct from Failed and does NOT stop other branches.
	if !e.shouldRun(n, ec) {
		_ = m.To(JobSkipped)
		store.Set(Result{JobID: n.ID, State: JobSkipped})
		e.publish(Event{Type: EventJobSkipped, Workflow: wf.Name, JobID: n.ID, State: JobSkipped})
		return
	}

	// 3. Run with retry.
	_ = m.To(JobRunning)
	e.publish(Event{Type: EventJobStarted, Workflow: wf.Name, JobID: n.ID, State: JobRunning, Attempt: 1})

	output, attempts, err := e.runWithRetry(ctx, wf, n, ec)
	switch {
	case err != nil && ctx.Err() != nil:
		// Cancelled mid-flight: report as Cancelled, not Failed.
		_ = m.To(JobCancelled)
		store.Set(Result{JobID: n.ID, State: JobCancelled, Err: ctx.Err(), Attempts: attempts})
		e.publish(Event{Type: EventJobCancelled, Workflow: wf.Name, JobID: n.ID, State: JobCancelled, Attempt: attempts, Err: ctx.Err()})
	case err != nil:
		_ = m.To(JobFailed)
		store.Set(Result{JobID: n.ID, State: JobFailed, Err: err, Attempts: attempts})
		e.publish(Event{Type: EventJobFailed, Workflow: wf.Name, JobID: n.ID, State: JobFailed, Attempt: attempts, Err: err})
	default:
		_ = m.To(JobSucceeded)
		store.Set(Result{JobID: n.ID, State: JobSucceeded, Output: output, Attempts: attempts})
		e.publish(Event{Type: EventJobSucceeded, Workflow: wf.Name, JobID: n.ID, State: JobSucceeded, Attempt: attempts})
	}
}

// runWithRetry executes the job up to its configured attempts, sleeping the
// backoff delay between tries and emitting a JobRetrying event before each
// retry. It stops early (and returns the context error) if the run is cancelled.
func (e *Engine) runWithRetry(ctx context.Context, wf *Workflow, n *Node, ec *ExecutionContext) (output any, attempts int, err error) {
	max := n.Retry.attempts()
	for attempt := 1; attempt <= max; attempt++ {
		if attempt > 1 {
			e.publish(Event{Type: EventJobRetrying, Workflow: wf.Name, JobID: n.ID, State: JobRunning, Attempt: attempt, Err: err})
			if serr := e.sleep(ctx, n.Retry.backoff(attempt, e.rnd)); serr != nil {
				return nil, attempt - 1, serr // cancelled during backoff
			}
		}
		output, err = n.Job.Execute(ctx, ec)
		if err == nil {
			return output, attempt, nil
		}
		if ctx.Err() != nil {
			return nil, attempt, ctx.Err() // don't keep retrying a cancelled run
		}
	}
	return nil, max, err
}

// shouldRun applies the node's gating rule: an explicit condition if present,
// else the safe default of "all dependencies Succeeded".
func (e *Engine) shouldRun(n *Node, ec *ExecutionContext) bool {
	if n.Condition != nil {
		return n.Condition.Eval(ec)
	}
	for _, dep := range n.DependsOn {
		r, ok := ec.Result(dep)
		if !ok || r.State != JobSucceeded {
			return false
		}
	}
	return true
}

// cancel records a node that never got to run because the context was cancelled.
func (e *Engine) cancel(wf *Workflow, n *Node, m *machine[JobState], store *ResultStore) {
	_ = m.To(JobCancelled)
	store.Set(Result{JobID: n.ID, State: JobCancelled})
	e.publish(Event{Type: EventJobCancelled, Workflow: wf.Name, JobID: n.ID, State: JobCancelled})
}

// finalState derives the workflow's terminal state from the run: Cancelled if
// the context was cancelled, Failed if any job failed, else Succeeded. Skipped
// jobs do not fail the workflow.
func (e *Engine) finalState(ctx context.Context, store *ResultStore) WorkflowState {
	if ctx.Err() != nil {
		return WorkflowCancelled
	}
	for _, r := range store.Snapshot() {
		if r.State == JobFailed {
			return WorkflowFailed
		}
	}
	return WorkflowSucceeded
}

// publish stamps the event time and forwards to the bus (if any).
func (e *Engine) publish(ev Event) {
	if e.bus == nil {
		return
	}
	ev.Time = e.now()
	e.bus.Publish(ev)
}

// defaultSleep waits for d or until the context is cancelled, whichever first.
func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
