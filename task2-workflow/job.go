package workflow

import (
	"context"
	"sync"
)

// Job is the unit of work in a workflow. The engine depends ONLY on this
// interface — never on a concrete job type — which is the property that lets a
// new job type be added with zero orchestrator changes.
//
// Execute receives the run context (for cancellation/timeouts) and an
// ExecutionContext that exposes the outputs of already-completed jobs and the
// workflow's inputs. It returns an arbitrary output (stored for downstream jobs
// and predicates to read) and an error. A non-nil error drives the job to the
// Failed state (after retries are exhausted).
type Job interface {
	Execute(ctx context.Context, ec *ExecutionContext) (output any, err error)
}

// JobFunc adapts a plain function to the Job interface — handy for tests and
// trivial inline jobs without declaring a struct.
type JobFunc func(ctx context.Context, ec *ExecutionContext) (any, error)

// Execute implements Job.
func (f JobFunc) Execute(ctx context.Context, ec *ExecutionContext) (any, error) {
	return f(ctx, ec)
}

// Result is the terminal outcome of a job: its final state, output, error, and
// how many attempts it took. It's the record other jobs and conditions read.
type Result struct {
	JobID    string
	State    JobState
	Output   any
	Err      error
	Attempts int
}

// ResultStore is the workflow's shared "blackboard": a concurrency-safe map from
// job ID to Result. Concurrency-safety is essential here (unlike Task 1's
// sequential pipeline) because independent branches write results from different
// goroutines simultaneously. An RWMutex lets the many predicate reads proceed in
// parallel while writes are serialized.
type ResultStore struct {
	mu sync.RWMutex
	m  map[string]Result
}

// NewResultStore returns an empty store.
func NewResultStore() *ResultStore {
	return &ResultStore{m: make(map[string]Result)}
}

// Set records a job's result.
func (s *ResultStore) Set(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[r.JobID] = r
}

// Get returns a job's result and whether it exists.
func (s *ResultStore) Get(jobID string) (Result, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.m[jobID]
	return r, ok
}

// Snapshot returns a copy of all results — used to assemble the final run
// report without exposing the live map.
func (s *ResultStore) Snapshot() map[string]Result {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Result, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}

// ExecutionContext is the read-only view a job (or a condition predicate) gets
// of the run: the outputs/states of completed jobs and the workflow's inputs.
// Keeping it read-only (no Set methods) means a job can only influence the graph
// through its own return value — no hidden side channels — which keeps data flow
// explicit and the engine the sole writer of state.
type ExecutionContext struct {
	store  *ResultStore
	inputs map[string]any
}

func newExecutionContext(store *ResultStore, inputs map[string]any) *ExecutionContext {
	return &ExecutionContext{store: store, inputs: inputs}
}

// Result returns the full Result of a previously-run job.
func (ec *ExecutionContext) Result(jobID string) (Result, bool) {
	return ec.store.Get(jobID)
}

// Output returns just the output value of a previously-run job.
func (ec *ExecutionContext) Output(jobID string) (any, bool) {
	r, ok := ec.store.Get(jobID)
	if !ok {
		return nil, false
	}
	return r.Output, true
}

// Input returns a workflow-level input by key (e.g. parameters passed to the run).
func (ec *ExecutionContext) Input(key string) (any, bool) {
	v, ok := ec.inputs[key]
	return v, ok
}
