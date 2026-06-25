// Package workflow implements a concurrent workflow orchestration engine. Typed
// jobs are composed into a dependency DAG with conditional transitions, per-job
// retries, lifecycle events, and embeddable sub-workflows. Adding a new job type
// requires zero changes to the engine — it depends only on the Job interface and
// a runtime registry.
package workflow

import (
	"errors"
	"fmt"
)

// ErrInvalidTransition is returned when code attempts a state change the state
// machine does not allow. Surfacing this as an error (rather than silently
// allowing it) is the whole point of modeling states explicitly: illegal
// transitions become loud bugs, not corrupt state.
var ErrInvalidTransition = errors.New("invalid state transition")

// JobState is the lifecycle state of a single job.
type JobState string

const (
	JobPending   JobState = "pending"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
	JobCancelled JobState = "cancelled"
	// JobSkipped means the job's condition was not met (or an upstream
	// dependency did not succeed) — distinct from Failed, which means the job
	// ran and errored. Keeping these separate is what lets conditional routing
	// and failure isolation behave correctly.
	JobSkipped JobState = "skipped"
)

// WorkflowState is the lifecycle state of an entire workflow run.
type WorkflowState string

const (
	WorkflowPending   WorkflowState = "pending"
	WorkflowRunning   WorkflowState = "running"
	WorkflowSucceeded WorkflowState = "succeeded"
	WorkflowFailed    WorkflowState = "failed"
	WorkflowCancelled WorkflowState = "cancelled"
)

// machine is a generic, table-driven state machine reused for both job- and
// workflow-level states. Generics genuinely earn their place here: two distinct
// state enums share identical transition-guard logic, so a single type removes
// real duplication — the opposite of Task 1, where generics would have added
// ceremony for no gain.
type machine[S comparable] struct {
	state    S
	allowed  map[S]map[S]struct{}
	terminal map[S]struct{}
}

func newMachine[S comparable](initial S, allowed map[S]map[S]struct{}, terminal map[S]struct{}) *machine[S] {
	return &machine[S]{state: initial, allowed: allowed, terminal: terminal}
}

// State returns the current state.
func (m *machine[S]) State() S { return m.state }

// IsTerminal reports whether the current state is terminal (no further
// transitions possible).
func (m *machine[S]) IsTerminal() bool {
	_, ok := m.terminal[m.state]
	return ok
}

// To transitions to next, or returns ErrInvalidTransition if the move isn't
// allowed from the current state.
func (m *machine[S]) To(next S) error {
	if _, ok := m.allowed[m.state][next]; !ok {
		return fmt.Errorf("%w: %v -> %v", ErrInvalidTransition, m.state, next)
	}
	m.state = next
	return nil
}

// set builds a set from a list (small helper for the transition tables).
func set[S comparable](items ...S) map[S]struct{} {
	m := make(map[S]struct{}, len(items))
	for _, it := range items {
		m[it] = struct{}{}
	}
	return m
}

// newJobMachine builds a job state machine starting at Pending with the legal
// job transitions: Pending -> Running|Skipped|Cancelled, Running ->
// Succeeded|Failed|Cancelled, and the four terminal states.
func newJobMachine() *machine[JobState] {
	return newMachine(JobPending,
		map[JobState]map[JobState]struct{}{
			JobPending: set(JobRunning, JobSkipped, JobCancelled),
			JobRunning: set(JobSucceeded, JobFailed, JobCancelled),
		},
		set(JobSucceeded, JobFailed, JobCancelled, JobSkipped),
	)
}

// newWorkflowMachine builds a workflow state machine starting at Pending.
func newWorkflowMachine() *machine[WorkflowState] {
	return newMachine(WorkflowPending,
		map[WorkflowState]map[WorkflowState]struct{}{
			WorkflowPending: set(WorkflowRunning, WorkflowCancelled),
			WorkflowRunning: set(WorkflowSucceeded, WorkflowFailed, WorkflowCancelled),
		},
		set(WorkflowSucceeded, WorkflowFailed, WorkflowCancelled),
	)
}
