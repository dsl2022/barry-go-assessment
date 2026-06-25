package pipeline

import (
	"context"
	"errors"
)

// Stage is a single, composable step in the pipeline. The engine only ever sees
// this interface — it has no knowledge of concrete stage types — which is what
// makes stages pluggable: adding a new one requires zero changes to the engine.
//
// Lifecycle contract (the three hooks the brief asks for):
//
//   - Setup    runs once, before any record flows, to acquire resources
//     (open files, DB handles, compile a schema, allocate a dedup set).
//   - Process  runs once per record.
//   - Teardown runs once, after the source is exhausted OR the context is
//     cancelled, to release whatever Setup acquired.
//
// The engine guarantees Teardown is called for every stage it successfully Set
// up, even on error or cancellation, so a stage can rely on symmetric cleanup.
type Stage interface {
	// Name identifies the stage in logs and dead-letter context.
	Name() string

	// Setup initializes resources. A Setup error is fatal to the run: if a
	// stage can't acquire its resources, the pipeline can't run correctly.
	Setup(ctx context.Context) error

	// Process handles exactly one record. It returns either:
	//   - the (possibly transformed) record and nil, to pass it downstream;
	//   - ErrSkip, to intentionally drop the record without it being a failure;
	//   - any other error, to route the record to the dead-letter queue.
	Process(ctx context.Context, rec Record) (Record, error)

	// Teardown releases resources acquired in Setup. It receives a context so
	// cleanup can itself be bounded/cancelled.
	Teardown(ctx context.Context) error
}

// ErrSkip is a sentinel a stage returns from Process to drop a record without
// treating it as an error. A deduplication stage, for example, drops duplicates
// — those aren't failures, so dead-lettering them would be misleading noise.
//
// This deliberately overloads the error return for control flow, exactly as the
// standard library does with io.EOF: a non-failure signalled through the error
// channel. The alternative — a third return value (Record, keep bool, err) —
// was rejected because it forces *every* stage to reason about "keep", even
// though only filter-type stages ever drop records. A sentinel keeps the common
// case (transform/validate) signature clean. Callers must compare with
// errors.Is(err, ErrSkip), never ==, so wrapped sentinels still match.
var ErrSkip = errors.New("pipeline: skip record")
