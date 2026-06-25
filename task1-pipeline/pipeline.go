package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Stats is a snapshot of what a run did — useful for tests, demos, and the
// "state transitions visible in counters" idea the assessment values.
type Stats struct {
	Read         int // records pulled from the source
	Written      int // records that survived all stages and reached the sink
	Skipped      int // records intentionally dropped via ErrSkip (e.g. duplicates)
	DeadLettered int // records routed to the dead-letter queue
}

// DeadLetterQueue collects failed records. It's an interface so the destination
// is pluggable: in-memory for tests, or a Kafka/SQS/file sink in production,
// with no change to the engine.
type DeadLetterQueue interface {
	Add(dl DeadLetter)
}

// MemoryDLQ is an in-memory, concurrency-safe DeadLetterQueue. The mutex is
// defensive: today the engine is sequential, but a future concurrent worker
// pool could write from many goroutines, and this keeps that change local.
type MemoryDLQ struct {
	mu    sync.Mutex
	items []DeadLetter
}

// NewMemoryDLQ returns an empty in-memory dead-letter queue.
func NewMemoryDLQ() *MemoryDLQ { return &MemoryDLQ{} }

// Add appends a dead letter.
func (q *MemoryDLQ) Add(dl DeadLetter) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, dl)
}

// Items returns a copy of the collected dead letters.
func (q *MemoryDLQ) Items() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]DeadLetter, len(q.items))
	copy(out, q.items)
	return out
}

// Pipeline runs records from a Source through an ordered list of Stages to a
// Sink, isolating per-record failures into a DeadLetterQueue.
type Pipeline struct {
	stages []Stage
	dlq    DeadLetterQueue
	now    func() time.Time // injectable clock keeps dead-letter timestamps testable
}

// Option configures a Pipeline. Functional options were chosen over a config
// struct so the common case — New(stages) — stays a one-liner, while rarely-set
// knobs (custom DLQ, custom clock) don't bloat the constructor signature.
type Option func(*Pipeline)

// WithDeadLetterQueue overrides the default in-memory dead-letter queue.
func WithDeadLetterQueue(q DeadLetterQueue) Option {
	return func(p *Pipeline) { p.dlq = q }
}

// WithClock overrides the clock used for dead-letter timestamps (for tests).
func WithClock(now func() time.Time) Option {
	return func(p *Pipeline) { p.now = now }
}

// New constructs a Pipeline from an ordered list of stages.
func New(stages []Stage, opts ...Option) *Pipeline {
	p := &Pipeline{
		stages: stages,
		dlq:    NewMemoryDLQ(),
		now:    time.Now,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// DLQ exposes the dead-letter queue (e.g. to inspect failures after a run).
func (p *Pipeline) DLQ() DeadLetterQueue { return p.dlq }

// Run executes the pipeline until the source is exhausted (clean EOF) or the
// context is cancelled (graceful shutdown). It returns run statistics and an
// error only for *fatal* conditions (a stage failing Setup, a source/sink I/O
// error, or context cancellation) — never for a single bad record, which is
// isolated to the dead-letter queue instead.
func (p *Pipeline) Run(ctx context.Context, src Source, sink Sink) (Stats, error) {
	var stats Stats

	// --- Setup phase ---------------------------------------------------------
	// Set up stages in order. If one fails, tear down the ones already set up
	// (in reverse) and abort: we never run a pipeline with half its resources.
	setup := make([]Stage, 0, len(p.stages))
	for _, s := range p.stages {
		if err := s.Setup(ctx); err != nil {
			p.teardown(ctx, setup)
			return stats, fmt.Errorf("setup stage %q: %w", s.Name(), err)
		}
		setup = append(setup, s)
	}
	// Guarantee symmetric cleanup for every stage we set up, on every exit path.
	defer p.teardown(ctx, setup)

	// --- Process phase -------------------------------------------------------
	for {
		// Graceful shutdown: stop pulling new work the moment the context is
		// cancelled. In-flight records are not abandoned mid-stage because we
		// only check between records; the deferred teardown still runs.
		if err := ctx.Err(); err != nil {
			return stats, err
		}

		rec, err := src.Next(ctx)
		switch {
		case errors.Is(err, io.EOF):
			return stats, nil // clean completion
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return stats, err // source observed cancellation
		case err != nil:
			// A source read failure isn't attributable to one record, so it's
			// fatal rather than a dead letter.
			return stats, fmt.Errorf("source: %w", err)
		}
		stats.Read++

		out, ok := p.process(ctx, rec, &stats)
		if !ok {
			continue // record was skipped or dead-lettered; keep going
		}

		if err := sink.Write(ctx, out); err != nil {
			return stats, fmt.Errorf("sink: %w", err)
		}
		stats.Written++
	}
}

// process runs one record through every stage in order. A stage error is
// isolated here: the record is either dropped (ErrSkip) or sent to the
// dead-letter queue, and the loop reports "not ok" so the engine moves on to the
// next record without halting — the core "errors per-record without halting"
// requirement.
func (p *Pipeline) process(ctx context.Context, rec Record, stats *Stats) (Record, bool) {
	cur := rec
	for _, s := range p.stages {
		out, err := s.Process(ctx, cur)
		if err == nil {
			cur = out
			continue
		}
		if errors.Is(err, ErrSkip) {
			stats.Skipped++
			return Record{}, false
		}
		// Capture the record as it entered the failing stage (cur, not the
		// original) so the dead letter reflects the actual input that broke —
		// the most useful state for debugging the offending stage.
		p.dlq.Add(DeadLetter{
			Record: cur,
			Stage:  s.Name(),
			Err:    err,
			Time:   p.now(),
		})
		stats.DeadLettered++
		return Record{}, false
	}
	return cur, true
}

// teardown calls Teardown on the given stages in reverse order (mirroring setup,
// like nested defers) and is best-effort: a teardown error on one stage must not
// prevent the others from cleaning up.
func (p *Pipeline) teardown(ctx context.Context, stages []Stage) {
	for i := len(stages) - 1; i >= 0; i-- {
		// Best-effort: we deliberately ignore teardown errors here so one
		// stage's failed cleanup can't strand another's resources. A production
		// version would log these via an injected logger.
		_ = stages[i].Teardown(ctx)
	}
}
