package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// --- test doubles -----------------------------------------------------------

// spyStage records its lifecycle calls into a shared log so tests can assert
// ordering (setup-before-process, reverse-order teardown, etc.).
type spyStage struct {
	name string
	log  *[]string
}

func (s *spyStage) Name() string { return s.name }
func (s *spyStage) Setup(ctx context.Context) error {
	*s.log = append(*s.log, "setup:"+s.name)
	return nil
}
func (s *spyStage) Process(ctx context.Context, rec Record) (Record, error) {
	*s.log = append(*s.log, "process:"+s.name)
	return rec, nil
}
func (s *spyStage) Teardown(ctx context.Context) error {
	*s.log = append(*s.log, "teardown:"+s.name)
	return nil
}

// failOnFieldStage returns an error for any record whose "bad" field is true,
// to exercise per-record dead-lettering.
type failOnFieldStage struct{}

func (failOnFieldStage) Name() string                       { return "failer" }
func (failOnFieldStage) Setup(ctx context.Context) error    { return nil }
func (failOnFieldStage) Teardown(ctx context.Context) error { return nil }
func (failOnFieldStage) Process(ctx context.Context, rec Record) (Record, error) {
	if bad, _ := rec.Data["bad"].(bool); bad {
		return Record{}, errors.New("record marked bad")
	}
	return rec, nil
}

// setupFailStage fails its Setup, to exercise the abort-and-teardown path.
type setupFailStage struct{ name string }

func (s setupFailStage) Name() string                                        { return s.name }
func (s setupFailStage) Setup(ctx context.Context) error                     { return errors.New("boom") }
func (s setupFailStage) Process(c context.Context, r Record) (Record, error) { return r, nil }
func (s setupFailStage) Teardown(ctx context.Context) error                  { return nil }

// failingSource yields its good records, then returns a non-EOF error to
// exercise the *fatal* source path: a read failure isn't attributable to one
// record, so it halts the run rather than being dead-lettered.
type failingSource struct {
	good []Record
	i    int
	err  error
}

func (s *failingSource) Next(ctx context.Context) (Record, error) {
	if s.i < len(s.good) {
		r := s.good[s.i]
		s.i++
		return r, nil
	}
	return Record{}, s.err
}

// failingSink always errors on Write, to exercise the fatal sink path.
type failingSink struct{ err error }

func (s failingSink) Write(ctx context.Context, rec Record) error { return s.err }

func recs(ms ...map[string]any) []Record {
	out := make([]Record, len(ms))
	for i, m := range ms {
		out[i] = Record{Data: m}
	}
	return out
}

// --- tests ------------------------------------------------------------------

func TestPipeline_HappyPath(t *testing.T) {
	src := NewSliceSource(recs(
		map[string]any{"id": "1", "name": "ada"},
		map[string]any{"id": "2", "name": "grace"},
	))
	sink := &SliceSink{}
	p := New([]Stage{
		NewValidateStage([]string{"id"}, nil),
		NewTransformStage(nil, []string{"name"}),
	})

	stats, err := p.Run(context.Background(), src, sink)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Read != 2 || stats.Written != 2 {
		t.Fatalf("stats = %+v, want Read=2 Written=2", stats)
	}
	if sink.Records[0].Data["name"] != "ADA" {
		t.Errorf("transform not applied: %v", sink.Records[0].Data["name"])
	}
}

func TestPipeline_DeadLetterIsolation(t *testing.T) {
	// A bad record in the middle must NOT halt the run; it goes to the DLQ and
	// the good records on either side still reach the sink.
	src := NewSliceSource(recs(
		map[string]any{"id": "1"},
		map[string]any{"id": "2", "bad": true},
		map[string]any{"id": "3"},
	))
	sink := &SliceSink{}
	dlq := NewMemoryDLQ()
	p := New([]Stage{failOnFieldStage{}}, WithDeadLetterQueue(dlq))

	stats, err := p.Run(context.Background(), src, sink)
	if err != nil {
		t.Fatalf("run should not fail on a bad record: %v", err)
	}
	if stats.Written != 2 || stats.DeadLettered != 1 {
		t.Fatalf("stats = %+v, want Written=2 DeadLettered=1", stats)
	}
	items := dlq.Items()
	if len(items) != 1 || items[0].Stage != "failer" || items[0].Err == nil {
		t.Fatalf("dead letter context wrong: %+v", items)
	}
	if items[0].Record.Data["id"] != "2" {
		t.Errorf("wrong record dead-lettered: %v", items[0].Record.Data["id"])
	}
}

func TestPipeline_DedupSkips(t *testing.T) {
	src := NewSliceSource(recs(
		map[string]any{"id": "a"},
		map[string]any{"id": "a"}, // dup -> skipped
		map[string]any{"id": "b"},
	))
	sink := &SliceSink{}
	dedup := NewDedupStage([]string{"id"})
	p := New([]Stage{dedup})

	stats, err := p.Run(context.Background(), src, sink)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Written != 2 || stats.Skipped != 1 || stats.DeadLettered != 0 {
		t.Fatalf("stats = %+v, want Written=2 Skipped=1 DeadLettered=0", stats)
	}
}

func TestPipeline_LifecycleOrdering(t *testing.T) {
	var log []string
	a := &spyStage{name: "A", log: &log}
	b := &spyStage{name: "B", log: &log}
	src := NewSliceSource(recs(map[string]any{"id": "1"}))
	p := New([]Stage{a, b})

	if _, err := p.Run(context.Background(), src, &SliceSink{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{
		"setup:A", "setup:B", // setup runs forward, before any processing
		"process:A", "process:B", // record flows A then B
		"teardown:B", "teardown:A", // teardown runs in REVERSE order
	}
	if fmt.Sprint(log) != fmt.Sprint(want) {
		t.Fatalf("lifecycle order:\n got %v\nwant %v", log, want)
	}
}

func TestPipeline_SetupFailureAbortsAndTearsDown(t *testing.T) {
	var log []string
	a := &spyStage{name: "A", log: &log}
	// Stage B fails Setup; A (already set up) must be torn down, and we must
	// never process any records.
	p := New([]Stage{a, setupFailStage{name: "B"}})

	_, err := p.Run(context.Background(), NewSliceSource(recs(map[string]any{"id": "1"})), &SliceSink{})
	if err == nil {
		t.Fatal("expected setup error")
	}
	want := []string{"setup:A", "teardown:A"} // no process:* at all
	if fmt.Sprint(log) != fmt.Sprint(want) {
		t.Fatalf("on setup failure:\n got %v\nwant %v", log, want)
	}
}

func TestPipeline_GracefulShutdown(t *testing.T) {
	// Cancel the context before running: the engine must stop without processing
	// and still tear down cleanly.
	var log []string
	a := &spyStage{name: "A", log: &log}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	p := New([]Stage{a})
	src := NewSliceSource(recs(map[string]any{"id": "1"}, map[string]any{"id": "2"}))
	stats, err := p.Run(ctx, src, &SliceSink{})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if stats.Read != 0 {
		t.Errorf("should not have processed any records, Read=%d", stats.Read)
	}
	// Teardown must still have run for the set-up stage.
	if fmt.Sprint(log) != fmt.Sprint([]string{"setup:A", "teardown:A"}) {
		t.Errorf("teardown should run on shutdown, log=%v", log)
	}
}

func TestPipeline_FatalSourceError(t *testing.T) {
	// A source read failure (not io.EOF) is fatal: the run stops and returns the
	// error. It is NOT a dead letter — the failure isn't attributable to a record.
	var log []string
	a := &spyStage{name: "A", log: &log}
	srcErr := errors.New("connection reset")
	src := &failingSource{good: recs(map[string]any{"id": "1"}), err: srcErr}
	p := New([]Stage{a})

	stats, err := p.Run(context.Background(), src, &SliceSink{})
	if err == nil {
		t.Fatal("expected fatal source error")
	}
	if !errors.Is(err, srcErr) {
		t.Errorf("run error should wrap the source error, got %v", err)
	}
	// The one good record before the failure flowed through normally.
	if stats.Read != 1 || stats.Written != 1 {
		t.Errorf("stats = %+v, want Read=1 Written=1", stats)
	}
	if stats.DeadLettered != 0 {
		t.Errorf("source error must not be dead-lettered, got DeadLettered=%d", stats.DeadLettered)
	}
	// Teardown must still run despite the fatal exit.
	if fmt.Sprint(log) != fmt.Sprint([]string{"setup:A", "process:A", "teardown:A"}) {
		t.Errorf("teardown should run on fatal source error, log=%v", log)
	}
}

func TestPipeline_FatalSinkError(t *testing.T) {
	// A sink write failure is fatal for the same reason: we can't keep emitting
	// to a broken destination. The record was read and processed, but never counted
	// as Written because the write failed.
	var log []string
	a := &spyStage{name: "A", log: &log}
	sinkErr := errors.New("disk full")
	src := NewSliceSource(recs(map[string]any{"id": "1"}))
	p := New([]Stage{a})

	stats, err := p.Run(context.Background(), src, failingSink{err: sinkErr})
	if err == nil {
		t.Fatal("expected fatal sink error")
	}
	if !errors.Is(err, sinkErr) {
		t.Errorf("run error should wrap the sink error, got %v", err)
	}
	if stats.Read != 1 || stats.Written != 0 {
		t.Errorf("stats = %+v, want Read=1 Written=0", stats)
	}
	if fmt.Sprint(log) != fmt.Sprint([]string{"setup:A", "process:A", "teardown:A"}) {
		t.Errorf("teardown should run on fatal sink error, log=%v", log)
	}
}

func TestRegistry_BuildFromConfigAndUnknownType(t *testing.T) {
	r := NewDefaultRegistry()
	cfgs := []StageConfig{
		{Type: "validate", Options: map[string]any{"required": []any{"id"}}},
		{Type: "dedup", Options: map[string]any{"key": []any{"id"}}},
	}
	stages, err := r.Build(cfgs)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(stages) != 2 || stages[0].Name() != "validate" || stages[1].Name() != "dedup" {
		t.Fatalf("unexpected stages: %+v", stages)
	}

	if _, err := r.Build([]StageConfig{{Type: "nope"}}); err == nil {
		t.Fatal("expected error for unknown stage type")
	}
}
