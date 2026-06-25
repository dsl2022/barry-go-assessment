// Package pipeline implements a pluggable, lifecycle-managed data processing
// pipeline. Records are pulled from a Source, passed through an ordered list of
// composable Stages, and either written to a Sink or routed to a dead-letter
// queue with error context. Stages are added via configuration (Registry) or a
// builder, so the engine never depends on any concrete stage type.
package pipeline

import "time"

// Record is the unit of data flowing through the pipeline.
//
// The payload is a dynamic map[string]any because the example stages
// (schema validation, field transformation, deduplication) all operate on
// arbitrary, source-defined fields rather than one fixed struct. Wrapping the
// map in a named struct (instead of passing a bare map around) means we can add
// metadata later — source offset, ingestion timestamp, trace IDs — without
// changing a single Stage signature. That extensibility is cheap insurance.
type Record struct {
	Data map[string]any
}

// Clone returns a shallow copy of the record's data. Stages that mutate fields
// should operate on a clone so they don't corrupt the caller's copy — this keeps
// the dead-letter queue able to capture the record exactly as it entered the
// failing stage, and keeps tests deterministic.
func (r Record) Clone() Record {
	d := make(map[string]any, len(r.Data))
	for k, v := range r.Data {
		d[k] = v
	}
	return Record{Data: d}
}

// DeadLetter captures a record that failed in a stage, with enough context to
// debug or replay it: which stage rejected it, the underlying error, and when.
type DeadLetter struct {
	Record Record
	Stage  string
	Err    error
	Time   time.Time
}
