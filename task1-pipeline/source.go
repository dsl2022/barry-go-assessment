package pipeline

import (
	"context"
	"io"
)

// Source streams records into the pipeline using a pull model: Next returns the
// next record, or io.EOF when exhausted.
//
// A pull model (vs. exposing a <-chan Record) is deliberate: it makes
// backpressure trivial — the engine pulls only when ready, so a slow stage
// naturally throttles the source with no buffering logic — and it makes sources
// dead simple to fake in tests. The context lets a blocking source (network,
// queue) abort mid-read on shutdown.
type Source interface {
	Next(ctx context.Context) (Record, error)
}

// Sink receives records that survive every stage. Keeping output behind an
// interface (rather than returning a giant slice) means the pipeline can stream
// to a file, queue, or DB without buffering the whole result set in memory.
type Sink interface {
	Write(ctx context.Context, rec Record) error
}

// SliceSource serves records from an in-memory slice — the canonical test/demo
// source.
type SliceSource struct {
	records []Record
	i       int
}

// NewSliceSource builds a Source over the given records.
func NewSliceSource(records []Record) *SliceSource {
	return &SliceSource{records: records}
}

// Next returns the next record or io.EOF. It also honors context cancellation so
// it behaves like a real (blocking) source under graceful shutdown.
func (s *SliceSource) Next(ctx context.Context) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if s.i >= len(s.records) {
		return Record{}, io.EOF
	}
	r := s.records[s.i]
	s.i++
	return r, nil
}

// SliceSink collects surviving records in memory — the canonical test/demo sink.
type SliceSink struct {
	Records []Record
}

// Write appends the record to the in-memory slice.
func (s *SliceSink) Write(ctx context.Context, rec Record) error {
	s.Records = append(s.Records, rec)
	return nil
}
