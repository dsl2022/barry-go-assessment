package pipeline

import (
	"context"
	"fmt"
	"strings"
)

// DedupStage drops records it has already seen, keyed by a configured set of
// fields. It's the stage that justifies the lifecycle hooks and ErrSkip:
//
//   - Setup allocates the "seen" set (a resource with a lifetime tied to the run).
//   - Process returns ErrSkip for a duplicate — not an error, so it's dropped
//     from the happy path without being dead-lettered.
//   - Teardown releases the set so a reused stage instance starts clean.
//
// The first time a key is seen the record passes through; subsequent identical
// keys are skipped. This stage is stateful and, by itself, not safe for
// concurrent Process calls — a fact the sequential engine relies on, and which a
// future concurrent engine would address by guarding `seen` with a mutex.
type DedupStage struct {
	keyFields []string
	seen      map[string]struct{}
}

// NewDedupStage builds a dedup stage keyed on the given fields (order matters,
// since the key is their concatenation).
func NewDedupStage(keyFields []string) *DedupStage {
	return &DedupStage{keyFields: keyFields}
}

func (s *DedupStage) Name() string { return "dedup" }

// Setup allocates the seen-set. Done here (not in the constructor) so the
// resource lifecycle is explicit and a stage can be set up/torn down repeatedly.
func (s *DedupStage) Setup(ctx context.Context) error {
	if len(s.keyFields) == 0 {
		return fmt.Errorf("dedup: at least one key field is required")
	}
	s.seen = make(map[string]struct{})
	return nil
}

// Teardown frees the seen-set.
func (s *DedupStage) Teardown(ctx context.Context) error {
	s.seen = nil
	return nil
}

// Process passes a record through the first time its key is seen, and returns
// ErrSkip for any later record with the same key.
func (s *DedupStage) Process(ctx context.Context, rec Record) (Record, error) {
	key := s.key(rec)
	if _, dup := s.seen[key]; dup {
		return Record{}, ErrSkip
	}
	s.seen[key] = struct{}{}
	return rec, nil
}

// key builds a composite key from the configured fields. Missing fields are
// rendered as a distinct "<nil>" token so two records that both omit a field
// collide deterministically rather than accidentally differing.
func (s *DedupStage) key(rec Record) string {
	parts := make([]string, len(s.keyFields))
	for i, f := range s.keyFields {
		if v, ok := rec.Data[f]; ok {
			parts[i] = fmt.Sprintf("%v", v)
		} else {
			parts[i] = "<nil>"
		}
	}
	// \x1f (unit separator) avoids collisions between e.g. {"a","bc"} and {"ab","c"}.
	return strings.Join(parts, "\x1f")
}

// dedupFactory builds a DedupStage from config options.
func dedupFactory(options map[string]any) (Stage, error) {
	key, err := optStringSlice(options, "key")
	if err != nil {
		return nil, err
	}
	return NewDedupStage(key), nil
}
