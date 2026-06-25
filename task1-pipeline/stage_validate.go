package pipeline

import (
	"context"
	"fmt"
)

// ValidateStage enforces a lightweight schema: required fields must be present,
// and (optionally) named fields must have an expected primitive type. A record
// that violates the schema is returned with an error, so the engine routes it to
// the dead-letter queue — exactly the behavior we want for bad input: drop it
// from the happy path, but keep it (with context) for inspection or replay.
//
// This stage is stateless, so Setup/Teardown are no-ops; it still implements
// them to satisfy the Stage contract.
type ValidateStage struct {
	required []string
	types    map[string]string // field -> "string" | "number" | "bool"
}

// NewValidateStage builds a validation stage.
func NewValidateStage(required []string, types map[string]string) *ValidateStage {
	return &ValidateStage{required: required, types: types}
}

func (s *ValidateStage) Name() string                       { return "validate" }
func (s *ValidateStage) Setup(ctx context.Context) error    { return nil }
func (s *ValidateStage) Teardown(ctx context.Context) error { return nil }

// Process validates the record without mutating it.
func (s *ValidateStage) Process(ctx context.Context, rec Record) (Record, error) {
	for _, field := range s.required {
		if _, ok := rec.Data[field]; !ok {
			return Record{}, fmt.Errorf("missing required field %q", field)
		}
	}
	for field, want := range s.types {
		v, ok := rec.Data[field]
		if !ok {
			continue // presence is the job of `required`; type-check only what exists
		}
		if !matchesType(v, want) {
			return Record{}, fmt.Errorf("field %q: want type %s, got %T", field, want, v)
		}
	}
	return rec, nil
}

// matchesType reports whether v matches a primitive type name. Numbers are
// checked loosely because JSON decodes every number to float64; this keeps the
// validator usable whether records come from JSON or are built in Go.
func matchesType(v any, want string) bool {
	switch want {
	case "string":
		_, ok := v.(string)
		return ok
	case "bool":
		_, ok := v.(bool)
		return ok
	case "number":
		switch v.(type) {
		case int, int32, int64, float32, float64:
			return true
		}
		return false
	default:
		return false
	}
}

// validateFactory builds a ValidateStage from config options.
func validateFactory(options map[string]any) (Stage, error) {
	required, err := optStringSlice(options, "required")
	if err != nil {
		return nil, err
	}
	types, err := optStringMap(options, "types")
	if err != nil {
		return nil, err
	}
	return NewValidateStage(required, types), nil
}
