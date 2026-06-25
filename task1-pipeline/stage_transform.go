package pipeline

import (
	"context"
	"strings"
)

// TransformStage performs simple field transformations: renaming fields and
// uppercasing string fields. It's intentionally modest — the point is to show
// the *mutation* path cleanly, not to build an expression language.
//
// Crucially it operates on rec.Clone(), never the input map, so a transform can
// never corrupt an upstream caller's copy or a record already captured in the
// dead-letter queue. Stateless, so Setup/Teardown are no-ops.
type TransformStage struct {
	rename    map[string]string // oldField -> newField
	uppercase []string          // fields whose string value is upper-cased
}

// NewTransformStage builds a transform stage.
func NewTransformStage(rename map[string]string, uppercase []string) *TransformStage {
	return &TransformStage{rename: rename, uppercase: uppercase}
}

func (s *TransformStage) Name() string                       { return "transform" }
func (s *TransformStage) Setup(ctx context.Context) error    { return nil }
func (s *TransformStage) Teardown(ctx context.Context) error { return nil }

// Process returns a transformed clone of the record.
func (s *TransformStage) Process(ctx context.Context, rec Record) (Record, error) {
	out := rec.Clone()
	for oldName, newName := range s.rename {
		if v, ok := out.Data[oldName]; ok {
			out.Data[newName] = v
			delete(out.Data, oldName)
		}
	}
	for _, field := range s.uppercase {
		if v, ok := out.Data[field]; ok {
			if str, ok := v.(string); ok {
				out.Data[field] = strings.ToUpper(str)
			}
		}
	}
	return out, nil
}

// transformFactory builds a TransformStage from config options.
func transformFactory(options map[string]any) (Stage, error) {
	rename, err := optStringMap(options, "rename")
	if err != nil {
		return nil, err
	}
	uppercase, err := optStringSlice(options, "uppercase")
	if err != nil {
		return nil, err
	}
	return NewTransformStage(rename, uppercase), nil
}
