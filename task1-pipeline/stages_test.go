package pipeline

import (
	"context"
	"errors"
	"testing"
)

// These tests exercise each stage in ISOLATION — no Pipeline involved — which is
// the whole point of stages being independently testable.

func TestValidateStage_RequiredAndTypes(t *testing.T) {
	s := NewValidateStage([]string{"id", "email"}, map[string]string{"age": "number"})

	tests := []struct {
		name    string
		data    map[string]any
		wantErr bool
	}{
		{"valid", map[string]any{"id": "1", "email": "a@b.com", "age": 30.0}, false},
		{"missing required", map[string]any{"id": "1"}, true},
		{"wrong type", map[string]any{"id": "1", "email": "a@b.com", "age": "thirty"}, true},
		{"optional typed field absent is fine", map[string]any{"id": "1", "email": "a@b.com"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Process(context.Background(), Record{Data: tc.data})
			if (err != nil) != tc.wantErr {
				t.Fatalf("got err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestTransformStage_RenameAndUppercase(t *testing.T) {
	s := NewTransformStage(map[string]string{"name": "fullName"}, []string{"fullName"})
	in := Record{Data: map[string]any{"name": "ada", "keep": 1}}

	out, err := s.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["fullName"] != "ADA" {
		t.Errorf("rename+uppercase failed: got %v", out.Data["fullName"])
	}
	if _, exists := out.Data["name"]; exists {
		t.Errorf("old field %q should have been removed", "name")
	}
	// The input record must be untouched (clone discipline).
	if in.Data["name"] != "ada" {
		t.Errorf("input record was mutated: %v", in.Data["name"])
	}
}

func TestDedupStage_SkipsDuplicates(t *testing.T) {
	s := NewDedupStage([]string{"id"})
	if err := s.Setup(context.Background()); err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer s.Teardown(context.Background())

	r1 := Record{Data: map[string]any{"id": "x"}}
	r2 := Record{Data: map[string]any{"id": "x"}} // duplicate key
	r3 := Record{Data: map[string]any{"id": "y"}}

	if _, err := s.Process(context.Background(), r1); err != nil {
		t.Fatalf("first record should pass: %v", err)
	}
	if _, err := s.Process(context.Background(), r2); !errors.Is(err, ErrSkip) {
		t.Fatalf("duplicate should return ErrSkip, got %v", err)
	}
	if _, err := s.Process(context.Background(), r3); err != nil {
		t.Fatalf("distinct record should pass: %v", err)
	}
}

func TestDedupStage_SetupRequiresKey(t *testing.T) {
	if err := NewDedupStage(nil).Setup(context.Background()); err == nil {
		t.Fatal("expected error when no key fields configured")
	}
}
