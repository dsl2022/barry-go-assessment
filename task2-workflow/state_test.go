package workflow

import (
	"errors"
	"testing"
)

func TestStateMachine_ValidTransitionsOnly(t *testing.T) {
	m := newJobMachine()

	if err := m.To(JobRunning); err != nil {
		t.Fatalf("pending->running should be allowed: %v", err)
	}
	if err := m.To(JobSucceeded); err != nil {
		t.Fatalf("running->succeeded should be allowed: %v", err)
	}
	if !m.IsTerminal() {
		t.Fatal("succeeded should be terminal")
	}
	// An illegal transition out of a terminal state must be refused.
	if err := m.To(JobRunning); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("succeeded->running should be invalid, got %v", err)
	}
}

func TestStateMachine_IllegalJump(t *testing.T) {
	m := newJobMachine()
	// pending -> succeeded is not allowed (must go through running).
	if err := m.To(JobSucceeded); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("pending->succeeded should be invalid, got %v", err)
	}
}
