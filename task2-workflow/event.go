package workflow

import (
	"sync"
	"time"
)

// EventType enumerates the lifecycle events the engine emits. They're strings so
// they serialize cleanly into logs/metrics and so new event types can be added
// without a coordinated change across subscribers.
type EventType string

const (
	EventWorkflowStarted   EventType = "workflow.started"
	EventWorkflowCompleted EventType = "workflow.completed"
	EventJobStarted        EventType = "job.started"
	EventJobSucceeded      EventType = "job.succeeded"
	EventJobFailed         EventType = "job.failed"
	EventJobRetrying       EventType = "job.retrying"
	EventJobSkipped        EventType = "job.skipped"
	EventJobCancelled      EventType = "job.cancelled"
)

// Event is a single lifecycle notification. It carries enough context for any
// subscriber to do its job (log a line, bump a counter, send an alert) without
// calling back into the engine.
type Event struct {
	Type      EventType
	Workflow  string
	JobID     string // empty for workflow-level events
	State     JobState
	Attempt   int    // 1-based attempt number for job run/retry events
	Err       error  // set on failure events
	Time      time.Time
	WorkflowState WorkflowState // set on workflow-level events
}

// Subscriber consumes events. It's a one-method interface so subscribers stay
// trivial to write and test — the observer pattern in its simplest useful form.
type Subscriber interface {
	OnEvent(e Event)
}

// SubscriberFunc adapts a function to the Subscriber interface.
type SubscriberFunc func(e Event)

// OnEvent implements Subscriber.
func (f SubscriberFunc) OnEvent(e Event) { f(e) }

// EventBus fans events out to all registered subscribers. Publish is safe to
// call from many job goroutines concurrently (RWMutex), and dispatch is
// synchronous: this keeps event ordering observable and tests deterministic.
// Subscribers are expected to be fast and non-blocking; an async/buffered bus
// would be a drop-in change behind this same interface if one became slow.
type EventBus struct {
	mu   sync.RWMutex
	subs []Subscriber
}

// NewEventBus creates a bus with an optional initial set of subscribers.
func NewEventBus(subs ...Subscriber) *EventBus {
	return &EventBus{subs: subs}
}

// Subscribe registers an additional subscriber.
func (b *EventBus) Subscribe(s Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, s)
}

// Publish delivers an event to every subscriber synchronously.
func (b *EventBus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, s := range b.subs {
		s.OnEvent(e)
	}
}
