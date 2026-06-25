package workflow

import (
	"fmt"
	"io"
	"sync"
)

// The three subscribers below are the pluggable consumers the brief calls for
// (logging, metrics, notifications). Each is independently testable and knows
// nothing about the engine — they only react to Events.

// LoggingSubscriber writes a human-readable line per event to an io.Writer.
type LoggingSubscriber struct {
	w io.Writer
}

// NewLoggingSubscriber writes events to w.
func NewLoggingSubscriber(w io.Writer) *LoggingSubscriber {
	return &LoggingSubscriber{w: w}
}

// OnEvent implements Subscriber.
func (l *LoggingSubscriber) OnEvent(e Event) {
	switch {
	case e.JobID == "":
		fmt.Fprintf(l.w, "[workflow %s] %s state=%s\n", e.Workflow, e.Type, e.WorkflowState)
	case e.Err != nil:
		fmt.Fprintf(l.w, "[workflow %s] %s job=%s attempt=%d err=%v\n", e.Workflow, e.Type, e.JobID, e.Attempt, e.Err)
	default:
		fmt.Fprintf(l.w, "[workflow %s] %s job=%s attempt=%d\n", e.Workflow, e.Type, e.JobID, e.Attempt)
	}
}

// MetricsSubscriber counts events by type. It's concurrency-safe because events
// arrive from multiple job goroutines at once.
type MetricsSubscriber struct {
	mu     sync.Mutex
	counts map[EventType]int
}

// NewMetricsSubscriber returns an empty metrics collector.
func NewMetricsSubscriber() *MetricsSubscriber {
	return &MetricsSubscriber{counts: make(map[EventType]int)}
}

// OnEvent implements Subscriber.
func (m *MetricsSubscriber) OnEvent(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[e.Type]++
}

// Count returns how many events of a type were observed.
func (m *MetricsSubscriber) Count(t EventType) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[t]
}

// NotificationSubscriber invokes a notifier callback for events matching a
// predicate (e.g. only failures). This models "send a Slack/email alert" while
// staying trivially testable: the test passes a notifier that appends to a slice.
type NotificationSubscriber struct {
	when     func(Event) bool
	notifier func(Event)
}

// NewNotificationSubscriber notifies via notifier for events where when(e) is true.
func NewNotificationSubscriber(when func(Event) bool, notifier func(Event)) *NotificationSubscriber {
	return &NotificationSubscriber{when: when, notifier: notifier}
}

// OnEvent implements Subscriber.
func (n *NotificationSubscriber) OnEvent(e Event) {
	if n.when(e) {
		n.notifier(e)
	}
}
