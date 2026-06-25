package workflow

import "fmt"

// Node is one job in a workflow plus its graph metadata: what it depends on,
// the condition that gates it, and its retry policy.
type Node struct {
	ID        string
	Job       Job
	DependsOn []string
	Condition Condition // nil => default: run iff all dependencies Succeeded
	Retry     RetryPolicy
}

// Workflow is a named DAG of nodes. It's built via the chainable Add API (or via
// the JSON registry, see registry.go) and validated before execution.
type Workflow struct {
	Name     string
	nodes    map[string]*Node
	order    []string // insertion order, for deterministic iteration/reporting
	buildErr error    // first error encountered while building (surfaced by Validate)
}

// NewWorkflow creates an empty workflow.
func NewWorkflow(name string) *Workflow {
	return &Workflow{Name: name, nodes: make(map[string]*Node)}
}

// NodeOption configures a node when it's added. Functional options keep Add's
// common form — Add(id, job) — clean while supporting deps/conditions/retry.
type NodeOption func(*Node)

// DependsOn declares the IDs this node depends on.
func DependsOn(ids ...string) NodeOption {
	return func(n *Node) { n.DependsOn = append(n.DependsOn, ids...) }
}

// When sets the condition gating this node.
func When(c Condition) NodeOption {
	return func(n *Node) { n.Condition = c }
}

// WithRetry sets the node's retry policy.
func WithRetry(p RetryPolicy) NodeOption {
	return func(n *Node) { n.Retry = p }
}

// Add registers a job under id with optional configuration. It's chainable so a
// whole workflow reads as a fluent definition. A duplicate id is recorded as a
// build error rather than panicking, so callers can keep chaining and get a
// single, clear failure from Validate.
func (w *Workflow) Add(id string, job Job, opts ...NodeOption) *Workflow {
	if _, exists := w.nodes[id]; exists {
		if w.buildErr == nil {
			w.buildErr = fmt.Errorf("duplicate job id %q", id)
		}
		return w
	}
	n := &Node{ID: id, Job: job}
	for _, o := range opts {
		o(n)
	}
	w.nodes[id] = n
	w.order = append(w.order, id)
	return w
}

// Nodes returns nodes in insertion order.
func (w *Workflow) Nodes() []*Node {
	out := make([]*Node, 0, len(w.order))
	for _, id := range w.order {
		out = append(out, w.nodes[id])
	}
	return out
}

// Validate checks the workflow is a runnable DAG: no build errors, every
// dependency refers to a real node, and there are no cycles. Validating up front
// means the engine can assume a well-formed graph and never deadlock on a cycle.
func (w *Workflow) Validate() error {
	if w.buildErr != nil {
		return w.buildErr
	}
	for _, n := range w.nodes {
		for _, dep := range n.DependsOn {
			if _, ok := w.nodes[dep]; !ok {
				return fmt.Errorf("job %q depends on unknown job %q", n.ID, dep)
			}
		}
	}
	return w.checkAcyclic()
}

// checkAcyclic runs a depth-first search with three colors (white/grey/black) and
// reports the first back-edge as a cycle. Standard, O(V+E), and gives a useful
// error naming the node where the cycle was detected.
func (w *Workflow) checkAcyclic() error {
	const (
		white = 0 // unvisited
		grey  = 1 // on the current DFS stack
		black = 2 // fully explored
	)
	color := make(map[string]int, len(w.nodes))

	var visit func(id string) error
	visit = func(id string) error {
		color[id] = grey
		for _, dep := range w.nodes[id].DependsOn {
			switch color[dep] {
			case grey:
				return fmt.Errorf("dependency cycle detected at job %q", dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}

	for _, id := range w.order {
		if color[id] == white {
			if err := visit(id); err != nil {
				return err
			}
		}
	}
	return nil
}
