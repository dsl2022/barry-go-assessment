package workflow

import (
	"fmt"
	"time"
)

// JobFactory builds a Job from its config map. A factory typically closes over
// injected dependencies (an HTTP client, a Mailer) that can't live in JSON —
// which is how config-driven workflows still get real, testable behavior.
type JobFactory func(config map[string]any) (Job, error)

// JobRegistry maps job type names to factories. Registering a factory is the
// only step needed to make a new job type usable from configuration — the engine
// and the config types never change.
type JobRegistry struct {
	factories map[string]JobFactory
}

// NewJobRegistry returns an empty registry.
func NewJobRegistry() *JobRegistry {
	return &JobRegistry{factories: make(map[string]JobFactory)}
}

// Register associates a type name with a factory; panics on duplicates
// (programmer error, fail loud at startup).
func (r *JobRegistry) Register(typeName string, f JobFactory) {
	if _, exists := r.factories[typeName]; exists {
		panic(fmt.Sprintf("workflow: job type %q already registered", typeName))
	}
	r.factories[typeName] = f
}

// --- config structs (map 1:1 to JSON/YAML) ---------------------------------

// WorkflowConfig is a serializable workflow definition.
type WorkflowConfig struct {
	Name string      `json:"name"`
	Jobs []JobConfig `json:"jobs"`
}

// JobConfig is one job entry: identity + type + opaque config, plus the graph
// metadata (dependencies, condition, retry).
type JobConfig struct {
	ID        string           `json:"id"`
	Type      string           `json:"type"`
	Config    map[string]any   `json:"config,omitempty"`
	DependsOn []string         `json:"dependsOn,omitempty"`
	When      *ConditionConfig `json:"when,omitempty"`
	Retry     *RetryConfig     `json:"retry,omitempty"`
}

// ConditionConfig is the serializable form of a Condition: match a dependency's
// state, or its output value.
type ConditionConfig struct {
	Job          string `json:"job"`
	State        string `json:"state,omitempty"`
	OutputEquals any    `json:"outputEquals,omitempty"`
}

func (c *ConditionConfig) toCondition() (Condition, error) {
	switch {
	case c.State != "":
		return OnState(c.Job, JobState(c.State)), nil
	case c.OutputEquals != nil:
		return OnOutputEquals(c.Job, c.OutputEquals), nil
	default:
		return nil, fmt.Errorf("condition on job %q must set state or outputEquals", c.Job)
	}
}

// RetryConfig is the serializable form of a RetryPolicy (delays in milliseconds
// so JSON stays human-friendly).
type RetryConfig struct {
	MaxAttempts int  `json:"maxAttempts"`
	BaseDelayMs int  `json:"baseDelayMs"`
	MaxDelayMs  int  `json:"maxDelayMs"`
	Jitter      bool `json:"jitter"`
}

func (c *RetryConfig) toPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: c.MaxAttempts,
		BaseDelay:   time.Duration(c.BaseDelayMs) * time.Millisecond,
		MaxDelay:    time.Duration(c.MaxDelayMs) * time.Millisecond,
		Jitter:      c.Jitter,
	}
}

// Build constructs a validated Workflow from config using the registry.
func (r *JobRegistry) Build(cfg WorkflowConfig) (*Workflow, error) {
	wf := NewWorkflow(cfg.Name)
	for _, jc := range cfg.Jobs {
		f, ok := r.factories[jc.Type]
		if !ok {
			return nil, fmt.Errorf("job %q: unknown type %q", jc.ID, jc.Type)
		}
		job, err := f(jc.Config)
		if err != nil {
			return nil, fmt.Errorf("job %q: %w", jc.ID, err)
		}

		var opts []NodeOption
		if len(jc.DependsOn) > 0 {
			opts = append(opts, DependsOn(jc.DependsOn...))
		}
		if jc.When != nil {
			cond, err := jc.When.toCondition()
			if err != nil {
				return nil, fmt.Errorf("job %q: %w", jc.ID, err)
			}
			opts = append(opts, When(cond))
		}
		if jc.Retry != nil {
			opts = append(opts, WithRetry(jc.Retry.toPolicy()))
		}
		wf.Add(jc.ID, job, opts...)
	}
	if err := wf.Validate(); err != nil {
		return nil, err
	}
	return wf, nil
}
