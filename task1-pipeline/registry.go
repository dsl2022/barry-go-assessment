package pipeline

import "fmt"

// StageConfig is one entry in a pipeline configuration: a stage type name plus
// arbitrary options. It maps directly to a JSON/YAML object, e.g.
//
//	{ "type": "validate", "options": { "required": ["id", "email"] } }
//
// Keeping options as map[string]any (rather than a typed struct per stage) is
// what lets the engine stay ignorant of concrete stages: the config layer only
// knows "type name + opaque options", and each stage's factory interprets its
// own options. That's the contract that makes the system open for extension.
type StageConfig struct {
	Type    string         `json:"type"`
	Options map[string]any `json:"options,omitempty"`
}

// StageFactory builds a Stage from its options. Each concrete stage provides one
// and registers it by name; the engine never imports the stage directly.
type StageFactory func(options map[string]any) (Stage, error)

// Registry maps stage type names to factories. This is the extension point:
// registering a new factory is the *only* thing needed to make a new stage
// usable from configuration — no engine, Pipeline, or Registry changes.
type Registry struct {
	factories map[string]StageFactory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]StageFactory)}
}

// Register associates a type name with a factory. It panics on a duplicate name
// because that's always a programming error (two stages claiming the same type)
// and should fail loudly at startup, not silently shadow.
func (r *Registry) Register(name string, f StageFactory) {
	if _, exists := r.factories[name]; exists {
		panic(fmt.Sprintf("pipeline: stage type %q already registered", name))
	}
	r.factories[name] = f
}

// Build turns an ordered list of StageConfigs into an ordered list of Stages,
// preserving order (pipeline order is significant). An unknown type is a
// configuration error, surfaced with the offending position for debuggability.
func (r *Registry) Build(configs []StageConfig) ([]Stage, error) {
	stages := make([]Stage, 0, len(configs))
	for i, c := range configs {
		f, ok := r.factories[c.Type]
		if !ok {
			return nil, fmt.Errorf("config[%d]: unknown stage type %q", i, c.Type)
		}
		s, err := f(c.Options)
		if err != nil {
			return nil, fmt.Errorf("config[%d] (%s): %w", i, c.Type, err)
		}
		stages = append(stages, s)
	}
	return stages, nil
}

// BuildPipeline is a convenience that builds the stages and wraps them in a
// Pipeline in one step.
func (r *Registry) BuildPipeline(configs []StageConfig, opts ...Option) (*Pipeline, error) {
	stages, err := r.Build(configs)
	if err != nil {
		return nil, err
	}
	return New(stages, opts...), nil
}
