package pipeline

// RegisterBuiltins registers the three concrete stages shipped with this package.
// It's the one place that wires stage names to factories — and the template for
// adding your own: write a Stage + a StageFactory, then r.Register("name", fn).
// Nothing in the engine, Pipeline, or Registry changes.
func RegisterBuiltins(r *Registry) {
	r.Register("validate", validateFactory)
	r.Register("transform", transformFactory)
	r.Register("dedup", dedupFactory)
}

// NewDefaultRegistry returns a Registry pre-loaded with the built-in stages.
func NewDefaultRegistry() *Registry {
	r := NewRegistry()
	RegisterBuiltins(r)
	return r
}
