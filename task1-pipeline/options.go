package pipeline

import "fmt"

// Helpers for interpreting a stage's map[string]any options. They exist because
// options usually arrive from JSON/YAML, where arrays decode to []any, objects
// to map[string]any, and numbers to float64 — so each factory would otherwise
// repeat the same brittle type assertions. Centralizing them keeps factories
// short and gives uniform, position-free error messages.

// optStringSlice reads options[key] as a []string. Missing key => nil, no error
// (the factory decides whether that's acceptable).
func optStringSlice(options map[string]any, key string) ([]string, error) {
	v, ok := options[key]
	if !ok {
		return nil, nil
	}
	raw, ok := v.([]any)
	if !ok {
		// Allow a native []string too (when built programmatically, not via JSON).
		if ss, ok := v.([]string); ok {
			return ss, nil
		}
		return nil, fmt.Errorf("option %q: want array of strings, got %T", key, v)
	}
	out := make([]string, 0, len(raw))
	for i, e := range raw {
		s, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("option %q[%d]: want string, got %T", key, i, e)
		}
		out = append(out, s)
	}
	return out, nil
}

// optStringMap reads options[key] as a map[string]string. Missing key => nil.
func optStringMap(options map[string]any, key string) (map[string]string, error) {
	v, ok := options[key]
	if !ok {
		return nil, nil
	}
	if m, ok := v.(map[string]string); ok { // programmatic path
		return m, nil
	}
	raw, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("option %q: want object, got %T", key, v)
	}
	out := make(map[string]string, len(raw))
	for k, e := range raw {
		s, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("option %q[%q]: want string, got %T", key, k, e)
		}
		out[k] = s
	}
	return out, nil
}
