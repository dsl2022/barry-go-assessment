package workflow

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRegistry_BuildFromJSONAndRun(t *testing.T) {
	const cfgJSON = `{
      "name": "json-wf",
      "jobs": [
        { "id": "seed", "type": "const", "config": { "value": 10 } },
        { "id": "double", "type": "double", "dependsOn": ["seed"],
          "when": { "job": "seed", "state": "succeeded" } }
      ]
    }`

	var cfg WorkflowConfig
	if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
		t.Fatalf("parse config: %v", err)
	}

	reg := NewJobRegistry()
	// "const" emits a configured value; note the JSON number arrives as float64.
	reg.Register("const", func(c map[string]any) (Job, error) {
		v := c["value"]
		return JobFunc(func(ctx context.Context, ec *ExecutionContext) (any, error) {
			return v, nil
		}), nil
	})
	// "double" transforms its single dependency's output.
	reg.Register("double", func(c map[string]any) (Job, error) {
		return &TransformJob{Source: "seed", Fn: func(in any) (any, error) {
			return in.(float64) * 2, nil
		}}, nil
	})

	wf, err := reg.Build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	res, err := testEngine().Run(context.Background(), wf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.State != WorkflowSucceeded {
		t.Fatalf("state = %s, want succeeded", res.State)
	}
	if res.Results["double"].Output != float64(20) {
		t.Errorf("double output = %v, want 20", res.Results["double"].Output)
	}
}

func TestRegistry_UnknownType(t *testing.T) {
	reg := NewJobRegistry()
	_, err := reg.Build(WorkflowConfig{Name: "x", Jobs: []JobConfig{{ID: "a", Type: "ghost"}}})
	if err == nil {
		t.Fatal("expected unknown-type error")
	}
}
