// Command pipelinedemo runs the Task 1 pipeline end-to-end from a JSON config,
// over a small in-memory dataset containing valid, invalid, and duplicate
// records, then prints the run statistics and the dead-letter queue.
//
//	go run ./cmd/pipelinedemo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	pipeline "github.com/2015rpro/fantasy-assessment/task1-pipeline"
)

// configJSON shows the pluggable-via-configuration story: the stage list and
// their options are pure data. Adding a stage here requires no code changes as
// long as its type is registered.
const configJSON = `[
  { "type": "validate",  "options": { "required": ["id", "email"], "types": { "id": "string" } } },
  { "type": "transform", "options": { "rename": { "email": "contact" }, "uppercase": ["name"] } },
  { "type": "dedup",     "options": { "key": ["id"] } }
]`

func main() {
	var configs []pipeline.StageConfig
	if err := json.Unmarshal([]byte(configJSON), &configs); err != nil {
		log.Fatalf("parse config: %v", err)
	}

	reg := pipeline.NewDefaultRegistry()
	p, err := reg.BuildPipeline(configs)
	if err != nil {
		log.Fatalf("build pipeline: %v", err)
	}

	src := pipeline.NewSliceSource([]pipeline.Record{
		{Data: map[string]any{"id": "1", "email": "ada@x.com", "name": "ada"}},
		{Data: map[string]any{"id": "2", "email": "grace@x.com", "name": "grace"}},
		{Data: map[string]any{"id": "1", "email": "dup@x.com", "name": "dup"}},    // duplicate id -> skipped
		{Data: map[string]any{"id": "3", "name": "missing email"}},                // invalid -> dead-letter
		{Data: map[string]any{"id": 4, "email": "n@x.com", "name": "wrong type"}}, // id not string -> dead-letter
	})
	sink := &pipeline.SliceSink{}

	stats, err := p.Run(context.Background(), src, sink)
	if err != nil {
		log.Fatalf("run: %v", err)
	}

	fmt.Printf("stats: read=%d written=%d skipped=%d deadlettered=%d\n",
		stats.Read, stats.Written, stats.Skipped, stats.DeadLettered)

	fmt.Println("\nsurviving records:")
	for _, r := range sink.Records {
		fmt.Printf("  %v\n", r.Data)
	}

	fmt.Println("\ndead letters:")
	for _, dl := range p.DLQ().(*pipeline.MemoryDLQ).Items() {
		fmt.Printf("  stage=%-9s err=%-28q record=%v\n", dl.Stage, dl.Err.Error(), dl.Record.Data)
	}
}
