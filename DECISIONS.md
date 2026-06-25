# Architectural Decisions

This document records the key design choices for each task, the alternatives I
considered and rejected, and where I used AI (and what I changed from its output).
It is written as a running log so the reasoning tracks the commit history.

## Repository-wide decisions

### Single Go module, one package per task
- **Decision:** One `go.mod` at the root; each task is a self-contained package
  (`task1-pipeline/`, `task2-workflow/`, `task3-httpx/`) with demos under `cmd/`.
- **Why:** The deliverable is explicitly *one repository* of related-but-independent
  work. A single module makes `go build ./...` / `go test ./...` cover everything in
  one command, and lets the three tasks share a consistent design vocabulary without
  copy-paste. Separate modules only pay off when units are versioned or released
  independently or have conflicting dependencies — none of which applies here.
  Choosing *not* to split is a deliberate "don't over-engineer" signal.
- **Trade-off / mitigation:** A single module means tasks *could* import each other
  and accidentally couple. I keep each task's package tree self-contained and do not
  import across tasks, treating the package boundary as a hard wall.
- **Rejected:** Three separate modules (needless `go.work` ceremony, harder to run);
  a single flat package for all three (no encapsulation, no isolation testing story).

---

## Task 1 — Pluggable Data Processing Pipeline

**Shape:** records are pulled from a `Source`, passed through an ordered list of
`Stage`s, and either written to a `Sink` or routed to a `DeadLetterQueue`. The
engine depends only on interfaces, so adding a stage requires zero engine changes.

### Key decisions

1. **`Stage` interface is the architecture.** The engine references only
   `Stage` (Name/Setup/Process/Teardown), never a concrete type. This is what
   makes stages pluggable and independently testable. *Alternative rejected:* a
   big switch over stage types in the engine — closed for extension.

2. **`Record` = `map[string]any` wrapped in a struct.** The example stages
   (validate/transform/dedup) operate on arbitrary fields, so a fixed struct
   would impose a schema the engine shouldn't own. Wrapping the map (vs. a bare
   map) lets us add metadata later without changing any `Stage` signature.
   *Alternative rejected:* generics (`Pipeline[T]`) — a transform changes record
   shape and dedup/validate over arbitrary `T` need awkward constraints; type
   safety the problem doesn't need at the cost of flexibility it does.

3. **`ErrSkip` sentinel for intentional drops.** A duplicate is not a failure,
   so it must not be dead-lettered. `Process` returns `ErrSkip` (matched with
   `errors.Is`), mirroring how `io.EOF` signals control flow through the error
   channel. *Alternative rejected:* a 3-value return `(Record, keep bool, error)`
   — forces every stage to reason about `keep` though only filters ever drop.

4. **Sequential engine, not a concurrent fan-out.** The requirements ask for
   pluggability, per-record error isolation, lifecycle, and graceful shutdown —
   none of which need concurrency, and two of which (dead-letter ordering, the
   stateful dedup set) concurrency would complicate. Choosing a readable
   sequential loop is the deliberate "don't over-engineer" call. *Extension
   path:* the `Source`/`Stage`/`Sink` interfaces are unchanged by swapping the
   loop for a bounded worker pool; `MemoryDLQ` is already mutex-guarded for that.

5. **Per-record vs. fatal errors are separate channels.** A bad record →
   dead-letter queue, loop continues. A bad `Setup`/`Source`/`Sink` → `Run`
   returns an error (the whole run is doomed). `Run`'s error return never means
   "one record was bad."

6. **Teardown is guaranteed and reverse-order.** Setup runs forward; a Setup
   failure tears down already-set-up stages and aborts (never run half-resourced).
   On every exit path `defer` tears down in reverse (like nested defers).
   Teardown is best-effort so one stage's failed cleanup can't strand another's.

7. **Config-driven composition via `Registry` + `StageFactory`.** Registering a
   factory by name is the *only* step to make a new stage usable from JSON/YAML
   config. `Register` panics on duplicate names (programmer error → fail loud);
   `Build` errors on unknown types (config error → fail soft).

8. **Functional options + injectable clock.** `New(stages)` stays a one-liner;
   custom DLQ/clock are opt-in `Option`s. Injectable `now` keeps dead-letter
   timestamps deterministic in tests.

### Testing strategy
Stage tests run each stage with no engine (true isolation). Orchestration tests
use a `spyStage` to assert lifecycle ordering and a `failOnFieldStage` to prove
a bad record is isolated to the DLQ while neighbors still flow. `cmd/pipelinedemo`
runs the whole thing from a JSON config over valid/invalid/duplicate records.

## Task 2 — Workflow Orchestrator Engine

_(filled in as we build)_

## Task 3 — Layered HTTP: Client & Server Middleware

_(filled in as we build)_

---

## Where I used AI

I used Claude (Claude Code) as a pair partner throughout. My role was to drive the
architecture and interrogate its output, not to accept it wholesale. A running log:

### Task 1
- **Used it for:** generating the boilerplate of the `Stage`/`Source`/`Sink`
  implementations, the option-coercion helpers, and the first pass of tests once
  I'd decided the interfaces.
- **What I directed / changed:**
  - Chose the **sequential** engine over the AI's instinct to reach for a
    channel-per-stage concurrent pipeline — it would have complicated dead-letter
    ordering and the dedup state for no required benefit.
  - Insisted on the `ErrSkip` sentinel (vs. a 3-value `Process` return) so the
    common stage signature stays clean.
  - Made dead letters capture the record *as it entered the failing stage*, and
    the timestamp clock injectable, for debuggability and deterministic tests.
  - Required the teardown-on-setup-failure path and a test that asserts it.

_(Tasks 2 and 3 AI notes added as we build them.)_
