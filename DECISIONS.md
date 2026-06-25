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

**Shape:** typed jobs are composed into a dependency DAG. The engine runs one
goroutine per node; each waits on its dependencies' completion channels, applies
a condition gate, then runs the job with retry. State machines guard every
transition; a lifecycle event bus decouples logging/metrics/notifications.

### Key decisions

1. **Concurrent engine — the deliberate opposite of Task 1.** Independent
   branches run in parallel goroutines. Here concurrency is *required by the
   spec* ("a failed job must not halt unrelated branches" only means something if
   branches run independently), whereas in Task 1 it was unjustified. Same
   engineer, opposite choice, for stated reasons.

2. **Goroutine-per-node + done-channels, no central scheduler.** Each node
   goroutine blocks on `<-done[dep]` for each dependency, then runs. Ordering,
   parallelism, failure isolation, and cancellation all *emerge* from these
   channel waits + a `select` on `ctx.Done()` — there's no scheduler loop to get
   wrong. *Alternative rejected:* a tick-based "find ready nodes" scheduler — more
   state, more bug surface, no benefit at this scale.

3. **"Typed jobs" = one `Job` interface + a shared result store.** All jobs
   implement `Execute(ctx, *ExecutionContext)`; they exchange data through a
   concurrency-safe blackboard keyed by job ID. *Alternative rejected:* generic
   `Job[In,Out]` — a heterogeneous DAG erases to `any` at graph boundaries
   anyway, so generics would add wiring ceremony for safety the graph can't keep.
   (Note the contrast: I *did* use generics for the state machine, where they
   remove real duplication — pattern where it helps, not everywhere.)

4. **Explicit state machines, valid transitions only.** A generic table-driven
   `machine[S]` enforces `Pending→Running→{Succeeded,Failed,Cancelled}` (jobs
   also have `Skipped`). Illegal transitions return `ErrInvalidTransition` rather
   than silently corrupting state.

5. **`Skipped` is distinct from `Failed`.** A job whose condition is unmet (or
   whose upstream didn't succeed) is `Skipped` — it never ran. A job that ran and
   errored is `Failed`. Conflating them would break conditional routing and make
   "did this fail?" unanswerable. Skipped jobs don't fail the workflow.

6. **Failure isolation falls out of the condition rule.** Default gate = "all
   deps Succeeded". So a failed job makes its dependents Skip, while branches not
   depending on it run untouched. `Run` returns an error only on validation
   failure — a job failing is a normal outcome in `RunResult`, never a `Run`
   error. That makes "doesn't halt the run" the default, not an opt-in.

7. **Conditional execution via a `Condition` interface.** `OnState`/
   `OnOutputEquals` read the blackboard; a node with no condition uses the safe
   default. Conditions are an interface (not bare funcs) so they self-describe in
   logs and serialize from JSON.

8. **Sub-workflows via the composite pattern.** `SubWorkflowJob` *is* a `Job`
   wrapping a `Workflow`, run on the **same engine** — which is why the engine
   holds no per-run state (it's reentrant). The sub-workflow shares the parent's
   event bus, so its events surface in the same stream.

9. **Event bus = observer pattern, fully decoupled.** The engine only calls
   `bus.Publish`. Subscribers (logging/metrics/notification) are added from
   outside and know nothing of the engine. Publish is synchronous + mutex-guarded
   (events arrive from many goroutines); an async/buffered bus is a drop-in
   behind the same interface if needed.

10. **Per-job retry with injectable sleeper/RNG.** Exponential backoff + cap +
    optional full jitter. The sleeper and RNG are injected so tests make retries
    instant and deterministic while still honoring cancellation during backoff.

11. **Registry + JSON: structure vs. behavior split.** Config declares the graph
    (deps/conditions/retry); registered factories supply behavior and inject
    non-serializable dependencies (HTTP client, Mailer). That's how config-driven
    workflows still get real, testable jobs.

### Testing strategy
State-machine unit tests (valid + illegal transitions); engine tests for linear
ordering, **failure isolation** (failed branch + independent branch), **conditional
routing** (run-on-failure vs run-on-success), retry-then-succeed and
retry-exhausted (attempt counts + event counts), **graceful shutdown** (Cancelled
not Failed, propagates to dependents), and DAG validation (cycle/unknown-dep/
duplicate). Job types tested in isolation with fakes; sub-workflow tested
end-to-end. **Entire suite passes under `go test -race`** — the key signal for the
concurrent engine.

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

### Task 2
- **Used it for:** scaffolding the engine's goroutine/channel plumbing, the
  generic state machine, the config structs, and the bulk of the table-driven
  tests once I'd settled the design.
- **What I directed / changed:**
  - Drove the **concurrent goroutine-per-node + done-channel** model and the
    explicit contrast with Task 1's sequential choice; rejected a central
    scheduler loop.
  - Settled the "typed jobs" interpretation (shared result store, not generics)
    and, separately, *did* introduce generics for the state machine where they
    remove real duplication.
  - Insisted `Skipped` be a first-class state distinct from `Failed`, and that
    `Run` not return an error on job failure (failure isolation as the default).
  - Required the whole concurrent suite to pass under `-race` before trusting it.

_(Task 3 AI notes added as we build it.)_
