# Fantasy Life — Engineering Assessment

Three Go tasks demonstrating architectural thinking around interfaces, composition,
error isolation, context cancellation, and testability.

| Task | Directory | Summary |
|------|-----------|---------|
| 1. Pluggable Data Processing Pipeline | [`task1-pipeline/`](./task1-pipeline) | Records flow through composable, lifecycle-managed stages with per-record dead-letter handling. |
| 2. Workflow Orchestrator Engine | [`task2-workflow/`](./task2-workflow) | Typed jobs in a dependency DAG with state machines, conditional branches, retries, sub-workflows, and an event bus. |
| 3. Layered HTTP: Client & Server Middleware | [`task3-httpx/`](./task3-httpx) | Composable client decorators (`HttpDoer`) and a server middleware chain, each independently testable. |

See [`DECISIONS.md`](./DECISIONS.md) for the architectural decisions, alternatives considered, and where/how AI was used.

## Running everything

```bash
go build ./...            # build all packages and demo binaries
go test ./... -race       # run the full test suite (race detector on)

go run ./cmd/pipelinedemo # Task 1: config-driven pipeline over valid/invalid/dup records
go run ./cmd/workflowdemo # Task 2: order pipeline — parallel/retry/conditional/sub-workflow
go run ./cmd/httpdemo     # Task 3: server stack -> handler -> client chain -> downstream
```

## The through-line

All three tasks lean on the same small toolkit, applied with deliberately
different judgment each time — which is the point of the set:

- **Interface as the seam + registry for extension** — `Stage` (T1), `Job` (T2),
  `HttpDoer`/`Middleware` (T3). Adding a unit never touches the engine.
- **Composition order is a decision**, made explicit and reversible (T3's chains
  most visibly).
- **Concurrency where it's required, not by reflex** — sequential pipeline (T1)
  vs. concurrent DAG (T2), each justified.
- **Testability by injection** — clocks, RNGs, sleepers, and fakes throughout, so
  every unit is testable in isolation and the suite is deterministic + race-clean.

## Layout

```
.
├── task1-pipeline/   # Task 1 library + tests
├── task2-workflow/   # Task 2 library + tests
├── task3-httpx/      # Task 3 client + server middleware + tests
└── cmd/              # runnable demo binaries (one per task)
```
# barry-go-assessment
