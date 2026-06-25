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
go build ./...   # build all packages and demo binaries
go test ./...    # run the full test suite
```

## Layout

```
.
├── task1-pipeline/   # Task 1 library + tests
├── task2-workflow/   # Task 2 library + tests
├── task3-httpx/      # Task 3 client + server middleware + tests
└── cmd/              # runnable demo binaries (one per task)
```
