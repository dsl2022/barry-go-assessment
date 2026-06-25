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

_(filled in as we build)_

## Task 2 — Workflow Orchestrator Engine

_(filled in as we build)_

## Task 3 — Layered HTTP: Client & Server Middleware

_(filled in as we build)_

---

## Where I used AI

_(running log of AI usage: what I asked, what it produced, what I changed and why)_
