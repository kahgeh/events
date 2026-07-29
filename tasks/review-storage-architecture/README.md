# Task: Review Storage Architecture

## Summary

The crate is at a design fork.

Option A keeps the current partition-per-file model on the write path and hardens it with explicit write-failure handling.

Option B moves the active write path into a single transactional database and treats physical partition export as an asynchronous archival concern.

This task is to compare both designs directly and decide which one should be the long-term storage architecture.

## Decision Drivers

- optimistic concurrency correctness
- write-path simplicity
- recovery model clarity
- startup cost
- operational availability
- storage growth behavior
- export and archival needs
- implementation and migration cost

## Option A: Partition-Per-File Write Path

### Shape

- active events are written directly into the current partition file
- catalog metadata lives in a separate database
- `stream_heads` is published after the partition commit
- incomplete writes are tracked explicitly in `pending_appends`
- if head publication fails after commit, the store becomes write-unhealthy until recovery resolves the intent

### Strengths

- preserves the existing physical partition model
- sealed partitions are already isolated as standalone files
- historical retention and file-level movement are straightforward
- active dataset can remain physically bounded by rotation policy
- no background exporter is required to get partition files

### Tradeoffs

- append is still a cross-database protocol, not a single atomic transaction
- correctness depends on a protocol:
  - create intent
  - commit partition write
  - publish head
  - resolve intent
- requires an unhealthy latch and explicit operator-visible recovery behavior
- failure handling is more complex because partial success is expected
- rotation remains part of the hot write path
- testing burden is higher because more interleavings must be covered

### Best Case

- strong correctness with explicit recovery
- good fit if standalone partition files are a hard requirement

### Worst Case

- more moving pieces in the write path
- more protocol state to reason about than a single-DB append

## Option B: Single Hot Transactional Writer DB

### Shape

- active writes go to one transactional database
- `events`, `stream_heads`, and partition metadata live in the same DB
- partitions are logical rows or IDs, not separate write-time files
- append uses one transaction:
  1. validate stream head
  2. insert events
  3. update stream head
  4. commit
- sealed logical partitions are exported later by a background archival process

### Strengths

- true atomic append plus head publication
- simpler correctness model for OCC
- no `pending_appends` protocol on the normal write path
- no scan-or-reconcile startup path for incomplete cross-DB writes
- rotation becomes metadata, not file creation plus cross-DB coordination
- fewer edge cases around restart and same-window rotation

### Tradeoffs

- physical partition files are no longer produced directly by the writer
- requires a separate exporter or archival process for cold storage
- storage cleanup becomes its own lifecycle problem
- hot DB size can grow if export or cleanup falls behind
- exporter and garbage collection need verification and operational controls
- migration from the current design is larger

### Best Case

- simplest mental model for correctness and concurrency
- best fit if correctness and operational predictability matter more than direct file partition output

### Worst Case

- background archival becomes a new subsystem
- delayed cleanup can turn into a storage-capacity problem

## Availability Considerations

### Option A

- no exporter is needed for normal operation
- availability risk is concentrated in the append protocol itself
- if the store fails closed correctly, the expected failure mode is write rejection until recovery completes

### Option B

- availability does not need to depend on the exporter if archival is fully asynchronous
- the expected failure mode for exporter problems is storage growth, not append unavailability
- availability risk appears if compaction or deletion is coupled too tightly to the hot DB

## Suggested Evaluation Criteria

- Can the system tolerate a fail-closed write state after partial success?
- Is direct file-per-partition output a hard product requirement or just a current implementation choice?
- How much operator tooling do we want for recovery and archival?
- Is the team more willing to own protocol complexity on the write path or background maintenance complexity off the write path?
- Which failure mode is more acceptable:
  - temporary write rejection until reconciliation
  - storage growth while archival catches up

## Suggested Outcome

Choose Option A if physical partition files on the primary write path are a hard requirement.

Choose Option B if the main priority is making append correctness structurally simple and moving complexity out of the hot path.

## Acceptance Criteria

- The task concludes with one selected target architecture.
- The chosen design has:
  - an explicit write protocol
  - a defined crash-recovery story
  - a defined rotation or archival story
  - a defined operator failure mode
- Follow-up implementation tasks are derived from the chosen option.

## Diagram

See `diagram.html` for a side-by-side comparison of the two storage architectures.
