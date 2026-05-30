# Docs Diataxis Streamline

- [x] Review the current docs index and page inventory.
- [x] Classify each page by Diataxis quadrant and reader job.
- [x] Identify streamlining options and risks.
- [x] Record a recommendation before implementation.

## Current Shape

- Tutorials: originally 3 pages. `getting-started.md` and `first-durable-stream.md` overlapped heavily around opening a partition, appending, and reading.
- How-to: originally 8 pages. This was the busiest quadrant, and several pages included explanation/reference material instead of staying task-first.
- Explanation: 4 pages. These are the right place for architecture, cursor semantics, partitioning, and progress streaming, but some of the same concepts are repeated in how-tos.
- Reference: 4 pages. This quadrant already has places for API, configuration, errors, and performance facts that can absorb lookup-style material from long how-tos.

## Streamlining Options

1. Merge the two beginner tutorials.
   - Keep `tutorial/getting-started.md` as the single first-run tutorial.
   - Fold the useful `ExpectedVersion::Exact` and workflow metadata steps from `tutorial/first-durable-stream.md` into it or link to the relevant how-tos.
   - Remove `first-durable-stream.md` from the docs index after inbound links are checked.

2. Make how-tos task-shaped.
   - `how-to/configure-rotation.md` should focus on choosing and applying `RotationPolicy`, then link to `reference/configuration.md` and `reference/performance.md` for limits, cache knobs, and performance notes.
   - `how-to/migrate-schema.md` should focus on the production migration workflow. Schema names and crate-owned table facts should move or link to `reference/api.md` / source rather than duplicating reference.
   - `how-to/monitor-production.md` should become a checklist-style operating guide. Detailed performance mechanics should link to `reference/performance.md`.

3. Consolidate handler guidance.
   - Remove `tutorial/building-projections.md`; the page was too short and procedural to justify a separate tutorial.
   - Use `how-to/implement-event-handlers.md` for the processing unit that can append with expected versions, update read-model projections, and resume resilient workflows.
   - Keep `worker-pool-over-per-partition-store.md` as the focused scaling how-to for running handlers across partition keys.

4. Preserve the explanation pages but reduce repeated summary material in how-tos.
   - Conceptual sections like "Understanding Rotation Policies" and "Understanding Migration Architecture" should either be one short orientation paragraph or link to explanation/reference pages.

## Recommendation

Start with a conservative pass that changes structure, not technical meaning:

1. Update `docs/README.md` to make recommended reader paths explicit: "new user", "implementing event handlers", "operating production", "looking up API/config". Done.
2. Merge or remove `tutorial/first-durable-stream.md` after moving any unique content. Done: `ExpectedVersion::Exact` and workflow metadata moved into `tutorial/getting-started.md`; the duplicate page was deleted.
3. Remove `how-to/migrate-schema.md`. Done: crate-owned schema migration is automatic when partition stores are opened, so a migration how-to is not a reader task.
4. Trim `configure-rotation`, `monitor-production`, and the consolidated event-handler how-to so each starts with the task, keeps steps/checklists, and links out for concepts or lookup facts. Done.
5. Recheck inbound links with `rg` and a link-existence pass before deleting any page. Done.
6. Consolidate `use-expected-version.md`, `implement-projection.md`, and `recover-workflows.md` into `how-to/implement-event-handlers.md`. Done: "event handler" is now the umbrella term for the processing code; "projection" means building a read model/view; "workflow" means a checkpointed multi-step process that can continue after disruption.
7. Fold `tutorial/building-projections.md` into `how-to/implement-event-handlers.md`. Done: the simple projection loop now lives in the handler how-to, and `getting-started.md` is the only tutorial.
8. Add `events_dev_cli schema app` so the docs' generated application SQL command exists. Done.
9. Simplify generated application SQL around serial handler processing. Done:
   - [x] Keep `last_processed_event` as the default processing checkpoint.
   - [x] Remove the separate active workflow table from generated SQL and docs.
   - [x] Include `workflow_failures` as optional operator-visible retry state alongside the checkpoint table.
   - [x] Remove the separate idempotency ledger table from generated SQL and docs.
   - [x] Re-run Rust, CLI, docs, and stale-term verification.

This should reduce page count by one and reduce the long how-to surface without losing the Diataxis quadrants.

## Verification Performed

- `find docs -maxdepth 3 -type f | sort` to inventory pages.
- `rg -n "^(#|##|###) " docs/...` to inspect document shapes.
- `wc -l docs/...` to identify oversized pages.
- Sampled the highest-risk tutorials and how-tos for overlap.
- `rg -n "migrate-schema|Migrate schema|first-durable-stream|First durable stream" docs README.md` found no live references after deletion.
- `rg -n "use-expected-version|implement-projection|recover-workflows|Use ExpectedVersion|Implement projection|Recover workflows" docs README.md` found no live references after consolidating event-handler docs.
- `rg -n "building-projections|Building projections|Building Projections" docs README.md` found no live references after folding the tutorial into the handler how-to.
- `cargo check --bin events_dev_cli` passed.
- `cargo run --quiet --bin events_dev_cli -- schema app` printed the application-owned SQL.
- `cargo run --quiet --bin events_dev_cli -- schema app --table last-processed-event` and `--table workflow-failures` printed individual table SQL after the application schema update.
- `cargo fmt --check` passed.
- `cargo test` passed.
- `cargo clippy --all-targets -- -D warnings` passed.
- `cargo doc --no-deps` passed.
- `git diff --check` passed.
- Stale table-name grep over `README.md`, `docs`, `src`, and `tasks/planning` found no removed active-workflow, idempotency-ledger, handled-events, or partition-offset table names.
- A markdown link-existence pass over `docs/**/*.md` found no broken local links.
- Completion reviewer initially rejected the change for stale README wording, unsafe multiple-read-model checkpoint wording, a missing negative schema test, and an ambiguous workflow cursor snippet. Fixed all four and reran verification.
- Second completion review rejected the workflow cursor snippet because `EventStreamVersion::new(0)` is invalid, and warned that external side effects cannot commit atomically with the checkpoint. Fixed the snippet to map `0` to `EventStreamVersion::start()` and clarified idempotency/outbox guidance.
- Third completion review found two adjacent examples passing the stored checkpoint integer directly to `load_after_version`. Fixed both examples to normalize `0` to `EventStreamVersion::start()` first.
- Clarified that a missing `last_processed_event` row also means `EventStreamVersion::start()`, so applications do not need to pre-initialize checkpoint rows before the first handler pass.
- Added the missing workflow failure lifecycle explanation: `is_retriable` is the default retry policy, `reset_at_ms > failed_at_ms` is the operator override, and a new failure clears the reset timestamp.
- Corrected reset semantics: the operator resets `is_retriable` so the recorded failure can be retried again; `reset_at_ms` records when that reset occurred.
- Removed independent-handler checkpoint extension guidance so docs do not encourage adding handler/projection dimensions to the default `last_processed_event` schema.
- Replaced order-flavored progress examples with `clients` / `client-a` so namespace and partition-key roles are clearer.
- Clarified that `StreamEvent.stream_id` is progress context metadata, often namespace plus partition key or workflow run context, not the durable event ID.
- Added `NotificationMaintenanceOptions` and `EventsRuntime::start_notification_maintenance_worker` so applications can start cleanup with an explicit cleanup interval and explicit shutdown wiring.
