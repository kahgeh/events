# Docs Content Refresh

## Goal

Keep the existing documentation structure, but refresh the content so it matches the current one-log-per-partition-store implementation. Work through documents one at a time, starting with the architecture and partitioning explanations because those define the vocabulary used by the rest of the docs.

## Evidence To Check

- [x] Inventory current docs and task notes.
- [x] Verify terminology against `CONTEXT.md`, current public API, and storage schema for the architecture pass.
- [x] Verify rotation/file naming details against implementation.
- [x] Verify examples compile after affected doc changes when examples are used as evidence.

## Document Pass Order

- [x] `docs/explanation/architecture.md`
- [ ] `docs/explanation/partitioning-strategy.md`
- [ ] `docs/explanation/cursor-mechanism.md`
- [ ] `docs/explanation/concurrency-control.md`
- [ ] `docs/how-to/worker-pool-over-per-partition-store.md`
- [ ] Remaining how-to/tutorial/reference docs that repeat partition/rotation/workflow terminology.

## Review Notes

- Keep the current Diataxis-style structure and file locations.
- Rename the public root manager to `EventNamespaces`.
- Add `EventNamespace` as the named namespace scope under the root manager.
- Keep `Partition` as the public reference to one selected partition.
- Rename the opened append/read API to `EventLog`.
- Rename the public cursor/version type to `EventLogVersion`.
- Rename internal catalog concepts from event-log language to event-log language: `event_log_head` and `event_file_ranges`.
- Make `EventLog` mean the application-facing ordered log API that abstracts physical rotation.
- Make "rotation" mean physical event database file rollover behind an `EventLog`.
- Make "workflow" identity separate from partition identity and file rotation.
- Avoid returning to tenant-specific language as the primary teaching model.
- Update `CONTEXT.md` to match the final vocabulary once code and docs are aligned.

## Verification

- [x] Run markdown/link/text consistency checks after edits.
- [x] Run `cargo test --no-run` or a narrower compile check if examples or public API references are changed materially.
- [x] Record final review and verification result here before reporting done.

### Architecture Pass

- Source vocabulary checked against `CONTEXT.md`.
- Public API and schema checked against `src/partitions.rs`, `src/eventstore.rs`, and `src/migration.rs`.
- Updated stale architecture wording so partition store, owner partition strategy, event log, rotation, and workflow started-by event ID are separated.
- Added missing `trace_id` and `span_id` fields to the architecture SQL snippet.
- Verification: `git diff --check -- docs/explanation/architecture.md tasks/docs-content-refresh/todo.md` passed; stale-term scan for tenant/stream/workflow-id phrasing returned no matches in `docs/explanation/architecture.md`.

### Naming Alignment Pass

- Renamed public API types:
  - `EventPartitions` to `EventNamespaces`
  - added `EventNamespace`
  - `OwnerEventStore` to `EventLog`
  - `OwnerLogVersion` to `EventLogVersion`
- Renamed catalog-facing concepts:
  - `OwnerLogHead` to `EventLogHead`
  - `PartitionRef` to `EventFileRange`
  - catalog table `owner_log` to `event_log_head`
  - catalog table `partitions` to `event_file_ranges`
- Updated `CONTEXT.md`, README, docs, examples, tests, and runtime accessors to the new vocabulary.
- Verification:
  - `cargo check`
  - `cargo test --no-run`
  - `cargo check --examples`
  - `cargo test`
  - `cargo clippy --all-targets --all-features`
  - `git diff --check -- README.md CONTEXT.md docs src examples tests tasks/docs-content-refresh/todo.md`
  - stale-name scan for `EventPartitions`, `OwnerEventStore`, `OwnerLogVersion`, `OwnerLogHead`, `PartitionRef`, `ensure_exists(`, and `event_partitions(`
  - markdown link existence script over README and docs
