# Events One Stream Per Store Todo

## Status

Implemented. Spec source: `tasks/events-one-stream-per-store/spec.md`.

## Phase 0 - Baseline And Compile Map

- [x] Run `cargo test --no-run` to capture current compile/test baseline.
- [x] Identify all public exports that must be removed or renamed:
  - [x] `EventStore`
  - [x] `PartitionedCursor`
  - [x] `ProjectorBatchOutcome`
  - [x] `process_next_batch_with_handler`
  - [x] `bootstrap_cursor`
  - [x] `checkpoint`
  - [x] `get_active_workflow`
  - [x] `load_since_event`
  - [x] `all_since`
  - [x] `all_since_with_positions`
- [x] Decide final file/module placement before editing:
  - [x] `src/eventstore.rs` for `OwnerEventStore`
  - [x] `src/partitions.rs` or equivalent for `EventPartitions`, `Partition`, `PartitionDescriptor`
  - [x] `src/catalog.rs` for owner-log catalog and rotated file ranges
  - [x] `src/migration.rs` for new schemas

## Phase 1 - Public Type Surface

- [x] Add `OwnerLogVersion` newtype.
  - [x] `OwnerLogVersion::start()` is the only before-first cursor.
  - [x] `OwnerLogVersion::new(0)` is invalid.
  - [x] `ExpectedVersion::Exact` cannot accept `OwnerLogVersion::start()`.
- [x] Update `ExpectedVersion` to use `OwnerLogVersion`.
  - [x] Keep `NoStream`.
  - [x] Keep `Any` as blind append after current owner-log head.
  - [x] Keep `Exact(OwnerLogVersion)` for OCC.
- [x] Add workflow input types.
  - [x] `WorkflowRef::None`
  - [x] `WorkflowRef::StartsThisWorkflow`
  - [x] `WorkflowRef::Continues { started_by_event_id }`
- [x] Update `NewEvent`.
  - [x] Remove owner identity from event input.
  - [x] Add optional `workflow_kind`.
  - [x] Add `workflow: WorkflowRef`.
- [x] Update `EventEnvelope`.
  - [x] Remove per-event owner identity and `stream_id`.
  - [x] Include `version: OwnerLogVersion`.
  - [x] Include resolved workflow metadata.
- [x] Update `AppendResult`.
  - [x] `first_version: OwnerLogVersion`
  - [x] `last_version: OwnerLogVersion`
  - [x] `events: Vec<EventEnvelope>`

## Phase 2 - Partition Manager

- [x] Add partition key validation.
  - [x] Lowercase ASCII letters, digits, and `-` only.
  - [x] Length `1..=128`.
  - [x] Reject all other characters, including `_`, uppercase, `.`, `/`, spaces, and email-like IDs.
- [x] Add `EventPartitions`.
  - [x] `EventPartitions::open(root, rotation_policy)`.
  - [x] One `RotationPolicy` per manager.
  - [x] Public cache knobs: `max_open_stores`, `idle_store_ttl`.
  - [x] No read-limit config knob.
- [x] Add `Partition`.
  - [x] Returned by `ensure_exists(namespace, partition_key)`.
  - [x] Represents existing storage, not open DB handle.
  - [x] `Partition::open() -> OwnerEventStore`.
- [x] Add namespace listing.
  - [x] `list(namespace)` is shallow.
  - [x] Only immediate child directories are considered.
  - [x] Files are ignored.
  - [x] Invalid child directory names are skipped.
  - [x] Results sorted by partition key ascending.
  - [x] Listing does not open or migrate owner stores.

## Phase 3 - Storage Schema And Catalog

- [x] Replace row-level stream storage.
  - [x] Remove event-row `stream_id`.
  - [x] Replace `UNIQUE(stream_id, version)` with owner-log `UNIQUE(version)`.
  - [x] Add `workflow_kind`.
  - [x] Add `workflow_started_by_event_id`.
  - [x] Add workflow index: `(workflow_started_by_event_id, version)`.
- [x] Replace catalog stream heads.
  - [x] Remove many-stream `stream_heads`.
  - [x] Add one owner-log head record.
  - [x] Track current owner-log version, last event ID, and active partition.
- [x] Add rotated file range catalog.
  - [x] Partition DB name/path.
  - [x] `first_version`.
  - [x] nullable `last_version`.
  - [x] sealed flag.
  - [x] Ensure ranges are contiguous and non-overlapping.
- [x] Remove events-owned consumer checkpoints.
  - [x] Remove `consumer_offsets`.
  - [x] Remove `workflow_stream_id`.
  - [x] Remove `workflow_event_id`.

## Phase 4 - OwnerEventStore Behavior

- [x] Rename/rework `EventStore` into `OwnerEventStore`.
  - [x] No public `EventStore` compatibility alias.
  - [x] Append/load methods do not accept `stream_id`.
- [x] Implement append.
  - [x] Validate event payloads and actor/request fields.
  - [x] Validate workflow metadata shape.
  - [x] Validate `workflow_kind` lowercase letters/digits/hyphen.
  - [x] Generate event IDs before insert.
  - [x] Resolve `StartsThisWorkflow` to `workflow_started_by_event_id = id`.
  - [x] Keep batch append atomic.
  - [x] Return stored envelopes in `AppendResult`.
- [x] Implement owner-log reads.
  - [x] `load_after_version(cursor, limit)`.
  - [x] Cursor is exclusive.
  - [x] `OwnerLogVersion::start()` returns event version `1`.
  - [x] Validate `1 <= limit <= internal MAX_READ_LIMIT`.
  - [x] Traverse rotated DB files using catalog ranges.
- [x] Implement workflow reads.
  - [x] `load_workflow_after_version(workflow_started_by_event_id, cursor, limit)`.
  - [x] Cursor is exclusive.
  - [x] Same internal read limit as owner-log reads.
  - [x] Unknown workflow anchor returns empty batch.

## Phase 5 - Projector API Removal

- [x] Remove public finite-batch projector API.
  - [x] Remove `ProjectorBatchOutcome`.
  - [x] Remove public `process_next_batch_with_handler`.
  - [x] Remove public events-owned checkpoint helpers.
- [x] Keep only helpers that still fit owner-local reads, or make them private/test-only.
- [x] Ensure app-owned batching can use `load_after_version` without crate checkpoint state.

## Phase 6 - Examples And Documentation

- [x] Replace `examples/tenant_projectors.rs`.
  - [x] Add `examples/partition_worker_pool.rs`.
  - [x] Demonstrate `ensure_exists -> open -> append`.
  - [x] Demonstrate application-owned offsets.
  - [x] Demonstrate active/pending bounded worker pool.
  - [x] Include workflow metadata in at least one event.
- [x] Replace `docs/how-to/partition-by-tenant.md`.
  - [x] Add `docs/how-to/worker-pool-over-per-partition-store.md`.
- [x] Update public docs for new API.
  - [x] `README.md`
  - [x] `docs/README.md`
  - [x] `docs/reference/api.md`
  - [x] `docs/reference/sql-schema.md`
  - [x] `docs/explanation/architecture.md`
  - [x] `docs/explanation/concurrency-control.md`
  - [x] `docs/how-to/recover-workflows.md`
- [x] Remove obsolete checkpoint/global cursor documentation.

## Phase 7 - Tests

- [x] Replace integration tests around streams/global cursors with owner-log tests.
- [x] Add tests for `OwnerLogVersion`.
  - [x] `start()`.
  - [x] `new(0)` invalid.
  - [x] `Exact(start())` invalid or unrepresentable.
- [x] Add partition manager tests.
  - [x] valid safe keys.
  - [x] invalid safe keys.
  - [x] `ensure_exists` creates storage.
  - [x] listing is shallow and sorted.
  - [x] listing skips files and invalid children.
- [x] Add append/read tests.
  - [x] first append version `1`.
  - [x] `NoStream`, `Any`, `Exact`.
  - [x] cross-owner isolation.
  - [x] batch append ordering.
  - [x] rotated file traversal.
  - [x] exclusive `load_after_version`.
  - [x] read limit validation.
- [x] Add workflow tests.
  - [x] workflow metadata validation.
  - [x] `StartsThisWorkflow` returns generated starter ID.
  - [x] `Continues` stores supplied starter ID.
  - [x] interleaved workflow recovery by starter event ID.
  - [x] unknown workflow anchor returns empty batch.
- [x] Add application-owned projection offset example/test where practical.

## Phase 8 - Verification

- [x] `cargo fmt`
- [x] `cargo test --no-run`
- [x] `cargo test`
- [x] `cargo run --example partition_worker_pool`
- [x] `cargo clippy --all-targets -- -D warnings`
- [ ] If workspace integration is updated in same branch:
  - [ ] `cargo test -p practice-management`
  - [ ] `cargo check --workspace --features "greenfields-of-cambridge/design-system"`
  - [ ] `make test`
- [x] Run a final grep for removed public surfaces.
  - [x] `ProjectorBatchOutcome`
  - [x] `process_next_batch_with_handler`
  - [x] `PartitionedCursor`
  - [x] `consumer_offsets`
  - [x] `stream_heads`
  - [x] public `EventStore`

## Notes

- Do not preserve compatibility for the existing public API.
- Keep `OwnerEventStore` as the opened handle name.
- Keep storage keys readable and safe: lowercase ASCII letters, digits, and `-` only.
- Keep Turso terminology precise; do not refer to Turso DB as SQLite or libSQL.

## Review

- `cargo fmt --check` passed after the implementation.
- `cargo test --no-run` passed after examples/tests/docs updates.
- `cargo test` passed: 33 unit tests, 9 integration tests, and 3 doc tests.
- `cargo run --example partition_worker_pool` passed and drained three owner-log events through the application-owned worker pool.
- Removed-surface grep passed for `ProjectorBatchOutcome`, `process_next_batch_with_handler`, `PartitionedCursor`, `bootstrap_cursor`, `get_active_workflow`, `load_since_event`, `all_since`, `consumer_offsets`, `stream_heads`, and public `EventStore`.
- `cargo clippy --all-targets -- -D warnings` passed.

## Completion Reviewer Follow-Up

- Fixed catalog drift handling so committed events with failed head updates return `CatalogDrift`.
- Removed fixed 512-partition workflow traversal cap.
- Replaced edited-in-place migration SQL with new destructive owner-log reset migration names.
- Made `OwnerEventStore::open_partitioned` crate-private so normal callers go through `EventPartitions`.
- Added public `with_max_open_stores` and `with_idle_store_ttl` resolver cache knobs.
- Replaced worker-pool tenant wording with neutral owner wording.

## Documentation Follow-Up

- Restored Diataxis shape for the owner-log API docs:
  - explanation pages keep problem context, architecture, and flow diagrams
  - how-to pages keep prerequisites, step-by-step procedures, and verification
  - reference pages keep complete API/config/error/performance details
- Reframed owner partitioning as an optional scaling feature, with a plain
  single-partition usage path.
- Verified docs do not mention unsupported public surfaces such as
  `EventStore`, `PartitionedCursor`, `ProjectorBatchOutcome`, or
  `process_next_batch_with_handler`.
- Re-ran `cargo fmt --check`, `cargo doc --no-deps`, `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, and `git diff --check`.
- Completion review found and fixed remaining docs issues:
  - `implement-projection.md` and `recover-workflows.md` now have explicit
    `Prerequisites`, `Steps`, and `Verification` sections.
  - `error-types.md` lists every public `EsError` variant.
  - `api.md` table of contents links to `Error Handling`.
