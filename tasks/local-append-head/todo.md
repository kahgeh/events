# Local Append Head Implementation

## Status

Planning complete. Do not start implementation until this plan is accepted.

## Context

- Spec: `tasks/local-append-head/spec.md`
- Current append path commits event rows, then updates `catalog.db.event_stream_head`.
- Target append path inserts events and advances `event_file_append_head` inside the same active event file DB transaction.
- Catalog remains responsible for `event_file_ranges` and rotated-file traversal.
- No transitional compatibility support is needed for `catalog.db.event_stream_head`; remove the old catalog-head path rather than retaining it as a derived cache.
- The worktree is already dirty. Keep this task scoped to storage/head changes and avoid reverting unrelated user work.

## Implementation Plan

- [x] Confirm baseline behavior and current test state.
  - [x] Added the narrow local-append-head tests first and confirmed they failed against the catalog-head implementation.
  - [x] Captured expected failures: missing `event_file_append_head`, stale catalog head controlling `current_version`, and `event_stream_head` still present.

- [x] Add the event-file append-head storage contract.
  - [x] Extend `partition_migrations()` with `event_file_append_head`.
  - [x] Add helpers to read, initialize, and update the local append head from an event file DB connection.
  - [x] Ensure a newly created first event file starts with `current_version = 0` and `last_event_id = NULL`.
  - [x] Ensure a newly rotated event file starts at the sealed previous head so its first append allocates `N + 1`.

- [x] Move append version allocation into the active event file DB transaction.
  - [x] Change expected-version validation to use the active event file connection/head.
  - [x] Insert events and update `event_file_append_head` before `COMMIT`.
  - [x] Remove the post-commit catalog head update from `append`.
  - [x] Revisit `map_uniqueness_to_incorrect_event_version` so it no longer reads the catalog head.

- [x] Move current-version and rotation decisions off `catalog.db.event_stream_head`.
  - [x] Change `current_version()` to read the active event file's local append head.
  - [x] Change rotation sealing to read the old active file local head.
  - [x] Create the next event file and initialize its append head before switching the in-memory active file.
  - [x] Keep read traversal through `event_file_ranges`.

- [ ] Remove or neutralize catalog head authority.
  - [x] Stop using `Catalog::get_head()` and `Catalog::update_head()` from append, expected-version validation, current-version reporting, and rotation.
  - [x] Remove `event_stream_head` from the live catalog migration if no remaining runtime code needs it.
  - [x] Remove exported `EventStreamHead`; do not add a compatibility/cache replacement.
  - [x] Narrow or remove `CatalogDrift` if it no longer describes an append-head failure.

- [x] Add catalog routing reconciliation.
  - [x] On open, validate active range count and rebuild from event file local heads when no active range exists.
  - [x] If no active range exists but event files exist, rebuild ranges and select the latest file as active.
  - [x] If multiple unsealed ranges exist, rebuild deterministically from event files.
  - [x] Make repair behavior deterministic and covered by tests before allowing writes.
  - [x] Add a focused test for multiple unsealed range repair if needed after broader test review.

- [ ] Add focused tests.
  - [x] Append commits events and local head together.
  - [x] Missing local-head row prevents partial event inserts.
  - [x] `ExpectedVersion` and `current_version()` ignore or remove stale catalog-head remnants.
  - [x] Rotation seals the old range using the old local append head and initializes the new local head.
  - [x] Reads still traverse sealed plus active event files in order.
  - [x] Reopen after stale or incomplete catalog routing metadata does not reuse versions.
  - [ ] Optional ignored stress test: process kill during single event-file append preserves `MAX(events.version) == event_file_append_head.current_version`.

- [x] Update docs after code behavior is verified.
  - [x] Update architecture docs to describe event-file-local append head and catalog routing.
  - [x] Update schema/migration docs.
  - [x] Update error docs so `CatalogDrift` no longer claims committed events lost their stream head, or remove that guidance if the error goes away.
  - [x] Grep for stale claims that `catalog.db.event_stream_head` is authoritative.

- [x] Verify.
  - [x] `cargo fmt --check`
  - [x] `cargo check`
  - [x] `cargo test`
  - [x] `git diff --check`
  - [x] Stale-term grep for `event_stream_head`, `CatalogDrift`, and append-authoritative catalog wording.
  - [x] Completion reviewer subagent review before reporting done, unless explicitly skipped.

## Verification Notes

- Added `tests/local_append_head_tests.rs` before implementation and confirmed the first four tests failed on the existing catalog-head design.
- `cargo test --test local_append_head_tests` now passes with seven local append-head tests.
- `cargo test` passes.
- `cargo fmt --check` passes.
- `cargo check` passes.
- `git diff --check` passes.
- Stale-term grep has no runtime or docs hits for `CatalogDrift` or append-authoritative catalog-head wording. Remaining `event_stream_head` hits are intentional: migration cleanup with `DROP TABLE IF EXISTS event_stream_head` and tests that prove stale remnants do not control appends.
- Completion reviewer was skipped because the user explicitly said no review was needed in this thread.

## Initial Implementation Order

1. Add local append-head schema/helpers.
2. Move append and expected-version validation to local head.
3. Move current-version and rotation to local head.
4. Add reconciliation for catalog routing.
5. Update tests.
6. Update docs and error wording.

## Known Risks

- Rotation still writes both event file metadata and catalog routing, so reconciliation must be deterministic before writes continue.
- Existing docs and tests may already be in flux in the dirty worktree; avoid sweeping unrelated doc changes into this task.
- Removing exported catalog head types may be a public API break, and that is acceptable for this task. Do not retain compatibility types solely to preserve the old catalog-head contract.
