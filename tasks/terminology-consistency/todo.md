# Terminology Consistency Todo

## Goal

Make the current code and live documentation consistently describe the crate as durable event streams for CQRS-style resilient mutation, not as a replay-oriented event store.

## Terminology Contract

- Use **durable event stream** for the high-level product capability.
- Use **EventStream** for the public append/read handle returned by `Partition::open()`.
- Use **EventStreamVersion** for positions inside one opened event stream.
- Use **partition store** for the durable storage selected by namespace and partition key.
- Use **projection** for application-owned read-model updates.
- Avoid **event store**, **EventStore**, and **eventstore** in current code and live documentation.
- Avoid broad **replay** framing unless referring to explicit application recovery behavior; prefer bounded reads, resume, rebuild, or reconnect depending on context.

## Inconsistencies Found

- [x] `CLAUDE.md` still frames the crate as a durable event store and mentions `EventStore`, cross-partition cursors, and event replay.
- [x] `src/lib.rs` exposes an internal module named `eventstore`.
- [x] `src/runtime.rs` rustdoc mentions event store partitions.
- [x] `docs/README.md` links to `tutorial/first-event-store.md`.
- [x] `docs/tutorial/first-event-store.md` and `docs/tutorial/getting-started.md` use event-store wording in headings or intro copy.
- [x] `docs/how-to/handle-concurrency.md` and `docs/how-to/monitor-production.md` use event-store headings.
- [x] `docs/reference/configuration.md` says "The event store" instead of naming the resolver/runtime surfaces.
- [x] `docs/explanation/progress-streaming.md` calls notifications "not a second event store".
- [x] Several docs use "replay" where the current intent is bounded read, rebuild, resume, or reconnect.
- [x] `EventLog` still names the logical append/read layer after the README moved examples toward `stream`.
- [x] `EventLogVersion` still names logical positions after the handle becomes an event stream.
- [x] Live docs still describe `EventLog` as the public API instead of `EventStream`.

## Plan

- [x] Rename the internal module file from `eventstore.rs` to `event_log.rs` and update imports/exports.
- [x] Update live docs and rustdoc to use the terminology contract.
- [x] Rename the first tutorial file away from event-store wording and update links.
- [x] Run grep checks for forbidden terms in current code/live docs.
- [x] Run formatting and test/doc verification.
- [x] Request completion reviewer subagent review before reporting done.
- [x] Rename public logical API from `EventLog` to `EventStream`.
- [x] Rename public logical cursor/version from `EventLogVersion` to `EventStreamVersion`.
- [x] Rename internal module file from `event_log.rs` to `event_stream.rs`.
- [x] Update examples, tests, README, live docs, and context terminology.
- [x] Rename the catalog head schema to `event_stream_head` because it represents the logical stream head.
- [x] Re-run grep checks for stale public logical-layer terms.
- [x] Re-run formatting and test/doc verification.
- [ ] Request completion reviewer subagent review for the new rename.

## Verification

- `rg -n "\beventstore\b|\bEventStore\b|event store|Event Store|events_store|DEFAULT_EVENTS_STORE|with_events_store|first-event-store|\breplay\b|\bReplay\b|replaying|replayed" README.md CLAUDE.md CONTEXT.md docs src examples tests Cargo.toml` returned no matches.
- `cargo fmt --check` passed.
- `cargo check` passed.
- `cargo test` passed.
- `cargo doc --no-deps` passed.
- `cargo clippy --all-targets -- -D warnings` passed.
- `git diff --check` passed.

## Review Notes

- Historical task files still contain old terminology where they describe prior work or obsolete specs. I left them intact because they are task history, not current live docs or public API.
- Completion reviewer found one git bookkeeping issue: renamed replacement files were untracked. I marked the new files with `git add -N` so they appear in `git diff` without staging their contents.
- Completion reviewer warned that renaming reset migration identities changes upgrade behavior for existing local data. The user explicitly said not to add migration complexity, so this task keeps the simple reset-migration model and does not add compatibility plumbing.

## EventStream Follow-up Verification

- `rg -n "\blog\b|\blogs\b|Log\b|Logs\b|EventLog|EventLogVersion|event_log_head|event_log_|event-log|event log|ordered log|let log =|\blog\.|let store = partition.open\(\)|\bstore\.load_after_version|\bstore\.append|eventstore|EventStore|event store|Event Store|\breplay\b|\bReplay\b|replaying|replayed" README.md CLAUDE.md CONTEXT.md docs src examples tests Cargo.toml` returned only `Cargo.toml`'s third-party `test-log` dependency name.
- `cargo fmt --check` passed.
- `cargo check` passed.
- `cargo test` passed.
- `cargo doc --no-deps` passed.
- `cargo clippy --all-targets -- -D warnings` passed.
- `git diff --check` passed.
