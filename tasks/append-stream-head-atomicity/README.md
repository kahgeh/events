# Task: Make Append and Stream-Head Updates Atomic Enough for OCC

## Summary

`append()` commits events to the active partition database before it updates `stream_heads` in the catalog. The optimistic concurrency check also reads `stream_heads` before the write lock and is not revalidated inside the critical section. That combination can return incorrect errors and leave durable data behind a stale catalog head.

## Severity

Critical

## Why This Matters

- The crate claims optimistic concurrency control.
- The authoritative event log and the version index can diverge.
- Callers can receive `Err` even though the write already committed.
- Later appends may observe stale versions and fail in surprising ways.

## Evidence

- `src/eventstore.rs:423` reads the current stream version from `stream_heads`.
- `src/eventstore.rs:367` starts the write path after that read has already happened.
- `src/eventstore.rs:386` commits the partition transaction before catalog repair.
- `src/eventstore.rs:394` updates `stream_heads` afterward.
- `src/migration.rs:179` only enforces `(stream_id, version)` uniqueness inside one partition file.

## Failure Mode

1. Writer A and writer B both read version 5 from `stream_heads`.
2. Writer A commits version 6.
3. Writer B enters the write path with stale version knowledge.
4. The loser does not reliably become `EsError::Concurrency`; it can become a raw DB uniqueness error instead.
5. If the catalog update fails after commit, the event is durable but the head remains stale and the caller sees an error.

## Desired Outcome

- Expected-version checks are revalidated inside the actual write critical section.
- The system never reports a failed append after the event is already durably committed without an explicit repair path.
- Catalog state cannot move backward or remain stale indefinitely.

## Suggested Direction

- Recheck the stream head under the same write coordination mechanism used for the append.
- Treat `stream_heads` updates as part of a recoverable write protocol, not a best-effort follow-up.
- Guard head updates so older versions cannot overwrite newer ones.
- Add recovery or reconciliation for catalog drift if cross-database atomicity is impossible.

## Acceptance Criteria

- Concurrent stale writers deterministically return `EsError::Concurrency`.
- A committed append cannot leave `stream_heads` in an older version without repair.
- Tests cover:
  - two concurrent writers with the same expected version
  - catalog update failure after partition commit
  - cross-partition rotation during append

## Diagram

See `diagram.html` for a comparison of the current split write path and a safer target protocol.
