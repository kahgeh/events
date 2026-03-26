# Task: Fix Global Cursor Ordering and Event Loss in `all_since()`

## Summary

`all_since()` currently advances projector cursors using `(created_at, id)`, while `append()` assigns the same timestamp to every event in a batch and uses random UUIDv4 values for `id`. That makes same-millisecond ordering unstable and can permanently skip events that arrive later in the same millisecond with a lexicographically smaller UUID.

## Severity

Critical

## Why This Matters

- CQRS projectors depend on a stable, replayable global order.
- The current cursor key can reorder events from the same append batch.
- Under load, later appends in the same millisecond can be lost forever from `all_since()` consumers.

## Evidence

- `src/eventstore.rs:349` captures a single `now_ms` per append call.
- `src/eventstore.rs:499` assigns the same `created_at` to every event in the batch.
- `src/eventstore.rs:789` checkpoints the last seen `(created_at, id)`.
- `src/eventstore.rs:866` queries forward using `(created_at > ts) OR (created_at = ts AND id > cursor_id)`.

## Failure Mode

1. Append a batch with events `v1` and `v2`.
2. Both rows get the same `created_at`.
3. Global reads sort them by random UUID instead of append order or stream version.
4. A projector checkpoints the highest UUID seen for that millisecond.
5. A later append in the same millisecond with a smaller UUID is now behind the cursor and never replayed.

## Desired Outcome

- Global replay is deterministic.
- Same-batch order is preserved.
- Same-millisecond appends cannot fall behind the cursor.

## Suggested Direction

- Introduce a monotonic ordering key for global scans.
- Make the cursor advance on that monotonic key instead of UUID lexical order.
- Ensure the key is assigned at insert time and is stable across restarts and partition traversal.
- Add regression tests for same-batch ordering and same-millisecond concurrent appends.

## Acceptance Criteria

- `all_since()` returns events in a deterministic order that matches persisted append order.
- No event can be skipped because its UUID sorts below the current cursor.
- A batch append of multiple events is replayed in the original append order.
- Tests cover:
  - multi-event single-batch append
  - multiple appends in the same millisecond
  - cursor resume after checkpoint

## Diagram

See `diagram.html` for a visual of the current failure mode and the target model.
