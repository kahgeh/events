# Task: Make Same-Window Size-Rotated Partitions Traversable

## Summary

When rotation happens because of `max_bytes`, the new partition keeps the same `start_ms` and only changes the same-window ordinal in the filename, for example `events_20241002T14_000001.db`, `events_20241002T14_000002.db`, and so on. `get_next_partition()` currently looks for `start_ms > current_start_ms`, so consumers cannot advance from one same-window partition to its immediate successor.

## Severity

High

## Why This Matters

- Size-based rotation is explicitly documented and part of the public configuration surface.
- Global replay stalls at the first sealed partition in a busy time window.
- A high-volume deployment can create unread partitions even though appends continue working.

## Evidence

- `src/eventstore.rs:265` keeps the same `start_ms` for same-window partitions created by size rotation.
- `src/catalog.rs:228` resolves the current partition by its `start_ms`.
- `src/catalog.rs:462` only searches for the next partition where `start_ms > current_start_ms`.

## Failure Mode

1. `events_20241002T14_000001.db` fills up and is sealed.
2. The store rotates to `events_20241002T14_000002.db`.
3. A projector reaches the end of the sealed first partition.
4. `get_next_partition()` searches for a partition with a strictly larger `start_ms`.
5. `events_20241002T14_000002.db` is ignored because it shares the same `start_ms`.

## Desired Outcome

- Partition traversal respects the real ordering of same-window partitions within the same time window.
- Consumers can move from `_000001` to `_000002`, `_000003`, and onward without manual repair.

## Suggested Direction

- Introduce an ordering dimension beyond `start_ms`, such as same-window ordinal or an insertion sequence.
- Use that ordering consistently in:
  - `get_next_partition()`
  - `get_all_partitions()`
  - any cursor repair or replay logic
- Add tests that force size-based rotation and verify replay across same-window ordinal partitions.

## Acceptance Criteria

- A projector can consume across `events_<window>_000001.db`, `events_<window>_000002.db`, and `events_<window>_000003.db`.
- `get_next_partition()` returns same-window successors in the correct order.
- Tests cover:
  - `_000001` to `_000002`
  - `_000002` to `_000003`
  - replay after restart in a multi-partition window

## Diagram

See `diagram.html` for the current dead end and the target successor chain.
