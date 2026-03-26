# Task: Make Same-Window Size-Rotated Partitions Traversable

## Summary

When rotation happens because of `max_bytes`, the new partition keeps the same `start_ms` and gets an alpha suffix: `events_20241002T14.db` → `events_20241002T14_a.db` → `events_20241002T14_b.db`, and so on up to `_z`. `get_next_partition()` previously looked for `start_ms > current_start_ms`, so consumers could not advance from one same-window partition to its immediate successor.

## Severity

High

## Why This Matters

- Size-based rotation is explicitly documented and part of the public configuration surface.
- Global replay stalls at the first sealed partition in a busy time window.
- A high-volume deployment can create unread partitions even though appends continue working.

## Evidence

- `src/eventstore.rs:265` keeps the same `start_ms` for same-window partitions created by size rotation.
- `src/catalog.rs` previously resolved the current partition by its `start_ms` and only searched for `start_ms > current_start_ms`.

## Failure Mode

1. `events_20241002T14.db` fills up and is sealed.
2. The store rotates to `events_20241002T14_a.db` (same `start_ms`, alpha suffix).
3. A projector reaches the end of the sealed first partition.
4. `get_next_partition()` searches for a partition with a strictly larger `start_ms`.
5. `events_20241002T14_a.db` is ignored because it shares the same `start_ms`.

## Fix Applied

- `get_next_partition()` uses `WHERE name > ?1 ORDER BY name LIMIT 1` — partition names sort correctly lexicographically (`events_20241002T14.db` < `events_20241002T14_a.db` < `events_20241002T14_b.db` < `events_20241002T15.db`).
- `get_all_partitions()` and `get_partitions_by_range()` use `ORDER BY start_ms, name`.
- `get_active_partition()` uses `ORDER BY name DESC`.
- `get_next_partition()` validates the current partition exists before querying for the successor (fail-fast on stale checkpoints).
- `initialize_active_partition()` restores the suffix from the partition name on startup instead of hardcoding `None`.

## Acceptance Criteria

- A projector can consume across `events_<window>.db`, `events_<window>_a.db`, and `events_<window>_b.db`.
- `get_next_partition()` returns same-window successors in the correct order.
- Tests cover:
  - base partition to `_a`
  - `_a` to `_b`
  - replay after restart in a multi-partition window
  - suffix correctly restored on startup

## Limitations

The current alpha suffix scheme supports up to 26 same-window rotations (`_a` through `_z`). See `tasks/numeric-ordinal-partition-naming/` for a follow-up that replaces this with padded numeric ordinals to remove the cap.

## Diagram

See `diagram.html` for the current dead end and the target successor chain.
