# Task: Replace Alpha Suffixes with Padded Numeric Ordinals

## Summary

Same-window size-rotated partitions currently use single-letter alpha suffixes (`_a` through `_z`), capping the system at 26 rotations per time window. Replace this with zero-padded numeric ordinals (e.g. `_000001`, `_000002`, ...) to remove the hard cap, make same-window ordering explicit and scalable in the filename, and align the naming scheme with high-volume production requirements.

## Severity

Medium

## Why This Matters

- **Removes the 26-rotation cap.** Under the current scheme, `get_next_suffix(Some('z'))` returns `None`, which causes `determine_new_partition_info` to error with "Exhausted suffixes for current time window". A workload that exceeds roughly `27 * max_bytes` in a single time window will fail to rotate and block all appends.
- **Makes ordering explicit.** Numeric ordinals (`_000001` < `_000002`) are unambiguous and scale indefinitely. Alpha suffixes require context to know that `_a` < `_b` and that the base (no suffix) partition comes first.
- **Aligns docs and implementation.** Task descriptions and architecture notes reference padded ordinals as the target; the implementation should match.

## Current State

- `src/rotation.rs`: `get_next_suffix()` increments a single `char` from `'a'` to `'z'`, returning `None` at `'z'`.
- `src/rotation.rs`: `generate_partition_name()` formats suffixes as `events_<label>_<char>.db`.
- `src/rotation.rs`: `parse_partition_name()` regex captures `(?:_([a-z]))?`.
- `src/eventstore.rs`: `ActivePartition.suffix` is `Option<char>`.
- `src/catalog.rs`: `get_next_partition()` relies on lexicographic name ordering, which works for both alpha and numeric suffixes.

## Desired Outcome

- Partition names use zero-padded numeric ordinals: `events_20241002T14.db`, `events_20241002T14_000001.db`, `events_20241002T14_000002.db`, etc.
- No hard cap on same-window rotations (practical limit set by padding width, e.g. 999999 with 6 digits).
- Lexicographic ordering of partition names continues to produce the correct traversal order.

## Changes Required

- `ActivePartition.suffix`: Change from `Option<char>` to `Option<u32>` (or similar).
- `get_next_suffix()`: Increment a counter instead of a char. Remove the `'z'` ceiling.
- `generate_partition_name()`: Format as `events_<label>_{:06}.db` for suffixed partitions.
- `parse_partition_name()`: Update regex to capture `(?:_(\d{6}))?` instead of `(?:_([a-z]))?`.
- `should_rotate_by_size()`: Remove the `get_next_suffix(...).is_some()` guard (no longer needed since suffixes are unbounded).
- Tests: Update `test_parse_partition_name`, `test_generate_partition_name`, `test_get_next_suffix`, and add a test that rotates beyond 26 partitions.

## When to Do This

Before either of these conditions is met:

- A production workload could exceed roughly `27 * max_bytes` of event data in a single time window (e.g. a 1-hour window with 10 MB max_bytes seeing more than 270 MB/hour).
- Size rotation is promoted to a primary safety valve under sustained high write volume, rather than a rare overflow mechanism.

If size rotation remains a rare fallback (most rotations are time-based), this is low urgency. If the system starts relying on size rotation under load, this becomes blocking.

## Acceptance Criteria

- Partitions are named with zero-padded numeric ordinals.
- Rotation beyond 26 same-window partitions succeeds.
- Lexicographic name ordering produces correct traversal order.
- All existing tests updated and passing.
- `parse_partition_name` round-trips with `generate_partition_name` for numeric ordinals.
