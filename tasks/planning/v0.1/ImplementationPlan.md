## 0) Prereqs

- Rust stable, Tokio runtime
- Turso DB (Rust rewrite)
- Decide `RotationPolicy::TimeWindow { window: Duration, max_bytes: Option<u64> }`
- Default payload size limit = 1 MiB

## 1) Migrations

- **Partition DB**: `0001_init.sql` (events, indices, \_migrations).
- **Catalog DB**: `0001_catalog.sql` (partitions, stream_heads, consumer_offsets, \_migrations).
- Shared migration runner:
  - Lexical order execution, SHA256 checksum, applied_at timestamp.
  - Abort if checksum mismatch for already applied file.

## 2) Catalog Module

- APIs:
  - `current_active() -> PartitionRef`
  - `seal_partition(name, end_ms)`
  - `create_partition(name, path, start_ms)`
  - `next_partition(name) -> Option<PartitionRef>`
  - `get_stream_head(stream_id) -> Option<Head>`
  - `upsert_stream_head(head)`
  - `read_offset(consumer) -> Option<PartitionedCursor>`
  - `upsert_offset(consumer, cursor)`
- All catalog updates under `BEGIN IMMEDIATE` for safety.

## 3) Rotation Policy Implementation

- Helpers:
  - `floor_to_window_ms(now_ms, window)` → start_ms
  - `label_for(start_ms, window)` → partition name
- `maybe_rotate()` logic:
  - Compute `now_start_ms`. If differs from active’s start_ms → rotate.
  - If `max_bytes` configured and file size ≥ limit → rotate with suffix (`_a`, `_b`, …`).
- Rotation steps:
  1. Tx on catalog.
  2. Seal active partition row (`end_ms=now`, sealed=1`).
  3. Create new partition DB, run migrations.
  4. Insert partition row.
  5. Swap `active` handle. Commit.

## 4) Append (OCC)

- Use `catalog.stream_heads`:
  - `NoStream`: fail if row exists.
  - `Exact(v)`: fail if head.version ≠ v.
  - `Any`: skip check.
- Insert batch into active partition in one tx.
- Update catalog `stream_heads` with new head (version, last_created_at, id, partition).

## 5) Load & Global Scan

- `load(stream_id)`:
  - Walk partitions in order; accumulate events for given stream.
- `all_since(cursor, limit)`:
  - Open cursor.partition; query `(created_at > ts) OR (created_at=ts AND id>id)` ordered by `(created_at,id)`.
  - If empty and sealed, advance to `next_partition(partition)` from catalog.
  - Return `(events, updated_cursor)`.

## 6) Projector Utilities

- Cursor: `{partition, created_at_ms, event_id}`.
- Loop:
  - Fetch N events via `all_since`.
  - Process inside a single transaction (apply updates, then checkpoint).
  - Upsert checkpoint into `consumer_offsets`.
- Optional leases: use `lease_owner/lease_expires_at` to coordinate multiple instances.

## 7) Testing

- **Partition rollover**:
  - Configure small `window=1m`, write across boundary → new file created, catalog updated.
- **Size threshold**:
  - Configure low `max_bytes`, append until overflow → suffixed file created.
- **OCC conflicts**:
  - Append with stale expected version → expect `EsError::Concurrency`.
- **Projector recovery**:
  - Process partial batch, crash, restart → resumes from last checkpoint.
- **Cross‑partition replay**:
  - Cursor at end of sealed partition → moves to next.
- **Rotation crash repair**:
  - Simulate two “active” rows; repair seals the older one.

## 8) Operational Guidance

- Archive sealed partitions to `archive/YYYY/MMDD/` (depending on window).
- Backups:
  - Catalog + active DB nightly.
  - Sealed DB on rotation (immutable).
- Periodic `PRAGMA integrity_check` before archiving.

## 9) Deliverables Checklist

- [ ] Catalog schema + migration
- [ ] Partition schema + migration
- [ ] Migration runner
- [ ] Catalog module (partitions, stream_heads, offsets, leases)
- [ ] EventStore with rotation + append/load/all_since
- [ ] Projector with PartitionedCursor
- [ ] Tests for rotation, OCC, recovery, replay
- [ ] README with usage examples

## 10) Example Snippets

### Rotation Helper

```rust
fn floor_to_window_ms(now_ms: i64, window: std::time::Duration) -> i64 {
    let w_ms = window.as_millis() as i64;
    (now_ms / w_ms) * w_ms
}
```

### all_since advancing logic

```rust
let mut cur = cursor.clone();
loop {
    let (events, exhausted) = fetch_from_partition(&cur.partition, cur.created_at_ms, cur.event_id, limit).await?;
    if !events.is_empty() {
        let last = events.last().unwrap();
        cur.created_at_ms = last.created_at_ms;
        cur.event_id = last.id;
        return Ok((events, cur));
    }
    if exhausted && let Some(next) = catalog.next_partition(&cur.partition).await? {
        cur.partition = next.name.clone();
        cur.created_at_ms = 0;
        cur.event_id = uuid::Uuid::from_u128(0);
        continue;
    }
    return Ok((Vec::new(), cur));
}
```

### OCC with stream_heads

```rust
let head = catalog.get_stream_head(stream_id).await?;
match expected {
  ExpectedVersion::NoStream if head.is_some() => return Err(EsError::Concurrency{ ... }),
  ExpectedVersion::Exact(v) if head.as_ref().map(|h| h.version) != Some(v) =>
      return Err(EsError::Concurrency{ ... }),
  _ => {}
}
// insert events...
catalog.upsert_stream_head(stream_id, new_version, last_ms, last_id, active_partition).await?;
```
