## 1. Architecture

- **Crate**: `eventstore` (Rust)
- **Backends**:
  - **Catalog DB** (`catalog.db`): partitions, stream heads, projector checkpoints, leases.
  - **Partition DBs**: `events_<label>[_suffix].db` — active for writes, sealed for read‑only. Label encodes time window start (UTC).

- **Public Surface**:
  - `open_partitioned(root, RotationPolicy::TimeWindow { window, max_bytes: Option<u64> })`
  - `maybe_rotate()` (safe to call before each append or on schedule)
  - `append`, `load`, `all_since`
  - Projector utilities (`PartitionedCursor`, durable checkpoints, batching, leases)

## 2. Partitioning

### Time Window

- Define a **window duration** `W` (e.g. 1 day, 1 hour, 15 min).
- Compute **window start**: `start_ms = floor(now_ms / W) * W` (UTC).
- Partition name encodes start time:
  - Daily: `events_20251002.db`
  - Hourly: `events_20251002T06.db`
  - 15m: `events_20251002T0615.db`
- All UTC to avoid DST issues.

### Size Threshold

- Optional `max_bytes`. If active file grows beyond limit, rotate to suffixed file in the same window: `events_20251002_a.db`, `events_20251002_b.db`.

### Catalog Role

- Tracks partitions with `(name, path, start_ms, end_ms, sealed)`.
- Provides `stream_heads` (latest per stream) for O(1) OCC.
- Stores projector checkpoints (partition + cursor).

## 3. Schemas

### Partition DB

```sql
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  version INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  UNIQUE (stream_id, version)
);
-- Note: The UNIQUE constraint on (stream_id, version) automatically creates the stream index
CREATE INDEX IF NOT EXISTS idx_events_global ON events(created_at, id);

CREATE TABLE IF NOT EXISTS _migrations (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  checksum TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
```

### Catalog DB

```sql
CREATE TABLE IF NOT EXISTS partitions (
  name TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  start_ms INTEGER NOT NULL,
  end_ms INTEGER,
  sealed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_partitions_range ON partitions(start_ms, end_ms);

CREATE TABLE IF NOT EXISTS stream_heads (
  stream_id TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  last_created_at_ms INTEGER NOT NULL,
  last_event_id TEXT NOT NULL,
  last_partition TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_offsets (
  consumer TEXT PRIMARY KEY,
  partition TEXT NOT NULL,
  cursor_created_at INTEGER NOT NULL,
  cursor_event_id TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  lease_owner TEXT,
  lease_expires_at INTEGER
);

CREATE TABLE IF NOT EXISTS _migrations (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  checksum TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
```

## 4. Types & Errors

```rust
pub enum RotationPolicy {
  TimeWindow { window: std::time::Duration, max_bytes: Option<u64> },
}

pub struct PartitionRef {
  pub name: String,
  pub path: String,
  pub start_ms: i64,
  pub end_ms: Option<i64>,
  pub sealed: bool,
}

#[derive(Debug, Clone)]
pub struct PartitionedCursor {
  pub partition: String,
  pub created_at_ms: i64,
  pub event_id: uuid::Uuid,
}
```

`EsError` unchanged from v2 (Db, IncorrectEventVersion, PayloadTooLarge, Serde, Uuid).

## 5. Connection Model

- Catalog: long‑lived handle.
- Active partition: long‑lived, swapped on rotation.
- Sealed partitions: opened read‑only as needed.
- `append` uses `BEGIN IMMEDIATE` inside active DB + catalog update for `stream_heads`.

## 6. Public API

```rust
pub struct EventStore {
  catalog: turso::Database,
  active:  turso::Database,
  root:    std::path::PathBuf,
  max_payload_bytes: usize,
  rotation: RotationPolicy,
  active_name: String,
  active_start_ms: i64,
}

impl EventStore {
  pub async fn open_partitioned(root: &str, rotation: RotationPolicy)
      -> Result<Self, EsError>;

  pub async fn maybe_rotate(&self) -> Result<(), EsError>;

  pub async fn append(&self, stream_id: &str, expected: ExpectedVersion,
                      events: impl IntoIterator<Item = NewEvent>)
      -> Result<AppendResult, EsError>;

  pub async fn load(&self, stream_id: &str) -> Result<Vec<EventEnvelope>, EsError>;

  pub async fn all_since(&self, cur: PartitionedCursor, limit: i64)
      -> Result<(Vec<EventEnvelope>, PartitionedCursor), EsError>;
}
```

## 7. Rotation Protocol

1. `BEGIN IMMEDIATE` on catalog.
2. Seal active partition (set `end_ms`, `sealed=1`).
3. Create new partition DB file (`events_<label>.db`). Run migrations.
4. Insert row in `partitions`.
5. Swap `active` handle. `COMMIT`.
6. Optionally archive sealed DB file.

Rotation triggers if:

- `floor(now/W) != active_start_ms` (time window passed), or
- active file size ≥ `max_bytes` (suffix appended).

## 8. OCC

- Read `stream_heads.version` for expected version check.
- Insert events into active partition (tx).
- Update `stream_heads` row.
- If absent → insert new.
- Conflicts → return `EsError::IncorrectEventVersion`.

## 9. Projector

- Cursor = `{partition, created_at_ms, event_id}`.
- `all_since` fetches from partition ordered by `(created_at, id)`.
- If no rows and partition sealed, advance to next partition (`start_ms` order).
- Batch processing inside transaction, checkpoint advanced once per batch.
- Idempotency required. Optional applied_events table if strict once‑only.
- Leases optional via `consumer_offsets` fields.

## 10. Indices & Performance

- Partition DB indices: `(stream_id, version)` + `(created_at, id)`.
- Catalog indices: `idx_partitions_range`, `stream_heads`.
- Pragmas: `journal_mode=WAL`, `synchronous=NORMAL` on active DB.
- Projectors batch N events to reduce commit cost.

## 11. Recovery

- Rotation crash: if two partitions are “active”, seal older one at max event time.
- Periodic `PRAGMA integrity_check` before archiving.
- Keep compressed backups of catalog + sealed DBs.

## 12. Utilities

### Floor and Label

```rust
fn floor_to_window_ms(now_ms: i64, window: std::time::Duration) -> i64 {
    let w_ms = window.as_millis() as i64;
    (now_ms / w_ms) * w_ms
}

fn label_for(start_ms: i64, window: std::time::Duration) -> String {
    use time::{OffsetDateTime, UtcOffset};
    let dt = OffsetDateTime::from_unix_timestamp_nanos((start_ms as i128) * 1_000_000).unwrap().to_offset(UtcOffset::UTC);
    let secs = window.as_secs();
    if secs >= 86400 {
        format!("{:04}{:02}{:02}", dt.year(), dt.month() as u8, dt.day())
    } else if secs >= 3600 {
        format!("{:04}{:02}{:02}T{:02}", dt.year(), dt.month() as u8, dt.day(), dt.hour())
    } else {
        format!("{:04}{:02}{:02}T{:02}{:02}", dt.year(), dt.month() as u8, dt.day(), dt.hour(), dt.minute())
    }
}
```
