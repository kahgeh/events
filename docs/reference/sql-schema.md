# SQL Schema Reference

Complete reference for the database schema used by the Events crate, including all tables, indexes, and relationships.

## Database Architecture

The Events crate uses a dual-database architecture:

1. **Catalog Database** (`catalog.db`): Central metadata store
2. **Partition Databases** (`events_*.db`): Individual event storage files

Both databases use Turso with WAL mode for optimal performance.

## Catalog Database Schema

### Partitions Table

Stores metadata about all partition files.

```sql
CREATE TABLE partitions (
    name TEXT PRIMARY KEY,           -- Partition filename (e.g., "events_20241002T1200_a.db")
    path TEXT NOT NULL,              -- Relative path from root directory
    start_ms INTEGER NOT NULL,       -- Partition start time (Unix timestamp ms)
    end_ms INTEGER,                  -- Partition end time (NULL for active partition)
    sealed INTEGER NOT NULL DEFAULT 0 -- 0=active, 1=sealed (read-only)
);
```

**Indexes:**
```sql
CREATE INDEX idx_partitions_range ON partitions(start_ms, end_ms);
```

**Constraints:**
- `name` must be unique and match partition filename
- `start_ms` must be ≤ `end_ms` when `end_ms` is set
- `sealed` can only transition from 0 to 1 (never back to 0)

### Stream Heads Table

Tracks the latest version for each stream across all partitions.

```sql
CREATE TABLE stream_heads (
    stream_id TEXT PRIMARY KEY,        -- Unique stream identifier
    version INTEGER NOT NULL,          -- Current stream version
    last_created_at_ms INTEGER NOT NULL, -- Timestamp of last event
    last_event_id TEXT NOT NULL,       -- UUID of last event
    last_partition TEXT NOT NULL       -- Partition containing last event
);
```

**Logical Relationships:**
- `last_partition` logically references `partitions.name` (application-enforced consistency, not a database foreign key)

**Constraints:**
- `version` must be ≥ 0
- `last_created_at_ms` must be ≥ partition start time
- `last_event_id` must be a valid UUID

### Consumer Offsets Table

Tracks consumer positions for reliable event processing and active workflow state.

```sql
CREATE TABLE consumer_offsets (
    consumer TEXT PRIMARY KEY,         -- Consumer identifier
    partition TEXT NOT NULL,          -- Current partition name
    cursor_created_at INTEGER NOT NULL, -- Timestamp of processed event
    cursor_event_id TEXT NOT NULL,    -- UUID of processed event
    updated_at INTEGER NOT NULL,      -- Last update timestamp
    lease_owner TEXT,                 -- Current lease holder (NULL=unlocked)
    lease_expires_at INTEGER,         -- Lease expiration timestamp (NULL=forever)
    workflow_stream_id TEXT,          -- Active workflow stream ID (NULL=no active workflow)
    workflow_event_id TEXT            -- Active workflow start event ID (NULL=no active workflow)
);
```

**Indexes:**
```sql
CREATE INDEX idx_consumer_offsets_consumer ON consumer_offsets(consumer);
CREATE INDEX idx_consumer_offsets_lease ON consumer_offsets(lease_owner, lease_expires_at);
```

**Relationships:**
- `partition` references `partitions.name`

**Constraints:**
- `cursor_created_at` must be ≥ partition start time
- `lease_expires_at` must be ≥ `updated_at` when set
- `lease_owner` and `lease_expires_at` must be both NULL or both set
- `workflow_stream_id` and `workflow_event_id` must be both NULL or both set

### Migrations Table

Internal tracking for schema migrations.

```sql
CREATE TABLE _migrations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);
```

**Constraints:**
- `name` must be unique
- `checksum` is SHA256 hash of migration SQL
- `applied_at` is Unix timestamp

## Partition Database Schema

### Events Table

Core table storing all events within a partition.

```sql
CREATE TABLE events (
    id TEXT,                          -- Event UUID
    stream_id TEXT NOT NULL,          -- Stream identifier
    type TEXT NOT NULL,               -- Event type name
    payload TEXT NOT NULL,            -- JSON event data
    version INTEGER NOT NULL,         -- Stream version number
    created_at INTEGER NOT NULL,      -- Event timestamp (Unix timestamp ms)
    trace_id TEXT,                    -- OpenTelemetry trace ID for correlation
    span_id TEXT,                     -- OpenTelemetry span ID for correlation
    request_id TEXT,                  -- Request ID for completion tracking
    actor_id TEXT NOT NULL,           -- Actor who initiated this event
    actor_type TEXT NOT NULL,         -- Actor type (User, System)
    PRIMARY KEY (id),
    UNIQUE (stream_id, version)
);
```

**Indexes:**
```sql
-- The UNIQUE constraint on (stream_id, version) automatically creates an index
-- No separate idx_events_stream index is needed
CREATE INDEX idx_events_global ON events(created_at, id);
CREATE INDEX idx_events_actor ON events(actor_id, actor_type);
CREATE INDEX idx_events_actor_type ON events(actor_type);
```

**Constraints:**
- `id` must be a valid UUID
- `stream_id` must be non-empty
- `type` must be non-empty
- `payload` must be valid JSON
- `version` must be ≥ 0
- `created_at` must be within partition time range
- `(stream_id, version)` must be unique (this constraint automatically creates the index for stream queries)
- `actor_id` must be non-empty
- `actor_type` must be either "User" or "System"
- Events are immutable once inserted

### Migrations Table

Same structure as catalog migrations table.

```sql
CREATE TABLE _migrations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);
```

## Data Types and Constraints

### UUIDs

All `id` fields use text UUIDs in the format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.

```sql
-- Valid example
'123e4567-e89b-12d3-a456-426614174000'

-- Validation (application-level)
CREATE TABLE events (
    id TEXT PRIMARY KEY CHECK (length(id) = 36)
);
```

### Timestamps

All timestamps are Unix milliseconds since epoch (UTC).

```sql
-- Example timestamps
1698624000000  -- 2023-10-29 00:00:00 UTC
1698627600000  -- 2023-10-29 01:00:00 UTC
```

### JSON Payloads

Event payloads are stored as JSON strings.

```sql
-- Example payload
'{"order_id": "ORD-123", "total": 9999, "items": ["item1", "item2"]}'

-- Validation notes
- Must be valid JSON
- Size limits apply (configurable, default 1MB)
- Can contain nested structures
- Preserves exact order from insertion
```

## Query Patterns

### Stream Queries

Get all events for a stream:

```sql
-- Single partition
SELECT * FROM events
WHERE stream_id = 'order-123'
ORDER BY version;

-- Across partitions (via catalog)
SELECT e.* FROM events e
JOIN stream_heads sh ON e.stream_id = sh.stream_id
WHERE e.stream_id = 'order-123'
ORDER BY e.version;
```

### Time Range Queries

Get events in time range:

```sql
-- Single partition
SELECT * FROM events
WHERE created_at >= 1698624000000
  AND created_at < 1698627600000
ORDER BY created_at, id;

-- Across partitions (via catalog)
SELECT e.* FROM events e
JOIN partitions p ON e.created_at >= p.start_ms
                AND (e.created_at < p.end_ms OR p.end_ms IS NULL)
WHERE e.created_at >= 1698624000000
  AND e.created_at < 1698627600000
ORDER BY e.created_at, e.id;
```

### Consumer Offsets

Get next events for consumer:

```sql
-- Get consumer position
SELECT partition, cursor_created_at, cursor_event_id
FROM consumer_offsets
WHERE consumer = 'order-processor';

-- Get next events
SELECT * FROM events
WHERE created_at > 1698624000000
   OR (created_at = 1698624000000 AND id > 'uuid-here')
ORDER BY created_at, id
LIMIT 100;
```

## Performance Characteristics

### Write Patterns

- **Append-only**: All writes insert new events
- **Sequential**: Events added in time order within partitions
- **Batch-friendly**: Multiple events can be inserted transactionally

```sql
-- Batch insert (application-level)
BEGIN TRANSACTION;
INSERT INTO events (id, stream_id, type, payload, version, created_at)
VALUES
  ('uuid1', 'stream1', 'Type1', '{}', 1, 1698624000000),
  ('uuid2', 'stream1', 'Type2', '{}', 2, 1698624001000);
COMMIT;
```

### Read Patterns

- **Stream reads**: Optimized by the `(stream_id, version)` unique constraint index
- **Time reads**: Optimized by `(created_at, id)` index
- **Global scans**: Rare, typically for migrations or analytics

### Index Usage

```sql
-- Stream queries use the unique constraint index on (stream_id, version)
EXPLAIN QUERY PLAN
SELECT * FROM events WHERE stream_id = 'test' ORDER BY version;
-- Output: Using INDEX sqlite_autoindex_events_1 (created by UNIQUE constraint)

-- Time queries use idx_events_global
EXPLAIN QUERY PLAN
SELECT * FROM events WHERE created_at > 1698624000000 ORDER BY created_at;
-- Output: Using INDEX idx_events_global
```

## Schema Evolution

### Adding Columns

```sql
-- Add new event metadata
ALTER TABLE events ADD COLUMN metadata TEXT DEFAULT '{}';

-- Add partition tags
ALTER TABLE partitions ADD COLUMN tags TEXT DEFAULT '[]';
```

### Adding Indexes

```sql
-- Index for event type queries
CREATE INDEX idx_events_type ON events(type);

-- Composite index for type + time
CREATE INDEX idx_events_type_time ON events(type, created_at);
```

### Migrating Data

```sql
-- Backfill default values
UPDATE events SET metadata = '{}' WHERE metadata IS NULL;

-- Validate data consistency
SELECT COUNT(*) FROM events WHERE version <= 0;
```

## Integrity Constraints

### Application-Level Validation

The Events crate enforces these rules:

1. **Stream versioning**: `version` must be `previous_version + 1`
2. **Time ordering**: `created_at` must be ≥ previous event in stream
3. **Partition bounds**: `created_at` must be within partition time range
4. **UUID uniqueness**: Global UUID uniqueness across all partitions
5. **JSON validity**: All payloads must be valid JSON

### Database Constraints

Turso enforces:

1. **Primary keys**: No duplicate primary key values
2. **Foreign keys**: Referential integrity (where enabled)
3. **Data types**: Basic type checking
4. **CHECK constraints**: Custom validation rules

## Maintenance Operations

### Vacuum and Analyze

```sql
-- Rebuild database file (offline operation)
VACUUM;

-- Update query planner statistics
ANALYZE;

-- Check integrity
PRAGMA integrity_check;
```

### Index Maintenance

```sql
-- Rebuild index
REINDEX;

-- Check index stats
PRAGMA index_list('events');
PRAGMA index_info('sqlite_autoindex_events_1');  -- The unique constraint index
```

### Backup and Restore

```sql
-- Create backup
VACUUM INTO 'backup_events.db';

-- Export schema
.schema

-- Export data
.dump
```

## Monitoring Queries

### Partition Health

```sql
-- Partition sizes (this requires querying partition files directly)
SELECT
    name,
    start_ms,
    end_ms,
    sealed
FROM partitions;

-- Note: To get event counts per partition, you need to query each partition
-- database file separately since events are stored across multiple DB files

-- Active partitions
SELECT * FROM partitions WHERE sealed = 0;
```

### Stream Statistics

```sql
-- Stream counts
SELECT COUNT(*) as total_streams FROM stream_heads;

-- Largest streams
SELECT stream_id, version FROM stream_heads
ORDER BY version DESC LIMIT 10;

-- Stale streams
SELECT stream_id, last_created_at_ms FROM stream_heads
WHERE last_created_at_ms < (strftime('%s', 'now') - 86400) * 1000;
```

### Consumer Status

```sql
-- Active consumers
SELECT consumer, updated_at, lease_owner, lease_expires_at
FROM consumer_offsets
WHERE lease_expires_at > strftime('%s', 'now') * 1000;

-- Lagging consumers
SELECT consumer, updated_at,
       (strftime('%s', 'now') * 1000 - updated_at) / 1000 as lag_seconds
FROM consumer_offsets
ORDER BY lag_seconds DESC;
```

This schema reference provides the complete technical specification for understanding and working with the Events crate's database structure.