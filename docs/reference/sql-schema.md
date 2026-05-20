# SQL Schema

Each partition store has its own catalog database and rotated event databases.

This schema is a deliberate destructive replacement for the earlier multi-stream
schema. Existing `001_*` migration records are left alone; the owner-log reset
migrations use new names and recreate the crate-owned catalog/event tables.

## Catalog Database

```sql
CREATE TABLE partitions (
    name TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    first_version INTEGER NOT NULL,
    last_version INTEGER,
    sealed INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_partitions_range
ON partitions(first_version, last_version);

CREATE TABLE owner_log (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_version INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT,
    active_partition TEXT
);
```

`partitions` maps owner-log version ranges to rotated database files. A sealed
partition has `last_version`; the active partition leaves it `NULL`.

`owner_log` is the single owner-log head record.

## Event Partition Database

```sql
CREATE TABLE events (
    id TEXT,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    workflow_kind TEXT,
    workflow_started_by_event_id TEXT,
    trace_id TEXT,
    span_id TEXT,
    request_id TEXT,
    actor_id TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (version),
    UNIQUE (sequence)
);

CREATE INDEX idx_events_version ON events(version);
CREATE INDEX idx_events_global ON events(created_at, sequence);
CREATE INDEX idx_events_workflow_started_version
ON events(workflow_started_by_event_id, version);
CREATE INDEX idx_events_actor ON events(actor_id, actor_type);
CREATE INDEX idx_events_actor_type ON events(actor_type);
```

There is no per-row routing field. The partition context is selected by the
directory resolved through `EventPartitions`.

Application projection offsets and active workflow state are not stored in this
schema. Store those in the application database with the read-model update that
advances them.
