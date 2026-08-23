# Cursor Mechanism

A cursor helps an event handler resume read-model projection work from the last event that was successfully handled.

## The Cursor Problem

An event handler handles an event batch, updates a read model, and saves progress. On the next run, it needs to ask for only the events that come after that saved progress.

```
stream versions:     v1  v2  v3  v4  v5
saved offset:            v2
next read returns:           v3  v4  v5
```

The saved offset and the read cursor are the same `EventStreamVersion` value in different roles. The application saves the last event version represented by the read-model projection, then uses that version as the cursor for the next bounded read.

The default application-owned `last_processed_event` row is keyed by:

1. the namespace
2. the partition key

The row stores the last processed `EventStreamVersion` as mutable checkpoint state.

## Cursor Architecture

### Cursor Definition

`EventStreamVersion` is the public cursor and event version type.

```rust
let start = EventStreamVersion::start();
let version = EventStreamVersion::new(42)?;
```

Stored events start at version `1`. `EventStreamVersion::start()` is a sentinel for "before the first event" and is only valid as a read cursor.

### Cursor Storage

`last_processed_event` belongs in the application database:

```sql
-- Abridged. Generate the example application SQL with:
-- events_dev_cli schema app
CREATE TABLE last_processed_event (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_processed_event_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace, partition_key)
);
```

Treat a missing row, or a row with `last_processed_event_version = 0`, as `EventStreamVersion::start()`. The row can be created by the first successful checkpoint; it does not need to exist before processing starts.

## Cursor Navigation

### Forward Navigation

Reads are exclusive:

```
cursor: start      -> returns versions 1, 2, 3...
cursor: version 2  -> returns versions 3, 4, 5...
cursor: version 5  -> returns versions 6, 7, 8...
```

```rust
let cursor = match last_processed_event_version {
    0 => EventStreamVersion::start(),
    version => EventStreamVersion::new(version)?,
};
let events = stream.load_after_version(cursor, 500).await?;
```

After applying a batch, save the last returned event version.

### Across Rotated Files

The catalog stores version ranges:

```
events_20260521T10.db         first=1     last=4000
events_20260521T10_000001.db  first=4001  last=7600
events_20260521T10_000002.db  first=7601  last=NULL
```

`EventStream::load_after_version` uses these ranges internally and returns
events ordered by event-stream version.

### Workflow-Specific Cursors

Workflow reads use the same cursor semantics and add a filter by starter event
ID:

```rust
let events = stream
    .load_workflow_after_version(started_by_event_id, last_seen, 100)
    .await?;
```

Unknown workflow anchors return an empty batch. Applications can add stricter domain validation when starter existence matters.

## Cursor Management

### Updating Cursors

Save offsets only after the read model has been updated:

```rust
for event in &events {
    apply_to_read_model(&event).await?;
}

let last_version = events.last().expect("non-empty batch").version;
save_last_processed_event(last_version).await?;
```

Commit the read-model changes and checkpoint together. The saved offset should be the last event version represented by the committed read-model state.

### Cursor Validation

The crate rejects:

- `EventStreamVersion::new(0)`
- `ExpectedVersion::Exact(EventStreamVersion::start())`
- read limits outside the bounded range

### Cursor Repair

If application offset state is lost, rebuild the read-model projection from
`EventStreamVersion::start()` for the affected partition key.

### Cursor State

A running event handler keeps its current offset in memory while processing batches. Checkpointing saves that offset to the application database after successful handling. After restart, reload the saved offset from the application database before reading the next batch.

### Checkpoint Boundary

Do not checkpoint ahead of committed read-model state. If handling fails before the application transaction commits, leave the saved offset unchanged and retry from the previous checkpoint.

## Use Cases and Patterns

### 1. Read-Model Rebuild

Start from `EventStreamVersion::start()` and rebuild a read model for one partition
key.

### 2. Workflow Recovery

Use `load_workflow_after_version(started_by_event_id, cursor, limit)` to rebuild state for one workflow run.

## Troubleshooting Cursor Issues

### Common Problems

- Saving the next version instead of the last processed version.
- Sharing one offset across multiple partition keys.
- Treating `EventStreamVersion` as a global version across all stores.

### Diagnostic Queries

Inspect application offsets and catalog version ranges for the affected partition key.

### Recovery Procedures

If an offset is ahead of the actual read model, reset it to the last known good version and continue from there. If no safe point is known, rebuild that read-model projection from `EventStreamVersion::start()`.
