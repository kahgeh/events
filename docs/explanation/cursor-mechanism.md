# Cursor Mechanism

Understanding cursors helps you build reliable projections over rotated event
files without storing physical file positions in application state.

## The Cursor Problem

Partition rotation creates multiple physical database files for one ordered
event log:

```
events_20260521T10.db  versions 1..4000
events_20260521T11.db  versions 4001..9000
events_20260521T12.db  versions 9001..
```

A projector that has processed version `6500` should not need to know which file
contains that version. The application only needs to store:

1. the partition key it is projecting
2. the last successfully projected `EventLogVersion`
3. the projection name

## Cursor Architecture

### Cursor Definition

`EventLogVersion` is the public cursor and event version type.

```rust
let start = EventLogVersion::start();
let version = EventLogVersion::new(42)?;
```

Stored events start at version `1`. `EventLogVersion::start()` is a sentinel for
"before the first event" and is only valid as a read cursor.

### Cursor Storage

Projection offsets belong in the application database:

```sql
CREATE TABLE projection_offsets (
    projection_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_projected_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (projection_name, namespace, partition_key)
);
```

Use `0` to represent `EventLogVersion::start()`.

## Cursor Navigation

### Forward Navigation

Reads are exclusive:

```
cursor: start      -> returns versions 1, 2, 3...
cursor: version 2  -> returns versions 3, 4, 5...
cursor: version 5  -> returns versions 6, 7, 8...
```

```rust
let events = store.load_after_version(last_projected, 500).await?;
```

After applying a batch, save the last returned event version.

### Across Rotated Files

The catalog stores version ranges:

```
events_20260521T10.db    first=1     last=4000
events_20260521T10_a.db  first=4001  last=7600
events_20260521T10_b.db  first=7601  last=NULL
```

`EventLog::load_after_version` uses these ranges internally and returns
events ordered by event-log version.

### Workflow-Specific Cursors

Workflow reads use the same cursor semantics and add a filter by starter event
ID:

```rust
let events = store
    .load_workflow_after_version(started_by_event_id, last_seen, 100)
    .await?;
```

Unknown workflow anchors return an empty batch. Applications can add stricter
domain validation when starter existence matters.

## Cursor Management

### Updating Cursors

Save offsets only after the read model has been updated:

```rust
for event in events {
    apply_to_read_model(&event).await?;
    save_projection_offset(event.version).await?;
}
```

For better transactional safety, apply a bounded batch and save the final version
in the same application database transaction.

### Cursor Validation

The crate rejects:

- `EventLogVersion::new(0)`
- `ExpectedVersion::Exact(EventLogVersion::start())`
- read limits outside the bounded range

### Cursor Repair

If application offset state is lost, rebuild the projection from
`EventLogVersion::start()` for the affected partition key.

## Cursor Performance Optimization

### Cursor Caching

Keep projection offsets in the application database. Cache them in memory only as
an optimization, and reload from durable state after restart.

### Batch Cursor Updates

For high-volume projections, update the offset once per committed batch rather
than once per event. Keep the batch bounded so replay after a crash remains
acceptable.

## Use Cases and Patterns

### 1. Event Replay

Start from `EventLogVersion::start()` and rebuild a read model for one partition
key.

### 2. Change Data Capture

Poll `load_after_version(last_seen, limit)` until it returns an empty batch, then
sleep or wait for a dirty-key notification.

### 3. Workflow Recovery

Use `load_workflow_after_version(started_by_event_id, cursor, limit)` to rebuild
state for one workflow run.

## Troubleshooting Cursor Issues

### Common Problems

- Saving the next version instead of the last processed version.
- Sharing one offset across multiple partition keys.
- Treating `EventLogVersion` as a global version across all stores.

### Diagnostic Queries

Inspect application offsets and catalog version ranges for the affected
partition key.

### Recovery Procedures

If an offset is ahead of the actual read model, reset it to the last known good
version and replay. If no safe point is known, rebuild that projection from
`EventLogVersion::start()`.
