# Architecture and Design Decisions

Understanding the architecture and design decisions behind the Events crate helps
you use it effectively and choose the right partition boundary for your
application.

## Overview

The Events crate implements a **partitioned event log**. Each partition store
contains one ordered log. Applications choose partition keys, and owner
partitioning is a scaling strategy layered on top of that plain model.

The architecture is designed around several key principles:

- **Immutability**: events are never modified once written
- **Append-only**: new facts are appended to the selected partition log
- **Partition stores**: a partition key selects one physical store and one log
- **Optimistic concurrency**: expected versions protect command decisions
- **Application-owned projection state**: read-model offsets live with the read
  model
- **Physical rotation**: event files rotate internally without changing public
  cursors

## Core Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EventPartitions                             │
│  • validates namespace and partition key                            │
│  • ensures partition directories exist                              │
│  • lists existing partitions without opening them                   │
│  • keeps a bounded idle cache of opened stores                      │
├─────────────────────────────────────────────────────────────────────┤
│                             Partition                               │
│  • public reference to existing partition storage                   │
│  • opens into OwnerEventStore                                       │
├─────────────────────────────────────────────────────────────────────┤
│                         OwnerEventStore                             │
│  • appends events with expected-version checks                      │
│  • reads bounded batches after OwnerLogVersion                      │
│  • filters workflow events by starter event ID                      │
│  • rotates event DB files internally                                │
├─────────────────────────────────────────────────────────────────────┤
│  catalog.db                                                         │
│  • owner_log head                                                   │
│  • rotated file version ranges                                      │
│                                                                     │
│  events_*.db                                                        │
│  • events(version, type, payload, workflow metadata, actor)         │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. EventPartitions

`EventPartitions` is the partition manager. It owns the root directory and the
rotation policy used by stores opened through it.

```rust
let partitions = EventPartitions::open("./data/events", rotation).await?;
let partition = partitions.ensure_exists("users", "user-123").await?;
let store = partition.open().await?;
```

Namespaces and partition keys are filesystem-safe path segments: lowercase ASCII
letters, digits, and `-`.

`list(namespace)` performs shallow discovery. It returns valid immediate child
directories sorted by partition key, and it does not open or migrate those
stores.

### 2. Partition Store

A partition store is the durable boundary for one ordered event log. The
partition key might represent an owner, account, client, region, workflow group,
or a plain `default` store.

The partition key is selected by application code before append/read:

```rust
let store = partitions
    .ensure_exists("orders", "order-123")
    .await?
    .open()
    .await?;
```

`OwnerEventStore` is the current public opened handle name. The name does not
require the partition key to represent a domain owner.

### 3. Catalog Database

Each partition store has a catalog database. The catalog stores event-log
metadata, not projection progress:

```sql
CREATE TABLE owner_log (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_version INTEGER NOT NULL,
    last_event_id TEXT,
    active_partition TEXT
);

CREATE TABLE partition_refs (
    name TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    first_version INTEGER NOT NULL,
    last_version INTEGER,
    sealed INTEGER NOT NULL
);
```

**Why a catalog database?**

- **Fast lookup**: find which rotated file contains a version range
- **Continuity**: preserve one monotonic `OwnerLogVersion` across files
- **Safety**: track the current owner-log head independently of event rows
- **Maintenance**: allow event files to rotate without changing public cursors

### 4. Partition Files

Each rotated file is a Turso database containing event rows for a contiguous
owner-log version range:

```sql
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    version INTEGER NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    workflow_kind TEXT,
    workflow_started_by_event_id TEXT,
    request_id TEXT,
    actor_id TEXT NOT NULL,
    actor_type TEXT NOT NULL
);
```

**Why separate files per rotation window?**

- **Performance**: active indexes stay bounded
- **Maintenance**: sealed files can be backed up or inspected independently
- **Resource management**: file size limits prevent unbounded active files
- **Cursor stability**: callers keep `OwnerLogVersion`, not file names

### 5. Rotation Engine

`RotationPolicy::TimeWindow` controls when a store creates a new physical event
file:

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(512 * 1024 * 1024),
}
```

Rotation is physical. It does not create a new logical stream, owner, or
projection cursor.

## Data Flow

### Writing Events

```
┌─────────────────┐    ┌────────────────────┐    ┌─────────────────┐
│   Application   │───▶│  EventPartitions   │───▶│ OwnerEventStore │
│                 │    │                    │    │                 │
│ choose namespace│    │ ensure partition   │    │ validate event  │
│ choose key      │    │ open cached store  │    │ check version   │
└─────────────────┘    └────────────────────┘    └────────┬────────┘
                                                           │
                                                           ▼
                                                  ┌─────────────────┐
                                                  │  events_*.db    │
                                                  │ insert rows     │
                                                  └────────┬────────┘
                                                           │
                                                           ▼
                                                  ┌─────────────────┐
                                                  │  catalog.db     │
                                                  │ update head     │
                                                  └─────────────────┘
```

1. **Partition selection**: application chooses namespace and partition key
2. **Validation**: validate safe path segments, payload size, actor fields, and
   workflow metadata
3. **Concurrency check**: verify `ExpectedVersion`
4. **Database write**: insert event rows into the active event file
5. **Catalog update**: advance the owner-log head and version range metadata

If event insertion commits but the catalog head update fails, append returns
`CatalogDrift`. Treat that as an operator problem before writing more to the
affected partition store.

### Reading Events

```
┌──────────────────────┐    ┌──────────────────────┐
│ Application offset   │───▶│ OwnerEventStore      │
│ OwnerLogVersion      │    │ load_after_version   │
└──────────────────────┘    └──────────┬───────────┘
                                        │
                                        ▼
                             ┌──────────────────────┐
                             │ catalog version      │
                             │ ranges               │
                             └──────────┬───────────┘
                                        │
                                        ▼
                             ┌──────────────────────┐
                             │ relevant event files │
                             │ ordered batch        │
                             └──────────────────────┘
```

Reads are exclusive: `load_after_version(v2, limit)` returns events after `v2`.
`OwnerLogVersion::start()` means "before the first event" for reads.

### Projection Processing

```
┌──────────────────┐    ┌────────────────────┐    ┌─────────────────┐
│   Projector      │───▶│  OwnerEventStore   │───▶│  Event batch    │
│                  │    │                    │    │                 │
│ load app offset  │    │ read after cursor  │    │ apply handlers  │
│ update read model│    │ bounded limit      │    │ save app offset │
└──────────────────┘    └────────────────────┘    └─────────────────┘
```

Projection offsets belong in the application database so read-model changes,
active workflow state, and the offset can commit together.

### Workflow Recovery

Workflow identity is independent of partition identity. A workflow run is
identified by the event ID that started it:

```
┌──────────────────────────────────────────────────────────────┐
│                       Partition log                          │
├──────────────────────────────────────────────────────────────┤
│ v1 UserRegistered                                            │
│ v2 ProvisioningStarted   workflow_started_by_event_id = v2.id│
│ v3 EmailChanged                                              │
│ v4 MachineCreated        workflow_started_by_event_id = v2.id│
└──────────────────────────────────────────────────────────────┘
```

`load_workflow_after_version(started_by_event_id, cursor, limit)` filters by the
starter event ID while preserving partition-log version order.

## Concurrency Model

### Optimistic Concurrency Control

The system uses optimistic concurrency control. A command reads state, decides
what should happen, and appends with an expected owner-log version:

```
Process A                     Process B
---------                     ---------
Read log head v5              Read log head v5
Decide command                Decide command
Append with Exact(v5) ──────▶ Append with Exact(v5)
Success                       Conflict: actual is v6
                              Reload and decide again
```

**Why optimistic concurrency?**

- **No read locks**: commands can inspect state without blocking writers
- **Clear conflicts**: stale decisions fail at append time
- **Domain control**: applications choose retry, merge, or reject behavior
- **Partition-local scope**: conflicts are limited to one partition store

### Version Numbers

`OwnerLogVersion` is monotonic inside one partition store:

- stored events start at version `1`
- `OwnerLogVersion::start()` is only a before-first read cursor
- `ExpectedVersion::NoStream` requires an empty log
- `ExpectedVersion::Exact(version)` requires the current head to match
- `ExpectedVersion::Any` blind-appends after the current head

## Error Handling Strategy

### Error Categories

| Category | Examples | Response |
| --- | --- | --- |
| Caller input | `InvalidSafeName`, `InvalidVersion`, `InvalidReadLimit` | reject or fix caller |
| Concurrency | `Concurrency` | reload state and decide again |
| Storage | `Db`, `Io`, `Migration` | retry if safe, otherwise alert |
| Catalog safety | `CatalogDrift` | stop writes and inspect store |

### Retry Strategy

Retry only when the operation is known to be safe. Do not blindly retry
`CatalogDrift`; it means event rows committed but catalog advancement failed.

## Performance Considerations

### Write Performance

Writes are serialized per partition store. Choosing partition keys that match
independent units of work distributes write contention.

### Read Performance

Reads are bounded by caller-supplied limits and catalog version ranges. Keep
batches large enough to amortize overhead and small enough to bound memory use.

### Memory Management

`EventPartitions` keeps a bounded idle cache of opened stores. Active
`OwnerEventStore` handles remain valid even if the resolver evicts its cached
entry.

## Design Trade-offs

### Plain Store vs. Owner Partition

A small service can use one stable key such as `app/default`. Owner partitioning
becomes useful when independent owners, accounts, clients, or tenants need
separate write contention and worker scheduling.

### Application-Owned Projection State

The event crate does not own projection checkpoints. That adds one application
responsibility, but it keeps read-model state and offsets in the same database
transaction.

### Physical Rotation vs. Public Cursor Simplicity

Rotation keeps files manageable. Hiding file cursors keeps application offsets
stable and easy to inspect.

## Future Considerations

### Scalability Limits

Partition count, active worker count, and open-store cache size should be
monitored together. More partition keys can improve scheduling while increasing
directory and catalog count.

### Potential Enhancements

- richer partition health reporting
- catalog repair tooling for `CatalogDrift`
- optional metrics helpers for worker-pool scheduling

### Migration Path

For existing data directories, follow [Migrate Schema](../how-to/migrate-schema.md)
before using the partition-log schema in production.

## Summary

The Events crate centers on one ordered log per partition store. The crate owns
append, read, rotation, and catalog mechanics. Applications choose partition
keys, store projection offsets, and manage active workflow state.
