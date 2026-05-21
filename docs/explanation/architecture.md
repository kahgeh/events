# Architecture and Design Decisions

Understanding the architecture and design decisions behind the Events crate helps
you choose the right partition store boundary and keep storage, workflow, and
rotation concerns separate.

## Overview

The Events crate implements a **partition-store event log**. A partition store is
the storage primitive: one application-chosen partition key receives one ordered
event log. Partitioning by owner or account is a scaling strategy layered on top of that plain
model when partition keys map to owners, accounts, clients, or other independent
units of work.

The architecture is designed around several key principles:

- **Immutability**: events are never modified once written
- **Append-only**: new facts are appended to the selected event log
- **Partition stores**: a partition key selects one physical store and one event
  log
- **Optimistic concurrency**: expected versions protect command decisions
- **Application-owned projection state**: read-model offsets live with the read
  model
- **Physical rotation**: event database files rotate inside one partition store
  without changing public cursors

## Core Architecture

```
API
┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│ EventNamespaces       │─▶│ EventNamespace        │─▶│ Partition             │─▶│ EventLog              │
│ Root manager for all  │  │ Named group such as   │  │ Reference to one      │  │ Append/read API for   │
│ event namespaces.     │  │ clients or orders.    │  │ selected partition.   │  │ one event log.        │
└───────────────────────┘  └───────────────────────┘  └───────────────────────┘  └───────────────────────┘
                                                               │
                                                               ▼
Store Coordination
┌─────────────────────────────────────┐          ┌─────────────────────────────────────┐
│ Catalog                             │◀────────▶│ RotationPolicy / rotation engine    │
│ Maintains the event log's position  │          │ Keeps one event log spread across   │
│ across rotated event files.         │          │ manageable physical event files.    │
│                                     │          │                                     │
└─────────────────────────────────────┘          └─────────────────────────────────────┘
                                     │
                                     ▼
Storage
┌───────────────────────────────────────────────────────────────────────────────────────────────┐
│ Partition store directory                                                                     │
│ The durable home for one application-selected event log.                                      │
│                                                                                               │
│ ┌─────────────────────────────────────┐          ┌──────────────────────────────────────────┐ │
│ │ catalog.db                          │          │ events_*.db                              │ │
│ │ Stores the map of the event log     │          │ Store the append-only event rows for    │  │
│ │ across event files.                 │          │ the event log.                          │  │
│ └─────────────────────────────────────┘          └──────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. EventNamespaces

`EventNamespaces` is the root manager for all event namespaces under one storage
root. It owns the root directory and the rotation policy used by event logs
opened through it.

```rust
let namespaces = EventNamespaces::open("./data/events", rotation).await?;
let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition_exists("user-123").await?;
let log = partition.open().await?;
```

Namespaces and partition keys are filesystem-safe path segments: lowercase ASCII
letters, digits, and `-`.

### 2. EventNamespace

`EventNamespace` is a named grouping of partitions, such as `users`, `clients`,
or `orders`. It ensures partitions exist and lists valid partition-store
directories without opening or migrating those stores.

### 3. Partition

`Partition` is the public reference to one selected partition in an
`EventNamespace`. It is not the append/read API itself; opening it returns an
`EventLog`.

The partition key is selected by application code before append/read:

```rust
let orders = namespaces.ensure_namespace("orders").await?;
let partition = orders.ensure_partition_exists("order-123").await?;
let log = partition.open().await?;
```

A partition store is the durable storage behind a `Partition`. The partition key
might be a plain `default` key, or it might represent an owner, account, client,
region, or other independent unit of work.

### 4. EventLog

`EventLog` is the public append/read API for one ordered event log. It abstracts
the catalog and rotated physical event files, so callers use `EventLogVersion`
rather than file names.

### 5. Catalog Database

Each partition store has a catalog database. The catalog stores event-log and
rotated-file metadata, not projection progress:

```sql
CREATE TABLE event_log_head (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_version INTEGER NOT NULL,
    last_event_id TEXT,
    active_partition TEXT
);

CREATE TABLE event_file_ranges (
    name TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    first_version INTEGER NOT NULL,
    last_version INTEGER,
    sealed INTEGER NOT NULL
);
```

`event_log_head` and `event_file_ranges` describe different parts of the same ordered
log:

```text
catalog.db
├── event_log_head
│   └── current_version = 125
│       active_partition = events_20260521T10_b.db
│
└── event_file_ranges
    ├── events_20260521T09.db    versions 1..50     sealed
    ├── events_20260521T10.db    versions 51..100   sealed
    └── events_20260521T10_b.db  versions 101..NULL active
```

`event_log_head` answers "what is the current head of this event log?"
`event_file_ranges` answers "which physical event files contain which event-log
version ranges?" Appends use `event_log_head.current_version` for expected-version
checks and the next version number. Reads use `event_file_ranges` to find the
rotated event files that may contain events after the requested
`EventLogVersion`.

**Why a catalog database?**

- **Fast lookup**: find which rotated event database file contains a version range
- **Continuity**: preserve one monotonic `EventLogVersion` across files
- **Safety**: track the current event log head independently of event rows
- **Maintenance**: allow event database files to rotate without changing public
  cursors

### 6. Rotated Event Files

Each rotated event file is a Turso database containing event rows for a
contiguous event-log version range:

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
    trace_id TEXT,
    span_id TEXT,
    request_id TEXT,
    actor_id TEXT NOT NULL,
    actor_type TEXT NOT NULL
);
```

**Why separate event files per rotation window?**

- **Performance**: active indexes stay bounded
- **Maintenance**: sealed files can be backed up or inspected independently
- **Resource management**: file size limits prevent unbounded active files
- **Cursor stability**: callers keep `EventLogVersion`, not file names

### 7. Rotation Engine

`RotationPolicy::TimeWindow` controls when a partition store creates a new
physical event database file:

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(512 * 1024 * 1024),
}
```

Rotation is physical. It creates another event database file inside the same
partition store. It does not create a new partition store, event log, workflow
run, or projection cursor.

## Data Flow

### Writing Events

```
┌─────────────────┐    ┌────────────────────┐    ┌─────────────────┐
│   Application   │───▶│ EventNamespaces    │───▶│    EventLog     │
│                 │    │                    │    │                 │
│ choose namespace│    │ select namespace   │    │ validate event  │
│ choose key      │    │ open partition     │    │ check version   │
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

1. **Partition store selection**: application chooses namespace and partition
   key
2. **Validation**: validate safe path segments, payload size, actor fields, and
   workflow metadata
3. **Concurrency check**: verify `ExpectedVersion`
4. **Database write**: insert event rows into the active event file
5. **Catalog update**: advance the event log head and rotated-file version range
   metadata

If event insertion commits but the catalog head update fails, append returns
`CatalogDrift`. Treat that as an operator problem before writing more to the
affected partition store.

### Reading Events

```
┌──────────────────────┐    ┌──────────────────────┐
│ Application offset   │───▶│       EventLog       │
│ EventLogVersion      │    │ load_after_version   │
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
`EventLogVersion::start()` means "before the first event" for reads.

### Projection Processing

```
┌──────────────────┐    ┌────────────────────┐    ┌─────────────────┐
│   Projector      │───▶│      EventLog      │───▶│  Event batch    │
│                  │    │                    │    │                 │
│ load app offset  │    │ read after cursor  │    │ apply handlers  │
│ update read model│    │ bounded limit      │    │ save app offset │
└──────────────────┘    └────────────────────┘    └─────────────────┘
```

Projection offsets belong in the application database so read-model changes,
active workflow state, and the offset can commit together.

### Workflow Recovery

Workflow identity is independent of partition-store identity. A workflow kind
names the retryable business process type, and a workflow started-by event ID
identifies one run of that process:

```
┌──────────────────────────────────────────────────────────────┐
│                         Event log                            │
├──────────────────────────────────────────────────────────────┤
│ v1 UserRegistered                                            │
│ v2 ProvisioningStarted   workflow_started_by_event_id = v2.id│
│ v3 EmailChanged                                              │
│ v4 MachineCreated        workflow_started_by_event_id = v2.id│
└──────────────────────────────────────────────────────────────┘
```

`load_workflow_after_version(started_by_event_id, cursor, limit)` filters by the
workflow started-by event ID while preserving event-log version order.

## Concurrency Model

### Optimistic Concurrency Control

The system uses optimistic concurrency control. A command reads state, decides
what should happen, and appends with an expected event-log version:

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
- **Partition-store scope**: conflicts are limited to one partition store

### Version Numbers

`EventLogVersion` is monotonic inside one partition store:

- stored events start at version `1`
- `EventLogVersion::start()` is only a before-first read cursor
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

`EventNamespaces` keeps a bounded idle cache of opened stores. Active
`EventLog` handles remain valid even if the resolver evicts its cached
entry.

## Design Trade-offs

### Partition Store vs. Partitioning by Owner or Account

A small service can use one stable key such as `app/default`. Partitioning by owner or account
becomes useful when independent owners, accounts, clients, or similar units need
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
before using the event-log schema in production.

## Summary

The Events crate centers on one ordered log per partition store. The crate owns
append, read, rotation, and catalog mechanics. Applications choose partition
keys, store projection offsets, and manage active workflow state.
