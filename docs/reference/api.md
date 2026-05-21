# API Reference

Complete API documentation for the events crate's partition-store model.

## Table Of Contents

- [Partition Resolution](#partition-resolution)
- [Core Types](#core-types)
- [Append API](#append-api)
- [Read API](#read-api)
- [Workflow Metadata](#workflow-metadata)
- [Error Handling](#error-handling)
- [Runtime](#runtime)

## Partition Resolution

```rust
let namespaces = EventNamespaces::open(root, rotation_policy).await?;
let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition_exists("user-123").await?;
let log = partition.open().await?;
```

`EventNamespaces::open(root, rotation_policy)` creates the root manager for all
event namespaces under a directory. All event logs opened through the manager use
the same rotation policy.

The root manager has bounded idle-store cache knobs:

```rust
let namespaces = EventNamespaces::open(root, rotation_policy)
    .await?
    .with_max_open_stores(128)?
    .with_idle_store_ttl(Duration::from_secs(300))?;
```

`ensure_namespace(namespace)` validates and creates the namespace directory.
`EventNamespace::ensure_partition_exists(partition_key)` validates the partition
key and creates the partition store directory. Valid segments are lowercase ASCII
letters, digits, and `-`, length `1..=128`.

`EventNamespace::list_partitions()` returns immediate child directories with
valid partition keys, sorted by key. It skips files and invalid directory names,
and does not open or migrate partition stores.

## Core Types

### EventLogVersion

`EventLogVersion` is the public event-log cursor and event version type.

- Stored events start at version `1`.
- `EventLogVersion::new(0)` returns an error.
- `EventLogVersion::start()` is a before-first read cursor.
- `ExpectedVersion::Exact(EventLogVersion::start())` is rejected.

### ExpectedVersion

```rust
pub enum ExpectedVersion {
    NoStream,
    Any,
    Exact(EventLogVersion),
}
```

`NoStream` is the creation precondition. `Exact` is the normal command
consistency precondition. `Any` blind-appends after the current head.

### NewEvent

```rust
pub struct NewEvent {
    pub r#type: String,
    pub payload: serde_json::Value,
    pub workflow_kind: Option<String>,
    pub workflow: WorkflowRef,
    pub request_id: Option<String>,
    pub actor_id: String,
    pub actor_type: ActorType,
}
```

`NewEvent` does not carry partition identity. Partition context is chosen by the
opened store.

### EventEnvelope

```rust
pub struct EventEnvelope {
    pub id: uuid::Uuid,
    pub r#type: String,
    pub payload: serde_json::Value,
    pub version: EventLogVersion,
    pub created_at: time::OffsetDateTime,
    pub sequence: i64,
    pub workflow_kind: Option<String>,
    pub workflow_started_by_event_id: Option<uuid::Uuid>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub request_id: Option<String>,
    pub actor_id: String,
    pub actor_type: ActorType,
}
```

### AppendResult

```rust
pub struct AppendResult {
    pub first_version: EventLogVersion,
    pub last_version: EventLogVersion,
    pub events: Vec<EventEnvelope>,
}
```

## Append API

```rust
let result = log.append(ExpectedVersion::NoStream, events).await?;
```

`ExpectedVersion` values:

- `NoStream`: the event log must be empty.
- `Any`: append after the current log head without caller-supplied OCC.
- `Exact(version)`: the current log head must equal `version`.

`AppendResult` contains:

- `first_version`
- `last_version`
- `events`

Each returned `EventEnvelope` includes generated event ID, assigned event-log
version, timestamps, payload, actor fields, request ID, trace/span IDs, and
resolved workflow metadata. It does not include partition identity.

## Read API

```rust
let events = log
    .load_after_version(EventLogVersion::start(), 500)
    .await?;
```

Reads are exclusive: the event at the cursor version is not returned. Limits
must be within the crate-enforced bounded range.

Workflow reads use the starter event ID:

```rust
let workflow_events = log
    .load_workflow_after_version(starter_event_id, cursor, 500)
    .await?;
```

Unknown workflow anchors return an empty batch.

## Workflow Metadata

```rust
WorkflowRef::None
WorkflowRef::StartsThisWorkflow
WorkflowRef::Continues { started_by_event_id }
```

`workflow_kind` must be `None` when `workflow` is `None`, and must be present
when `workflow` starts or continues a workflow. Workflow kinds use lowercase
ASCII letters, digits, and `-`.

`Continues` is shape-validated only. Domain code may add stricter checks when it
needs to prove that a starter exists or matches a process type.

## Error Handling

Common public errors:

- `EsError::Concurrency`: expected version mismatch.
- `EsError::InvalidVersion`: invalid use of `EventLogVersion`.
- `EsError::InvalidWorkflowMetadata`: workflow kind/ref mismatch.
- `EsError::InvalidSafeName`: unsafe namespace, partition key, or workflow kind.
- `EsError::InvalidReadLimit`: read limit outside the bounded range.
- `EsError::CatalogDrift`: events committed but catalog head update failed.

## Runtime

`EventsRuntime` exposes `event_namespaces()` for partition-store resolution and
keeps the notification/broadcast helpers separate from durable event storage.
