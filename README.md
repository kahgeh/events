# events

`events` is a small event stream library for Rust applications that implements CQRS-style workflows without running a separate queue, streaming platform, or middleware service.

It provides an embedded durable event stream backed by Turso DB. Your application chooses a namespace and partition key, appends immutable JSON events with optimistic concurrency checks, and reads ordered batches by stable event-stream versions. Workflow metadata helps retry long-running processes, while optional progress notifications provide request-status updates when it's required.

## Usage

```rust
use events::{
    ActorType, EventNamespaces, ExpectedVersion, NewEvent, EventStreamVersion,
    RotationPolicy, WorkflowRef,
};
use serde_json::json;
use std::time::Duration;

# async fn example() -> events::Result<()> {
let namespaces = EventNamespaces::open(
    "./data/events",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: Some(512 * 1024 * 1024),
    },
)
.await?;

let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition("user-123").await?;
let stream = partition.open().await?;

let result = stream
    .append(
        ExpectedVersion::NoStream,
        [NewEvent {
            r#type: "UserCreated".into(),
            payload: json!({ "name": "Ada" }),
            workflow_kind: None,
            workflow: WorkflowRef::None,
            request_id: None,
            actor_id: "user_xxxx".into(),
            actor_type: ActorType::User,
        }],
    )
    .await?;

let next = stream
    .load_after_version(EventStreamVersion::start(), 100)
    .await?;

assert_eq!(result.last_version, next[0].version);
# Ok(())
# }
```

## Storage Model

- `EventNamespaces` owns a root directory and one `RotationPolicy`.
- `ensure_namespace(namespace)` selects or creates one namespace.
- `EventNamespace::ensure_partition(partition_key)` creates the
  partition store directory if needed.
- `Partition::open()` opens the existing partition store as an `EventStream`.
- Event versions are local to the opened partition store and start at `1`.
- `EventStreamVersion::start()` is only a before-first read cursor.
- Rotated files are internal. Reads use event-stream versions, not file cursors.
- `last_processed_event` and workflow failure state belong in the application DB.

Safe namespace and partition keys use only lowercase ASCII letters, digits, and
`-`, with length `1..=128`.

## Workflow Metadata

Workflow identity is independent of partition-store identity. A retryable
workflow run is identified by the event ID that started it:

- `WorkflowRef::None` requires `workflow_kind: None`.
- `WorkflowRef::StartsThisWorkflow` stores the generated event ID as
  `workflow_started_by_event_id`.
- `WorkflowRef::Continues { started_by_event_id }` stores the supplied starter
  ID. The crate shape-validates this but does not prove the starter exists.

Use `load_workflow_after_version(starter_id, cursor, limit)` to read bounded
events for one workflow run within the opened event stream.

## Development

```bash
cargo fmt
cargo test --no-run
cargo test
cargo run --example basic_usage
cargo run --example partition_worker_pool
```
