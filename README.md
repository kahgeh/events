# events

Durable partition-store event logs for CQRS-style Rust services.

The crate stores one ordered log per partition store. Applications resolve a
safe partition key with `EventNamespaces`, explicitly ensure the partition store
exists, then open an `EventLog` for appends and bounded reads. Treating a
partition key as an owner is a scaling strategy, not a requirement of the plain
storage model.

```rust
use events::{
    ActorType, EventNamespaces, ExpectedVersion, NewEvent, EventLogVersion,
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
let partition = users.ensure_partition_exists("user-123").await?;
let store = partition.open().await?;

let result = store
    .append(
        ExpectedVersion::NoStream,
        [NewEvent {
            r#type: "UserCreated".into(),
            payload: json!({ "name": "Ada" }),
            workflow_kind: None,
            workflow: WorkflowRef::None,
            request_id: None,
            actor_id: "system-provisioning".into(),
            actor_type: ActorType::System,
        }],
    )
    .await?;

let next = store
    .load_after_version(EventLogVersion::start(), 100)
    .await?;

assert_eq!(result.last_version, next[0].version);
# Ok(())
# }
```

## Storage Model

- `EventNamespaces` owns a root directory and one `RotationPolicy`.
- `ensure_namespace(namespace)` selects or creates one namespace.
- `EventNamespace::ensure_partition_exists(partition_key)` creates the
  partition store directory if needed.
- `Partition::open()` opens the existing partition store as an `EventLog`.
- Event versions are local to the opened partition store and start at `1`.
- `EventLogVersion::start()` is only a before-first read cursor.
- Rotated files are internal. Reads use event-log versions, not file cursors.
- Projection offsets and active workflow state belong in the application DB.

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
events for one workflow run within the opened event log.

## Development

```bash
cargo fmt
cargo test --no-run
cargo test
cargo run --example basic_usage
cargo run --example partition_worker_pool
```
