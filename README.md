# events

Durable partition-store event logs for CQRS-style Rust services.

The crate stores one ordered log per partition store. Applications resolve a
safe partition key with `EventPartitions`, explicitly ensure the partition store
exists, then open an `OwnerEventStore` for appends and bounded reads. Treating a
partition key as an owner is a scaling strategy, not a requirement of the plain
storage model.

```rust
use events::{
    ActorType, EventPartitions, ExpectedVersion, NewEvent, OwnerLogVersion,
    RotationPolicy, WorkflowRef,
};
use serde_json::json;
use std::time::Duration;

# async fn example() -> events::Result<()> {
let partitions = EventPartitions::open(
    "./data/events",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: Some(512 * 1024 * 1024),
    },
)
.await?;

let partition = partitions.ensure_exists("users", "user-123").await?;
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
    .load_after_version(OwnerLogVersion::start(), 100)
    .await?;

assert_eq!(result.last_version, next[0].version);
# Ok(())
# }
```

## Storage Model

- `EventPartitions` owns a root directory and one `RotationPolicy`.
- `ensure_exists(namespace, partition_key)` validates lowercase filesystem-safe
  segments and creates the partition store directory if needed.
- `Partition::open()` opens the existing partition store as an `OwnerEventStore`.
- Event versions are local to the opened partition store and start at `1`.
- `OwnerLogVersion::start()` is only a before-first read cursor.
- Rotated files are internal. Reads use owner-log versions, not file cursors.
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
events for one workflow run within the opened partition log.

## Development

```bash
cargo fmt
cargo test --no-run
cargo test
cargo run --example basic_usage
cargo run --example partition_worker_pool
```
