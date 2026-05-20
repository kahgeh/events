# Getting Started

This tutorial creates a partition event store, appends an event, and reads it back.

## 1. Open The Resolver

```rust
use events::{EventPartitions, RotationPolicy};
use std::time::Duration;

let partitions = EventPartitions::open(
    "./data/events",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: Some(512 * 1024 * 1024),
    },
)
.await?;
```

## 2. Ensure A Partition Store

```rust
let partition = partitions.ensure_exists("users", "user-123").await?;
let store = partition.open().await?;
```

## 3. Append An Event

```rust
use events::{ActorType, ExpectedVersion, NewEvent, WorkflowRef};
use serde_json::json;

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
```

## 4. Read From The Start

```rust
use events::OwnerLogVersion;

let events = store
    .load_after_version(OwnerLogVersion::start(), 100)
    .await?;
```

The first stored event has version `1`. `OwnerLogVersion::start()` is only the
before-first read cursor.
