# Getting Started

This tutorial creates a durable event stream for one partition, appends an event, and reads it back.

## 1. Open The Resolver

```rust
use events::{EventNamespaces, RotationPolicy};
use std::time::Duration;

let namespaces = EventNamespaces::open(
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
let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition_exists("user-123").await?;
let stream = partition.open().await?;
```

## 3. Append An Event

```rust
use events::{ActorType, ExpectedVersion, NewEvent, WorkflowRef};
use serde_json::json;

let result = stream
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
use events::EventStreamVersion;

let events = stream
    .load_after_version(EventStreamVersion::start(), 100)
    .await?;
```

The first stored event has version `1`. `EventStreamVersion::start()` is only the
before-first read cursor.
