# Getting Started

This tutorial creates a durable event stream for one partition store, appends events, and reads them back.

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
let partition = users.ensure_partition("user-123").await?;
let stream = partition.open().await?;
```

## 3. Append The First Event

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

The `workflow_*` fields are for resilient multi-step workflows and can stay empty for a basic append. The `actor_*` fields record who or what caused the event, such as a user, service, or system process.

## 4. Append The Next Event

Use the returned `last_version` when a later command depends on the stream state you just observed:

```rust
let next = stream
    .append(
        ExpectedVersion::Exact(result.last_version),
        [NewEvent {
            r#type: "UserEmailChanged".into(),
            payload: json!({ "email": "ada@example.com" }),
            workflow_kind: None,
            workflow: WorkflowRef::None,
            request_id: None,
            actor_id: "system-provisioning".into(),
            actor_type: ActorType::System,
        }],
    )
    .await?;
```

## 5. Read From The Start

```rust
use events::EventStreamVersion;

let events = stream
    .load_after_version(EventStreamVersion::start(), 100)
    .await?;
```

The first stored event has version `1`. `EventStreamVersion::start()` is only the
before-first read cursor.

For expected-version appends, read-model projections, and resilient workflows, see [Implement event handlers](../how-to/implement-event-handlers.md).
