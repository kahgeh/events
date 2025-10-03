# Getting Started with Event Sourcing

Welcome to the Events crate! This tutorial will guide you through your first event store implementation. You'll learn the fundamental concepts of event sourcing and how to use this crate effectively.

## What You'll Learn

- What event sourcing is and why it's useful
- How to set up your first event store
- Publishing events to streams
- Reading events back
- Understanding basic error handling

## Prerequisites

- Basic knowledge of Rust and async/await
- Understanding of JSON serialization (serde_json)
- About 15 minutes to complete

## What is Event Sourcing?

Instead of storing the current state of your data, event sourcing stores a sequence of events that describe every change that has occurred. Think of it like a bank account:

- **Traditional approach**: Store `balance: 150`
- **Event sourcing**: Store `[$100 deposit, $50 withdrawal, $100 deposit]`

This gives you a complete audit trail and the ability to reconstruct state at any point in time.

## Step 1: Add the Dependency

Add this to your `Cargo.toml`:

```toml
[dependencies]
events = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
serde_json = "1.0"
```

## Step 2: Create Your First Event Store

Let's create a simple program to manage a shopping cart:

```rust
use events::{EventStore, ExpectedVersion, NewEvent, RotationPolicy};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), events::EsError> {
    // 1. Create an event store with hourly partitions
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600), // 1 hour
            max_bytes: Some(512 * 1024 * 1024), // 512MB per partition
        },
    ).await?;

    println!("Event store created successfully!");
    Ok(())
}
```

Run this with `cargo run`. You should see a `./data` directory created with your first partition files.

## Step 3: Define Your Events

Events are just JSON data with a type. Let's define some shopping cart events:

```rust
// Events are simple structs with a type and JSON payload
let item_added = NewEvent {
    r#type: "ItemAdded".into(),
    payload: json!({
        "product_id": "prod-123",
        "quantity": 2,
        "price": 2999
    }),
};

let item_removed = NewEvent {
    r#type: "ItemRemoved".into(),
    payload: json!({
        "product_id": "prod-456",
        "quantity": 1
    }),
};

let cart_cleared = NewEvent {
    r#type: "CartCleared".into(),
    payload: json!({}),
};
```

## Step 4: Publish Events to a Stream

A **stream** represents the history of one entity (like a shopping cart). Let's publish some events:

```rust
// Append events to a new stream
let result = store.append(
    "cart-user-123",  // Stream ID (identifies this shopping cart)
    ExpectedVersion::NoStream,  // This stream should not exist yet
    vec![
        NewEvent {
            r#type: "CartCreated".into(),
            payload: json!({"user_id": "user-123"}),
        },
        item_added,
    ],
).await?;

println!("Successfully appended {} events", result.events.len());
println!("New stream version: {}", result.version);
```

### Understanding ExpectedVersion

The `ExpectedVersion` prevents concurrent modifications:

- `ExpectedVersion::NoStream`: Stream must not exist
- `ExpectedVersion::Any`: Don't check the version
- `ExpectedVersion::Exact(n)`: Stream must be at version `n`

## Step 5: Read Events Back

Now let's read the events from our stream:

```rust
// Load all events from a stream
let events = store.load("cart-user-123").await?;

println!("Found {} events in cart-user-123:", events.len());

for (i, event) in events.iter().enumerate() {
    println!("  {}: {} at {}",
        i + 1,
        event.r#type,
        event.created_at
    );
}
```

## Step 6: Handle Errors

Error handling is crucial in production code:

```rust
match store.append(
    "cart-user-123",
    ExpectedVersion::Exact(2), // Expect version 2
    vec![item_removed],
).await {
    Ok(result) => {
        println!("Successfully appended event, new version: {}", result.version);
    }
    Err(events::EsError::Concurrency { expected, actual, stream_id }) => {
        println!("Concurrency conflict on stream {}: expected {}, got {}",
            stream_id, expected, actual);
        // You might want to retry or notify the user
    }
    Err(e) => {
        println!("Other error: {}", e);
        return Err(e);
    }
}
```

## Complete Example

Here's the complete program:

```rust
use events::{EventStore, ExpectedVersion, NewEvent, RotationPolicy, EsError};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), EsError> {
    // Create event store
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    ).await?;

    let cart_id = "cart-user-123";

    // Create a new shopping cart
    let result = store.append(
        cart_id,
        ExpectedVersion::NoStream,
        vec![
            NewEvent {
                r#type: "CartCreated".into(),
                payload: json!({"user_id": "user-123"}),
            },
            NewEvent {
                r#type: "ItemAdded".into(),
                payload: json!({
                    "product_id": "prod-123",
                    "quantity": 2,
                    "price": 2999
                }),
            },
        ],
    ).await?;

    println!("Created cart with {} events, version: {}",
        result.events.len(), result.version);

    // Add another item
    let result = store.append(
        cart_id,
        ExpectedVersion::Exact(result.version), // Use the version from previous result
        vec![
            NewEvent {
                r#type: "ItemAdded".into(),
                payload: json!({
                    "product_id": "prod-456",
                    "quantity": 1,
                    "price": 1999
                }),
            },
        ],
    ).await?;

    println!("Added item, new version: {}", result.version);

    // Read all events
    let events = store.load(cart_id).await?;
    println!("Cart history ({} events):", events.len());

    for event in &events {
        println!("  - {} at {}", event.r#type, event.created_at);
    }

    Ok(())
}
```

## What's Next?

Congratulations! You've created your first event store. You've learned:

- How to create an event store with partitioning
- How to publish events to streams
- How to read events back
- How to handle concurrency conflicts

Ready for more? Try our [First Project tutorial](first-event-store.md) to build a complete application, or jump to the [Building Projections tutorial](building-projections.md) to learn how to create read models from your events.

## Common Questions

**Q: Why do we need rotation policies?**
A: Rotation policies prevent individual files from becoming too large. They help with:
- Performance (smaller files are faster to query)
- Backup/restore (can work with time-based chunks)
- Archival (old partitions can be moved to cold storage)

**Q: What happens if my application crashes mid-operation?**
A: The event store is ACID-compliant. Either all events in an append operation are saved, or none are. You'll never have a partially completed operation.

**Q: Can I query events across multiple streams?**
A: Yes! Use the `all_since()` method with a cursor to read events globally. This is covered in the Projections tutorial.