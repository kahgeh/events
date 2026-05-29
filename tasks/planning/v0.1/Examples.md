# Examples.md

This document shows how to **use the event store SDK** for:
- Publishing events with optimistic concurrency
- Consuming events with projectors and checkpoints
- Rotating partitioned event DB files

All examples assume the **v5 design** (time‑window partitions, catalog, OCC, projector).

---

## 1. Publishing Events

```rust
use eventstore::{
    EventStore, ExpectedVersion, NewEvent, RotationPolicy, EsError,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), EsError> {
    // Open a store with daily partitions and 512MiB size cap
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: std::time::Duration::from_secs(24 * 3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    ).await?;

    // Rotate proactively (safe to call any time)
    store.maybe_rotate().await?;

    // Publish events to a stream with OCC
    let stream = "order-123";

    // First append: expect the stream to be empty
    store.append(
        stream,
        ExpectedVersion::NoStream,
        [
            NewEvent { r#type: "OrderCreated".into(), payload: json!({"sku":"ABC","qty":1}) },
            NewEvent { r#type: "PaymentAuthorized".into(), payload: json!({"amount": 1299}) },
        ]
    ).await?;

    // Next append: expect head version 2
    if let Err(EsError::IncorrectEventVersion { expected, actual, .. }) = store.append(
        stream,
        ExpectedVersion::Exact(2),
        [ NewEvent { r#type: "OrderPacked".into(), payload: json!({"warehouse":"W1"}) } ],
    ).await {
        eprintln!("conflict: expected {:?}, actual {:?}", expected, actual);
        // Resolve conflict by reloading stream, recomputing command, retrying with latest version
    }

    Ok(())
}
```

---

## 2. Consuming Events (Projector)

```rust
use eventstore::{EventStore, PartitionedCursor, RotationPolicy, EsError};

const CONSUMER: &str = "orders-readmodel";
const BATCH: i64 = 500;

#[tokio::main]
async fn main() -> Result<(), EsError> {
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: std::time::Duration::from_secs(24*3600),
            max_bytes: None,
        }
    ).await?;

    // Bootstrap cursor (from catalog or earliest)
    let mut cur: PartitionedCursor = eventstore::bootstrap_cursor(&store).await?;

    loop {
        // Fetch next batch (auto-advances partitions when sealed)
        let (events, next_cur) = store.all_since(cur.clone(), BATCH).await?;
        if events.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }

        // Apply projection atomically
        eventstore::with_projection_tx(&store, CONSUMER, |conn| async move {
            for e in &events {
                apply_order_projection(conn, e).await?;
            }
            eventstore::checkpoint(conn, CONSUMER, &next_cur).await?;
            Ok::<_, EsError>(())
        }).await?;

        cur = next_cur;
    }
}

// Example read model logic (pseudo-code)
async fn apply_order_projection(_conn: &eventstore::Conn, e: &eventstore::EventEnvelope) -> Result<(), EsError> {
    match e.r#type.as_str() {
        "OrderCreated" => { /* insert new row */ }
        "PaymentAuthorized" => { /* set paid flag */ }
        "OrderPacked" => { /* set packed flag */ }
        _ => {}
    }
    Ok(())
}
```

---

## 3. Rotating Event DB Files

### A) Background rotation loop

```rust
use eventstore::{EventStore, RotationPolicy, EsError};

#[tokio::main]
async fn main() -> Result<(), EsError> {
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: std::time::Duration::from_secs(3600), // hourly
            max_bytes: Some(256 * 1024 * 1024),           // 256MiB cap
        },
    ).await?;

    let s = store.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = s.maybe_rotate().await {
                eprintln!("rotation error: {e}");
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    // ... writers and projectors run here ...
    Ok(())
}
```

### B) Opportunistic rotation before append

```rust
store.maybe_rotate().await?; // cheap, no-op if not needed
store.append(...).await?;
```

---

## Testing Tip

For end-to-end tests, configure a **short window (e.g., 15s)** and a **tiny size cap (e.g., 128KiB)**. Append until rotation occurs, then assert:
- New partition file is created.
- `catalog.partitions` marks the old one as sealed.
- Projector cursor hops partitions seamlessly.
