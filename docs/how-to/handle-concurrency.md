# Handle Concurrency in the Event Store

Event store systems often need to handle concurrent access to the same streams. This guide shows you how to manage concurrency conflicts, implement retry strategies, and build robust concurrent systems.

## What You'll Learn

- Understanding optimistic concurrency control
- Handling version conflicts
- Implementing retry strategies
- Managing concurrent projections
- Dealing with race conditions

## Understanding Concurrency in the Event Store

Concurrency conflicts occur when multiple processes try to append events to the same stream simultaneously. The Events crate uses **optimistic concurrency control** to prevent data corruption.

### The Problem Scenario

```
Process A: Read stream (version 5)
Process B: Read stream (version 5)
Process A: Append event expecting version 5 → Success (new version 6)
Process B: Append event expecting version 5 → Conflict! (stream is now version 6)
```

## ExpectedVersion Types

```rust
use events::ExpectedVersion;

pub enum ExpectedVersion {
    NoStream,      // Stream must not exist
    Any,          // Don't check version (use with caution)
    Exact(i64),   // Expect specific version
}
```

### When to Use Each Type

#### ExpectedVersion::NoStream
```rust
// For creating new streams
let result = store.append(
    "new-user-123",
    ExpectedVersion::NoStream,
    vec![user_created_event],
).await?;
```

#### ExpectedVersion::Exact(version)
```rust
// For updating existing streams
let result = store.append(
    "order-456",
    ExpectedVersion::Exact(current_version),
    vec![order_updated_event],
).await?;
```

#### ExpectedVersion::Any
```rust
// Rare cases where you don't care about conflicts
// WARNING: Can lead to lost updates
let result = store.append(
    "log-stream",
    ExpectedVersion::Any,
    vec![log_event],
).await?;
```

## Handling Concurrency Conflicts

### Basic Conflict Handling

```rust
use events::{EsError, ExpectedVersion};

pub async fn append_with_retry(
    store: &EventStore,
    stream_id: &str,
    events: Vec<NewEvent>,
    max_retries: u32,
) -> Result<AppendResult, EsError> {
    let mut retries = 0;

    loop {
        // Load current stream to get version
        let current_events = store.load(stream_id).await?;
        let expected_version = if current_events.is_empty() {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Exact(current_events.len() as i64)
        };

        match store.append(stream_id, expected_version, events.clone()).await {
            Ok(result) => return Ok(result),
            Err(EsError::Concurrency { expected, actual, .. }) => {
                retries += 1;
                if retries >= max_retries {
                    return Err(EsError::Concurrency {
                        expected,
                        actual,
                        stream_id: stream_id.to_string(),
                    });
                }

                // Wait before retrying (exponential backoff)
                let delay = Duration::from_millis(100 * 2_u64.pow(retries - 1));
                tokio::time::sleep(delay).await;

                println!("Retry {}/{} for stream {} (expected {}, got {})",
                    retries, max_retries, stream_id, expected, actual);
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Advanced Conflict Resolution

```rust
pub struct ConflictResolver {
    store: EventStore,
    merge_strategy: MergeStrategy,
}

pub enum MergeStrategy {
    Fail,
    Retry,
    MergeAndRetry,
    Custom(Box<dyn Fn(&[EventEnvelope], &[NewEvent]) -> Result<Vec<NewEvent>, EsError>>),
}

impl ConflictResolver {
    pub async fn resolve_and_append(
        &self,
        stream_id: &str,
        events: Vec<NewEvent>,
        expected_version: ExpectedVersion,
    ) -> Result<AppendResult, EsError> {
        loop {
            match self.store.append(stream_id, expected_version.clone(), events.clone()).await {
                Ok(result) => return Ok(result),
                Err(EsError::Concurrency { actual, .. }) => {
                    match &self.merge_strategy {
                        MergeStrategy::Fail => {
                            return Err(EsError::Concurrency {
                                expected: match expected_version {
                                    ExpectedVersion::Exact(v) => v,
                                    _ => -1,
                                },
                                actual,
                                stream_id: stream_id.to_string(),
                            });
                        }
                        MergeStrategy::Retry => {
                            // Reload and retry with new version
                            let current_events = self.store.load(stream_id).await?;
                            expected_version = ExpectedVersion::Exact(current_events.len() as i64);
                            continue;
                        }
                        MergeStrategy::MergeAndRetry => {
                            // More sophisticated merge logic
                            let current_events = self.store.load(stream_id).await?;
                            let merged_events = self.merge_events(&current_events, &events)?;
                            expected_version = ExpectedVersion::Exact(current_events.len() as i64);
                            events = merged_events;
                            continue;
                        }
                        MergeStrategy::Custom(merge_fn) => {
                            let current_events = self.store.load(stream_id).await?;
                            let merged_events = merge_fn(&current_events, &events)?;
                            expected_version = ExpectedVersion::Exact(current_events.len() as i64);
                            events = merged_events;
                            continue;
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn merge_events(
        &self,
        existing: &[EventEnvelope],
        new: &[NewEvent],
    ) -> Result<Vec<NewEvent>, EsError> {
        // Example: Merge shopping cart events
        let mut merged = Vec::new();
        let mut cart_state = HashMap::new();

        // Process existing events
        for event in existing {
            match event.r#type.as_str() {
                "ItemAdded" => {
                    let payload: ItemAdded = serde_json::from_value(event.payload.clone())?;
                    *cart_state.entry(payload.product_id).or_insert(0) += payload.quantity;
                }
                "ItemRemoved" => {
                    let payload: ItemRemoved = serde_json::from_value(event.payload.clone())?;
                    *cart_state.entry(payload.product_id).or_insert(0) -= payload.quantity;
                }
                _ => {}
            }
        }

        // Process new events and update state
        for event in new {
            match event.r#type.as_str() {
                "ItemAdded" => {
                    let payload: ItemAdded = serde_json::from_value(event.payload.clone())?;
                    *cart_state.entry(payload.product_id.clone()).or_insert(0) += payload.quantity;
                    merged.push(event.clone());
                }
                "ItemRemoved" => {
                    let payload: ItemRemoved = serde_json::from_value(event.payload.clone())?;
                    let current_qty = cart_state.get(&payload.product_id).unwrap_or(&0);
                    if *current_qty >= payload.quantity {
                        *cart_state.entry(payload.product_id.clone()).or_insert(0) -= payload.quantity;
                        merged.push(event.clone());
                    } else {
                        // Skip removal - not enough items
                        println!("Skipping item removal - insufficient quantity");
                    }
                }
                _ => merged.push(event.clone()),
            }
        }

        Ok(merged)
    }
}
```

## Concurrent Projections

### Lock-Based Projection Processing

```rust
use tokio::sync::Mutex;

pub struct ConcurrentProjection {
    projection: Box<dyn Projection>,
    locks: Arc<Mutex<HashMap<String, ()>>>,
}

impl ConcurrentProjection {
    pub async fn process_events(&self, events: Vec<EventEnvelope>) -> Result<(), EsError> {
        // Group events by stream for sequential processing
        let mut stream_groups: HashMap<String, Vec<EventEnvelope>> = HashMap::new();

        for event in events {
            let stream_id = self.extract_stream_id(&event);
            stream_groups.entry(stream_id).or_default().push(event);
        }

        // Process each stream's events sequentially
        let handles: Vec<_> = stream_groups.into_iter()
            .map(|(stream_id, events)| {
                let projection = &self.projection;
                let locks = Arc::clone(&self.locks);

                tokio::spawn(async move {
                    // Acquire lock for this stream
                    let _lock = {
                        let mut locks = locks.lock().await;
                        locks.entry(stream_id.clone()).or_insert(())
                    };

                    // Process events
                    projection.process_events(events).await
                })
            })
            .collect();

        // Wait for all processing to complete
        for handle in handles {
            handle.await??;
        }

        Ok(())
    }

    fn extract_stream_id(&self, event: &EventEnvelope) -> String {
        // Extract stream ID from event payload or metadata
        serde_json::from_value::<serde_json::Value>(event.payload.clone())
            .ok()
            .and_then(|v| v.get("order_id").or_else(|| v.get("user_id")).or_else(|| v.get("stream_id")))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string()
    }
}
```

## Testing Concurrent Scenarios

### Concurrent Append Tests

```rust
#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use std::sync::Arc;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn test_concurrent_append_with_conflicts() -> Result<(), EsError> {
        let store = Arc::new(EventStore::open_in_memory().await?);
        let stream_id = "test-concurrent";

        // Create initial event
        store.append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![NewEvent {
                r#type: "Created".into(),
                payload: json!({"initial": true}),
            }],
        ).await?;

        // Spawn multiple concurrent writers
        let mut handles: Vec<JoinHandle<Result<(), EsError>>> = Vec::new();

        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let stream_id_clone = stream_id.to_string();

            let handle = tokio::spawn(async move {
                for j in 0..5 {
                    let events = vec![NewEvent {
                        r#type: "Update".into(),
                        payload: json!({"writer": i, "update": j}),
                    }];

                    // Use retry logic
                    append_with_retry(&store_clone, &stream_id_clone, events, 3).await?;
                }
                Ok(())
            });

            handles.push(handle);
        }

        // Wait for all writers to complete
        for handle in handles {
            handle.await??;
        }

        // Verify all events were written
        let final_events = store.load(stream_id).await?;
        assert_eq!(final_events.len(), 1 + 10 * 5); // Initial + all updates

        Ok(())
    }

    #[tokio::test]
    async fn test_optimistic_concurrency() -> Result<(), EsError> {
        let store = Arc::new(EventStore::open_in_memory().await?);
        let stream_id = "test-optimistic";

        // Create initial stream
        let result = store.append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![NewEvent {
                r#type: "Created".into(),
                payload: json!({}),
            }],
        ).await?;

        let initial_version = result.version;

        // Try to append with wrong version
        let wrong_version = initial_version + 10;
        let result = store.append(
            stream_id,
            ExpectedVersion::Exact(wrong_version),
            vec![NewEvent {
                r#type: "Update".into(),
                payload: json!({}),
            }],
        ).await;

        assert!(matches!(result, Err(EsError::Concurrency { .. })));

        // Verify stream wasn't modified
        let events = store.load(stream_id).await?;
        assert_eq!(events.len(), 1);

        Ok(())
    }
}
```

## Best Practices for Concurrency

### 1. Always Handle Concurrency Errors

```rust
// Bad: Ignores concurrency errors
let _ = store.append(stream_id, ExpectedVersion::Exact(version), events).await;

// Good: Handles conflicts properly
match store.append(stream_id, ExpectedVersion::Exact(version), events).await {
    Ok(result) => handle_success(result),
    Err(EsError::Concurrency { .. }) => handle_conflict(),
    Err(e) => handle_other_error(e),
}
```

### 2. Use Appropriate Retry Strategies

```rust
// Exponential backoff with jitter
async fn retry_with_backoff<F, T, E>(mut f: F) -> Result<T, E>
where
    F: FnMut() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
    E: std::fmt::Display,
{
    let mut delay = Duration::from_millis(100);
    let mut attempts = 0;

    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) if attempts < 5 => {
                attempts += 1;

                // Add jitter to avoid thundering herd
                let jitter = rand::random::<f64>() * 0.1;
                let actual_delay = Duration::from_millis(
                    (delay.as_millis() as f64 * (1.0 + jitter)) as u64
                );

                println!("Attempt {} failed: {}, retrying in {:?}", attempts + 1, e, actual_delay);
                tokio::time::sleep(actual_delay).await;

                delay *= 2; // Exponential backoff
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 3. Design for Idempotency

```rust
// Design events that can be safely applied multiple times
pub struct ItemAdded {
    pub product_id: String,
    pub quantity: i32,
    pub timestamp: DateTime<Utc>,
}

// When processing, check if already applied
pub fn apply_item_added(cart: &mut Cart, event: ItemAdded) -> bool {
    if cart.items.contains_key(&event.product_id) {
        // Check if this event was already processed
        if let Some(last_update) = cart.last_updates.get(&event.product_id) {
            if *last_update >= event.timestamp {
                return false; // Skip duplicate
            }
        }
    }

    cart.items.insert(event.product_id.clone(), event.quantity);
    cart.last_updates.insert(event.product_id, event.timestamp);
    true
}
```

### 4. Use Proper Idempotency

```rust
// For projections, ensure operations are idempotent since transactions are per-statement
async fn process_projection_events(events: Vec<EventEnvelope>) -> Result<(), EsError> {
    for event in events {
        // Each operation runs as a separate statement (auto-commit per statement)
        process_event(&event).await?;
    }
    Ok(())
}
```

## Common Concurrency Pitfalls

### 1. Lost Updates

```rust
// Wrong: Using ExpectedVersion::Any can lose updates
store.append("stream", ExpectedVersion::Any, events).await?;

// Correct: Use proper version checking
let current_events = store.load("stream").await?;
let expected = ExpectedVersion::Exact(current_events.len() as i64);
store.append("stream", expected, events).await?;
```

### 2. Race Conditions in Projections

```rust
// Wrong: Processing events concurrently without coordination
for event in events {
    tokio::spawn(process_event(event)); // Can cause race conditions
}

// Correct: Process events sequentially or with proper locking
for event in events {
    process_event(event).await?; // Sequential processing
}
```

### 3. Inconsistent Read Models

```rust
// Wrong: Updating read models without idempotency checks
process_event(event).await?;
update_read_model(event).await?; // Can fail, leaving inconsistent state

// Correct: Ensure operations are idempotent
process_event(event).await?;  // Each statement is auto-committed
update_read_model(event).await?;  // Use upserts and idempotent operations
```

## Next Steps

- [Monitor Production](monitor-production.md) - Track concurrency issues
- [Scale Consumers](scale-consumers.md) - Handle high concurrent load
- [Reference: Error Types](../reference/error-types.md) - Complete error handling reference