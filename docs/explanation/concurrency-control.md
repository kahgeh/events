# Concurrency Control

Understanding how the Events crate manages concurrent access to ensure data consistency and prevent race conditions in distributed systems.

## The Concurrency Problem

In event-sourced systems, multiple processes may try to write to the same stream simultaneously:

```rust
// Process A: Read current state
let events = store.read_stream("order-123", StreamVersion::Start).await?;
let current_version = events.last().unwrap().version; // = 5

// Process B: Also reads current state (same version)
let events = store.read_stream("order-123", StreamVersion::Start).await?;
let current_version = events.last().unwrap().version; // = 5

// Process A: Attempts to append
store.append("order-123", ExpectedVersion::Exact(5), vec![event_a]).await?; // Success

// Process B: Attempts to append (old version)
store.append("order-123", ExpectedVersion::Exact(5), vec![event_b]).await?; // FAILURE!
```

Without proper concurrency control, both processes might think they're writing to version 5, leading to:
- **Lost Updates**: One process overwrites the other's changes
- **Inconsistent State**: Stream version becomes unreliable
- **Race Conditions**: Non-deterministic behavior

## Optimistic Concurrency Control

The Events crate uses **Optimistic Concurrency Control (OCC)**, which assumes conflicts are rare and detects them at write time.

### How It Works

1. **Read Phase**: Read current stream version
2. **Validation Phase**: Check if version unchanged during write
3. **Write Phase**: Append if valid, reject if conflict

```rust
#[derive(Debug, Clone)]
pub enum ExpectedVersion {
    NoStream,    // Stream must not exist
    Any,         // Don't check version (dangerous)
    Exact(i64),  // Expect specific version
}

impl EventStore {
    pub async fn append(
        &self,
        stream_id: &str,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent>
    ) -> Result<AppendResult> {
        // 1. Get current stream state
        let current_state = self.get_stream_state(stream_id).await?;

        // 2. Validate expected version
        match expected_version {
            ExpectedVersion::NoStream => {
                if current_state.exists {
                    return Err(EsError::ConcurrencyError(
                        format!("Stream {} already exists", stream_id)
                    ));
                }
            }
            ExpectedVersion::Exact(expected) => {
                if current_state.version != expected {
                    return Err(EsError::ConcurrencyError(
                        format!("Expected version {}, found {}", expected, current_state.version)
                    ));
                }
            }
            ExpectedVersion::Any => {
                // No validation - dangerous, can cause data corruption!
            }
        }

        // 3. Append events atomically
        self.append_events_internal(stream_id, events, &current_state).await
    }
}
```

## Conflict Detection and Resolution

### Types of Conflicts

1. **Version Mismatch**: Most common - stream version changed
2. **Stream Existence**: Expected new stream but it exists
3. **Concurrent Append**: Two writers targeting same version

```rust
#[derive(Debug)]
pub enum ConcurrencyError {
    VersionMismatch {
        stream_id: String,
        expected: i64,
        actual: i64,
    },
    StreamAlreadyExists {
        stream_id: String,
    },
    ConcurrentWrite {
        stream_id: String,
        writer_id: String,
    },
}
```

### Resolution Strategies

#### 1. Retry with Updated Version

```rust
pub async fn append_with_retry<F>(
    &self,
    stream_id: &str,
    event_factory: F,
    max_retries: usize
) -> Result<AppendResult>
where
    F: Fn(i64) -> Vec<NewEvent>,
{
    let mut retries = 0;

    loop {
        // Read current version
        let current_version = self.get_stream_version(stream_id).await?;

        // Create events based on current state
        let events = event_factory(current_version);

        // Try to append
        match self.append(stream_id, ExpectedVersion::Exact(current_version), events).await {
            Ok(result) => return Ok(result),
            Err(EsError::ConcurrencyError(_)) if retries < max_retries => {
                retries += 1;
                // Backoff strategy
                tokio::time::sleep(Duration::from_millis(100 * retries)).await;
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}
```

**Usage Example:**
```rust
let result = store.append_with_retry("order-123", |current_version| {
    // Read current state and compute new events
    let current_state = read_aggregate_state(current_version)?;

    // Apply business logic
    let new_events = process_order(&current_state, &order_request)?;

    new_events
}, 3).await?;
```

#### 2. Command Sourcing Pattern

```rust
#[derive(Debug)]
pub struct Command {
    pub id: String,
    pub stream_id: String,
    pub expected_version: Option<i64>,
    pub data: CommandData,
}

pub async fn execute_command(
    &self,
    command: Command
) -> Result<Vec<EventEnvelope>> {
    // Load current aggregate state
    let current_state = self.load_aggregate(&command.stream_id).await?;

    // Validate command against current state
    let validation_result = validate_command(&current_state, &command.data)?;
    if !validation_result.is_valid {
        return Err(EsError::ValidationError(validation_result.errors));
    }

    // Execute command to produce events
    let new_events = execute_command_logic(&current_state, &command.data)?;

    // Determine expected version
    let expected_version = match command.expected_version {
        Some(v) => ExpectedVersion::Exact(v),
        None => ExpectedVersion::Exact(current_state.version),
    };

    // Append events
    let new_event_data: Vec<NewEvent> = new_events.into_iter()
        .map(|e| NewEvent {
            r#type: e.event_type,
            payload: e.payload,
        })
        .collect();

    let result = self.append(
        &command.stream_id,
        expected_version,
        new_event_data
    ).await?;

    Ok(result.events)
}
```

#### 3. Saga Pattern for Long Operations

```rust
pub struct SagaStep {
    pub name: String,
    pub execute: Box<dyn Fn() -> BoxFuture<'static, Result<Vec<NewEvent>>>>,
    pub compensate: Box<dyn Fn() -> BoxFuture<'static, Result<Vec<NewEvent>>>>,
}

pub async fn execute_saga(
    &self,
    saga_id: &str,
    steps: Vec<SagaStep>
) -> Result<()> {
    let mut executed_steps = Vec::new();

    for step in steps {
        match step.execute().await {
            Ok(events) => {
                // Store step execution
                let step_events = vec![
                    NewEvent {
                        r#type: "SagaStepStarted".into(),
                        payload: json!({
                            "saga_id": saga_id,
                            "step_name": step.name,
                        }),
                    },
                    NewEvent {
                        r#type: "SagaStepCompleted".into(),
                        payload: json!({
                            "saga_id": saga_id,
                            "step_name": step.name,
                            "events_count": events.len(),
                        }),
                    }
                ];

                let append_result = self.append(
                    saga_id,
                    ExpectedVersion::Any, // Saga uses its own concurrency
                    step_events
                ).await?;

                executed_steps.push((step, events));
            }
            Err(error) => {
                // Compensate executed steps
                for (executed_step, _) in executed_steps.iter().rev() {
                    if let Err(compensation_error) = executed_step.compensate().await {
                        tracing::error!(
                            "Compensation failed for step {}: {}",
                            executed_step.name,
                            compensation_error
                        );
                    }
                }

                return Err(error);
            }
        }
    }

    Ok(())
}
```

## Database-Level Concurrency

### Write Lock and Transaction Isolation

The append flow uses a layered defense for correctness:

1. **Write lock** — serializes the OCC-check-then-insert-then-catalog-update window
2. **BEGIN IMMEDIATE** — acquires a reserved DB lock eagerly to prevent deadlocks
3. **UNIQUE(stream_id, version)** — per-partition safety net catches any slip-through
4. **Version-guarded catalog upsert** — prevents older writes from regressing `stream_heads`
5. **Startup recovery** — scans the active partition on open to repair crash-induced drift

```rust
impl EventStore {
    async fn append(
        &self,
        stream_id: &str,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<AppendResult> {
        // Acquire write lock — held through both partition commit AND catalog update
        let active = self.active.write().await;

        // OCC check inside the write lock (not before) to prevent stale reads
        let current_version = self.validate_expected_version(stream_id, &expected_version).await?;

        let conn = self.pool.get_connection(&active.name).await?;

        // BEGIN IMMEDIATE prevents reader-to-writer upgrade deadlocks
        conn.execute("BEGIN IMMEDIATE", ()).await?;

        // Insert events; uniqueness constraint on (stream_id, version) is the safety net
        let result_events = self.insert_events(&conn, stream_id, events, current_version).await?;

        conn.execute("COMMIT", ()).await?;

        // Update catalog stream_heads — still under the write lock to prevent
        // cross-partition duplicate versions if another writer rotates partitions
        // Version guard: WHERE excluded.version > stream_heads.version
        self.update_stream_head_with_retry(stream_id, &active.name, &result_events).await?;

        drop(active); // Release write lock only after catalog is updated

        Ok(AppendResult { version: final_version, events: result_events })
    }
}
```

If the catalog update fails after exhausting retries, `append()` returns
`EsError::CatalogDrift` — a fatal, non-retryable error. Callers must address the
underlying cause and call `reconcile_stream_head` before resuming writes.
```

### Connection Pool Management

```rust
impl DatabasePool {
    pub async fn get_connection(&self) -> Result<PooledConnection> {
        // Wait for available connection with timeout
        tokio::time::timeout(
            Duration::from_secs(30),
            self.pool.acquire()
        ).await?
    }

    pub async fn with_transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Transaction) -> BoxFuture<'_, Result<R>>,
    {
        let mut conn = self.get_connection().await?;
        let mut tx = conn.begin_immediate().await?;

        match f(&mut tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
}
```

## Cross-Partition Concurrency

### Stream Head Coordination

```rust
impl Catalog {
    pub async fn update_stream_head(
        &self,
        stream_id: &str,
        version: i64,
        event_id: &str,
        partition_name: &str
    ) -> Result<()> {
        // Use UPSERT to handle concurrent updates
        self.pool.get().await?.execute(
            r#"
            INSERT INTO stream_heads (stream_id, version, last_created_at_ms, last_event_id, last_partition)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(stream_id) DO UPDATE SET
                version = excluded.version,
                last_created_at_ms = excluded.last_created_at_ms,
                last_event_id = excluded.last_event_id,
                last_partition = excluded.last_partition
            WHERE version < excluded.version
            "#,
            (
                stream_id,
                version,
                time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
                event_id,
                partition_name
            )
        ).await?;

        Ok(())
    }
}
```

### Distributed Consumer Coordination

```rust
impl ConsumerCoordination {
    pub async fn claim_lease(
        &self,
        consumer_id: &str,
        ttl: Duration
    ) -> Result<Lease> {
        let expires_at = time::OffsetDateTime::now_utc() + ttl;

        let result = self.pool.get().await?.execute(
            r#"
            INSERT INTO consumer_offsets (consumer, partition, cursor_created_at, cursor_event_id, updated_at, lease_owner, lease_expires_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(consumer) DO UPDATE SET
                lease_owner = excluded.lease_owner,
                lease_expires_at = excluded.lease_expires_at,
                updated_at = excluded.updated_at
            WHERE lease_expires_at < ?8 OR lease_owner = ?1
            "#,
            (
                consumer_id,
                self.initial_partition,
                0, // Initial cursor
                "",
                time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000,
                consumer_id,
                expires_at.unix_timestamp_nanos() / 1_000_000,
                expires_at.unix_timestamp_nanos() / 1_000_000
            )
        ).await?;

        if result.rows_affected() > 0 {
            Ok(Lease {
                consumer_id: consumer_id.to_string(),
                expires_at,
            })
        } else {
            Err(EsError::LeaseDenied(
                format!("Consumer {} lease denied", consumer_id)
            ))
        }
    }
}
```

## Performance Considerations

### Contention Points

1. **Stream Head Updates**: High contention on popular streams
2. **Active Partition**: All writes go to same partition
3. **Catalog Database**: Central coordination point

### Mitigation Strategies

#### 1. Stream Sharding

```rust
// Instead of single stream, use multiple shards
let stream_shards = [
    format!("orders-{}-{}", order_id, order_id % 10), // 10 shards
    format!("inventory-{}-{}", product_id, product_id % 5), // 5 shards
];

// Write to appropriate shard
let shard_id = calculate_shard(stream_id, shard_count);
store.append(&shard_id, expected_version, events).await?;
```

#### 2. Partition-Level Locking

```rust
impl EventStore {
    pub async fn append_with_partition_lock(
        &self,
        stream_id: &str,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent>
    ) -> Result<AppendResult> {
        // Acquire partition-specific lock
        let partition_name = self.get_active_partition_name().await?;
        let _lock = self.partition_locks.lock(&partition_name).await?;

        // Perform append with lock held
        self.append(stream_id, expected_version, events).await
    }
}
```

#### 3. Optimistic Lock-Free Reads

```rust
impl EventStore {
    pub async fn read_stream_optimistic(
        &self,
        stream_id: &str,
        from_version: StreamVersion
    ) -> Result<Vec<EventEnvelope>> {
        // Use snapshot data when possible
        if let Some(snapshot) = self.stream_cache.get(stream_id) {
            if snapshot.version >= from_version.min_version() {
                return Ok(snapshot.events.clone());
            }
        }

        // Fallback to database read
        self.read_stream_from_db(stream_id, from_version).await
    }
}
```

## Testing Concurrency

### Concurrent Write Test

```rust
#[tokio::test]
async fn test_concurrent_appends() {
    let store = EventStore::open_partitioned("./test_data", RotationPolicy::default()).await.unwrap();
    let stream_id = "test-concurrent";

    // Spawn concurrent writers
    let mut handles = Vec::new();
    for writer_id in 0..10 {
        let store_clone = store.clone();
        let stream_clone = stream_id.to_string();

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let event = NewEvent {
                    r#type: format!("Writer{}Event{}", writer_id, i),
                    payload: json!({"writer": writer_id, "index": i}),
                };

                match store_clone.append(&stream_clone, ExpectedVersion::Any, vec![event]).await {
                    Ok(_) => {},
                    Err(EsError::ConcurrencyError(_)) => {
                        // Expected - retry logic would handle this
                    },
                    Err(e) => panic!("Unexpected error: {}", e),
                }

                // Small delay to increase contention
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all events were written
    let events = store.read_stream(stream_id, StreamVersion::Start).await.unwrap();
    assert_eq!(events.len(), 1000); // 10 writers × 100 events
}
```

### Version Conflict Test

```rust
#[tokio::test]
async fn test_version_conflicts() {
    let store = EventStore::open_partitioned("./test_data", RotationPolicy::default()).await.unwrap();
    let stream_id = "test-conflict";

    // Create initial event
    let result = store.append(stream_id, ExpectedVersion::NoStream, vec![
        NewEvent { r#type: "Initial".into(), payload: json!({}) }
    ]).await.unwrap();
    assert_eq!(result.version, 1);

    // Two concurrent reads
    let read1 = store.read_stream(stream_id, StreamVersion::Start).await.unwrap();
    let read2 = store.read_stream(stream_id, StreamVersion::Start).await.unwrap();

    // First writer succeeds
    let result1 = store.append(stream_id, ExpectedVersion::Exact(1), vec![
        NewEvent { r#type: "Update1".into(), payload: json!({}) }
    ]).await.unwrap();
    assert_eq!(result1.version, 2);

    // Second writer fails with version conflict
    let result2 = store.append(stream_id, ExpectedVersion::Exact(1), vec![
        NewEvent { r#type: "Update2".into(), payload: json!({}) }
    ]).await;

    assert!(matches!(result2, Err(EsError::ConcurrencyError(_))));
}
```

## Best Practices

### 1. Always Use Expected Version

```rust
// Good - specify expected version
store.append("order-123", ExpectedVersion::Exact(current_version), events).await?;

// Bad - no version checking
store.append("order-123", ExpectedVersion::Any, events).await?; // Dangerous! Can cause data corruption
```

### 2. Implement Retry Logic

```rust
pub async fn safe_append<F>(
    &self,
    stream_id: &str,
    event_factory: F
) -> Result<AppendResult>
where
    F: Fn(i64) -> Vec<NewEvent>,
{
    let mut retries = 0;
    loop {
        let current_version = self.get_stream_version(stream_id).await?;
        let events = event_factory(current_version);

        match self.append(stream_id, ExpectedVersion::Exact(current_version), events).await {
            Ok(result) => return Ok(result),
            Err(EsError::ConcurrencyError(_)) if retries < 3 => {
                retries += 1;
                tokio::time::sleep(Duration::from_millis(100 * retries)).await;
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}
```

### 3. Design for Idempotency

```rust
#[derive(Debug, Clone)]
pub struct IdempotentEvent {
    pub id: String,
    pub r#type: String,
    pub payload: serde_json::Value,
}

impl EventStore {
    pub async fn append_idempotent(
        &self,
        stream_id: &str,
        expected_version: ExpectedVersion,
        events: Vec<IdempotentEvent>
    ) -> Result<AppendResult> {
        // Check for duplicate event IDs
        let existing_ids = self.check_event_exists(&events.iter().map(|e| &e.id).collect::<Vec<_>>()).await?;

        let new_events: Vec<NewEvent> = events.into_iter()
            .filter(|e| !existing_ids.contains(&e.id))
            .map(|e| NewEvent {
                r#type: e.r#type,
                payload: e.payload,
            })
            .collect();

        if new_events.is_empty() {
            // All events were duplicates
            return Ok(AppendResult {
                version: self.get_stream_version(stream_id).await?,
                events: vec![],
            });
        }

        self.append(stream_id, expected_version, new_events).await
    }
}
```

Concurrency control ensures data consistency in distributed systems while maintaining high performance through optimistic techniques and proper conflict resolution strategies.