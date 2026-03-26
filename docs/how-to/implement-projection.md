# Implement Robust Event Projections

Projections transform your event streams into queryable read models. This guide shows you how to implement production-ready projections that are reliable, performant, and maintainable.

## What You'll Learn

- Designing effective read models
- Implementing idempotent processing
- Managing checkpoints and recovery
- Handling failures and retries
- Optimizing projection performance

## Before You Start

Complete the [Building Projections tutorial](../tutorial/building-projections.md) first to understand the basics.

## Projection Design Patterns

### 1. Materialized View Pattern

Create denormalized tables optimized for specific queries:

```rust
// Order status view for dashboard queries
pub struct OrderStatusProjection {
    store: EventStore,
    conn: turso::Connection,
}

impl OrderStatusProjection {
    pub async fn process_order_created(&self, event: &EventEnvelope) -> Result<(), EsError> {
        let payload: OrderCreated = serde_json::from_value(event.payload.clone())?;

        self.conn.execute(
            "INSERT INTO order_status_view (order_id, customer_id, customer_email, status, total_amount, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                payload.order_id,
                payload.customer.customer_id,
                payload.customer.email,
                "Created".to_string(),
                calculate_initial_total(&payload.items),
                payload.created_at,
                payload.created_at
            ),
        ).await?;

        Ok(())
    }
}
```

### 2. Aggregate Pattern

Maintain running aggregates for metrics:

```rust
pub struct SalesMetricsProjection {
    conn: turso::Connection,
}

impl SalesMetricsProjection {
    pub async fn process_payment_processed(&self, event: &EventEnvelope) -> Result<(), EsError> {
        let payload: PaymentProcessed = serde_json::from_value(event.payload.clone())?;

        // Update daily sales
        let date = payload.processed_at.format("%Y-%m-%d").to_string();

        self.conn.execute(
            r#"
            INSERT INTO daily_sales (date, total_sales, order_count)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(date) DO UPDATE SET
                total_sales = total_sales + ?2,
                order_count = order_count + ?3
            "#,
            (date, 1, 1),
        ).await?;

        // Update customer metrics
        self.conn.execute(
            r#"
            INSERT INTO customer_metrics (customer_id, lifetime_value, order_count)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(customer_id) DO UPDATE SET
                lifetime_value = lifetime_value + ?2,
                order_count = order_count + ?3
            "#,
            (payload.customer_id, payload.amount, 1),
        ).await?;

        Ok(())
    }
}
```

### 3. Lookup Table Pattern

Create efficient lookup tables for relationships:

```rust
pub struct ProductLookupProjection {
    conn: turso::Connection,
}

impl ProductLookupProjection {
    pub async fn process_item_added(&self, event: &EventEnvelope) -> Result<(), EsError> {
        let payload: OrderItemAdded = serde_json::from_value(event.payload.clone())?;

        // Track product popularity
        self.conn.execute(
            r#"
            INSERT INTO product_popularity (
                product_id, order_count, total_quantity, last_ordered
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(product_id) DO UPDATE SET
                order_count = order_count + ?2,
                total_quantity = total_quantity + ?3,
                last_ordered = ?4
            "#,
            (
                payload.item.product_id,
                1,
                payload.item.quantity,
                payload.added_at
            ),
        ).await?;

        Ok(())
    }
}
```

## Idempotent Processing

### Event Tracking Table

```rust
pub struct IdempotentProcessor {
    conn: turso::Connection,
    projection_name: String,
}

impl IdempotentProcessor {
    pub async fn is_event_processed(&self, event_id: &str) -> Result<bool, EsError> {
        let mut rows = self.conn.query(
            "SELECT 1 FROM processed_events WHERE event_id = ?1 AND projection_name = ?2 LIMIT 1",
            (event_id, &self.projection_name)
        ).await?;
        let result = rows.next().await?.is_some();

        Ok(result)
    }

    pub async fn mark_event_processed(&self, event_id: &str) -> Result<(), EsError> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

        self.conn.execute(
            "INSERT INTO processed_events (event_id, projection_name, processed_at) VALUES (?1, ?2, ?3)",
            (event_id, &self.projection_name, now),
        ).await?;

        Ok(())
    }

    pub async fn process_event_idempotently<F, Fut>(
        &self,
        event_id: &str,
        processor: F,
    ) -> Result<(), EsError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), EsError>>,
    {
        // Check if already processed
        if self.is_event_processed(event_id).await? {
            return Ok(());
        }

        // Process the event
        processor().await?;

        // Mark as processed
        self.mark_event_processed(event_id).await?;

        Ok(())
    }
}
```

### Idempotent Processing

```rust
impl OrderStatusProjection {
    pub async fn process_events_idempotently(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<(), EsError> {
        let conn = self.store.catalog.get_connection().await?;

        for event in events {
            // Skip if already processed
            let already_processed = {
                let mut rows = conn.query(
                    "SELECT 1 FROM processed_events WHERE event_id = ?1 LIMIT 1",
                    (event.id.to_string(),)
                ).await?;
                rows.next().await?.is_some()
            };

            if already_processed {
                continue;
            }

            // Process event (each statement is auto-committed)
            match event.r#type.as_str() {
                "OrderCreated" => self.process_order_created(&conn, &event).await?,
                "OrderConfirmed" => self.process_order_confirmed(&conn, &event).await?,
                "PaymentProcessed" => self.process_payment_processed(&conn, &event).await?,
                _ => {}
            }

            // Mark as processed
            let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
            conn.execute(
                "INSERT INTO processed_events (event_id, projection_name, processed_at) VALUES (?1, ?2, ?3)",
                (event.id.to_string(), "order_status", now),
            ).await?;
        }

        Ok(())
    }
}
```

## Checkpoint Management

### Storing Checkpoints

```rust
pub struct ProjectionCheckpoint {
    conn: turso::Connection,
    projection_name: String,
}

impl ProjectionCheckpoint {
    pub async fn save_checkpoint(&self, cursor: &PartitionedCursor) -> Result<(), EsError> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

        self.conn.execute(
            r#"
            INSERT INTO projection_checkpoints (projection_name, partition, created_at_ms, event_id, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(projection_name) DO UPDATE SET
                partition = excluded.partition,
                created_at_ms = excluded.created_at_ms,
                event_id = excluded.event_id,
                updated_at = excluded.updated_at
            "#,
            (
                &self.projection_name,
                &cursor.partition,
                cursor.created_at_ms,
                cursor.event_id.to_string(),
                now
            ),
        ).await?;

        Ok(())
    }

    pub async fn load_checkpoint(&self) -> Result<Option<PartitionedCursor>, EsError> {
        let mut rows = self.conn.query(
            "SELECT partition, created_at_ms, event_id FROM projection_checkpoints WHERE projection_name = ?1",
            (&self.projection_name,),
        ).await?;

        if let Some(row) = rows.next().await? {
            let partition = self.get_text_safe(&row, 0)?;
            let created_at_ms = self.get_integer_safe(&row, 1)?;
            let event_id_str = self.get_text_safe(&row, 2)?;
            let event_id = uuid::Uuid::parse_str(&event_id_str)
                .map_err(|e| EsError::Cursor(format!("Invalid UUID in checkpoint: {}", e)))?;

            Ok(Some(PartitionedCursor {
                partition,
                created_at_ms,
                event_id,
            }))
        } else {
            Ok(None)
        }
    }

    fn get_text_safe(&self, row: &turso::Row, index: usize) -> Result<String> {
        row.get_value(index)?
            .as_text()
            .ok_or_else(|| EsError::Cursor(format!("Expected text value at column {}", index)))
            .map(|s| s.to_string())
    }

    fn get_integer_safe(&self, row: &turso::Row, index: usize) -> Result<i64> {
        row.get_value(index)?
            .as_integer()
            .ok_or_else(|| EsError::Cursor(format!("Expected integer value at column {}", index)))
            .copied()
    }
}
```

### Bootstrap from Checkpoint

```rust
pub struct ProjectionRunner {
    store: EventStore,
    projection: Box<dyn Projection>,
    checkpoint: ProjectionCheckpoint,
}

impl ProjectionRunner {
    pub async fn start(&mut self) -> Result<(), EsError> {
        // Load last checkpoint
        let cursor = self.checkpoint.load_checkpoint().await?
            .unwrap_or_else(|| bootstrap_cursor(&self.store, &self.projection_name).await?);

        println!("Starting projection from cursor: {:?}", cursor);

        let mut current_cursor = cursor;

        loop {
            // Fetch batch of events
            let (events, next_cursor) = self.store.all_since(current_cursor.clone(), 1000).await?;

            if events.is_empty() {
                // No new events, wait and retry
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            // Process events
            self.projection.process_events(events).await?;

            // Update cursor
            current_cursor = next_cursor.clone();

            // Save checkpoint periodically
            self.checkpoint.save_checkpoint(&current_cursor).await?;
        }
    }
}
```

## Error Handling and Recovery

### Handler Error Contract (`run_with_handler`)

When using `Projector::run_with_handler()`, the handler controls what happens on failure
through its return value:

- **Return `Err`** for retryable failures (transient DB errors, network timeouts). The
  projector stops the batch, checkpoints up to the last successful event, and retries the
  failed event after a backoff.

- **Return `Ok(())`** for non-retryable failures (unknown event type, corrupt payload,
  domain validation errors). Handle the error inside the handler — log it, write a failure
  record to your domain state, emit a stream event if needed — then return `Ok(())` so the
  projector can checkpoint past it and continue.

```rust
impl ProjectorHandler for MyHandler {
    async fn handle_event(
        &self,
        event: &EventEnvelope,
        sender: &StreamEventSender,
    ) -> Result<(), ProjectorHandlerError> {
        match self.process(event).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_transient() => {
                // Retryable — return Err, projector will retry this event
                Err(ProjectorHandlerError::Handler(e.to_string()))
            }
            Err(e) => {
                // Non-retryable — record failure in domain state, move on
                tracing::error!(event_id = %event.id, error = %e, "permanent handler failure");
                self.record_failure(event, &e).await?;
                Ok(())
            }
        }
    }
}
```

This keeps error classification in the handler where the domain knowledge lives. The
projector infrastructure does not need to distinguish error types.

### Custom Retry Mechanism

```rust
pub struct RetryableProjection {
    inner: Box<dyn Projection>,
    max_retries: u32,
    retry_delay: Duration,
}

impl RetryableProjection {
    pub async fn process_events_with_retry(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<(), EsError> {
        let mut retries = 0;

        loop {
            match self.inner.process_events(events.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if retries < self.max_retries => {
                    retries += 1;
                    eprintln!("Projection failed (attempt {}): {}, retrying in {:?}", retries, e, self.retry_delay);
                    tokio::time::sleep(self.retry_delay).await;
                }
                Err(e) => {
                    eprintln!("Projection failed after {} retries: {}", retries, e);
                    return Err(e);
                }
            }
        }
    }
}
```

### Dead Letter Queue

```rust
pub struct ProjectionDLQ {
    conn: turso::Connection,
    projection_name: String,
}

impl ProjectionDLQ {
    pub async fn add_failed_event(
        &self,
        event: &EventEnvelope,
        error: &str,
        retry_count: i32,
    ) -> Result<(), EsError> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

        self.conn.execute(
            r#"
            INSERT INTO projection_dlq (
                projection_name, event_id, event_type, event_data,
                error_message, retry_count, failed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            (
                &self.projection_name,
                event.id.to_string(),
                &event.r#type,
                serde_json::to_string(&event).unwrap(),
                error,
                retry_count,
                now
            ),
        ).await?;

        Ok(())
    }

    pub async fn get_retryable_events(&self, max_retry_count: i32) -> Result<Vec<FailedEvent>, EsError> {
        let mut rows = self.conn.query(
            r#"
            SELECT event_id, event_type, event_data, retry_count
            FROM projection_dlq
            WHERE projection_name = ?1 AND retry_count < ?2
            ORDER BY failed_at ASC
            LIMIT 100
            "#,
            (&self.projection_name, max_retry_count),
        ).await?;

        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            events.push(FailedEvent {
                id: self.get_text_safe(&row, 0)?,
                event_type: self.get_text_safe(&row, 1)?,
                event_data: self.get_text_safe(&row, 2)?,
                retry_count: self.get_integer_safe(&row, 3)? as i32,
            });
        }

        Ok(events)
    }

    fn get_text_safe(&self, row: &turso::Row, index: usize) -> Result<String> {
        row.get_value(index)?
            .as_text()
            .ok_or_else(|| EsError::Cursor(format!("Expected text value at column {}", index)))
            .map(|s| s.to_string())
    }

    fn get_integer_safe(&self, row: &turso::Row, index: usize) -> Result<i64> {
        row.get_value(index)?
            .as_integer()
            .ok_or_else(|| EsError::Cursor(format!("Expected integer value at column {}", index)))
            .copied()
    }
}

pub struct FailedEvent {
    pub id: String,
    pub event_type: String,
    pub event_data: String,
    pub retry_count: i32,
}
```

## Performance Optimization

### Batch Processing

```rust
pub struct BatchProjection {
    batch_size: usize,
    flush_interval: Duration,
    pending_events: Vec<EventEnvelope>,
    last_flush: Instant,
}

impl BatchProjection {
    pub async fn add_event(&mut self, event: EventEnvelope) -> Result<(), EsError> {
        self.pending_events.push(event);

        let should_flush = self.pending_events.len() >= self.batch_size
            || self.last_flush.elapsed() >= self.flush_interval;

        if should_flush {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), EsError> {
        if self.pending_events.is_empty() {
            return Ok(());
        }

        let events = std::mem::take(&mut self.pending_events);
        self.process_batch(events).await?;
        self.last_flush = Instant::now();

        Ok(())
    }

    async fn process_batch(&self, events: Vec<EventEnvelope>) -> Result<(), EsError> {
        // Group events by type for efficient processing
        let mut grouped_events: HashMap<String, Vec<EventEnvelope>> = HashMap::new();

        for event in events {
            grouped_events.entry(event.r#type.clone())
                .or_default()
                .push(event);
        }

        // Process each group
        for (event_type, events) in grouped_events {
            match event_type.as_str() {
                "OrderCreated" => self.process_order_created_batch(events).await?,
                "PaymentProcessed" => self.process_payment_batch(events).await?,
                _ => self.process_generic_batch(events).await?,
            }
        }

        Ok(())
    }
}
```

### Parallel Processing

```rust
pub struct ParallelProjection {
    workers: usize,
    projection: Arc<dyn Projection>,
}

impl ParallelProjection {
    pub async fn process_events_parallel(
        &self,
        events: Vec<EventEnvelope>,
    ) -> Result<(), EsError> {
        // Split events into chunks for parallel processing
        let chunks: Vec<_> = events.chunks(self.workers).collect();

        let handles: Vec<_> = chunks.into_iter()
            .map(|chunk| {
                let projection = Arc::clone(&self.projection);
                let chunk = chunk.to_vec();

                tokio::spawn(async move {
                    projection.process_events(chunk).await
                })
            })
            .collect();

        // Wait for all workers to complete
        for handle in handles {
            handle.await??;
        }

        Ok(())
    }
}
```

## Schema Management

### Migration System

```rust
pub struct ProjectionMigration {
    conn: turso::Connection,
    projection_name: String,
}

impl ProjectionMigration {
    pub async fn migrate(&self) -> Result<(), EsError> {
        // Create migration tracking table
        self.conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS projection_migrations (
                projection_name TEXT NOT NULL,
                version INTEGER NOT NULL,
                applied_at INTEGER NOT NULL,
                PRIMARY KEY (projection_name, version)
            )
            "#
        ).await?;

        // Get current version
        let mut rows = self.conn.query(
            "SELECT MAX(version) FROM projection_migrations WHERE projection_name = ?1",
            (&self.projection_name,),
        ).await?;

        let current_version = if let Some(row) = rows.next().await? {
            self.get_integer_safe(&row, 0)? as i32
        } else {
            0
        };

        // Apply pending migrations
        let migrations = self.get_migrations();
        for (version, migration) in migrations {
            if version > current_version {
                println!("Applying migration {} for {}", version, self.projection_name);
                migration(&self.conn).await?;

                let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
                self.conn.execute(
                    "INSERT INTO projection_migrations (projection_name, version, applied_at) VALUES (?1, ?2, ?3)",
                    (&self.projection_name, version, now),
                ).await?;
            }
        }

        Ok(())
    }

    fn get_migrations(&self) -> Vec<(i32, MigrationFn)> {
        vec![
            (1, |conn| Box::pin(async move {
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY, status TEXT)"
                ).await?;
                Ok(())
            })),
            (2, |conn| Box::pin(async move {
                conn.execute(
                    "ALTER TABLE orders ADD COLUMN created_at INTEGER"
                ).await?;
                Ok(())
            })),
        ]
    }

    fn get_integer_safe(&self, row: &turso::Row, index: usize) -> Result<i64> {
        row.get_value(index)?
            .as_integer()
            .ok_or_else(|| EsError::Cursor(format!("Expected integer value at column {}", index)))
            .copied()
    }
}

type MigrationFn = Box<dyn FnOnce(&turso::Connection) -> Pin<Box<dyn Future<Output = Result<(), EsError>> + Send>> + Send>;
```

## Monitoring and Observability

### Metrics Collection

```rust
pub struct ProjectionMetrics {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    processing_time: AtomicU64,
    last_event_time: AtomicU64,
}

impl ProjectionMetrics {
    pub fn record_event_processed(&self, processing_time_ms: u64) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.processing_time.fetch_add(processing_time_ms, Ordering::Relaxed);
        self.last_event_time.store(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            Ordering::Relaxed
        );
    }

    pub fn record_event_failed(&self) {
        self.events_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> ProjectionStats {
        let events_processed = self.events_processed.load(Ordering::Relaxed);
        let events_failed = self.events_failed.load(Ordering::Relaxed);
        let total_processing_time = self.processing_time.load(Ordering::Relaxed);

        ProjectionStats {
            events_processed,
            events_failed,
            success_rate: if events_processed + events_failed > 0 {
                events_processed as f64 / (events_processed + events_failed) as f64
            } else {
                0.0
            },
            avg_processing_time_ms: if events_processed > 0 {
                total_processing_time / events_processed
            } else {
                0
            },
            last_event_timestamp: self.last_event_time.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct ProjectionStats {
    pub events_processed: u64,
    pub events_failed: u64,
    pub success_rate: f64,
    pub avg_processing_time_ms: u64,
    pub last_event_timestamp: u64,
}
```

## Best Practices

1. **Use proper time handling**: Always use `time::OffsetDateTime` instead of `Utc::now()`
2. **Use parameter binding**: Bind parameters as tuples with proper `?1, ?2, ...` syntax
3. **Handle cursor-based queries**: Use `while let Some(row) = rows.next().await?` for iteration
4. **Implement idempotency**: Handle duplicate events gracefully with tracking tables
5. **Track progress**: Save checkpoints regularly using proper cursor management
6. **Monitor health**: Track processing metrics and alert on issues
7. **Plan for failures**: Implement retry mechanisms and dead letter queues
8. **Test recovery**: Verify you can rebuild projections from scratch
9. **Version schemas**: Use migration systems for schema changes
10. **Validate database operations**: Use helper functions for safe row value extraction

## Next Steps

- [Handle Concurrency](handle-concurrency.md) - Manage concurrent event processing
- [Scale Consumers](scale-consumers.md) - Handle high-volume scenarios
- [Monitor Production](monitor-production.md) - Comprehensive production monitoring