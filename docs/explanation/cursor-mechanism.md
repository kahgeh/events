# Cursor Mechanism

Understanding how the Events crate enables efficient cross-partition navigation and reliable event processing through cursor-based positioning.

## The Cursor Problem

In partitioned event stores, consumers need to track their position across multiple partition files:

```
Partition 1: events_20241001T1200_a.db  [events 1-1000]
Partition 2: events_20241001T1300_a.db  [events 1001-2000]
Partition 3: events_20241001T1400_a.db  [events 2001-3000]
                ↑ Consumer processed up to here
```

A consumer might stop at event 1500, which is in the middle of partition 2. When it restarts, how does it know:
1. Which partition to start reading from?
2. What position within that partition?
3. How to continue seamlessly to partition 3?

## Cursor Architecture

### Cursor Definition

A cursor is a precise pointer to a specific event in the event stream:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub partition_name: String,        // e.g., "events_20241001T1300_a.db"
    pub event_id: String,              // UUID of specific event
    pub created_at: i64,               // Unix timestamp milliseconds
    pub stream_position: Option<i64>,  // Position within stream (if applicable)
}

impl Cursor {
    pub fn new(partition_name: String, event_id: String, created_at: i64) -> Self {
        Self {
            partition_name,
            event_id,
            created_at,
            stream_position: None,
        }
    }

    pub fn start_of_time() -> Self {
        Self {
            partition_name: String::new(),
            event_id: String::new(),
            created_at: 0,
            stream_position: None,
        }
    }
}
```

### Cursor Storage

Cursors are persisted in the catalog database:

```sql
CREATE TABLE consumer_offsets (
    consumer TEXT PRIMARY KEY,         -- Consumer identifier
    partition TEXT NOT NULL,          -- Current partition name
    cursor_created_at INTEGER NOT NULL, -- Timestamp of processed event
    cursor_event_id TEXT NOT NULL,    -- UUID of processed event
    updated_at INTEGER NOT NULL       -- Last update timestamp
);
```

## Cursor Navigation

### Forward Navigation

The most common use case - reading events forward from a cursor position:

```rust
impl EventStore {
    pub async fn read_from_cursor(
        &self,
        cursor: Option<Cursor>,
        limit: Option<usize>
    ) -> Result<Vec<EventEnvelope>> {
        let mut events = Vec::new();
        let mut remaining = limit.unwrap_or(usize::MAX);

        // Determine starting point
        let (start_partition, start_event_id, start_created_at) = match cursor {
            Some(c) => (c.partition_name, c.event_id, c.created_at),
            None => {
                // Start from earliest partition
                let earliest = self.catalog.get_earliest_partition().await?;
                (earliest.name, String::new(), 0)
            }
        };

        // Get partitions to read (in chronological order)
        let partitions = self.catalog.get_partitions_from_time(start_created_at).await?;

        for partition_name in partitions {
            if remaining == 0 {
                break;
            }

            let partition_db = self.open_partition(&partition_name).await?;

            let query = if partition_name == start_partition {
                // Start from specific event in first partition
                r#"
                SELECT * FROM events
                WHERE created_at > ?1 OR (created_at = ?1 AND id > ?2)
                ORDER BY created_at, id
                LIMIT ?
                "#
            } else {
                // Read entire partition from start
                r#"
                SELECT * FROM events
                ORDER BY created_at, id
                LIMIT ?
                "#
            };

            let params = if partition_name == start_partition {
                (&start_created_at, &start_event_id, remaining as i64)
            } else {
                (&remaining as i64,)
            };

            let mut rows = partition_db.query(query, params).await?;
            while let Some(row) = rows.next().await? {
                events.push(self.row_to_event_envelope(row)?);
                remaining -= 1;

                if remaining == 0 {
                    break;
                }
            }
        }

        Ok(events)
    }
}
```

### Backward Navigation

For scenarios like replaying recent events:

```rust
impl EventStore {
    pub async fn read_backward_from_cursor(
        &self,
        cursor: Cursor,
        limit: usize
    ) -> Result<Vec<EventEnvelope>> {
        let mut events = Vec::new();
        let mut remaining = limit;

        // Get partitions in reverse chronological order
        let partitions = self.catalog.get_partitions_before_time(cursor.created_at).await?;

        for partition_name in partitions {
            if remaining == 0 {
                break;
            }

            let partition_db = self.open_partition(&partition_name).await?;

            let query = r#"
            SELECT * FROM events
            WHERE created_at < ?1 OR (created_at = ?1 AND id < ?2)
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            "#;

            let mut rows = partition_db.query(query, (&cursor.created_at, &cursor.event_id, remaining as i64)).await?;
            while let Some(row) = rows.next().await? {
                events.push(self.row_to_event_envelope(row)?);
                remaining -= 1;

                if remaining == 0 {
                    break;
                }
            }
        }

        // Reverse to maintain chronological order
        events.reverse();
        Ok(events)
    }
}
```

### Stream-Specific Cursors

For reading specific streams efficiently:

```rust
impl EventStore {
    pub async fn read_stream_from_cursor(
        &self,
        stream_id: &str,
        cursor: Option<StreamCursor>,
        limit: usize
    ) -> Result<Vec<EventEnvelope>> {
        match cursor {
            Some(cursor) => {
                // Find partition containing cursor position
                let partition_info = self.catalog.find_partition_for_event(&cursor.event_id).await?;
                let partition_db = self.open_partition(&partition_info.name).await?;

                // Read from cursor position in that partition
                let query = r#"
                SELECT * FROM events
                WHERE stream_id = ?1 AND version > ?2
                ORDER BY version
                LIMIT ?
                "#;

                let mut events = Vec::new();
                let mut rows = partition_db.query(query, (stream_id, cursor.version, limit as i64)).await?;
                while let Some(row) = rows.next().await? {
                    events.push(self.row_to_event_envelope(row)?);
                }

                // If we didn't get enough events, continue to newer partitions
                if events.len() < limit {
                    let remaining = limit - events.len();
                    let newer_partitions = self.catalog.get_partitions_after(&partition_info.name).await?;

                    for partition_name in newer_partitions {
                        if remaining == 0 {
                            break;
                        }

                        let more_events = self.read_stream_from_partition(
                            &partition_name,
                            stream_id,
                            0, // Start from beginning
                            remaining
                        ).await?;

                        events.extend(more_events);
                        remaining -= more_events.len();
                    }
                }

                Ok(events)
            }
            None => {
                // No cursor - start from beginning
                self.read_stream(stream_id, StreamVersion::Start).await
            }
        }
    }
}
```

## Cursor Management

### Updating Cursors

```rust
impl Consumer {
    pub async fn update_cursor(
        &self,
        last_processed_event: &EventEnvelope
    ) -> Result<()> {
        let cursor = Cursor {
            partition_name: self.current_partition.clone(),
            event_id: last_processed_event.id.to_string(),
            created_at: last_processed_event.created_at.unix_timestamp_nanos() / 1_000_000,
            stream_position: Some(last_processed_event.version),
        };

        self.catalog.update_consumer_offset(
            &self.consumer_id,
            &cursor
        ).await?;

        Ok(())
    }
}

impl Catalog {
    pub async fn update_consumer_offset(
        &self,
        consumer_id: &str,
        cursor: &Cursor
    ) -> Result<()> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;

        self.pool.get().await?.execute(
            r#"
            INSERT INTO consumer_offsets
            (consumer, partition, cursor_created_at, cursor_event_id, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(consumer) DO UPDATE SET
                partition = excluded.partition,
                cursor_created_at = excluded.cursor_created_at,
                cursor_event_id = excluded.cursor_event_id,
                updated_at = excluded.updated_at
            WHERE consumer = ?1
            "#,
            (
                consumer_id,
                cursor.partition_name,
                cursor.created_at,
                cursor.event_id,
                now,
            )
        ).await?;

        Ok(())
    }
}
```

### Cursor Validation

```rust
impl Cursor {
    pub fn validate(&self, catalog: &Catalog) -> Result<()> {
        // Check if partition exists
        if !catalog.partition_exists(&self.partition_name)? {
            return Err(EsError::CursorError(
                format!("Partition {} does not exist", self.partition_name)
            ));
        }

        // Check if event ID exists in partition
        if !catalog.event_exists_in_partition(&self.partition_name, &self.event_id)? {
            return Err(EsError::CursorError(
                format!("Event {} not found in partition {}", self.event_id, self.partition_name)
            ));
        }

        // Validate timestamp consistency
        let event_timestamp = catalog.get_event_timestamp(&self.event_id)?;
        if event_timestamp != self.created_at {
            return Err(EsError::CursorError(
                format!("Timestamp mismatch for event {}: expected {}, found {}",
                    self.event_id, self.created_at, event_timestamp)
            ));
        }

        Ok(())
    }
}
```

### Cursor Repair

When cursors become invalid (e.g., partition deleted), they need repair:

```rust
impl CursorRepair {
    pub async fn repair_cursor(
        &self,
        consumer_id: &str,
        invalid_cursor: &Cursor
    ) -> Result<Cursor> {
        // Strategy 1: Find next available event
        if let Some(next_event) = self.find_next_event(invalid_cursor).await? {
            return Ok(Cursor::new(
                next_event.partition_name,
                next_event.id.to_string(),
                next_event.created_at.unix_timestamp_nanos() / 1_000_000,
            ));
        }

        // Strategy 2: Find previous available event
        if let Some(prev_event) = self.find_previous_event(invalid_cursor).await? {
            return Ok(Cursor::new(
                prev_event.partition_name,
                prev_event.id.to_string(),
                prev_event.created_at.unix_timestamp_nanos() / 1_000_000,
            ));
        }

        // Strategy 3: Start from beginning/end of time
        Ok(Cursor::start_of_time())
    }

    async fn find_next_event(&self, cursor: &Cursor) -> Result<Option<EventEnvelope>> {
        // Search forward from cursor position
        let partitions = self.catalog.get_partitions_from_time(cursor.created_at).await?;

        for partition_name in partitions {
            let partition_db = self.open_partition(&partition_name).await?;

            let query = r#"
            SELECT * FROM events
            WHERE created_at > ?1 OR (created_at = ?1 AND id > ?2)
            ORDER BY created_at, id
            LIMIT 1
            "#;

            let mut rows = partition_db.query(query, (&cursor.created_at, &cursor.event_id)).await?;
            if let Some(row) = rows.next().await? {
                return Ok(Some(self.row_to_event_envelope(row)?));
            }
        }

        Ok(None)
    }
}
```

## Cursor Performance Optimization

### Cursor Caching

```rust
struct CursorCache {
    cache: Arc<RwLock<HashMap<String, CachedCursor>>>,
    ttl: Duration,
}

struct CachedCursor {
    cursor: Cursor,
    cached_at: Instant,
    partition_info: PartitionInfo,
}

impl CursorCache {
    pub async fn get_or_load<F, Fut>(
        &self,
        consumer_id: &str,
        loader: F
    ) -> Result<Cursor>
    where
        F: FnOnce(&str) -> Fut,
        Fut: Future<Output = Result<Cursor>>,
    {
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(consumer_id) {
                if cached.cached_at.elapsed() < self.ttl {
                    return Ok(cached.cursor.clone());
                }
            }
        }

        // Load from database
        let cursor = loader(consumer_id).await?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(consumer_id.to_string(), CachedCursor {
                cursor: cursor.clone(),
                cached_at: Instant::now(),
                partition_info: self.get_partition_info(&cursor.partition_name)?,
            });
        }

        Ok(cursor)
    }
}
```

### Batch Cursor Updates

```rust
impl BatchCursorManager {
    pub async fn update_many(
        &self,
        updates: Vec<(String, EventEnvelope)>
    ) -> Result<()> {
        let mut tx = self.catalog.pool.get().await?.begin().await?;

        for (consumer_id, event) in updates {
            let cursor = Cursor::new(
                self.current_partition.clone(),
                event.id.to_string(),
                event.created_at.unix_timestamp_nanos() / 1_000_000,
            );

            tx.execute(
                r#"
                INSERT INTO consumer_offsets
                (consumer, partition, cursor_created_at, cursor_event_id, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(consumer) DO UPDATE SET
                    partition = excluded.partition,
                    cursor_created_at = excluded.cursor_created_at,
                    cursor_event_id = excluded.cursor_event_id,
                    updated_at = excluded.updated_at
                "#,
                (
                    consumer_id,
                    cursor.partition_name,
                    cursor.created_at,
                    cursor.event_id,
                    time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000
                )
            ).await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
```

## Use Cases and Patterns

### 1. Event Replay

```rust
pub struct EventReplayer {
    store: EventStore,
    consumer_id: String,
}

impl EventReplayer {
    pub async fn replay_from_timestamp(
        &self,
        from_timestamp: time::OffsetDateTime
    ) -> Result<Vec<EventEnvelope>> {
        // Create cursor from timestamp
        let cursor = self.store.find_cursor_at_timestamp(from_timestamp).await?;

        // Read all events from that point
        self.store.read_from_cursor(Some(cursor), None).await
    }

    pub async fn replay_stream(
        &self,
        stream_id: &str,
        from_version: i64
    ) -> Result<Vec<EventEnvelope>> {
        let cursor = self.store.find_stream_cursor(stream_id, from_version).await?;
        self.store.read_stream_from_cursor(stream_id, Some(cursor), usize::MAX).await
    }
}
```

### 2. Change Data Capture

```rust
pub struct CDCProcessor {
    store: EventStore,
    consumer_id: String,
    checkpoint_interval: usize,
}

impl CDCProcessor {
    pub async fn start_processing(&mut self) -> Result<()> {
        let mut cursor = self.store.get_consumer_cursor(&self.consumer_id).await?;
        let mut processed_count = 0;

        loop {
            let events = self.store.read_from_cursor(
                Some(cursor.clone()),
                Some(1000) // Batch size
            ).await?;

            if events.is_empty() {
                // No new events, wait a bit
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Process events
            for event in &events {
                self.process_event(event).await?;
                cursor = Cursor::new(
                    self.store.get_current_partition_name().await?,
                    event.id.to_string(),
                    event.created_at.unix_timestamp_nanos() / 1_000_000,
                );

                processed_count += 1;

                // Checkpoint periodically
                if processed_count % self.checkpoint_interval == 0 {
                    self.store.update_cursor(&self.consumer_id, &cursor).await?;
                    processed_count = 0;
                }
            }

            // Final checkpoint for batch
            if processed_count > 0 {
                self.store.update_cursor(&self.consumer_id, &cursor).await?;
            }
        }
    }
}
```

### 3. Time-Travel Queries

```rust
impl EventStore {
    pub async fn get_state_at_time(
        &self,
        stream_id: &str,
        target_time: time::OffsetDateTime
    ) -> Result<Vec<EventEnvelope>> {
        // Find cursor just before target time
        let cursor = self.find_cursor_before_time(stream_id, target_time).await?;

        // Read all events up to that point
        self.store.read_stream_from_cursor(stream_id, Some(cursor), usize::MAX).await
    }

    pub async fn get_state_at_version(
        &self,
        stream_id: &str,
        target_version: i64
    ) -> Result<Vec<EventEnvelope>> {
        let cursor = self.find_cursor_at_version(stream_id, target_version).await?;
        self.store.read_stream_from_cursor(stream_id, Some(cursor), usize::MAX).await
    }
}
```

## Troubleshooting Cursor Issues

### Common Problems

1. **Stale Cursors**: Consumer falls behind and partition is archived
2. **Invalid Partitions**: Referenced partition no longer exists
3. **Corrupted Offsets**: Database inconsistency in offset table
4. **Time Skew**: Clock differences cause cursor ordering issues

### Diagnostic Queries

```sql
-- Find consumers with stale cursors
SELECT
    consumer,
    partition,
    cursor_created_at,
    julianday('now') - julianday(cursor_created_at / 1000, 'unixepoch') as days_behind
FROM consumer_offsets
WHERE cursor_created_at < (strftime('%s', 'now') - 7*24*60*60) * 1000;

-- Find consumers referencing non-existent partitions
SELECT co.consumer, co.partition
FROM consumer_offsets co
LEFT JOIN partitions p ON co.partition = p.name
WHERE p.name IS NULL;

-- Check cursor ordering within partitions
SELECT
    consumer,
    partition,
    MIN(cursor_created_at) as min_cursor,
    MAX(cursor_created_at) as max_cursor,
    COUNT(*) as offset_count
FROM consumer_offsets
GROUP BY consumer, partition
ORDER BY consumer, min_cursor;
```

### Recovery Procedures

```rust
impl CursorRecovery {
    pub async fn recover_stale_consumer(&self, consumer_id: &str) -> Result<Cursor> {
        // Find consumer's current cursor
        let current_cursor = self.get_consumer_cursor(consumer_id).await?;

        // Check if partition exists
        if !self.catalog.partition_exists(&current_cursor.partition_name)? {
            tracing::warn!("Partition {} for consumer {} no longer exists",
                current_cursor.partition_name, consumer_id);

            // Find next available partition
            if let Some(next_partition) = self.find_next_available_partition(&current_cursor.partition_name).await? {
                // Create new cursor at start of next partition
                let first_event = self.get_first_event_in_partition(&next_partition).await?;
                return Ok(Cursor::new(
                    next_partition,
                    first_event.id.to_string(),
                    first_event.created_at.unix_timestamp_nanos() / 1_000_000,
                ));
            }
        }

        // Partition exists, validate cursor
        if let Ok(()) = current_cursor.validate(&self.catalog) {
            return Ok(current_cursor);
        }

        // Cursor invalid, attempt repair
        self.repair_cursor(consumer_id, &current_cursor).await
    }
}
```

The cursor mechanism provides reliable navigation across partition boundaries while maintaining high performance and supporting complex event processing patterns.