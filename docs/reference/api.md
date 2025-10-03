# API Reference

Complete API documentation for the Events crate, including all types, methods, and usage examples.

## Table of Contents

- [Core Types](#core-types)
- [EventStore](#eventstore)
- [Projector](#projector)
- [Rotation Policy](#rotation-policy)
- [Database Pool](#database-pool)
- [Utility Types](#utility-types)

## Core Types

### ExpectedVersion

Controls optimistic concurrency when appending events.

```rust
pub enum ExpectedVersion {
    NoStream,      // Stream must not exist
    Any,          // Skip version checking (dangerous, can cause data corruption)
    Exact(i64),   // Expect specific version
}
```

**Examples:**

```rust
// Create new stream
ExpectedVersion::NoStream

// Append to existing stream
ExpectedVersion::Exact(5)

// Append without version checking (use with caution)
ExpectedVersion::Any
```

### NewEvent

Represents a new event to be appended to a stream.

```rust
pub struct NewEvent {
    pub r#type: String,           // Event type identifier
    pub payload: serde_json::Value, // Event data
}
```

**Examples:**

```rust
let event = NewEvent {
    r#type: "OrderCreated".to_string(),
    payload: json!({
        "order_id": "order-123",
        "customer_id": "customer-456",
        "total": 9999
    }),
};
```

### EventEnvelope

Contains a stored event with metadata.

```rust
pub struct EventEnvelope {
    pub id: uuid::Uuid,                    // Unique event ID
    pub stream_id: String,                 // Stream identifier
    pub r#type: String,                    // Event type
    pub payload: serde_json::Value,        // Event data
    pub created_at: time::OffsetDateTime,  // Event timestamp (milliseconds precision)
    pub version: i64,                      // Position in stream
}
```

### PartitionedCursor

Position marker for reading events across partitions.

```rust
pub struct PartitionedCursor {
    pub partition: String,    // Partition name
    pub created_at_ms: i64,   // Event timestamp in milliseconds
    pub event_id: uuid::Uuid, // Event ID
}
```

## EventStore

The main interface for storing and retrieving events.

### Constructors

#### `open_partitioned`

Opens a partitioned event store with rotation policy.

```rust
pub async fn open_partitioned(
    root: &str,
    rotation: RotationPolicy,
) -> Result<EventStore, EsError>
```

**Parameters:**
- `root`: Directory path for storing partition files
- `rotation`: Policy for creating new partitions

**Example:**

```rust
let store = EventStore::open_partitioned(
    "./data",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600), // 1 hour
        max_bytes: Some(512 * 1024 * 1024), // 512MB
    },
).await?;
```

### Methods

#### `append`

Appends events to a stream with optimistic concurrency control.

```rust
pub async fn append(
    &self,
    stream_id: &str,
    expected: ExpectedVersion,
    events: impl IntoIterator<Item = NewEvent>,
) -> Result<AppendResult, EsError>
```

**Parameters:**
- `stream_id`: Unique identifier for the stream
- `expected`: Expected version for concurrency control
- `events`: Collection of events to append

**Returns:**
`AppendResult` containing the appended events and new stream version

**Example:**

```rust
let result = store.append(
    "order-123",
    ExpectedVersion::NoStream,
    vec![
        NewEvent {
            r#type: "OrderCreated".into(),
            payload: json!({"total": 9999}),
        },
    ],
).await?;

println!("Appended {} events, new version: {}",
         result.events.len(), result.version);
```

#### `load`

Loads all events from a specific stream.

```rust
pub async fn load(&self, stream_id: &str) -> Result<Vec<EventEnvelope>, EsError>
```

**Parameters:**
- `stream_id`: Stream identifier

**Returns:**
Vector of events in chronological order

**Example:**

```rust
let events = store.load("order-123").await?;
for event in events {
    println!("Event {}: {}", event.version, event.r#type);
}
```

#### `all_since`

Reads events from a cursor position across all partitions.

```rust
pub async fn all_since(
    &self,
    cursor: PartitionedCursor,
    limit: i64,
) -> Result<(Vec<EventEnvelope>, PartitionedCursor), EsError>
```

**Parameters:**
- `cursor`: Starting position
- `limit`: Maximum number of events to return

**Returns:**
Tuple of events and next cursor position

**Example:**

```rust
let cursor = bootstrap_cursor(&store, "consumer_name").await?;
let (events, next_cursor) = store.all_since(cursor, 1000).await?;

for event in events {
    println!("Global event: {} at {}", event.r#type, event.created_at);
}
```

#### `maybe_rotate`

Checks if rotation is needed and performs it if necessary.

```rust
pub async fn maybe_rotate(&self) -> Result<(), EsError>
```

**Example:**

```rust
// Force rotation check before important operation
store.maybe_rotate().await?;
```

#### `get_stream_version`

Gets the current version of a stream (0 if stream doesn't exist).

```rust
pub async fn get_stream_version(&self, stream_id: &str) -> Result<i64, EsError>
```

**Parameters:**
- `stream_id`: Stream identifier

**Returns:**
Current stream version (0 for non-existent streams)

**Example:**

```rust
let version = store.get_stream_version("order-123").await?;
if version == 0 {
    println!("Stream doesn't exist yet");
} else {
    println!("Current version: {}", version);
}
```

#### `pool_stats`

Returns statistics about the database connection pool.

```rust
pub async fn pool_stats(&self) -> PoolStats
```

**Returns:**
`PoolStats` with connection and database information

**Example:**

```rust
let stats = store.pool_stats().await;
println!("Cached databases: {}", stats.cached_databases);
println!("Total active connections: {}", stats.total_active_connections);
```

## Projector

Handles continuous processing of events into read models.

### Constructors

#### `new`

Creates a new projector instance.

```rust
pub fn new(store: EventStore, consumer: String) -> Projector
```

**Parameters:**
- `store`: Event store to read from
- `consumer`: Unique identifier for this consumer/projection

### Configuration Methods

#### `with_batch_size`

Sets the batch size for event processing.

```rust
pub fn with_batch_size(self, batch_size: i64) -> Self
```

**Example:**

```rust
let projector = Projector::new(store, "my_projection".to_string())
    .with_batch_size(500);
```


### Methods

#### `run`

Starts continuous event processing.

```rust
pub async fn run<F, Fut>(&self, processor: F) -> Result<(), EsError>
where
    F: Fn(&[EventEnvelope]) -> Fut + Clone,
    Fut: Future<Output = Result<(), EsError>>,
```

**Parameters:**
- `processor`: Async function that processes event batches

**Example:**

```rust
projector.run(|events| async move {
    for event in events {
        match event.r#type.as_str() {
            "OrderCreated" => {
                // Process order created event
            }
            _ => {}
        }
    }
    Ok(())
}).await?;
```

## Rotation Policy

Controls when new partitions are created.

### Types

#### `TimeWindow`

Rotates based on time intervals and optional size limits.

```rust
pub enum RotationPolicy {
    TimeWindow {
        window: Duration,       // Time window size
        max_bytes: Option<u64>, // Optional size limit
    }
}
```

**Examples:**

```rust
// Hourly rotation with 512MB limit
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(512 * 1024 * 1024),
}

// Daily rotation with no size limit
RotationPolicy::TimeWindow {
    window: Duration::from_secs(24 * 3600),
    max_bytes: None,
}
```

### Utility Functions

#### `label_for`

Generates a human-readable label for a time window.

```rust
pub fn label_for(start_ms: i64, window: Duration) -> Result<String, EsError>
```

**Parameters:**
- `start_ms`: Start time in milliseconds since epoch
- `window`: Time window duration

**Returns:**
Human-readable label string

**Examples:**

```rust
use events::rotation::label_for;
use std::time::Duration;

// Daily label
let window = Duration::from_secs(24 * 3600);
let ms = 1727827200000; // 2024-10-02 00:00:00 UTC
let label = label_for(ms, window)?; // "20241002"

// Hourly label
let window = Duration::from_secs(3600);
let ms = 1727884800000; // 2024-10-02 16:00:00 UTC
let label = label_for(ms, window)?; // "20241002T16"

// 15-minute label
let window = Duration::from_secs(15 * 60);
let ms = 1727872200000; // 2024-10-02 12:30:00 UTC
let label = label_for(ms, window)?; // "20241002T1230"
```

#### `floor_to_window_ms`

Floors a timestamp to the start of its time window.

```rust
pub fn floor_to_window_ms(now_ms: i64, window: Duration) -> i64
```

**Parameters:**
- `now_ms`: Timestamp in milliseconds since epoch
- `window`: Time window duration

**Returns:**
Floored timestamp in milliseconds

## Database Pool

Manages Turso database connections for partition access.

### Constructors

#### `new`

Creates a new connection pool with default settings.

```rust
pub fn new<P: AsRef<Path>>(root: P) -> Result<DatabasePool, EsError>
```

**Parameters:**
- `root`: Root directory for database files

### Configuration Methods

#### `with_max_cached_databases`

Sets the maximum number of cached database instances.

```rust
pub fn with_max_cached_databases(self, max: usize) -> Self
```

### Methods

#### `get_connection`

Gets a connection for a specific partition.

```rust
pub async fn get_connection<P: AsRef<Path>>(&self, db_path: P) -> Result<PooledConnection, EsError>
```

#### `get_catalog_connection`

Gets a connection to the catalog database.

```rust
pub async fn get_catalog_connection(&self) -> Result<PooledConnection, EsError>
```

#### `stats`

Returns pool statistics.

```rust
pub async fn stats(&self) -> PoolStats
```

## Utility Types

### AppendResult

Result of a successful append operation.

```rust
pub struct AppendResult {
    pub events: Vec<EventEnvelope>, // Appended events with IDs
    pub version: i64,               // New stream version
}
```

### TableNameValidator

Validates table names to prevent SQL injection attacks.

```rust
pub struct TableNameValidator;
```

#### Methods

##### `validate_table_name`

Validates that a table name is safe to use in SQL queries.

```rust
pub fn validate_table_name(name: &str) -> Result<(), EsError>
```

**Rules:**
- Must be 3-64 characters long
- Can only contain letters, numbers, and underscores
- Must start with a letter
- Cannot be a SQL reserved word

### IdempotentProcessor

Utility for idempotent event processing that prevents duplicate processing.

```rust
pub struct IdempotentProcessor {
    // private fields
}
```

#### Constructors

##### `new`

Creates a new idempotent processor with a tracking table.

```rust
pub async fn new(conn: turso::Connection, table_name: String) -> Result<IdempotentProcessor, EsError>
```

#### Methods

##### `is_applied`

Checks if an event has already been processed.

```rust
pub async fn is_applied(&self, event_id: &uuid::Uuid) -> Result<bool, EsError>
```

##### `mark_applied`

Marks an event as processed.

```rust
pub async fn mark_applied(&self, event_id: &uuid::Uuid) -> Result<(), EsError>
```

##### `process`

Processes an event idempotently (only if not already processed).

```rust
pub async fn process<F, Fut>(&self, event_id: &uuid::Uuid, f: F) -> Result<bool, EsError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), EsError>>,
```

**Returns:**
`true` if the event was newly processed, `false` if it was already applied

### PoolStats

Database pool statistics.

```rust
pub struct PoolStats {
    pub cached_databases: usize,                // Number of cached database instances
    pub max_cached_databases: usize,            // Maximum cache size
    pub total_active_connections: usize,        // Total active connections across all databases
    pub instances: Vec<DatabaseInstanceStats>,  // Per-database statistics
}

pub struct DatabaseInstanceStats {
    pub path: String,                           // Database file path
    pub active_connections: usize,              // Number of active connections
    pub last_access: std::time::Instant,        // Last access time for LRU
}
```

### Lease Management Types

#### `acquire_lease`

Acquires an exclusive lease for a projection.

```rust
pub async fn acquire_lease(
    store: &EventStore,
    consumer: &str,
    owner: &str,
    ttl_secs: i64,
) -> Result<bool, EsError>
```

#### `renew_lease`

Renews an existing lease.

```rust
pub async fn renew_lease(
    store: &EventStore,
    consumer: &str,
    owner: &str,
    ttl_secs: i64,
) -> Result<bool, EsError>
```

#### `release_lease`

Releases a lease.

```rust
pub async fn release_lease(
    store: &EventStore,
    consumer: &str,
    owner: &str,
) -> Result<bool, EsError>
```

**Returns:**
`true` if the lease was released, `false` if the lease was not found or owned by another worker

#### `is_lease_valid`

Checks if a lease is still valid.

```rust
pub async fn is_lease_valid(
    store: &EventStore,
    consumer: &str,
) -> Result<bool, EsError>
```

**Returns:**
`true` if the lease exists and has not expired, `false` otherwise

### Checkpoint Management

#### `bootstrap_cursor`

Loads the last cursor position for a projection.

```rust
pub async fn bootstrap_cursor(
    store: &EventStore,
    consumer: &str,
) -> Result<PartitionedCursor, EsError>
```

#### `checkpoint`

Saves a cursor position as a checkpoint.

```rust
pub async fn checkpoint(
    conn: &turso::Connection,
    consumer: &str,
    cursor: &PartitionedCursor,
) -> Result<(), EsError>
```

#### `with_projection_tx`

Execute a projection function within a transaction context.

```rust
pub async fn with_projection_tx<F, Fut>(
    store: &EventStore,
    consumer: &str,
    f: F,
) -> Result<(), EsError>
where
    F: FnOnce(&turso::Connection) -> Fut,
    Fut: Future<Output = Result<(), EsError>>,
```

## Error Handling

All methods return `Result<T, EsError>` where `EsError` is defined as:

```rust
pub enum EsError {
    Db(turso::Error),                              // Database operation errors
    Concurrency { expected: i64, actual: i64, stream_id: String }, // Optimistic concurrency conflicts
    PayloadTooLarge { size: usize, max: usize },   // Event payload exceeds size limit
    Serde(serde_json::Error),                      // JSON serialization/deserialization errors
    Uuid(uuid::Error),                             // UUID parsing/generation errors
    Time(time::error::ComponentRange),             // Time-related errors
    Io(std::io::Error),                            // File system I/O errors
    Migration(String),                             // Database migration errors
    InvalidPartition(String),                      // Invalid or corrupted partition state
    Cursor(String),                                // Cursor-related errors
    InvalidPath(String),                           // Invalid file system paths
    InvalidTableName(String),                      // Table name validation errors
}
```

See the [Error Types Reference](error-types.md) for detailed error handling information.

## Examples

### Basic Usage

```rust
use events::{EventStore, ExpectedVersion, NewEvent, RotationPolicy};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create event store
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(512 * 1024 * 1024),
        },
    ).await?;

    // Append events
    let result = store.append(
        "my-stream",
        ExpectedVersion::NoStream,
        vec![
            NewEvent {
                r#type: "Started".into(),
                payload: json!({"value": 42}),
            },
        ],
    ).await?;

    // Load events
    let events = store.load("my-stream").await?;
    println!("Loaded {} events", events.len());

    Ok(())
}
```

### Projection Example

```rust
use events::{EventStore, Projector, bootstrap_cursor};

let store = EventStore::open_partitioned("./data", rotation_policy).await?;
let cursor = bootstrap_cursor(&store, "my_projection").await?;

let projector = Projector::new(store, "my_projection".to_string())
    .with_batch_size(1000);

projector.run(|events| async move {
    for event in events {
        println!("Processing event: {}", event.r#type);
        // Your projection logic here
    }
    Ok(())
}).await?;
```

### Concurrency Example

```rust
use events::{EventStore, ExpectedVersion, EsError};

async fn append_with_retry(
    store: &EventStore,
    stream_id: &str,
    events: Vec<NewEvent>,
) -> Result<AppendResult, EsError> {
    loop {
        // Get current version
        let current_events = store.load(stream_id).await?;
        let expected = if current_events.is_empty() {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Exact(current_events.len() as i64)
        };

        // Try to append
        match store.append(stream_id, expected, events.clone()).await {
            Ok(result) => return Ok(result),
            Err(EsError::Concurrency { .. }) => {
                // Conflict, retry
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Performance Considerations

- Use appropriate batch sizes for projections (100-1000 events)
- Set reasonable partition time windows (1-24 hours)
- Monitor database connection pool usage
- Consider WAL mode for better write performance
- Use size limits to prevent overly large partitions

## Thread Safety

- `EventStore` is thread-safe and can be shared across async tasks
- Each connection from the pool should be used by only one task at a time
- Projection processing should handle concurrent access to read models carefully