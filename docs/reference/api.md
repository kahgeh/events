# API Reference

Complete API documentation for the Events crate, including all types, methods, and usage examples.

## Table of Contents

- [Core Types](#core-types)
- [EventStore](#eventstore)
- [Projector](#projector)
- [Rotation Policy](#rotation-policy)
- [Database Pool](#database-pool)
- [Progress Streaming](#progress-streaming)
- [EventsRuntime](#eventsruntime)
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
    pub r#type: String,              // Event type identifier
    pub payload: serde_json::Value,  // Event data
    pub request_id: Option<String>,  // Optional request ID for completion tracking
    pub actor_id: String,            // Actor who initiated this event
    pub actor_type: ActorType,       // Type of actor (User, System)
}
```

**Examples:**

```rust
use events::ActorType;

let event = NewEvent {
    r#type: "OrderCreated".to_string(),
    payload: json!({
        "order_id": "order-123",
        "customer_id": "customer-456",
        "total": 9999
    }),
    request_id: Some("req-abc123".to_string()),
    actor_id: "user_12345".to_string(),
    actor_type: ActorType::User,
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
    pub version: i64,                      // Position in stream
    pub created_at: time::OffsetDateTime,  // Event timestamp (milliseconds precision)
    pub trace_id: Option<String>,          // OpenTelemetry trace ID for correlation
    pub span_id: Option<String>,           // OpenTelemetry span ID for correlation
    pub request_id: Option<String>,        // Request ID for completion tracking
    pub actor_id: String,                  // Actor who initiated this event
    pub actor_type: ActorType,             // Type of actor (User, System)
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

### ActiveWorkflow

Represents an active workflow that may need recovery on restart.

```rust
pub struct ActiveWorkflow {
    pub stream_id: String,  // Stream ID where workflow events are stored
    pub event_id: Uuid,     // Event ID of workflow start event (e.g., PROVISION_REQUESTED)
}
```

**Use case:** Track in-progress workflows so they can be recovered if the projector crashes mid-workflow.

### ActorType

Identifies the type of actor who initiated an event.

```rust
pub enum ActorType {
    User,    // Human user (e.g., Clerk user ID)
    System,  // System component (e.g., projector, self-healer)
}
```

**Examples:**

```rust
// User actor
let actor_type = ActorType::User;
let actor_id = "user_12345".to_string();

// System actor
let actor_type = ActorType::System;
let actor_id = "system:provisioning-projector".to_string();
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

**Startup recovery:** On open, the store automatically scans the active partition to repair any streams whose catalog head is stale due to a prior crash (events committed to partition but `stream_heads` update did not land). This keeps startup cost proportional to the active partition, not the total dataset.

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

#### `load_since_event`

Loads events from a specific stream starting from a given event ID (inclusive).

```rust
pub async fn load_since_event(
    &self,
    stream_id: &str,
    from_event_id: uuid::Uuid,
) -> Result<Vec<EventEnvelope>, EsError>
```

**Parameters:**
- `stream_id`: Stream identifier
- `from_event_id`: Event ID to start from (inclusive)

**Returns:**
Vector of events from the specified event ID onwards

**Use case:** Workflow recovery - given the workflow start event ID, load all events from that point to derive current state.

**Example:**

```rust
// Recover workflow state after crash
let workflow = get_active_workflow(&store, "provisioning-projector").await?;
if let Some(wf) = workflow {
    let events = store.load_since_event(&wf.stream_id, wf.event_id).await?;
    // Derive current workflow state from events
    for event in events {
        println!("Workflow event: {} v{}", event.r#type, event.version);
    }
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

#### `reconcile_stream_head`

Reconciles the catalog stream head for a single stream with the actual maximum version found in partition databases. Use this to repair catalog drift for a specific stream.

```rust
pub async fn reconcile_stream_head(&self, stream_id: &str) -> Result<i64, EsError>
```

**Parameters:**
- `stream_id`: Stream to reconcile

**Returns:**
The reconciled version (0 if no events exist for the stream)

**Example:**

```rust
// After handling a CatalogDrift error
let repaired_version = store.reconcile_stream_head("order-123").await?;
println!("Stream repaired to version {}", repaired_version);
```

#### `recover_all_stale_heads`

Scans **all** partitions (including sealed historical ones) for streams whose actual max version exceeds the catalog head, and repairs the catalog. Use this as an admin/maintenance operation for full-dataset reconciliation.

```rust
pub async fn recover_all_stale_heads(&self) -> Result<(), EsError>
```

**Must be called when no concurrent appends are in progress.**

Cost is proportional to total stream cardinality across all partitions.

**Example:**

```rust
// Admin maintenance — full sweep
store.recover_all_stale_heads().await?;
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

## Progress Streaming

The progress streaming system enables real-time feedback for async operations.

### EventKind

Discriminates between event types.

```rust
pub enum EventKind {
    Progress,   // Intermediate progress update
    Completed,  // Operation completed successfully
    Failed,     // Operation failed
}
```

#### Methods

##### `is_terminal`

Returns true if this is a terminal event (Completed or Failed).

```rust
pub fn is_terminal(&self) -> bool
```

### ItemStatus

Status for individual items in batch operations.

```rust
pub enum ItemStatus {
    Pending,     // Not yet started
    InProgress,  // Currently processing
    Completed,   // Finished successfully
    Failed,      // Failed
}
```

### ItemProgress

Progress for individual items in batch operations.

```rust
pub struct ItemProgress {
    pub item_id: String,   // Unique identifier for the item
    pub status: ItemStatus, // Current status
    pub message: String,    // Human-readable message
}
```

### StreamEvent

A stream event for broadcasting to subscribers.

```rust
pub struct StreamEvent {
    pub request_id: String,              // Request ID for correlation
    pub stream_id: String,               // Stream ID where event originated
    pub timestamp: i64,                  // Unix timestamp
    pub kind: EventKind,                 // Progress, Completed, or Failed
    pub current_step: u32,               // Current step number (1-indexed)
    pub total_steps: u32,                // Total number of steps
    pub step_name: String,               // Human-readable step name
    pub payload: Option<serde_json::Value>, // Completion payload
    pub error_message: Option<String>,   // Error message (for Failed)
    pub retriable: Option<bool>,         // Whether operation can be retried
    pub items: Vec<ItemProgress>,        // Per-item progress (for batches)
}
```

#### Constructors

##### `progress`

Creates a progress event.

```rust
pub fn progress(
    request_id: String,
    stream_id: String,
    current_step: u32,
    total_steps: u32,
    step_name: String,
) -> Self
```

**Example:**

```rust
let event = StreamEvent::progress(
    "req-123".to_string(),
    "user:456".to_string(),
    2,
    5,
    "Creating machine".to_string(),
);
```

##### `completed`

Creates a completion event.

```rust
pub fn completed(
    request_id: String,
    stream_id: String,
    total_steps: u32,
    payload: Option<serde_json::Value>,
) -> Self
```

**Example:**

```rust
let event = StreamEvent::completed(
    "req-123".to_string(),
    "user:456".to_string(),
    5,
    Some(json!({"machine_id": "m-789", "url": "https://..."})),
);
```

##### `failed`

Creates a failure event.

```rust
pub fn failed(
    request_id: String,
    stream_id: String,
    current_step: u32,
    total_steps: u32,
    error: String,
    retriable: bool,
) -> Self
```

**Example:**

```rust
let event = StreamEvent::failed(
    "req-123".to_string(),
    "user:456".to_string(),
    3,
    5,
    "Network timeout".to_string(),
    true,  // can retry
);
```

#### Methods

##### `with_items`

Adds item progress for batch operations.

```rust
pub fn with_items(self, items: Vec<ItemProgress>) -> Self
```

##### `is_terminal`

Returns true if this is a terminal event.

```rust
pub fn is_terminal(&self) -> bool
```

### StreamEventSender

Handle for sending stream events from projectors.

```rust
pub struct StreamEventSender {
    // private fields
}
```

#### Methods

##### `send`

Sends a stream event asynchronously.

```rust
pub async fn send(&self, event: StreamEvent) -> Result<(), StreamEventSendError>
```

**Returns:** Error if the broadcast loop has been dropped.

##### `try_send`

Tries to send a stream event without blocking.

```rust
pub fn try_send(&self, event: StreamEvent) -> Result<(), StreamEventSendError>
```

**Returns:** Error if the channel is full or closed.

### StreamEventSendError

Error type for send operations.

```rust
pub enum StreamEventSendError {
    ChannelClosed,  // Broadcast loop stopped
    ChannelFull,    // Channel buffer is full
}
```

### StreamEventSubscriber

Handle for subscribing to stream event broadcasts.

```rust
pub struct StreamEventSubscriber {
    // private fields
}
```

#### Methods

##### `subscribe`

Subscribes to receive stream events.

```rust
pub fn subscribe(&self) -> broadcast::Receiver<Arc<StreamEvent>>
```

**Returns:** A receiver that will receive all stream events. If the receiver falls behind, older events will be dropped.

##### `subscriber_count`

Gets the current number of active subscribers.

```rust
pub fn subscriber_count(&self) -> usize
```

### StreamEventBroadcastLoop

The broadcast loop that receives and fans out stream events.

```rust
pub struct StreamEventBroadcastLoop {
    // private fields
}
```

#### Methods

##### `run`

Runs the broadcast loop until the sender channel is closed.

```rust
pub async fn run(self)
```

### NotificationsStore

Store for managing progress notifications with TTL-based expiration.

```rust
pub struct NotificationsStore {
    // private fields
}
```

#### Constructors

##### `new`

Opens or creates a notifications store with default TTL (5 minutes).

```rust
pub async fn new(path: &Path) -> Result<Self, EsError>
```

The database file (`notifications_store.db`) is created inside the given directory if it doesn't exist, or opened if it already exists. Data persists across restarts.

##### `with_ttl`

Opens or creates a notifications store with custom TTL.

```rust
pub async fn with_ttl(path: &Path, ttl: Duration) -> Result<Self, EsError>
```

#### Methods

##### `record`

Records a stream event for a request.

```rust
pub async fn record(&self, event: &StreamEvent) -> Result<(), EsError>
```

Uses UPSERT semantics - newer events replace older ones for the same request_id.

##### `get`

Looks up the latest event for a request_id.

```rust
pub async fn get(&self, request_id: &str) -> Result<Option<StreamEvent>, EsError>
```

**Returns:** None if no event exists or the event has expired.

##### `cleanup`

Cleans up expired events.

```rust
pub async fn cleanup(&self) -> Result<u64, EsError>
```

**Returns:** The number of deleted records.

### Factory Functions

#### `create_broadcast_system`

Creates a new stream event broadcast system with default capacities.

```rust
pub fn create_broadcast_system() -> (
    StreamEventSender,
    StreamEventSubscriber,
    StreamEventBroadcastLoop,
)
```

#### `create_broadcast_system_with_capacity`

Creates a new stream event broadcast system with custom capacities.

```rust
pub fn create_broadcast_system_with_capacity(
    sender_capacity: usize,
    broadcast_capacity: usize,
) -> (
    StreamEventSender,
    StreamEventSubscriber,
    StreamEventBroadcastLoop,
)
```

**Parameters:**
- `sender_capacity`: mpsc channel capacity (projector → broadcast loop)
- `broadcast_capacity`: broadcast channel capacity (broadcast loop → subscribers)

## EventsRuntime

The events runtime wires together the event store, notifications store, and broadcast system.

### RuntimeConfig

Configuration for EventsRuntime.

```rust
pub struct RuntimeConfig {
    pub data_dir: String,             // Data directory for databases
    pub events_store_ttl: Duration,   // TTL for stream events (default: 5 min)
    pub rotation_policy: RotationPolicy, // Partition rotation policy
}
```

#### Constructors

##### `new`

Creates a new config with the given data directory.

```rust
pub fn new(data_dir: impl Into<String>) -> Self
```

#### Methods

##### `with_events_store_ttl`

Sets the events store TTL.

```rust
pub fn with_events_store_ttl(mut self, ttl: Duration) -> Self
```

##### `with_rotation_policy`

Sets the rotation policy.

```rust
pub fn with_rotation_policy(mut self, policy: RotationPolicy) -> Self
```

### EventsRuntime

```rust
pub struct EventsRuntime {
    // private fields
}
```

#### Constructors

##### `new`

Creates a new EventsRuntime with the given configuration.

```rust
pub async fn new(config: RuntimeConfig) -> Result<Self, EsError>
```

##### `with_data_dir`

Creates a new EventsRuntime with default configuration.

```rust
pub async fn with_data_dir(data_dir: impl Into<String>) -> Result<Self, EsError>
```

#### Methods

##### `event_store`

Gets the event store for appending events.

```rust
pub fn event_store(&self) -> Arc<EventStore>
```

##### `notifications_store`

Gets the notifications store for recording and querying stream events.

```rust
pub fn notifications_store(&self) -> Arc<NotificationsStore>
```

##### `stream_event_sender`

Gets the stream event sender for projectors.

```rust
pub fn stream_event_sender(&self) -> StreamEventSender
```

##### `stream_event_subscriber`

Gets the stream event subscriber for services.

```rust
pub fn stream_event_subscriber(&self) -> StreamEventSubscriber
```

##### `take_broadcast_loop`

Takes the broadcast loop to spawn it manually.

```rust
pub fn take_broadcast_loop(&mut self) -> Option<StreamEventBroadcastLoop>
```

##### `spawn_broadcast_loop`

Spawns the broadcast loop and returns the join handle.

```rust
pub fn spawn_broadcast_loop(&mut self) -> Option<tokio::task::JoinHandle<()>>
```

**Example:**

```rust
use events::{EventsRuntime, RuntimeConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RuntimeConfig::new("./data")
        .with_events_store_ttl(Duration::from_secs(600));

    let mut runtime = EventsRuntime::new(config).await?;

    // Spawn the broadcast loop
    let _handle = runtime.spawn_broadcast_loop();

    // Access components
    let event_store = runtime.event_store();
    let sender = runtime.stream_event_sender();
    let subscriber = runtime.stream_event_subscriber();

    // Use sender in projectors
    let progress = StreamEvent::progress(
        "req-123".to_string(),
        "stream-1".to_string(),
        1, 3,
        "Processing".to_string(),
    );
    sender.send(progress).await?;

    Ok(())
}
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

#### `get_active_workflow`

Gets the active workflow for a consumer (if any).

```rust
pub async fn get_active_workflow(
    store: &EventStore,
    consumer: &str,
) -> Result<Option<ActiveWorkflow>, EsError>
```

**Returns:** The active workflow that was in progress when the consumer last checkpointed, or `None` if no workflow is active.

**Use case:** On projector startup, check if there's an incomplete workflow that needs recovery.

**Example:**

```rust
// On projector startup
let workflow = get_active_workflow(&store, "provisioning-projector").await?;
if let Some(wf) = workflow {
    tracing::info!(
        stream_id = %wf.stream_id,
        event_id = %wf.event_id,
        "Recovering incomplete workflow"
    );
    // Load events and recover state...
}
```

#### `checkpoint`

Saves a cursor position as a checkpoint with optional workflow tracking.

```rust
pub async fn checkpoint(
    store: &EventStore,
    consumer: &str,
    cursor: &PartitionedCursor,
    active_workflow: Option<&ActiveWorkflow>,
) -> Result<(), EsError>
```

**Parameters:**
- `store`: The event store
- `consumer`: Consumer/projection name
- `cursor`: Current cursor position
- `active_workflow`: Optional active workflow. `Some(workflow)` sets the workflow, `None` clears it (workflow complete or no workflow)

**Example:**

```rust
// Checkpoint with active workflow (mid-workflow)
let workflow = ActiveWorkflow {
    stream_id: "user:123".to_string(),
    event_id: provision_requested_event.id,
};
checkpoint(&store, "provisioning-projector", &cursor, Some(&workflow)).await?;

// Checkpoint with no workflow (workflow complete)
checkpoint(&store, "provisioning-projector", &cursor, None).await?;
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
    CatalogDrift { stream_id: String, committed_version: i64, source: Box<EsError> }, // Fatal: events committed but catalog stale — not retryable, recovery required
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