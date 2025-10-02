# Partitioned Event Store

A production-ready, partitioned event store implementation in Rust with time-based partition rotation, optimistic concurrency control, and projector utilities.

## Features

- **Time-based Partitioning**: Automatic rotation of event files based on configurable time windows (daily, hourly, 15-minute intervals)
- **Size-based Rotation**: Optional size limits with alphabetical suffixes (_a, _b, etc.)
- **Optimistic Concurrency Control**: Prevents concurrent modifications to streams using version numbers
- **Atomic Cross-partition Cursors**: Seamless event replay across multiple partitions
- **Lease-based Consumer Support**: Multiple consumer instances with lease management
- **SHA256 Migration Verification**: Safe schema migrations with checksum verification
- **Production-ready**: Comprehensive error handling, logging, and performance optimizations

## Architecture

The system consists of:

- **Catalog DB**: Metadata about partitions, stream heads, and consumer offsets
- **Partition DBs**: Time-based event storage files (e.g., `events_20241002.db`, `events_20241002T12.db`)
- **Rotation Policy**: Determines when to create new partitions
- **Projectors**: Event processing utilities with checkpoint management

## Quick Start

```rust
use events::{
    EventStore, ExpectedVersion, NewEvent, RotationPolicy, EsError,
};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), EsError> {
    // Open event store with hourly partitions and 512MB size limit
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600), // 1 hour
            max_bytes: Some(512 * 1024 * 1024), // 512MB
        },
    ).await?;

    // Publish events with optimistic concurrency control
    let result = store.append(
        "order-123",
        ExpectedVersion::NoStream,
        vec![
            NewEvent {
                r#type: "OrderCreated".into(),
                payload: json!({"sku": "ABC", "qty": 1}),
            },
            NewEvent {
                r#type: "PaymentAuthorized".into(),
                payload: json!({"amount": 2999}),
            },
        ],
    ).await?;

    println!("Appended {} events, stream version: {}",
             result.events.len(), result.version);

    // Load events from a stream
    let events = store.load("order-123").await?;
    println!("Loaded {} events", events.len());

    Ok(())
}
```

## Core Concepts

### Event Store

The main interface for storing and retrieving events:

```rust
pub struct EventStore {
    // Internal implementation
}

impl EventStore {
    // Open a partitioned event store
    pub async fn open_partitioned(root: &str, rotation: RotationPolicy) -> Result<Self, EsError>;

    // Check if rotation is needed and perform it
    pub async fn maybe_rotate(&self) -> Result<(), EsError>;

    // Append events to a stream with OCC
    pub async fn append(&self, stream_id: &str, expected: ExpectedVersion,
                       events: impl IntoIterator<Item = NewEvent>) -> Result<AppendResult, EsError>;

    // Load all events from a stream
    pub async fn load(&self, stream_id: &str) -> Result<Vec<EventEnvelope>, EsError>;

    // Get events since a cursor across partitions
    pub async fn all_since(&self, cursor: PartitionedCursor, limit: i64)
        -> Result<(Vec<EventEnvelope>, PartitionedCursor), EsError>;
}
```

### Rotation Policy

Controls when partitions are rotated:

```rust
pub enum RotationPolicy {
    TimeWindow {
        window: Duration,           // Time window size
        max_bytes: Option<u64>,     // Optional size limit
    },
}
```

### Expected Version

Optimistic concurrency control:

```rust
pub enum ExpectedVersion {
    NoStream,      // Stream should not exist
    Any,          // Don't check version
    Exact(i64),   // Expect specific version
}
```

### Partitioned Cursor

Cross-partition navigation:

```rust
#[derive(Debug, Clone)]
pub struct PartitionedCursor {
    pub partition: String,      // Partition name
    pub created_at_ms: i64,     // Event timestamp
    pub event_id: Uuid,         // Event ID
}
```

## Projectors

Event processing with automatic checkpointing:

```rust
use events::{Projector, bootstrap_cursor};

let projector = Projector::new(store, "my-projection".to_string())
    .with_batch_size(500);

// Process events continuously
projector.run(|events| async move {
    for event in events {
        match event.r#type.as_str() {
            "OrderCreated" => {
                // Update read model
            }
            "PaymentAuthorized" => {
                // Update payment status
            }
            _ => {}
        }
    }
    Ok::<(), EsError>(())
}).await?;
```

### Advanced Projector Usage

With lease management and idempotent processing:

```rust
use events::{
    Projector, IdempotentProcessor, acquire_lease, release_lease,
    bootstrap_cursor, with_projection_tx, checkpoint,
};

// Acquire lease for exclusive processing
let lease_acquired = acquire_lease(&store, "my-projection", "worker-1", 60).await?;

if lease_acquired {
    let projector = Projector::new(store, "my-projection".to_string());

    projector.run(|events| async move {
        // Process within transaction
        with_projection_tx(&store, "my-projection", |conn| async move {
            for event in events {
                // Idempotent processing
                let processor = IdempotentProcessor::new(conn.clone(), "applied_events".to_string())?;
                processor.process(&event.id, || async move {
                    // Apply business logic
                    apply_event(event).await
                }).await?;
            }

            // Update checkpoint
            checkpoint(conn, "my-projection", &next_cursor).await?;
            Ok::<(), EsError>(())
        }).await
    }).await?;

    // Release lease
    release_lease(&store, "my-projection", "worker-1").await?;
}
```

## Partition Names

Partition names encode the time window:

- Daily: `events_20241002.db` (October 2, 2024)
- Hourly: `events_20241002T12.db` (October 2, 2024, 12:00 UTC)
- 15-minute: `events_20241002T1230.db` (October 2, 2024, 12:30 UTC)
- Size suffix: `events_20241002_a.db`, `events_20241002_b.db`

## Error Handling

The library uses a comprehensive error type:

```rust
pub enum EsError {
    Db(rusqlite::Error),
    Concurrency { expected: i64, actual: i64, stream_id: String },
    PayloadTooLarge { size: usize, max: usize },
    Serde(serde_json::Error),
    Uuid(uuid::Error),
    Io(std::io::Error),
    Migration(String),
    InvalidPartition(String),
    Cursor(String),
}
```

## Performance Considerations

- **WAL Mode**: Active partitions use Write-Ahead Logging for better performance
- **Batch Processing**: Projectors process events in batches to reduce transaction overhead
- **Connection Pooling**: Catalog connections are reused efficiently
- **Lazy Loading**: Sealed partitions are opened read-only as needed

## Testing

Run the comprehensive test suite:

```bash
cargo test
```

Run specific integration tests:

```bash
cargo test --test integration_tests
```

## Examples

- `examples/basic_usage.rs`: Complete example with event publishing and projector
- `tests/integration_tests.rs`: Comprehensive test coverage

## Configuration

### Rotation Windows

Choose appropriate windows based on your event volume:

```rust
// High volume systems
RotationPolicy::TimeWindow {
    window: Duration::from_secs(15 * 60), // 15 minutes
    max_bytes: Some(1024 * 1024 * 1024), // 1GB
}

// Typical systems
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600), // 1 hour
    max_bytes: Some(512 * 1024 * 1024), // 512MB
}

// Low volume systems
RotationPolicy::TimeWindow {
    window: Duration::from_secs(24 * 3600), // 1 day
    max_bytes: None, // No size limit
}
```

### Payload Size Limits

The default payload limit is 1MB. Configure based on your needs:

```rust
// In the event store implementation
max_payload_bytes: 10 * 1024 * 1024, // 10MB
```

## Production Deployment

### Backup Strategy

- Regular backups of the catalog database
- Archive sealed partition files
- Use checksums to verify backup integrity

### Monitoring

- Monitor partition rotation frequency
- Track lag for projectors
- Alert on database connection errors
- Monitor disk usage for partition files

### Scaling

- Multiple consumer instances using leases
- Horizontal scaling of read models
- Partition archiving for long-term storage

## License

This project is licensed under the MIT License.