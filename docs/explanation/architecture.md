# Architecture and Design Decisions

Understanding the architecture and design decisions behind the Events crate helps you use it effectively and make informed decisions about your event store implementation.

## Overview

The Events crate implements a **partitioned event store** that combines the benefits of append-only event streams with practical considerations for production systems. The architecture is designed around several key principles:

- **Immutability**: Events are never modified once written
- **Append-only**: New events are always appended to streams
- **Partitioning**: Time-based organization for performance and maintainability
- **Optimistic Concurrency**: Version-based conflict detection and resolution
- **ACID Compliance**: Reliable transaction handling with Turso

## Core Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Event Store API                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────┐             │
│  │   Catalog DB    │    │ Connection Pool     │             │
│  │ (Metadata)      │    │                     │             │
│  │ • Partitions    │    │ • Active connections│             │
│  │ • Stream heads  │    │ • Load balancing    │             │
│  │ • Cursors       │    │ • Connection reuse  │             │
│  └─────────────────┘    └─────────────────────┘             │
├─────────────────────────────────────────────────────────────┤
│                Partition Files                              │
│  ┌─────────────┐ ┌───────────────┐ ┌──────────────┐         │
│  │ events_...  │ │ events_...    │ │ events_...   │         │
│  │ 20241002.db │ │ 20241002T14.db│ │ 20241002_a.db│         │
│  │ (Active)    │ │ (Sealed)      │ │ (Sealed)     │         │
│  └─────────────┘ └───────────────┘ └──────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. Catalog Database

The catalog is a Turso database that stores metadata about the event store:

```sql
-- Partitions table
CREATE TABLE partitions (
    name TEXT PRIMARY KEY,           -- Partition filename (e.g., "events_20241002T1200_a.db")
    path TEXT NOT NULL,              -- Relative path from root directory
    start_ms INTEGER NOT NULL,       -- Partition start time (Unix timestamp ms)
    end_ms INTEGER,                  -- Partition end time (NULL for active partition)
    sealed INTEGER NOT NULL DEFAULT 0 -- 0=active, 1=sealed (read-only)
);

-- Stream heads table
CREATE TABLE stream_heads (
    stream_id TEXT PRIMARY KEY,        -- Unique stream identifier
    version INTEGER NOT NULL,          -- Current stream version
    last_created_at_ms INTEGER NOT NULL, -- Timestamp of last event
    last_event_id TEXT NOT NULL,       -- UUID of last event
    last_partition TEXT NOT NULL       -- Partition containing last event
);

-- Consumer offsets table
CREATE TABLE consumer_offsets (
    consumer TEXT PRIMARY KEY,         -- Consumer identifier
    partition TEXT NOT NULL,          -- Current partition name
    cursor_created_at INTEGER NOT NULL, -- Timestamp of processed event
    cursor_event_id TEXT NOT NULL,    -- UUID of processed event
    updated_at INTEGER NOT NULL,      -- Last update timestamp
    workflow_stream_id TEXT,          -- Active workflow stream (for crash recovery)
    workflow_event_id TEXT            -- Workflow start event ID (for crash recovery)
);
```

**Why a catalog database?**

- **Fast Lookups**: Quickly find which partition contains a stream
- **Metadata**: Track partition state and statistics
- **Checkpoints**: Store consumer positions for projections

### 2. Partition Files

Each partition is a separate Turso database containing events:

```sql
-- Events table
CREATE TABLE events (
    id TEXT PRIMARY KEY,              -- Event UUID
    stream_id TEXT NOT NULL,          -- Stream identifier
    type TEXT NOT NULL,               -- Event type name
    payload TEXT NOT NULL,            -- JSON event data
    version INTEGER NOT NULL,         -- Stream version number
    created_at INTEGER NOT NULL       -- Event timestamp (Unix timestamp ms)
);

-- Indexes for performance
-- Note: The UNIQUE constraint on (stream_id, version) automatically creates the stream index
CREATE INDEX idx_events_global ON events(created_at, id);
```

**Why separate files per partition?**

- **Performance**: Smaller databases are faster to query and backup
- **Concurrent Access**: Different partitions can be accessed simultaneously
- **Maintenance**: Old partitions can be archived or compacted independently
- **Resource Management**: Limits memory usage per partition

### 3. Rotation Engine

The rotation engine determines when to create new partitions:

```rust
pub struct RotationEngine {
    policy: RotationPolicy,
    current_partition: Option<Partition>,
}

impl RotationEngine {
    pub async fn should_rotate(&self, partition: &Partition) -> bool {
        match &self.policy {
            RotationPolicy::TimeWindow { window, max_bytes } => {
                let time_elapsed = partition.age() > *window;
                let size_exceeded = max_bytes.map_or(false, |max| partition.size_bytes() > max);
                time_elapsed || size_exceeded
            }
        }
    }
}
```

**Design considerations for rotation:**

- **Predictable Scheduling**: Time-based rotation creates predictable patterns
- **Size Limits**: Prevents individual files from becoming too large
- **Continuity**: Seamless reading across partition boundaries
- **Performance**: Balances file count vs. file size

## Data Flow

### Writing Events

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Application   │───▶│   Event Store    │───▶│  Partition DB   │
│                 │    │                  │    │                 │
│ NewEvent {      │    │ 1. Validate      │    │ 3. Insert       │
│   type: "..."   │    │ 2. Check version │    │ 4. Update index │
│   payload: {}   │    │ 5. Update catalog│    │ 5. Return ID    │
│ }               │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

1. **Validation**: Check event format and size limits
2. **Concurrency Check**: Verify expected version
3. **Partition Selection**: Choose current or create new partition
4. **Database Write**: Insert event into partition database
5. **Catalog Update**: Update stream head and statistics

### Reading Events

```
┌─────────────────┐    ┌───────────────────┐    ┌─────────────────┐
│   Application   │◀───│   Event Store     │◀───│  Partition DBs  │
│                 │    │                   │    │                 │
│ Load "order-123"│    │ 1. Lookup stream  │    │ 3. Query events │
│                 │    │ 2. Open partition │    │ 4. Return data  │
│                 │    │ 3. Cross-partition│    │                 │
│                 │    │    navigation     │    │                 │
└─────────────────┘    └───────────────────┘    └─────────────────┘
```

1. **Stream Lookup**: Find which partitions contain the stream
2. **Partition Access**: Open relevant partition databases
3. **Event Query**: Retrieve events in chronological order
4. **Cross-partition**: Seamlessly handle partition boundaries

### Projection Processing

```
┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Projector      │───▶│   Event Store   │───▶│  All Events     │
│                  │    │                 │    │                 │
│ Process batch    │    │ 1. Get cursor   │    │ 3. Stream events│
│ Update read model│    │ 2. Read events  │    │ 4. Update cursor│
│ Save checkpoint  │    │ 5. Track offset │    │                 │
└──────────────────┘    └─────────────────┘    └─────────────────┘
```

### Workflow Recovery

For multi-step workflows (like provisioning), the system tracks active workflows to enable recovery after crashes:

```
┌────────────────────────────────────────────────────────────────┐
│                    Projector Startup                           │
├────────────────────────────────────────────────────────────────┤
│  1. Check for active workflow (get_active_workflow)            │
│     ┌────────────────────────────────────────────────────┐     │
│     │ consumer_offsets                                   │     │
│     │ ├─ workflow_stream_id: "user:123"                  │     │
│     │ └─ workflow_event_id: "uuid-of-provision-requested"│     │
│     └────────────────────────────────────────────────────┘     │
│                                                                │
│  2. If workflow exists, load events since workflow start       │
│     ┌────────────────────────────────────────────────────┐     │
│     │ load_since_event("user:123", workflow_event_id)    │     │
│     │ → [PROVISION_REQUESTED, MACHINE_CREATED, ...]      │     │
│     └────────────────────────────────────────────────────┘     │
│                                                                │
│  3. Derive current state and decide: resume or cleanup         │
│                                                                │
│  4. Continue normal event processing                           │
└────────────────────────────────────────────────────────────────┘
```

**Checkpoint with workflow tracking:**

```rust
// When starting a workflow
let workflow = ActiveWorkflow {
    stream_id: "user:123".to_string(),
    event_id: provision_requested_event.id,
};
checkpoint(&store, consumer, &cursor, Some(&workflow)).await?;

// When workflow completes (success or failure)
checkpoint(&store, consumer, &cursor, None).await?;
```

## Concurrency Model

### Optimistic Concurrency Control

The system uses optimistic concurrency control rather than pessimistic locking:

```
Process A                     Process B
--------                     --------
Read stream (v5)              Read stream (v5)
                             |
Calculate new state           Calculate new state
                             |
Append with v5  ─────────────▶ Append with v5
Success                      |
                             Conflict! (stream is now v6)
                             |
                            Retry with v6
```

**Why optimistic concurrency?**

- **Performance**: No locking overhead during reads
- **Scalability**: Better for distributed systems
- **Deadlock Prevention**: No lock contention
- **User Experience**: Faster response times

### Version Numbers

Each stream maintains a monotonically increasing version number:

```
Stream: "order-123"
Version 0: (empty)
Version 1: OrderCreated { total: 100 }
Version 2: ItemAdded { item: "ABC", qty: 2 }
Version 3: PaymentProcessed { amount: 100 }
```

**Benefits of version numbers:**

- **Conflict Detection**: Easy to detect concurrent modifications
- **Ordering**: Guarantees event order within a stream
- **Checkpoints**: Useful for projection recovery
- **Optimizations**: Can skip already processed events

## Error Handling Strategy

### Error Categories

```rust
pub enum EsError {
    // Database errors - retryable
    Db(turso::Error),

    // Concurrency conflicts - retryable
    Concurrency { expected: i64, actual: i64, stream_id: String },

    // Configuration errors - not retryable
    PayloadTooLarge { size: usize, max: usize },
    InvalidPartition(String),

    // System errors - may be retryable
    Io(std::io::Error),
    Migration(String),
}
```

### Retry Strategy

Different error types require different handling:

1. **Retryable Errors** (Database, Concurrency): Use exponential backoff
2. **Non-retryable Errors** (Validation, Configuration): Fail fast
3. **System Errors** (IO, Migration): Context-dependent retry logic

## Performance Considerations

### Write Performance

- **WAL Mode**: Turso Write-Ahead Logging for better concurrency
- **Batch Operations**: Group multiple events in single transactions
- **Connection Pooling**: Reuse database connections efficiently
- **Partition Rotation**: Prevents individual files from becoming bottlenecks

### Read Performance

- **Indexes**: Strategic indexes on common query patterns
- **Lazy Loading**: Open partitions only when needed
- **Caching**: Cache partition metadata and connection handles
- **Parallel Queries**: Multiple partitions can be queried simultaneously

### Memory Management

- **Streaming**: Process events in batches rather than loading all
- **Connection Limits**: Control maximum concurrent connections
- **Buffer Management**: Efficient serialization/deserialization
- **Garbage Collection**: Proper cleanup of unused resources

## Design Trade-offs

### Partitioning vs. Single Database

**Single Database Pros:**

- Simpler implementation
- Easier transactions across all data
- Single backup target

**Partitioning Pros:**

- Better performance (smaller files)
- Parallel access patterns
- Independent maintenance
- Scalable to large datasets

**Decision**: Partitioning provides better production characteristics despite complexity.

### Turso Database Advantages

**Turso Pros:**

- Zero configuration
- Edge-optimized architecture
- ACID compliant
- Excellent read performance
- Embedded storage with a path to external routing or storage integration when the application needs it
- Modern cloud-native design

**Turso Considerations:**

- Optimized for append-heavy workloads
- Read replica support belongs outside this crate's local ownership model
- Edge deployment capabilities
- Modern distributed architecture

**Decision**: Turso's modern architecture and built-in features make it ideal for durable event store systems.

### Time-based vs. Size-based Rotation

**Time-based Pros:**

- Predictable patterns
- Easy archival strategies
- Simple monitoring

**Size-based Pros:**

- Guarantees performance bounds
- Prevents runaway growth
- More precise control

**Decision**: Combine both approaches for optimal balance.

## Future Considerations

### Scalability Limits

Current architecture scales well up to:

- **Event Volume**: Millions of events per partition
- **Concurrent Writers**: Dozens per partition
- **Partition Count**: Hundreds before catalog becomes bottleneck

### Potential Enhancements

1. **Application Routing Hooks**: Make owner routing easier without changing the local store contract
2. **Compression**: Add optional compression for archived partitions
3. **Alternative Storage**: Support for other storage backends
4. **Stream Clustering**: Group related streams for optimization

### Migration Path

The architecture is designed to allow:

- **Schema Evolution**: Backward-compatible event schemas
- **Migration Tools**: Automated data migration between versions
- **Compatibility Layers**: Support for older client versions

## Summary

The Events crate architecture balances several competing concerns:

- **Simplicity vs. Performance**: Turso provides simplicity while partitioning adds performance
- **Consistency vs. Availability**: Strong consistency within partitions with high availability across partitions
- **Flexibility vs. Predictability**: Configurable policies with predictable behavior

This design enables a small embedded event store while maintaining the core benefits of immutability, auditability, and temporal querying that make durable event streams useful.
