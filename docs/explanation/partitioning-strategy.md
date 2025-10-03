# Partitioning Strategy

Understanding why and how the Events crate uses partitioning to achieve scalability, performance, and maintainability in event-sourced systems.

## What is Partitioning?

Partitioning is the practice of splitting event data into multiple separate database files based on time windows, rather than storing all events in a single monolithic database.

### Traditional Approach (Monolithic)

```
events.db (single file)
├── events (table with millions of rows)
├── indexes (growing continuously)
└── queries (scanning entire table)
```

### Partitioned Approach

```
events_20241001T0000_a.db  (12AM-1AM)
events_20241001T0100_b.db  (1AM-2AM, overflow)
events_20241001T0200_a.db  (2AM-3AM)
events_20241001T0300_a.db  (3AM-4AM)
...
catalog.db                 (metadata only)
```

## Why Partitioning Matters

### 1. Performance Optimization

**Problem with Monolithic Databases:**
As event count grows, several performance problems emerge:

- **Index Bloat**: Indexes grow linearly with event count
- **Query Performance**: Full table scans become slower
- **Write Amplification**: Every write updates multiple large indexes
- **Backup Times**: Entire database must be backed up together

**Partitioning Solution:**
Each partition contains a bounded subset of events:

- **Smaller Indexes**: Each partition has compact, efficient indexes
- **Bounded Queries**: Time-range queries only scan relevant partitions
- **Localized Writes**: Recent events write to active partition only
- **Incremental Backups**: Only changed partitions need backup

### 2. Operational Benefits

**File System Operations:**
```bash
# Archive old data
mv events_20240101T*.db /archive/2024/01/

# Delete test data (safe operation)
rm events_20240301T*_TEST.db

# Copy specific time range
cp events_20240601T* /backup/urgent/
```

**Maintenance Windows:**
- **Vacuum Operations**: Run on individual sealed partitions
- **Index Rebuilding**: Optimize one partition at a time
- **Schema Migrations**: Apply to specific partitions only

### 3. Scalability Patterns

**Horizontal Scaling:**
```rust
// Different streams can be on different partitions
let orders_partition = "events_20241001T1200_a.db";
let inventory_partition = "events_20241001T1200_b.db";
let user_events_partition = "events_20241001T1200_c.db";
```

**Time-based Distribution:**
```rust
// Recent data in memory-optimized partitions
let recent = EventStore::open_partitioned_with_config(
    "./data",
    RotationPolicy::TimeWindow { window: Duration::from_secs(15 * 60), max_bytes: Some(100 * 1024 * 1024) }
).await?;

// Historical data in compressed partitions
let historical = EventStore::open_partitioned_with_config(
    "./archive",
    RotationPolicy::TimeWindow { window: Duration::from_secs(24 * 60 * 60), max_bytes: Some(1024 * 1024 * 1024) }
).await?;
```

## Partition Naming Convention

The Events crate uses a systematic naming scheme:

```
events_{timestamp}_{suffix}.db

Examples:
events_20241001T1200_a.db    # Oct 1, 2024, 12:00 PM, first partition
events_20241001T1200_b.db    # Same time window, overflow partition
events_20241001T1300_a.db    # Oct 1, 2024, 1:00 PM, first partition
```

**Breaking Down the Name:**

1. **Prefix**: `events_` - Identifies partition files
2. **Timestamp**: `20241001T1200` - ISO 8601 format (YYYYMMDDTHHMM)
3. **Suffix**: `_A`, `_B`, `_C` - Overflow partitions when size limits exceeded

## Rotation Policies

### Time-Based Rotation

Events are grouped by fixed time windows:

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(60 * 60),     // 1 hour
    max_bytes: Some(1024 * 1024 * 1024),     // 1GB max
}
```

**Time Window Options:**

| Window Size | Use Case | Partitions/Day | Typical File Size |
|-------------|----------|----------------|-------------------|
| 15 minutes  | High volume trading | 96 | 100MB-2GB |
| 1 hour      | General purpose | 24 | 50MB-1GB |
| 4 hours     | Medium volume | 6 | 100MB-500MB |
| 24 hours    | Low volume | 1 | 10MB-200MB |

### Size-Based Rotation

Partitions split when they exceed size limits:

```rust
// Creates overflow partitions
RotationPolicy::TimeWindow {
    window: Duration::from_secs(60 * 60),
    max_bytes: Some(512 * 1024 * 1024), // 512MB limit
}
```

**Overflow Example:**
```
events_20241001T1200_a.db  (reaches 512MB)
events_20241001T1200_b.db  (created for overflow)
events_20241001T1200_c.db  (if b also fills)
```

## Partition Lifecycle

### 1. Creation

```rust
// New partition created automatically
let store = EventStore::open_partitioned("./data", rotation_policy).await?;

// Partition creation process:
// 1. Generate partition name based on current time
// 2. Create new Turso database file
// 3. Apply partition migrations
// 4. Register in catalog database
// 5. Set as active partition for writes
```

### 2. Active Phase

While a partition is active:
- **Writes**: All new events go to active partition
- **Reads**: Can read from any partition
- **Monitoring**: Track size and time limits
- **Rotation**: Check if rotation criteria met

```rust
// Active partition management
impl EventStore {
    async fn check_rotation(&self) -> Result<bool> {
        let active = self.active.read().await;

        // Check time window
        let should_rotate_time = should_rotate_by_time(&active.start_ms, self.rotation.window());

        // Check size limit
        let should_rotate_size = should_rotate_by_size(&active.db, self.rotation.max_bytes());

        Ok(should_rotate_time || should_rotate_size)
    }
}
```

### 3. Sealing

When rotation criteria met:
1. **Stop Writes**: Redirect new writes to new partition
2. **Update Catalog**: Mark partition as sealed
3. **Optimize**: Run VACUUM and ANALYZE
4. **Archive**: Move to long-term storage if needed

```rust
// Partition sealing process
async fn seal_partition(&self, partition_name: &str) -> Result<()> {
    // Mark as sealed in catalog
    self.catalog.seal_partition(partition_name).await?;

    // Optimize the sealed partition
    let partition_db = self.open_partition(partition_name).await?;
    partition_db.execute("VACUUM", ()).await?;
    partition_db.execute("ANALYZE", ()).await?;

    Ok(())
}
```

### 4. Archival

Old partitions can be:
- **Compressed**: Reduce storage requirements
- **Moved**: Transfer to cheaper storage
- **Deleted**: Remove data beyond retention period
- **Replicated**: Copy to disaster recovery locations

## Query Patterns with Partitioning

### Stream Reads

Stream reads automatically work across partitions:

```rust
// Reading a stream spanning multiple partitions
let events = store.read_stream("order-123", StreamVersion::Start).await?;

// Internally:
// 1. Find stream head in catalog
// 2. Locate containing partition
// 3. Read events from that partition
// 4. If needed, check previous partitions
// 5. Return events in order
```

**Partition Navigation:**
```rust
struct StreamReadPlan {
    partitions: Vec<PartitionReadPlan>,
}

struct PartitionReadPlan {
    partition_name: String,
    start_version: Option<i64>,
    end_version: Option<i64>,
    direction: ReadDirection, // Forward or backward
}
```

### Time Range Reads

Time range queries use partition metadata for efficiency:

```rust
// Efficient time range query
let events = store.read_time_range(
    TimeRange::new(start_time, end_time)
).await?;

// Query optimization:
// 1. Find overlapping partitions using catalog
// 2. Query only relevant partitions
// 3. Merge results in chronological order
// 4. Skip non-overlapping partitions entirely
```

**Partition Selection Logic:**
```rust
fn select_partitions_for_time_range(
    catalog: &Catalog,
    start_ms: i64,
    end_ms: i64
) -> Result<Vec<String>> {
    catalog.query_partitions(|partition| {
        // Check if partition overlaps with time range
        partition.start_ms <= end_ms &&
        (partition.end_ms.is_none() || partition.end_ms.unwrap() >= start_ms)
    })
}
```

## Catalog Database Role

The catalog database is the "map" to your partitioned data:

### Partition Registry
```sql
CREATE TABLE partitions (
    name TEXT PRIMARY KEY,           -- Partition filename (e.g., "events_20241002T1200_a.db")
    path TEXT NOT NULL,              -- Relative path from root directory
    start_ms INTEGER NOT NULL,       -- Partition start time (Unix timestamp ms)
    end_ms INTEGER,                  -- Partition end time (NULL for active partition)
    sealed INTEGER NOT NULL DEFAULT 0 -- 0=active, 1=sealed (read-only)
);
```

### Stream Head Tracking
```sql
CREATE TABLE stream_heads (
    stream_id TEXT PRIMARY KEY,        -- Unique stream identifier
    version INTEGER NOT NULL,          -- Current stream version
    last_created_at_ms INTEGER NOT NULL, -- Timestamp of last event
    last_event_id TEXT NOT NULL,       -- UUID of last event
    last_partition TEXT NOT NULL       -- Partition containing last event
);
```

### Query Planning
```rust
impl Catalog {
    async fn plan_stream_read(&self, stream_id: &str, from_version: i64) -> Result<ReadPlan> {
        // Find stream head
        let head = self.get_stream_head(stream_id).await?;

        // Determine which partitions to read
        let mut partitions = Vec::new();
        let mut current_partition = &head.last_partition;
        let mut current_version = head.version;

        while current_version >= from_version {
            let partition_info = self.get_partition_info(current_partition).await?;
            partitions.push(PartitionReadPlan {
                name: current_partition.clone(),
                from_version: from_version.max(partition_info.min_version),
                to_version: current_version,
            });

            // Move to previous partition if needed
            if partition_info.min_version > from_version {
                current_partition = partition_info.previous_partition.as_ref()?;
                current_version = partition_info.min_version - 1;
            } else {
                break;
            }
        }

        Ok(ReadPlan { partitions })
    }
}
```

## Performance Implications

### Write Performance

**Active Partition Writes:**
- Single target for all writes
- No lock contention with old data
- Compact indexes maintain performance
- Predictable write latency

**Partition Overhead:**
- Creation cost amortized over partition lifetime
- Catalog updates are minimal
- No performance degradation as data grows

### Read Performance

**Stream Reads:**
- Usually single partition lookup (most recent events)
- Direct navigation through catalog metadata
- No full table scans required

**Time Range Reads:**
- Partition elimination based on time ranges
- Parallel queries across multiple partitions possible
- Predictable performance based on partition count

**Historical Data Access:**
- Old partitions can be moved to slower storage
- Query planner automatically handles file locations
- Cache optimization for frequently accessed partitions

## Best Practices

### 1. Choose Appropriate Time Windows

```rust
// High-frequency systems (financial trading)
RotationPolicy::TimeWindow {
    window: Duration::from_secs(15 * 60), // 15 minutes
    max_bytes: Some(2 * 1024 * 1024 * 1024), // 2GB
}

// General business applications
RotationPolicy::TimeWindow {
    window: Duration::from_secs(60 * 60), // 1 hour
    max_bytes: Some(1024 * 1024 * 1024), // 1GB
}

// Low-volume systems (audit logs)
RotationPolicy::TimeWindow {
    window: Duration::from_secs(4 * 60 * 60), // 4 hours
    max_bytes: Some(100 * 1024 * 1024), // 100MB
}
```

### 2. Monitor Partition Health

```rust
// Regular partition maintenance
async fn maintain_partitions(&self) -> Result<()> {
    let partitions = self.catalog.list_partitions().await?;

    for partition in partitions {
        if partition.sealed {
            // Archive old partitions
            if partition.age() > Duration::from_secs(30 * 24 * 60 * 60) {
                self.archive_partition(&partition.name).await?;
            }

            // Optimize performance
            if partition.fragmentation() > 0.3 {
                self.optimize_partition(&partition.name).await?;
            }
        }
    }

    Ok(())
}
```

### 3. Plan for Data Growth

```rust
// Capacity planning
fn calculate_storage_needs(
    events_per_second: u64,
    avg_event_size: usize,
    retention_days: u64,
    partition_window_hours: u64
) -> StoragePlan {
    let events_per_day = events_per_second * 24 * 60 * 60;
    let bytes_per_day = events_per_day * avg_event_size;
    let total_bytes = bytes_per_day * retention_days;

    let partitions_per_day = 24 / partition_window_hours;
    let total_partitions = partitions_per_day * retention_days;

    StoragePlan {
        total_storage_gb: total_bytes / (1024 * 1024 * 1024),
        partition_count: total_partitions,
        avg_partition_size_mb: (bytes_per_day / partitions_per_day) / (1024 * 1024),
    }
}
```

## Trade-offs and Considerations

### Advantages
- **Scalable Performance**: Consistent performance regardless of total data size
- **Operational Flexibility**: Archive, backup, and delete specific time periods
- **Resource Efficiency**: Smaller indexes and better cache locality
- **Parallel Processing**: Multiple partitions can be processed simultaneously

### Considerations
- **Complexity**: More moving parts than monolithic approach
- **Cross-partition Queries**: Require planning and coordination
- **Catalog Dependency**: Catalog database becomes critical component
- **File Management**: More files to manage and monitor

### When to Use Partitioning

**Ideal for:**
- High write volume (>1K events/second)
- Long-term data retention (months/years)
- Time-based query patterns
- Regulatory compliance requirements
- Distributed processing needs

**May Be Overkill for:**
- Low volume (<100 events/day)
- Short retention (<1 week)
- Simple read-only workloads
- Rapid prototyping scenarios

Partitioning is a fundamental strategy that enables event stores to scale from simple prototypes to production systems handling billions of events while maintaining performance and operational flexibility.