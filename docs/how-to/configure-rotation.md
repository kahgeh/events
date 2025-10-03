# Configure Partition Rotation

Partition rotation is crucial for maintaining performance and manageability in your event store. This guide shows you how to configure rotation policies for different use cases and volumes.

## What You'll Learn

- How rotation policies work
- Choosing the right time windows
- Setting appropriate size limits
- Managing partition lifecycle
- Performance optimization strategies

## Understanding Rotation Policies

Rotation policies determine when the event store creates new partition files. This prevents individual files from becoming too large and helps with:

- **Performance**: Smaller files are faster to query and back up
- **Maintenance**: Easier to archive or delete old partitions
- **Parallelism**: Multiple partitions can be accessed simultaneously

## Basic Configuration

```rust
use events::{EventStore, RotationPolicy};
use std::time::Duration;

// Hourly rotation with 512MB size limit
let store = EventStore::open_partitioned(
    "./data",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600), // 1 hour
        max_bytes: Some(512 * 1024 * 1024), // 512MB
    },
).await?;
```

## Choosing Time Windows

### High Volume Systems (>10,000 events/second)

```rust
// 15-minute windows for very high volume
RotationPolicy::TimeWindow {
    window: Duration::from_secs(15 * 60), // 15 minutes
    max_bytes: Some(1024 * 1024 * 1024), // 1GB
}
```

**Use case**: Financial trading systems, IoT data ingestion
**Partition pattern**: `events_20241002T1430.db`, `events_20241002T1445.db`

### Medium Volume Systems (100-10,000 events/second)

```rust
// Hourly windows for typical applications
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600), // 1 hour
    max_bytes: Some(512 * 1024 * 1024), // 512MB
}
```

**Use case**: E-commerce platforms, SaaS applications
**Partition pattern**: `events_20241002T14.db`, `events_20241002T15.db`

### Low Volume Systems (<100 events/second)

```rust
// Daily windows for low traffic
RotationPolicy::TimeWindow {
    window: Duration::from_secs(24 * 3600), // 1 day
    max_bytes: Some(256 * 1024 * 1024), // 256MB
}
```

**Use case**: Internal tools, personal projects, batch processing
**Partition pattern**: `events_20241002.db`, `events_20241003.db`

## Size Limits Configuration

### Size-based Rotation Within Time Windows

When a partition reaches the size limit before the time window expires, it creates additional partitions with alphabetical suffixes:

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600), // 1 hour
    max_bytes: Some(100 * 1024 * 1024), // 100MB limit
}
```

**Result**: If many events occur in one hour:
- `events_20241002T14.db` (reaches 100MB)
- `events_20241002T14_a.db` (reaches 100MB)
- `events_20241002T14_b.db` (reaches 100MB)

### No Size Limits

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600), // 1 hour
    max_bytes: None, // No size limit
}
```

**Use case**: When you prefer predictable partition names and can handle larger files

## Advanced Configuration Patterns

### Tiered Rotation Strategy

For systems with varying traffic patterns:

```rust
struct TieredRotationConfig {
    peak_hours_policy: RotationPolicy,
    normal_hours_policy: RotationPolicy,
    weekend_policy: RotationPolicy,
}

impl TieredRotationConfig {
    fn get_current_policy(&self) -> &RotationPolicy {
        let now = Utc::now();
        let hour = now.hour();
        let weekday = now.weekday();

        match (hour, weekday) {
            (9..=17, Weekday::Mon..=Weekday::Fri) => &self.peak_hours_policy,
            (_, Weekday::Sat..=Weekday::Sun) => &self.weekend_policy,
            _ => &self.normal_hours_policy,
        }
    }
}
```

### Event-type Based Rotation

Create separate event stores for different event types:

```rust
// High-frequency events (clicks, metrics)
let metrics_store = EventStore::open_partitioned(
    "./data/metrics",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(15 * 60), // 15 minutes
        max_bytes: Some(2 * 1024 * 1024 * 1024), // 2GB
    },
).await?;

// Business events (orders, users)
let business_store = EventStore::open_partitioned(
    "./data/business",
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600), // 1 hour
        max_bytes: Some(512 * 1024 * 1024), // 512MB
    },
).await?;
```

## Monitoring Rotation Performance

### Track Partition Statistics

```rust
use events::PoolStats;

async fn monitor_rotation(store: &EventStore) -> Result<()> {
    let stats = store.pool_stats().await;

    println!("Partition Statistics:");
    println!("  Cached databases: {}", stats.cached_databases);
    println!("  Total active connections: {}", stats.total_active_connections);

    // Monitor partition instances
    for instance in &stats.instances {
        println!("  Partition {}: {} active connections", instance.path, instance.active_connections);
    }

    Ok(())
}
```

### Automatic Rotation Monitoring

```rust
use tokio::time::{interval, Duration};

async fn rotation_monitor(store: EventStore) {
    let mut interval = interval(Duration::from_secs(300)); // Check every 5 minutes

    loop {
        interval.tick().await;

        if let Err(e) = store.maybe_rotate().await {
            eprintln!("Rotation failed: {}", e);
        }

        if let Err(e) = monitor_rotation(&store).await {
            eprintln!("Monitoring failed: {}", e);
        }
    }
}
```

## Partition Lifecycle Management

### Automated Archival Strategy

```rust
async fn archive_old_partitions(root_dir: &str, retention_days: i64) -> Result<(), std::io::Error> {
    let cutoff_time = Utc::now() - Duration::from_secs(retention_days * 24 * 3600);

    for entry in std::fs::read_dir(root_dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            if filename.starts_with("events_") && filename.ends_with(".db") {
                // Extract timestamp from filename
                if let Some(timestamp) = extract_timestamp_from_filename(filename) {
                    if timestamp < cutoff_time {
                        let archive_path = format!("{}/archive/{}", root_dir, filename);
                        std::fs::create_dir_all(format!("{}/archive", root_dir))?;
                        std::fs::rename(&path, archive_path)?;
                        println!("Archived partition: {}", filename);
                    }
                }
            }
        }
    }

    Ok(())
}

fn extract_timestamp_from_filename(filename: &str) -> Option<DateTime<Utc>> {
    // Parse patterns like:
    // - events_20241002.db
    // - events_20241002T14.db
    // - events_20241002T1430.db

    let clean_name = filename.trim_start_matches("events_").trim_end_matches(".db");

    if clean_name.len() == 8 {
        // Daily: 20241002
        NaiveDate::parse_from_str(clean_name, "%Y%m%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| DateTime::from_utc(dt, Utc))
    } else if clean_name.len() == 11 {
        // Hourly: 20241002T14
        NaiveDateTime::parse_from_str(clean_name, "%Y%m%dT%H")
            .ok()
            .map(|dt| DateTime::from_utc(dt, Utc))
    } else if clean_name.len() == 13 {
        // 15-minute: 20241002T1430
        NaiveDateTime::parse_from_str(clean_name, "%Y%m%dT%H%M")
            .ok()
            .map(|dt| DateTime::from_utc(dt, Utc))
    } else {
        None
    }
}
```

### Partition Compaction

```rust
async fn compact_partition(partition_path: &str) -> Result<(), EsError> {
    // Create a temporary store for the compacted data
    let temp_path = format!("{}.compacting", partition_path);

    // Note: This example shows the concept but uses non-existent APIs
    // In practice, you would need to implement custom compaction logic
    // using the existing EventStore methods

    // Stream all events and write to compacted store
    let mut cursor = bootstrap_cursor(&original_store, "compaction").await?;

    loop {
        let (events, next_cursor) = original_store.all_since(cursor, 1000).await?;

        if events.is_empty() {
            break;
        }

        for event in events {
            // Note: append_raw doesn't exist - you would need to create NewEvents
            // from the existing event envelopes
            // compact_store.append_raw(event).await?;
        }

        cursor = next_cursor;
    }

    // Replace original with compacted
    std::fs::remove_file(partition_path)?;
    std::fs::rename(&temp_path, partition_path)?;

    Ok(())
}
```

## Performance Optimization

### Connection Pool Tuning

```rust
use events::DatabasePool;

// Configure connection pool for high throughput
let pool = DatabasePool::new("./data")?
    .with_max_cached_databases(50); // Cache more databases for high throughput
```

### Batch Size Optimization

```rust
// For projections, tune batch size based on your event volume
let projector = Projector::new(store, "my_projection".to_string())
    .with_batch_size(match events_per_second {
        0..=100 => 100,    // Low volume
        101..=1000 => 500,  // Medium volume
        1001..=10000 => 1000, // High volume
        _ => 2000,         // Very high volume
    });
```

### WAL Mode Configuration

```rust
// Ensure WAL mode is enabled for better write performance
async fn configure_wal_mode(conn: &turso::Connection) -> Result<(), EsError> {
    conn.execute("PRAGMA journal_mode = WAL", ()).await?;
    conn.execute("PRAGMA synchronous = NORMAL", ()).await?; // Less strict than FULL
    conn.execute("PRAGMA cache_size = 10000", ()).await?; // Increase cache
    conn.execute("PRAGMA temp_store = MEMORY", ()).await?;
    Ok(())
}
```

## Troubleshooting Common Issues

### Problem: Partitions Growing Too Large

**Symptoms**: Slow queries, high memory usage
**Solution**: Reduce size limit or time window

```rust
// Before: Too large
RotationPolicy::TimeWindow {
    window: Duration::from_secs(24 * 3600), // 1 day
    max_bytes: Some(2 * 1024 * 1024 * 1024), // 2GB
}

// After: Better sizing
RotationPolicy::TimeWindow {
    window: Duration::from_secs(12 * 3600), // 12 hours
    max_bytes: Some(512 * 1024 * 1024), // 512MB
}
```

### Problem: Too Many Small Partitions

**Symptoms**: High file count, slow directory operations
**Solution**: Increase time window or minimum size

```rust
// Before: Too many partitions
RotationPolicy::TimeWindow {
    window: Duration::from_secs(15 * 60), // 15 minutes
    max_bytes: Some(10 * 1024 * 1024), // 10MB
}

// After: Fewer, larger partitions
RotationPolicy::TimeWindow {
    window: Duration::from_secs(2 * 3600), // 2 hours
    max_bytes: Some(256 * 1024 * 1024), // 256MB
}
```

### Problem: Rotation Delays

**Symptoms**: Events written to wrong time window
**Solution**: Implement manual rotation checks

```rust
async fn ensure_correct_partition(store: &EventStore) -> Result<(), EsError> {
    // Force rotation check before important operations
    store.maybe_rotate().await?;
    Ok(())
}
```

## Testing Rotation Configuration

### Load Testing Script

```rust
#[cfg(test)]
mod rotation_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_size_based_rotation() -> Result<(), EsError> {
        let temp_dir = TempDir::new().unwrap();
        let small_size_limit = 1024 * 1024; // 1MB

        let store = EventStore::open_partitioned(
            temp_dir.path(),
            RotationPolicy::TimeWindow {
                window: Duration::from_secs(3600),
                max_bytes: Some(small_size_limit),
            },
        ).await?;

        // Generate enough events to trigger rotation
        for i in 0..1000 {
            store.append(
                &format!("test-stream-{}", i),
                ExpectedVersion::NoStream,
                vec![NewEvent {
                    r#type: "TestEvent".into(),
                    payload: json!({"data": "x".repeat(2000)}), // Large payload
                }],
            ).await?;
        }

        // Check that multiple partitions were created
        let partitions: Vec<_> = std::fs::read_dir(temp_dir.path())?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name().to_str()
                    .map(|n| n.starts_with("events_"))
                    .unwrap_or(false)
            })
            .collect();

        assert!(partitions.len() > 1, "Should have created multiple partitions");

        Ok(())
    }
}
```

## Best Practices Summary

1. **Start Conservative**: Begin with larger partitions and adjust based on performance
2. **Monitor Growth**: Regularly check partition sizes and rotation frequency
3. **Plan Archival**: Have a strategy for old partitions before you need it
4. **Test Load**: Validate your rotation policy under realistic load
5. **Document Strategy**: Keep clear records of your rotation configuration and rationale

## Next Steps

- [Monitor Production](monitor-production.md) - Set up comprehensive monitoring
- [Scale Consumers](scale-consumers.md) - Handle high-volume scenarios
- [Performance Reference](../reference/performance.md) - Detailed performance tuning