# Configuration Reference

Complete guide to configuring the Events crate for different use cases and environments.

## Configuration Overview

The Events crate provides a simple configuration approach focused on partition management:

1. **Rotation Policy** - How and when to create new partitions (primary configuration)
2. **Root Directory** - Location where event data is stored
3. **Internal Settings** - Fixed values optimized for most use cases

The event store uses a fixed `max_payload_bytes` of 1MB (1024 * 1024) and other internal settings that are optimized for performance and reliability.

## Event Store Configuration

### Creating an Event Store

The `EventStore` is created using the `open_partitioned` constructor:

```rust
use events::{EventStore, RotationPolicy};
use std::time::Duration;

// Create an event store with hourly rotation
let rotation_policy = RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),     // 1 hour windows
    max_bytes: Some(512 * 1024 * 1024),  // Optional: 512MB per partition
};

let store = EventStore::open_partitioned("./data", rotation_policy).await?;
```

#### Constructor Parameters

- **root** (`&str`): Root directory where all event data will be stored
- **rotation** (`RotationPolicy`): Policy for partition rotation

The event store automatically:
- Creates the root directory if it doesn't exist
- Sets up internal connection pooling (default: 50 cached databases)
- Configures a maximum payload size of 1MB per event
- Initializes partition management and catalog

## Rotation Policy Configuration

### TimeWindow Rotation

The `RotationPolicy::TimeWindow` controls when new partitions are created:

```rust
use events::RotationPolicy;
use std::time::Duration;

// Basic hourly rotation
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),     // 1 hour
    max_bytes: Some(512 * 1024 * 1024),  // Optional: 512MB limit per partition
}

// Daily rotation without size limit
RotationPolicy::TimeWindow {
    window: Duration::from_secs(24 * 3600), // 24 hours
    max_bytes: None,                        // No size limit
}

// High-frequency rotation (15 minutes)
RotationPolicy::TimeWindow {
    window: Duration::from_secs(15 * 60),   // 15 minutes
    max_bytes: Some(1024 * 1024 * 1024),   // 1GB limit per partition
}
```

#### Rotation Behavior

- **Time-based**: New partitions are created when the current time window expires
- **Size-based**: If `max_bytes` is set, rotation occurs when partition exceeds the size limit
- **Suffix handling**: When multiple rotations occur in the same time window, suffixes (a, b, c...) are used

#### Recommended Settings by Volume

| Event Rate | Time Window | Size Limit | Use Case |
|------------|-------------|------------|----------|
| < 100/sec  | 24 hours    | 256MB      | Internal tools, low traffic |
| 100-1000/sec | 1 hour     | 512MB      | Typical web applications |
| 1000-10000/sec | 15 minutes | 1GB       | High-volume systems |
| > 10000/sec | 5 minutes   | 2GB       | IoT, financial systems |

## Database Pool Configuration

### Automatic Pool Management

The event store automatically creates and manages a `DatabasePool` with sensible defaults:

```rust
// Pool is created automatically with these defaults:
// - Max cached databases: 50
// - LRU eviction when cache is full
// - Automatic connection management
```

### Manual Pool Configuration (Advanced)

If you need custom pool settings, you can create the pool manually:

```rust
use events::DatabasePool;

// Create pool with custom cache size
let pool = DatabasePool::new("./data")?
    .with_max_cached_databases(100);     // Maximum cached database instances

// Note: EventStore::open_partitioned creates its own pool internally
// Custom pool configuration is typically only needed for advanced use cases
```

### Performance Tuning

```rust
// High-throughput configuration
let pool = DatabasePool::new("./data")?
    .with_max_cached_databases(100);  // Cache more databases for high throughput

// Low-resource configuration
let pool = DatabasePool::new("./data")?
    .with_max_cached_databases(10);   // Cache fewer databases for low resource usage
```

## Fixed Internal Configuration

### Event Store Limits

The following values are fixed and cannot be configured:

- **max_payload_bytes**: 1,048,576 bytes (1MB) - Maximum size of a single event payload
- **Connection pooling**: Automatic management with LRU eviction
- **Database settings**: Optimized Turso DB configuration applied automatically

### Why These Limits?

- **1MB payload**: Balances flexibility with performance and storage efficiency
- **Connection pooling**: Prevents resource exhaustion while maintaining good performance
- **Database settings**: Optimized for concurrent access and reliability

## Projection Configuration

### Basic Projection Setup

```rust
use events::Projector;

let projector = Projector::new(store, "my_projection".to_string())
    .with_batch_size(1000);  // Process 1000 events per batch
```

### Batch Size Tuning

```rust
// Calculate optimal batch size based on event volume
fn calculate_optimal_batch_size(events_per_second: f64) -> usize {
    match events_per_second {
        0.0..=10.0 => 100,      // Low volume - smaller batches
        10.0..=100.0 => 500,    // Medium volume
        100.0..=1000.0 => 1000, // High volume
        _ => 2000,              // Very high volume - larger batches
    }
}

// Example usage
let events_per_sec = 500.0; // Your measured rate
let batch_size = calculate_optimal_batch_size(events_per_sec);

let projector = Projector::new(store, "projection".to_string())
    .with_batch_size(batch_size);
```

## Environment-Specific Configuration

### Development Environment

```rust
pub fn dev_rotation_policy() -> RotationPolicy {
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(60),     // 1 minute for testing
        max_bytes: Some(10 * 1024 * 1024),  // 10MB limit
    }
}

// Usage
let store = EventStore::open_partitioned("./dev_data", dev_rotation_policy()).await?;
```

### Production Environment

```rust
pub fn production_rotation_policy() -> RotationPolicy {
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),   // 1 hour
        max_bytes: Some(1024 * 1024 * 1024), // 1GB limit
    }
}

// Usage
let store = EventStore::open_partitioned("./prod_data", production_rotation_policy()).await?;
```

### High-Security Environment

```rust
pub fn secure_rotation_policy() -> RotationPolicy {
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),   // 1 hour
        max_bytes: Some(256 * 1024 * 1024), // 256MB limit for security
    }
}

// Usage
let store = EventStore::open_partitioned("./secure_data", secure_rotation_policy()).await?;
```

## Configuration from Environment

### Environment Variable Support

```rust
use std::env;
use std::time::Duration;

pub fn rotation_policy_from_env() -> RotationPolicy {
    let window_secs = env::var("EVENT_ROTATION_WINDOW_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600); // Default: 1 hour

    let max_bytes = env::var("EVENT_ROTATION_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok());

    RotationPolicy::TimeWindow {
        window: Duration::from_secs(window_secs),
        max_bytes,
    }
}

// Usage
let policy = rotation_policy_from_env();
let store = EventStore::open_partitioned("./data", policy).await?;
```

### Configuration File Support

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct EventsConfig {
    pub root_directory: String,
    pub rotation: RotationConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RotationConfig {
    pub window_seconds: u64,
    pub max_bytes_mb: Option<u64>,
}

impl EventsConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: EventsConfig = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn to_rotation_policy(&self) -> RotationPolicy {
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(self.rotation.window_seconds),
            max_bytes: self.rotation.max_bytes_mb.map(|mb| mb * 1024 * 1024),
        }
    }
}

// Example configuration file (config.toml)
/*
root_directory = "./data"

[rotation]
window_seconds = 3600        # 1 hour
max_bytes_mb = 512           # 512MB
*/
```

## Best Practices

### 1. Choose Appropriate Time Windows

- **Short windows (5-15 minutes)**: High-frequency event sources
- **Medium windows (1-6 hours)**: Typical applications
- **Long windows (24 hours)**: Low-volume systems

### 2. Set Size Limits

Always set `max_bytes` to prevent partitions from growing too large:

- **10-100MB**: Development and testing
- **256-512MB**: Production applications
- **1GB+**: High-volume systems

### 3. Monitor Storage Usage

- Track partition rotation frequency
- Monitor disk space usage
- Set up alerts for unusual growth patterns

### 4. Performance Considerations

- Smaller partitions = faster queries and compaction
- More partitions = more file management overhead
- Balance based on your query patterns

### 5. Backup Strategy

- Backup the entire root directory
- Consider partition-level backups for large systems
- Test restore procedures regularly

## Common Configuration Patterns

### Web Application

```rust
let rotation = RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),    // 1 hour
    max_bytes: Some(512 * 1024 * 1024),  // 512MB
};
let store = EventStore::open_partitioned("./events", rotation).await?;
```

### IoT Data Collection

```rust
let rotation = RotationPolicy::TimeWindow {
    window: Duration::from_secs(15 * 60), // 15 minutes
    max_bytes: Some(1024 * 1024 * 1024), // 1GB
};
let store = EventStore::open_partitioned("./iot_events", rotation).await?;
```

### Analytics Pipeline

```rust
let rotation = RotationPolicy::TimeWindow {
    window: Duration::from_secs(24 * 3600), // 24 hours
    max_bytes: None,                        // No size limit
};
let store = EventStore::open_partitioned("./analytics", rotation).await?;
```

## Next Steps

- [API Reference](api.md) - Complete API documentation
- [Error Types Reference](error-types.md) - Error handling guide
- [Performance Reference](performance.md) - Performance optimization
- [Rotation Policy Guide](../guides/rotation-policies.md) - Detailed rotation strategies
