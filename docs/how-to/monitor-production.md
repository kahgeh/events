# Monitor Production Event Store

Production monitoring is crucial for maintaining healthy event store systems. This guide shows you how to set up comprehensive monitoring for your Events crate deployment.

## What You'll Monitor

- Event store performance and health
- Projection processing and lag
- Database connection usage
- Partition rotation and storage
- Error rates and system alerts

## Key Metrics to Track

### 1. Event Store Metrics

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct EventStoreMetrics {
    events_appended: AtomicU64,
    events_loaded: AtomicU64,
    append_latency_ms: AtomicU64,
    load_latency_ms: AtomicU64,
    concurrent_writes: AtomicU64,
}

impl EventStoreMetrics {
    pub fn record_append(&self, event_count: u64, latency_ms: u64) {
        self.events_appended.fetch_add(event_count, Ordering::Relaxed);
        self.append_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
    }

    pub fn record_load(&self, event_count: u64, latency_ms: u64) {
        self.events_loaded.fetch_add(event_count, Ordering::Relaxed);
        self.load_latency_ms.fetch_add(latency_ms, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> EventStoreStats {
        let events_appended = self.events_appended.load(Ordering::Relaxed);
        let events_loaded = self.events_loaded.load(Ordering::Relaxed);
        let total_append_latency = self.append_latency_ms.load(Ordering::Relaxed);
        let total_load_latency = self.load_latency_ms.load(Ordering::Relaxed);

        EventStoreStats {
            events_appended,
            events_loaded,
            avg_append_latency_ms: if events_appended > 0 {
                total_append_latency / events_appended
            } else {
                0
            },
            avg_load_latency_ms: if events_loaded > 0 {
                total_load_latency / events_loaded
            } else {
                0
            },
            concurrent_writes: self.concurrent_writes.load(Ordering::Relaxed),
        }
    }
}

pub struct EventStoreStats {
    pub events_appended: u64,
    pub events_loaded: u64,
    pub avg_append_latency_ms: u64,
    pub avg_load_latency_ms: u64,
    pub concurrent_writes: u64,
}
```

### 2. Database Connection Monitoring

```rust
use events::PoolStats;

pub async fn monitor_connection_pool(store: &EventStore) -> Result<(), EsError> {
    let stats = store.pool_stats().await;

    // Alert if connection pool is nearly exhausted
    let usage_ratio = stats.active_connections as f64 / stats.total_connections as f64;
    if usage_ratio > 0.8 {
        eprintln!("WARNING: Connection pool usage is {:.1%} ({} / {})",
            usage_ratio, stats.active_connections, stats.total_connections);
    }

    // Check database sizes
    for instance in &stats.database_instances {
        let size_mb = instance.size_bytes / (1024 * 1024);
        if size_mb > 1000 { // Alert if partition > 1GB
            eprintln!("WARNING: Large partition {}: {}MB", instance.path, size_mb);
        }
    }

    Ok(())
}
```

### 3. Projection Health Monitoring

```rust
pub struct ProjectionMonitor {
    projection_name: String,
    last_processed_timestamp: AtomicU64,
    events_processed_count: AtomicU64,
    error_count: AtomicU64,
}

impl ProjectionMonitor {
    pub fn record_event_processed(&self, event_timestamp: i64) {
        self.events_processed_count.fetch_add(1, Ordering::Relaxed);
        self.last_processed_timestamp.store(
            event_timestamp as u64,
            Ordering::Relaxed
        );
    }

    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn calculate_lag(&self, current_timestamp: i64) -> i64 {
        let last_processed = self.last_processed_timestamp.load(Ordering::Relaxed) as i64;
        current_timestamp - last_processed
    }

    pub fn is_healthy(&self, current_timestamp: i64) -> HealthStatus {
        let lag = self.calculate_lag(current_timestamp);
        let errors = self.error_count.load(Ordering::Relaxed);

        match (lag, errors) {
            (lag, _) if lag > 300_000 => HealthStatus::Unhealthy(format!(
                "Projection lag is {} seconds", lag / 1000
            )),
            (_, errors) if errors > 10 => HealthStatus::Unhealthy(format!(
                "Too many errors: {}", errors
            )),
            (lag, _) if lag > 60_000 => HealthStatus::Warning(format!(
                "Projection lag is {} seconds", lag / 1000
            )),
            _ => HealthStatus::Healthy,
        }
    }
}

pub enum HealthStatus {
    Healthy,
    Warning(String),
    Unhealthy(String),
}
```

## Health Check Endpoints

### HTTP Health Check Server

```rust
use axum::{response::Json, routing::get, Router};
use serde_json::json;

pub async fn health_check_server(store: EventStore, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics))
        .with_state(store);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!("Health check server listening on port {}", port);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_check(
    axum::extract::State(store): axum::extract::State<EventStore>,
) -> Json<serde_json::Value> {
    let stats = store.pool_stats().await.unwrap_or_default();

    Json(json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "connections": {
            "active": stats.active_connections,
            "total": stats.total_connections
        },
        "partitions": stats.database_instances.len()
    }))
}

async fn metrics(
    axum::extract::State(store): axum::extract::State<EventStore>,
) -> Json<serde_json::Value> {
    let stats = store.pool_stats().await.unwrap_or_default();

    Json(json!({
        "events_appended_total": 0, // Add actual metrics
        "events_loaded_total": 0,
        "partition_count": stats.database_instances.len(),
        "active_connections": stats.active_connections,
        "total_connections": stats.total_connections
    }))
}
```

## Alerting Strategies

### 1. Performance Alerts

```rust
pub struct PerformanceAlerting {
    append_latency_threshold_ms: u64,
    error_rate_threshold: f64,
    connection_usage_threshold: f64,
}

impl PerformanceAlerting {
    pub async fn check_and_alert(&self, metrics: &EventStoreStats, pool_stats: &PoolStats) {
        // Check append latency
        if metrics.avg_append_latency_ms > self.append_latency_threshold_ms {
            self.send_alert(&format!(
                "High append latency: {}ms (threshold: {}ms)",
                metrics.avg_append_latency_ms, self.append_latency_threshold_ms
            )).await;
        }

        // Check connection pool usage
        let usage_ratio = pool_stats.active_connections as f64 / pool_stats.total_connections as f64;
        if usage_ratio > self.connection_usage_threshold {
            self.send_alert(&format!(
                "High connection pool usage: {:.1}% (threshold: {:.1}%)",
                usage_ratio * 100.0, self.connection_usage_threshold * 100.0
            )).await;
        }
    }

    async fn send_alert(&self, message: &str) {
        eprintln!("ALERT: {}", message);

        // Integration with monitoring systems
        #[cfg(feature = "slack")]
        self.send_slack_alert(message).await;

        #[cfg(feature = "pagerduty")]
        self.send_pagerduty_alert(message).await;
    }
}
```

### 2. Storage Monitoring

```rust
pub async fn monitor_storage_usage(root_dir: &str) -> Result<(), std::io::Error> {
    let mut total_size = 0u64;
    let mut partition_count = 0;
    let mut old_partitions = Vec::new();

    let cutoff_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() - (7 * 24 * 3600); // 7 days ago

    for entry in std::fs::read_dir(root_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("db") {
            let metadata = entry.metadata()?;
            let size = metadata.len();
            total_size += size;
            partition_count += 1;

            let modified = metadata.modified()?.duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_secs();

            if modified < cutoff_time {
                old_partitions.push((path, size));
            }
        }
    }

    println!("Storage Usage:");
    println!("  Total size: {} MB", total_size / (1024 * 1024));
    println!("  Partition count: {}", partition_count);
    println!("  Old partitions (>7 days): {}", old_partitions.len());

    // Alert if storage is getting high
    let total_gb = total_size / (1024 * 1024 * 1024);
    if total_gb > 100 {
        eprintln!("WARNING: High storage usage: {} GB", total_gb);
    }

    Ok(())
}
```

## Logging Strategy

### Structured Logging

```rust
use tracing::{info, warn, error, debug, instrument};

#[instrument(skip(store))]
pub async fn monitored_append(
    store: &EventStore,
    stream_id: &str,
    expected: ExpectedVersion,
    events: Vec<NewEvent>,
) -> Result<AppendResult, EsError> {
    let start = std::time::Instant::now();

    debug!(
        stream_id = %stream_id,
        expected_version = ?expected,
        event_count = events.len(),
        "Starting event append"
    );

    match store.append(stream_id, expected, events).await {
        Ok(result) => {
            let latency = start.elapsed().as_millis() as u64;

            info!(
                stream_id = %stream_id,
                event_count = result.events.len(),
                new_version = result.version,
                latency_ms = latency,
                "Events appended successfully"
            );

            Ok(result)
        }
        Err(EsError::Concurrency { expected, actual, stream_id }) => {
            warn!(
                stream_id = %stream_id,
                expected_version = expected,
                actual_version = actual,
                "Concurrency conflict during append"
            );
            Err(EsError::Concurrency { expected, actual, stream_id })
        }
        Err(e) => {
            error!(
                stream_id = %stream_id,
                error = %e,
                "Failed to append events"
            );
            Err(e)
        }
    }
}
```

### Configuration

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "events=info".into())
        ))
        .with(tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .json()) // Structured logging
        .init();
}
```

## Dashboard Examples

### Grafana Dashboard Queries

```sql
-- Events per minute
SELECT
    date_trunc('minute', timestamp) as minute,
    COUNT(*) as events
FROM events
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute;

-- Append latency percentiles
SELECT
    percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) as p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) as p99
FROM event_metrics
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Connection pool usage
SELECT
    timestamp,
    active_connections,
    total_connections,
    (active_connections::float / total_connections::float) * 100 as usage_percentage
FROM pool_metrics
WHERE timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;
```

## Production Readiness Checklist

### 1. Monitoring Setup
- [ ] Health check endpoint configured
- [ ] Key metrics are being collected
- [ ] Alerts configured for critical metrics
- [ ] Dashboard for visualization
- [ ] Log aggregation in place

### 2. Performance Monitoring
- [ ] Append latency monitoring
- [ ] Connection pool usage tracking
- [ ] Storage growth monitoring
- [ ] Partition rotation frequency tracking

### 3. Error Handling
- [ ] Error rate monitoring
- [ ] Retry mechanism health
- [ ] Dead letter queue monitoring
- [ ] Circuit breaker pattern if needed

### 4. Capacity Planning
- [ ] Storage growth projections
- [ ] Connection pool sizing
- [ ] Performance benchmarking
- [ ] Scaling triggers defined

## Troubleshooting Common Issues

### High Append Latency

**Symptoms**: Slow event writes, growing queue

**Investigation**:
```bash
# Check database locks (sqlite3 works with Turso due to SQLite compatibility)
sqlite3 catalog.db ".schema" "PRAGMA lock_status;"

# Check partition sizes
ls -lh data/events_*.db | sort -k5 -hr

# Check connection pool
curl http://localhost:8080/metrics
```

**Solutions**:
- Reduce batch size
- Check for long-running transactions
- Optimize database indexes
- Consider more frequent partition rotation

### Projection Lag

**Symptoms**: Read models out of sync with events

**Investigation**:
```rust
// Check projection cursor
let cursor = bootstrap_cursor(&store, "projection_name").await?;
println!("Projection cursor: {:?}", cursor);

// Check recent events
let (recent_events, _) = store.all_since(cursor, 10).await?;
println!("Recent events count: {}", recent_events.len());
```

**Solutions**:
- Increase projection batch size
- Optimize projection processing
- Check for dead letter queue items
- Consider scaling projection horizontally

### Connection Pool Exhaustion

**Symptoms**: Failed to get database connection errors

**Investigation**:
```rust
// Monitor connection usage
let stats = store.pool_stats().await;
println!("Active connections: {}", stats.active_connections);
println!("Total connections: {}", stats.total_connections);
```

**Solutions**:
- Increase pool size
- Check for connection leaks
- Optimize query performance
- Reduce concurrent operations

## Best Practices

1. **Set Meaningful Thresholds**: Base alert thresholds on your specific requirements
2. **Monitor Trends**: Track metrics over time, not just instantaneous values
3. **Create Runbooks**: Document procedures for common issues
4. **Test Alerts**: Verify alerting works during planned maintenance
5. **Regular Reviews**: Periodically review and adjust monitoring configuration

## Next Steps

- [Handle Concurrency](handle-concurrency.md) - Managing concurrent access patterns
- [Configure Rotation](configure-rotation.md) - Optimizing partition strategies
- [API Reference](../reference/api.md) - Detailed API documentation