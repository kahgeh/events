# Lease Management

Understanding how the Events crate coordinates multiple consumers through distributed lease management to prevent duplicate processing and ensure reliable event delivery.

## The Coordination Problem

In distributed systems, multiple consumers might process the same event stream:

```
Consumer A: Processes events 1-100
Consumer B: Processes events 1-100  ← Duplicate!
Consumer C: Processes events 1-100  ← Triple processing!
```

Without coordination, this leads to:
- **Duplicate Processing**: Same events processed multiple times
- **Resource Waste**: Multiple consumers doing the same work
- **Inconsistent State**: Different consumers may update state differently
- **Race Conditions**: Competing updates to shared resources

## Lease-Based Coordination

The Events crate uses a **lease-based coordination** mechanism where consumers acquire exclusive rights to process event streams.

### Lease Concept

A lease is a time-bound contract that gives a consumer exclusive rights:

```rust
#[derive(Debug, Clone)]
pub struct Lease {
    pub consumer_id: String,           // Who owns the lease
    pub resource_id: String,           // What's being leased (stream, partition, etc.)
    pub acquired_at: i64,              // When lease was acquired
    pub expires_at: i64,               // When lease expires
    pub metadata: LeaseMetadata,       // Additional lease information
}

#[derive(Debug, Clone)]
pub struct LeaseMetadata {
    pub last_heartbeat: i64,           // Last heartbeat timestamp
    pub processed_count: u64,          // Events processed in this lease
    pub status: LeaseStatus,           // Current lease status
}

#[derive(Debug, Clone)]
pub enum LeaseStatus {
    Active,                            // Currently valid
    Expiring,                          // About to expire (warning)
    Expired,                           // No longer valid
    Revoked,                           // Forcefully terminated
}
```

### Lease Lifecycle

1. **Acquisition**: Consumer requests lease for a resource
2. **Active Period**: Consumer processes events with exclusive rights
3. **Heartbeat**: Consumer periodically extends lease
4. **Expiration**: Lease expires if not renewed
5. **Release**: Consumer voluntarily releases lease
6. **Reacquisition**: Available for other consumers to acquire

## Implementation Architecture

### Lease Table Schema

```sql
CREATE TABLE consumer_offsets (
    consumer TEXT PRIMARY KEY,         -- Consumer identifier
    partition TEXT NOT NULL,          -- Currently assigned partition
    cursor_created_at INTEGER NOT NULL, -- Processing position
    cursor_event_id TEXT NOT NULL,    -- Last processed event
    updated_at INTEGER NOT NULL,      -- Last update timestamp
    lease_owner TEXT,                 -- Current lease holder (NULL=unlocked)
    lease_expires_at INTEGER          -- Lease expiration timestamp (NULL=forever)
);
```

### Lease Helper Functions

The Events crate provides simple helper functions for lease management rather than a complex manager. These functions operate on the `consumer_offsets` table:

```rust
// Lease acquisition is handled by simple helper functions
pub async fn acquire_lease(
    store: &EventStore,
    resource_id: &str,
    consumer_id: &str,
    lease_duration_secs: i64,
) -> Result<bool, EsError>

pub async fn renew_lease(
    store: &EventStore,
    resource_id: &str,
    consumer_id: &str,
) -> Result<bool, EsError>

pub async fn release_lease(
    store: &EventStore,
    resource_id: &str,
    consumer_id: &str,
) -> Result<(), EsError>

pub async fn is_lease_valid(
    store: &EventStore,
    resource_id: &str,
    consumer_id: &str,
) -> Result<bool, EsError>
```

The implementation uses the `consumer_offsets` table with these columns:
- `consumer`: Consumer identifier
- `partition`: Resource being leased
- `lease_owner`: Current lease holder (NULL=unlocked)
- `lease_expires_at`: Lease expiration timestamp (NULL=forever)
- `updated_at`: Last update timestamp

Note: This is a simplified coordination mechanism suitable for basic use cases. For complex distributed scenarios, consider implementing a more sophisticated coordination service.

## Consumer Coordination Patterns

### 1. Single Consumer Per Stream

```rust
use events::{acquire_lease, renew_lease, release_lease, is_lease_valid};

pub struct StreamConsumer {
    store: EventStore,
    stream_id: String,
    consumer_id: String,
    lease_acquired: bool,
    event_handler: Box<dyn EventHandler>,
}

impl StreamConsumer {
    pub async fn start_processing(&mut self) -> Result<()> {
        loop {
            // Try to acquire lease for stream
            match acquire_lease(&self.store, &self.stream_id, &self.consumer_id, 30).await {
                Ok(true) => {
                    self.lease_acquired = true;
                    tracing::info!("Acquired lease for stream {}", self.stream_id);

                    // Process events while lease is valid
                    self.process_with_lease().await?;
                }
                Ok(false) => {
                    tracing::info!("Stream {} already leased, waiting...", self.stream_id);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn process_with_lease(&mut self) -> Result<()> {
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Process events
                result = self.process_next_batch() => {
                    match result {
                        Ok(Some(events)) => {
                            // Update cursor
                            self.update_cursor(&events.last().unwrap()).await?;
                        }
                        Ok(None) => {
                            // No events available, wait a bit
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }

                // Maintain lease
                _ = heartbeat_interval.tick() => {
                    match renew_lease(&self.store, &self.stream_id, &self.consumer_id).await {
                        Ok(true) => {
                            tracing::debug!("Lease renewed for {}", self.stream_id);
                        }
                        Ok(false) => {
                            tracing::warn!("Failed to renew lease for {}", self.stream_id);
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }

            // Check if lease is still valid
            if !is_lease_valid(&self.store, &self.stream_id, &self.consumer_id).await? {
                tracing::warn!("Lease no longer valid for {}", self.stream_id);
                break;
            }
        }

        // Release lease gracefully
        self.lease_acquired = false;
        if let Err(error) = release_lease(&self.store, &self.stream_id, &self.consumer_id).await {
            tracing::error!("Failed to release lease: {}", error);
        }

        Ok(())
    }
}
```

### 2. Partition-Based Coordination

```rust
use events::{acquire_lease, release_lease};

pub struct PartitionConsumer {
    store: EventStore,
    consumer_id: String,
    assigned_partitions: Vec<String>,
    max_partitions: usize,
}

impl PartitionConsumer {
    pub async fn balance_partitions(&mut self) -> Result<()> {
        // Get all available partitions
        let all_partitions = self.get_available_partitions().await?;

        // Get current lease assignments
        let current_assignments = self.get_current_assignments().await?;

        // Calculate optimal distribution
        let desired_partitions = self.calculate_desired_partitions(
            &all_partitions,
            &current_assignments
        );

        // Release excess partitions
        for partition in &self.assigned_partitions {
            if !desired_partitions.contains(partition) {
                release_lease(&self.store, partition, &self.consumer_id).await?;
            }
        }

        // Acquire needed partitions
        for partition in &desired_partitions {
            if !self.assigned_partitions.contains(partition) {
                match acquire_lease(&self.store, partition, &self.consumer_id, 300).await? {
                    true => { /* Lease acquired */ }
                    false => {
                        tracing::warn!("Failed to acquire lease for partition {}", partition);
                    }
                }
            }
        }

        self.assigned_partitions = desired_partitions;
        Ok(())
    }

    fn calculate_desired_partitions(
        &self,
        all_partitions: &[String],
        current_assignments: &HashMap<String, String>
    ) -> Vec<String> {
        let mut available: Vec<String> = all_partitions
            .iter()
            .filter(|p| {
                // Partition is unassigned or assigned to us
                match current_assignments.get(*p) {
                    None => true,
                    Some(owner) => owner == &self.consumer_id,
                }
            })
            .cloned()
            .collect();

        // Sort by priority (e.g., most recent first)
        available.sort_by(|a, b| b.cmp(a)); // Reverse chronological

        // Take up to our maximum
        available.truncate(self.max_partitions);
        available
    }
}
```

### 3. Leader Election Pattern

```rust
use events::{acquire_lease, renew_lease, release_lease};

pub struct LeaderElection {
    store: EventStore,
    election_key: String,
    consumer_id: String,
    is_leader: bool,
}

impl LeaderElection {
    pub async fn participate_in_election(&mut self) -> Result<bool> {
        match acquire_lease(&self.store, &self.election_key, &self.consumer_id, 30).await? {
            true => {
                self.is_leader = true;
                tracing::info!("Consumer {} elected as leader", self.consumer_id);

                // Maintain leadership
                self.maintain_leadership().await?;

                Ok(true)
            }
            false => {
                self.is_leader = false;
                tracing::info!("Consumer {} is follower", self.consumer_id);

                // Wait and try again
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(false)
            }
        }
    }

    async fn maintain_leadership(&mut self) -> Result<()> {
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    match renew_lease(&self.store, &self.election_key, &self.consumer_id).await? {
                        true => {
                            tracing::debug!("Leadership renewed");
                        }
                        false => {
                            tracing::warn!("Lost leadership");
                            self.is_leader = false;
                            break;
                        }
                    }
                }

                // Check for voluntary stepdown
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Stepping down as leader");
                    release_lease(&self.store, &self.election_key, &self.consumer_id).await?;
                    self.is_leader = false;
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}
```

## Lease Reclamation and Recovery

### Stale Lease Detection

```rust
impl LeaseReclamation {
    pub async fn reclaim_stale_leases(&self) -> Result<Vec<String>> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let threshold = now - (self.reclaim_threshold.as_millis() as i64);

        let rows = self.pool.get().await?.query(
            r#"
            SELECT consumer, partition, lease_expires_at
            FROM consumer_offsets
            WHERE lease_expires_at IS NOT NULL
              AND lease_expires_at < ?
            "#,
            (threshold,)
        ).await?;

        let mut reclaimed = Vec::new();
        while let Some(row) = rows.next().await? {
            let consumer: String = row.get_value(0)?.as_text().unwrap().to_string();
            let partition: String = row.get_value(1)?.as_text().unwrap().to_string();
            let expires_at: i64 = row.get_value(2)?.as_integer().unwrap();

            tracing::warn!(
                "Reclaiming stale lease: consumer={}, partition={}, expired_at={}",
                consumer, partition, expires_at
            );

            // Clear the stale lease
            self.pool.get().await?.execute(
                r#"
                UPDATE consumer_offsets
                SET lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE consumer = ? AND partition = ?
                "#,
                (now, &consumer, &partition)
            ).await?;

            reclaimed.push(partition);
        }

        Ok(reclaimed)
    }
}
```

### Consumer Recovery

```rust
impl ConsumerRecovery {
    pub async fn recover_consumer(&self, consumer_id: &str) -> Result<()> {
        // Check consumer's current state
        let consumer_state = self.get_consumer_state(consumer_id).await?;

        match consumer_state {
            ConsumerState::Active(lease) => {
                // Check if lease is still valid
                if self.is_lease_valid(&lease).await? {
                    tracing::info!("Consumer {} has valid lease", consumer_id);
                    return Ok(());
                } else {
                    tracing::warn!("Consumer {} has expired lease, clearing", consumer_id);
                    self.clear_consumer_leases(consumer_id).await?;
                }
            }
            ConsumerState::Inactive => {
                tracing::info!("Consumer {} is inactive", consumer_id);
            }
            ConsumerState::Unknown => {
                tracing::warn!("Consumer {} in unknown state, resetting", consumer_id);
                self.reset_consumer_state(consumer_id).await?;
            }
        }

        Ok(())
    }

    async fn reset_consumer_state(&self, consumer_id: &str) -> Result<()> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;

        self.pool.get().await?.execute(
            r#"
            UPDATE consumer_offsets
            SET lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE consumer = ?
            "#,
            (now, consumer_id)
        ).await?;

        Ok(())
    }
}
```

## Monitoring and Observability

### Lease Metrics

```rust
pub struct LeaseMetrics {
    active_leases: AtomicU64,
    expired_leases: AtomicU64,
    lease_renewals: AtomicU64,
    lease_contentions: AtomicU64,
}

impl LeaseMetrics {
    pub fn record_lease_acquired(&self) {
        self.active_leases.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lease_expired(&self) {
        self.active_leases.fetch_sub(1, Ordering::Relaxed);
        self.expired_leases.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lease_renewal(&self) {
        self.lease_renewals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lease_contention(&self) {
        self.lease_contentions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_metrics(&self) -> LeaseMetricsSnapshot {
        LeaseMetricsSnapshot {
            active_leases: self.active_leases.load(Ordering::Relaxed),
            expired_leases: self.expired_leases.load(Ordering::Relaxed),
            lease_renewals: self.lease_renewals.load(Ordering::Relaxed),
            lease_contentions: self.lease_contentions.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeaseMetricsSnapshot {
    pub active_leases: u64,
    pub expired_leases: u64,
    pub lease_renewals: u64,
    pub lease_contentions: u64,
}
```

### Health Checks

```rust
impl LeaseHealthCheck {
    pub async fn check_lease_health(&self) -> Result<LeaseHealthReport> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;

        // Count active leases
        let active_count: i64 = self.pool.get().await?.query_one(
            "SELECT COUNT(*) FROM consumer_offsets WHERE lease_expires_at > ?",
            (now,)
        ).await?.get_value(0)?.as_integer().unwrap();

        // Count expired leases
        let expired_count: i64 = self.pool.get().await?.query_one(
            "SELECT COUNT(*) FROM consumer_offsets WHERE lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
            (now,)
        ).await?.get_value(0)?.as_integer().unwrap();

        // Find leases expiring soon
        let soon_window = now + (5 * 60 * 1000); // 5 minutes
        let expiring_soon: i64 = self.pool.get().await?.query_one(
            "SELECT COUNT(*) FROM consumer_offsets WHERE lease_expires_at > ? AND lease_expires_at <= ?",
            (now, soon_window)
        ).await?.get_value(0)?.as_integer().unwrap();

        // Check for long-running leases (potential issues)
        let long_running_threshold = now - (60 * 60 * 1000); // 1 hour
        let long_running: i64 = self.pool.get().await?.query_one(
            "SELECT COUNT(*) FROM consumer_offsets WHERE lease_expires_at > ? AND updated_at < ?",
            (now, long_running_threshold)
        ).await?.get_value(0)?.as_integer().unwrap();

        Ok(LeaseHealthReport {
            active_leases: active_count as u64,
            expired_leases: expired_count as u64,
            expiring_soon: expiring_soon as u64,
            long_running: long_running as u64,
            checked_at: now,
        })
    }
}
```

## Best Practices

### 1. Lease TTL Configuration

```rust
// High-frequency processing (short leases)
let lease_manager = LeaseManager::new(pool)
    .with_ttl(Duration::from_secs(15))
    .with_heartbeat_interval(Duration::from_secs(5));

// Standard processing (medium leases)
let lease_manager = LeaseManager::new(pool)
    .with_ttl(Duration::from_secs(60))
    .with_heartbeat_interval(Duration::from_secs(20));

// Batch processing (long leases)
let lease_manager = LeaseManager::new(pool)
    .with_ttl(Duration::from_secs(300))
    .with_heartbeat_interval(Duration::from_secs(60));
```

### 2. Graceful Shutdown

```rust
impl Consumer {
    pub async fn shutdown(&mut self) -> Result<()> {
        tracing::info!("Shutting down consumer {}", self.consumer_id);

        // Stop accepting new work
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // Wait for current work to complete
        self.wait_for_inflight_work().await?;

        // Release leases
        if let Some(lease) = &self.current_lease {
            self.lease_manager.release_lease(lease).await?;
            tracing::info!("Released lease for resource {}", lease.resource_id);
        }

        Ok(())
    }
}
```

### 3. Error Handling and Retry Logic

```rust
impl LeaseManager {
    pub async fn acquire_lease_with_retry(
        &self,
        consumer_id: &str,
        resource_id: &str,
        max_retries: usize
    ) -> Result<Lease> {
        let mut retries = 0;

        loop {
            match self.acquire_lease(consumer_id, resource_id).await {
                Ok(lease) => return Ok(lease),
                Err(EsError::LeaseDenied(_)) if retries < max_retries => {
                    retries += 1;
                    let backoff = Duration::from_millis(100 * (1 << retries)); // Exponential backoff
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }
}
```

Lease management ensures reliable coordination between distributed consumers while maintaining high availability and preventing duplicate processing through carefully designed lease acquisition, renewal, and reclamation mechanisms.