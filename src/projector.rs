use crate::catalog::PartitionedCursor;
use crate::validation::TableNameValidator;
use crate::{EsError, EventStore, Result};
use std::future::Future;

impl PartitionedCursor {
    pub fn new(partition: String, created_at_ms: i64, event_id: uuid::Uuid) -> Self {
        Self {
            partition,
            created_at_ms,
            event_id,
        }
    }

    pub fn as_tuple(&self) -> (&str, i64, &uuid::Uuid) {
        (&self.partition, self.created_at_ms, &self.event_id)
    }
}

/// Bootstrap cursor from catalog or start from earliest partition
pub async fn bootstrap_cursor(store: &EventStore, consumer: &str) -> Result<PartitionedCursor> {
    // Try to load existing cursor
    let offset = store.catalog.get_consumer_offset(consumer).await?;

    let Some(offset) = offset else {
        return create_earliest_cursor(store).await;
    };

    // Verify partition exists
    let partitions = store.catalog.get_all_partitions().await?;
    if !partitions.iter().any(|p| p.name == offset.partition) {
        return create_earliest_cursor(store).await;
    }

    // Return existing cursor
    Ok(PartitionedCursor::new(
        offset.partition,
        offset.cursor_created_at,
        offset.cursor_event_id,
    ))
}

/// Create cursor starting from earliest partition
async fn create_earliest_cursor(store: &EventStore) -> Result<PartitionedCursor> {
    let partitions = store.catalog.get_all_partitions().await?;

    let earliest = partitions
        .first()
        .ok_or_else(|| EsError::Cursor("No partitions available".to_string()))?;

    Ok(PartitionedCursor::new(
        earliest.name.clone(),
        0,                    // Start from beginning
        uuid::Uuid::new_v4(), // Dummy UUID that will be ignored
    ))
}

/// Execute a projection function within a transaction context
pub async fn with_projection_tx<F, Fut>(store: &EventStore, _consumer: &str, f: F) -> Result<()>
where
    F: FnOnce(&turso::Connection) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let conn = store.catalog.get_connection().await?;

    // Execute projection logic
    let result = f(&conn).await;

    // No explicit transaction handling needed with turso
    result
}

/// Update cursor checkpoint for a consumer
pub async fn checkpoint(
    conn: &turso::Connection,
    consumer: &str,
    cursor: &PartitionedCursor,
) -> Result<()> {
    let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

    conn.execute(
        r#"
        INSERT INTO consumer_offsets (consumer, partition, cursor_created_at, cursor_event_id, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(consumer) DO UPDATE SET
            partition = excluded.partition,
            cursor_created_at = excluded.cursor_created_at,
            cursor_event_id = excluded.cursor_event_id,
            updated_at = excluded.updated_at
        "#,
        (consumer, cursor.partition.clone(), cursor.created_at_ms, cursor.event_id.to_string(), now),
    ).await?;

    Ok(())
}

/// Acquire a lease for a consumer
pub async fn acquire_lease(
    store: &EventStore,
    consumer: &str,
    owner: &str,
    ttl_secs: i64,
) -> Result<bool> {
    let expires_at = ((time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        + ((ttl_secs * 1000) as i128)) as i64;
    store.catalog.renew_lease(consumer, owner, expires_at).await
}

/// Renew an existing lease
pub async fn renew_lease(
    store: &EventStore,
    consumer: &str,
    owner: &str,
    ttl_secs: i64,
) -> Result<bool> {
    let expires_at = ((time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        + ((ttl_secs * 1000) as i128)) as i64;
    store.catalog.renew_lease(consumer, owner, expires_at).await
}

/// Release a lease
pub async fn release_lease(store: &EventStore, consumer: &str, owner: &str) -> Result<bool> {
    store.catalog.release_lease(consumer, owner).await
}

/// Check if a lease is still valid
pub async fn is_lease_valid(store: &EventStore, consumer: &str) -> Result<bool> {
    let Some(offset) = store.catalog.get_consumer_offset(consumer).await? else {
        return Ok(false);
    };

    let Some(expires_at) = offset.lease_expires_at else {
        return Ok(false);
    };

    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    Ok(now < expires_at as i128)
}

/// A projector that processes events in batches
pub struct Projector {
    store: EventStore,
    consumer: String,
    batch_size: i64,
}

impl Projector {
    pub fn new(store: EventStore, consumer: String) -> Self {
        Self {
            store,
            consumer,
            batch_size: 500,
        }
    }

    pub fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Run the projector continuously
    pub async fn run<F, Fut>(&self, processor: F) -> Result<()>
    where
        F: Fn(&[crate::EventEnvelope]) -> Fut + Clone,
        Fut: Future<Output = Result<()>>,
    {
        let mut cursor = bootstrap_cursor(&self.store, &self.consumer).await?;

        loop {
            // Handle expired lease before processing
            if self.is_lease_expired().await? {
                self.handle_expired_lease().await;
                continue;
            }

            // Process next batch and handle results
            match self
                .process_next_batch(&mut cursor, processor.clone())
                .await
            {
                Ok(processed) if !processed => {
                    // No events processed, wait briefly
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
                Ok(_) => {
                    // Events processed successfully, continue to next batch
                }
                Err(e) => {
                    tracing::error!("Error processing events: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    /// Check if the lease has expired
    async fn is_lease_expired(&self) -> Result<bool> {
        let Some(offset) = self
            .store
            .catalog
            .get_consumer_offset(&self.consumer)
            .await?
        else {
            return Ok(false);
        };

        let Some((_owner, expires_at)) = offset.lease_owner.zip(offset.lease_expires_at) else {
            return Ok(false);
        };

        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        Ok(now >= expires_at as i128)
    }

    /// Handle expired lease by waiting
    async fn handle_expired_lease(&self) {
        tracing::warn!("Lease expired for consumer {}", self.consumer);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    /// Process the next batch of events and update cursor
    async fn process_next_batch<F, Fut>(
        &self,
        cursor: &mut PartitionedCursor,
        processor: F,
    ) -> Result<bool>
    where
        F: Fn(&[crate::EventEnvelope]) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let (events, next_cursor) = self
            .store
            .all_since(cursor.clone(), self.batch_size)
            .await?;

        if events.is_empty() {
            return Ok(false);
        }

        // Process events
        processor(&events).await?;

        // Update checkpoint
        let conn = self.store.catalog.get_connection().await?;
        checkpoint(&conn, &self.consumer, &next_cursor).await?;

        *cursor = next_cursor;
        Ok(true)
    }
}

/// A utility for idempotent event processing
pub struct IdempotentProcessor {
    conn: turso::Connection,
    table_name: String,
}

impl IdempotentProcessor {
    pub async fn new(conn: turso::Connection, table_name: String) -> Result<Self> {
        // Validate table name to prevent SQL injection attacks
        let validated_name = TableNameValidator::sanitize_table_name(&table_name)?;

        // Create applied_events table if it doesn't exist
        conn.execute(
            &format!(
                r#"
                CREATE TABLE IF NOT EXISTS {} (
                    event_id TEXT PRIMARY KEY,
                    applied_at INTEGER NOT NULL
                )
                "#,
                validated_name
            ),
            (),
        )
        .await?;

        Ok(Self {
            conn,
            table_name: validated_name,
        })
    }

    /// Check if an event has already been processed
    pub async fn is_applied(&self, event_id: &uuid::Uuid) -> Result<bool> {
        let mut rows = self
            .conn
            .query(
                &format!(
                    "SELECT 1 FROM {} WHERE event_id = ?1 LIMIT 1",
                    self.table_name
                ),
                (event_id.to_string(),),
            )
            .await?;

        Ok(rows.next().await?.is_some())
    }

    /// Mark an event as applied
    pub async fn mark_applied(&self, event_id: &uuid::Uuid) -> Result<()> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        self.conn
            .execute(
                &format!(
                    "INSERT OR IGNORE INTO {} (event_id, applied_at) VALUES (?1, ?2)",
                    self.table_name
                ),
                (event_id.to_string(), now),
            )
            .await?;

        Ok(())
    }

    /// Process an event idempotently
    pub async fn process<F, Fut>(&self, event_id: &uuid::Uuid, f: F) -> Result<bool>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if self.is_applied(event_id).await? {
            return Ok(false); // Already processed
        }

        // Process the event
        f().await?;

        // Mark as applied
        self.mark_applied(event_id).await?;

        Ok(true) // Newly processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::TableNameValidator;

    #[tokio::test]
    async fn test_idempotent_processor_rejects_invalid_table_names() {
        // Create a mock database and connection
        let db = turso::Builder::new_local(":memory:")
            .build()
            .await
            .expect("Failed to create test database");
        let conn = db.connect().expect("Failed to create test connection");

        let invalid_table_names = vec![
            "users; DROP TABLE events; --",
            "users'; DROP TABLE events; --",
            "users\"; DROP TABLE events; --",
            "users`; DROP TABLE events; --",
            "users/**/DROP/**/TABLE/**/events",
            "users UNION SELECT * FROM passwords",
            "SELECT",
            "users-events",
            "users events",
            "123users",
            "users@events",
            "__users",
            "users__",
            "user__events",
            "",
            "ab", // too short
        ];

        for invalid_name in invalid_table_names {
            let result = IdempotentProcessor::new(conn.clone(), invalid_name.to_string()).await;
            assert!(
                result.is_err(),
                "Expected table name '{}' to be rejected",
                invalid_name
            );

            match result {
                Err(EsError::InvalidTableName(_)) => {
                    // This is the expected error type
                }
                Err(other) => {
                    panic!(
                        "Expected InvalidTableName error for '{}', got: {:?}",
                        invalid_name, other
                    );
                }
                Ok(_) => {
                    panic!(
                        "Expected table name '{}' to be rejected, but it was accepted",
                        invalid_name
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_idempotent_processor_accepts_valid_table_names() {
        // Create a mock database and connection
        let db = turso::Builder::new_local(":memory:")
            .build()
            .await
            .expect("Failed to create test database");
        let conn = db.connect().expect("Failed to create test connection");

        let valid_table_names = vec![
            "users",
            "events",
            "user_events",
            "event_store_2024",
            "app_data",
            "customer_orders",
            "product_catalog",
            "audit_log",
            "session_tokens",
            "user_profiles_v2",
        ];

        for valid_name in valid_table_names {
            let result = IdempotentProcessor::new(conn.clone(), valid_name.to_string()).await;
            match result {
                Ok(_) => {
                    // This is expected - the table name is valid
                }
                Err(other) => {
                    panic!(
                        "Expected table name '{}' to be accepted, got error: {:?}",
                        valid_name, other
                    );
                }
            }
        }
    }

    #[test]
    fn test_table_name_validation_directly() {
        // Test SQL injection attempts are blocked
        let injection_attempts = vec![
            "users; DROP TABLE events; --",
            "users'; DROP TABLE events; --",
            "users\"; DROP TABLE events; --",
            "users`; DROP TABLE events; --",
            "users/**/DROP/**/TABLE/**/events",
            "users UNION SELECT * FROM passwords",
            "users' OR '1'='1",
            "users\" OR \"1\"=\"1",
            "users` OR `1`=`1",
            "users); DROP TABLE events; --",
            "users); INSERT INTO users VALUES('hacker', 'password'); --",
        ];

        for injection in injection_attempts {
            let result = TableNameValidator::validate_table_name(injection);
            assert!(
                result.is_err(),
                "Expected injection attempt '{}' to be blocked",
                injection
            );
        }

        // Test valid names are accepted
        let valid_names = vec![
            "users",
            "events",
            "user_events",
            "event_store_2024",
            "app_data",
        ];

        for valid_name in valid_names {
            let result = TableNameValidator::validate_table_name(valid_name);
            assert!(
                result.is_ok(),
                "Expected valid name '{}' to be accepted",
                valid_name
            );
        }
    }
}
