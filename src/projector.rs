use crate::broadcast::StreamEventSender;
use crate::catalog::PartitionedCursor;
use crate::validation::TableNameValidator;
use crate::{EsError, EventEnvelope, EventStore, Result};
use std::future::Future;
use std::sync::Arc;
use uuid::Uuid;

/// Represents an active workflow that may need recovery on restart.
///
/// The workflow state is stored in the `consumer_offsets` table as separate columns,
/// not as a serialized blob, hence no Serde derives are needed.
#[derive(Debug, Clone)]
pub struct ActiveWorkflow {
    /// The stream ID where the workflow events are stored
    pub stream_id: String,
    /// The event ID of the workflow start event (e.g., PROVISION_REQUESTED)
    pub event_id: Uuid,
}

/// Error returned by ProjectorHandler implementations
#[derive(Debug, thiserror::Error)]
pub enum ProjectorHandlerError {
    #[error("Event store error: {0}")]
    EventStore(#[from] EsError),

    #[error("Handler error: {0}")]
    Handler(String),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Trait for domain-specific event handling
///
/// Services implement this trait to define how events are processed.
/// The events crate provides the runtime loop that:
/// 1. Bootstraps cursor from checkpoint
/// 2. Polls for new events
/// 3. Calls handle_event for each event
/// 4. Checkpoints progress
pub trait ProjectorHandler: Send + Sync + 'static {
    /// Process a single event.
    ///
    /// Implementations should:
    /// - Parse the event payload based on event type
    /// - Update domain state (database)
    /// - Send stream events via stream_event_sender if event has request_id
    ///
    /// # Error contract
    ///
    /// Return `Err` only for **retryable** failures (transient DB errors, network
    /// timeouts, etc.). The projector will stop the current batch, checkpoint up to
    /// the last successful event, and retry the failed event after a backoff.
    ///
    /// For **non-retryable** failures (unknown event type, corrupt payload, domain
    /// validation errors), handle the error inside the handler — log it, write a
    /// failure record to your domain state, emit a stream event if needed — and
    /// return `Ok(())`. This lets the projector checkpoint past the event and
    /// continue processing.
    ///
    /// The usual at-least-once window applies if the process crashes between a
    /// handler side-effect and the checkpoint write.
    fn handle_event(
        &self,
        event: &EventEnvelope,
        stream_event_sender: &StreamEventSender,
    ) -> impl Future<Output = std::result::Result<(), ProjectorHandlerError>> + Send;
}

impl PartitionedCursor {
    pub fn new(partition: String, created_at_ms: i64, sequence: i64) -> Self {
        Self {
            partition,
            created_at_ms,
            sequence,
        }
    }

    pub fn as_tuple(&self) -> (&str, i64, i64) {
        (&self.partition, self.created_at_ms, self.sequence)
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
        offset.cursor_sequence,
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
        0, // Start from beginning
        0, // Start from sequence 0
    ))
}

/// Get active workflow for a consumer (if any)
///
/// Returns the active workflow that was in progress when the consumer last checkpointed.
/// This is used on startup to recover incomplete workflows.
pub async fn get_active_workflow(
    store: &EventStore,
    consumer: &str,
) -> Result<Option<ActiveWorkflow>> {
    let offset = store.catalog.get_consumer_offset(consumer).await?;

    let Some(offset) = offset else {
        return Ok(None);
    };

    // Both fields must be present for a valid active workflow
    match (offset.workflow_stream_id, offset.workflow_event_id) {
        (Some(stream_id), Some(event_id)) => Ok(Some(ActiveWorkflow {
            stream_id,
            event_id,
        })),
        _ => Ok(None),
    }
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

/// Update cursor checkpoint for a consumer with optional active workflow
///
/// # Arguments
/// * `store` - The event store
/// * `consumer` - The consumer name
/// * `cursor` - The current cursor position
/// * `active_workflow` - Optional active workflow. `Some(workflow)` sets the workflow,
///   `None` clears it (workflow complete or no workflow)
pub async fn checkpoint(
    store: &EventStore,
    consumer: &str,
    cursor: &PartitionedCursor,
    active_workflow: Option<&ActiveWorkflow>,
) -> Result<()> {
    let conn = store.catalog.get_connection().await?;
    let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

    let (workflow_stream_id, workflow_event_id): (Option<String>, Option<String>) =
        match active_workflow {
            Some(wf) => (Some(wf.stream_id.clone()), Some(wf.event_id.to_string())),
            None => (None, None),
        };

    conn.execute(
        r#"
        INSERT INTO consumer_offsets (consumer, partition, cursor_created_at, cursor_sequence, updated_at, workflow_stream_id, workflow_event_id)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(consumer) DO UPDATE SET
            partition = excluded.partition,
            cursor_created_at = excluded.cursor_created_at,
            cursor_sequence = excluded.cursor_sequence,
            updated_at = excluded.updated_at,
            workflow_stream_id = excluded.workflow_stream_id,
            workflow_event_id = excluded.workflow_event_id
        "#,
        (consumer, cursor.partition.clone(), cursor.created_at_ms, cursor.sequence, now, workflow_stream_id, workflow_event_id),
    ).await?;

    Ok(())
}

/// A projector that processes events in batches
pub struct Projector {
    store: Arc<EventStore>,
    consumer: String,
    batch_size: i64,
}

impl Projector {
    pub fn new(store: Arc<EventStore>, consumer: String) -> Self {
        Self {
            store,
            consumer,
            batch_size: 500,
        }
    }

    /// Set the number of events read per polling cycle. Defaults to 500.
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
            // Process next batch and handle results
            let processed = match self
                .process_next_batch(&mut cursor, processor.clone())
                .await
            {
                Ok(processed) => processed,
                Err(e) => {
                    tracing::error!("Error processing events: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            if !processed {
                // No events processed, wait briefly
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }

            // Events processed successfully, continue to next batch
        }
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

        // Update checkpoint (no workflow tracking in generic Projector)
        checkpoint(&self.store, &self.consumer, &next_cursor, None).await?;

        *cursor = next_cursor;
        Ok(true)
    }

    /// Run the projector with a handler implementing ProjectorHandler
    ///
    /// Reads events in batches (controlled by `with_batch_size`) and processes
    /// them one at a time. On success the whole batch is checkpointed once.
    /// On failure the cursor advances only to the last successful event, so
    /// only the failed event is retried — not the entire batch prefix.
    ///
    /// The only unavoidable duplicate window is a process crash between a
    /// handler side-effect and its checkpoint write.
    pub async fn run_with_handler<H: ProjectorHandler>(
        &self,
        handler: &H,
        stream_event_sender: &StreamEventSender,
    ) -> Result<()> {
        let mut cursor = bootstrap_cursor(&self.store, &self.consumer).await?;

        loop {
            // Read a batch with per-event position info
            let positioned_events = self
                .store
                .all_since_with_positions(cursor.clone(), self.batch_size)
                .await?;

            if positioned_events.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }

            // Walk the batch, tracking the last successfully handled cursor
            let mut last_success_cursor: Option<PartitionedCursor> = None;
            let mut failed = false;

            for positioned in &positioned_events {
                if let Err(e) = handler
                    .handle_event(&positioned.event, stream_event_sender)
                    .await
                {
                    tracing::error!(
                        event_id = %positioned.event.id,
                        error = %e,
                        "Handler failed to process event, will retry"
                    );
                    failed = true;
                    break;
                }
                last_success_cursor = Some(positioned.cursor.clone());
            }

            // Checkpoint up to the last successful event
            if let Some(success_cursor) = last_success_cursor {
                checkpoint(&self.store, &self.consumer, &success_cursor, None).await?;
                cursor = success_cursor;
            }

            if failed {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
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
