use crate::{
    actor::ActorType,
    catalog::{Catalog, PartitionedCursor},
    error::{EsError, Result},
    migration::partition_migrations,
    pool::{configure_connection, configure_database, DatabasePool},
    rotation::{
        floor_to_window_ms, generate_partition_name, get_next_suffix, should_rotate, RotationPolicy,
    },
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use turso::Database;

/// Helper function to safely extract text value from database row
fn get_text_safe(row: &turso::Row, index: usize) -> Result<String> {
    row.get_value(index)?
        .as_text()
        .ok_or_else(|| EsError::Migration(format!("Expected text value at column {}", index)))
        .map(|s| s.to_string())
}

/// Helper function to safely extract integer value from database row
fn get_integer_safe(row: &turso::Row, index: usize) -> Result<i64> {
    row.get_value(index)?
        .as_integer()
        .ok_or_else(|| EsError::Migration(format!("Expected integer value at column {}", index)))
        .copied()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEvent {
    pub r#type: String,
    pub payload: serde_json::Value,
    /// Optional request ID for completion tracking.
    /// When set, enables FOH to await async operation completion via broadcast.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// The ID of the actor who initiated this event.
    /// For users: Clerk user ID (e.g., `user_xxxxx`)
    /// For system: component identifier (e.g., `system:provisioning-projector`)
    pub actor_id: String,
    /// The type of actor who initiated this event.
    pub actor_type: ActorType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: uuid::Uuid,
    pub stream_id: String,
    pub r#type: String,
    pub payload: serde_json::Value,
    pub version: i64,
    pub created_at: time::OffsetDateTime,
    /// Trace ID from OpenTelemetry context when the event was appended.
    /// Enables correlation between events and distributed traces.
    pub trace_id: Option<String>,
    /// Span ID from OpenTelemetry context when the event was appended.
    /// Enables correlation to the specific span that appended the event.
    pub span_id: Option<String>,
    /// Request ID for completion tracking.
    /// Used by projectors to send completion notifications back to FOH.
    pub request_id: Option<String>,
    /// The ID of the actor who initiated this event.
    /// For users: Clerk user ID (e.g., `user_xxxxx`)
    /// For system: component identifier (e.g., `system:provisioning-projector`)
    pub actor_id: String,
    /// The type of actor who initiated this event.
    pub actor_type: ActorType,
}

#[derive(Debug, Clone)]
pub enum ExpectedVersion {
    NoStream,
    Any,
    Exact(i64),
}

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub version: i64,
    pub events: Vec<EventEnvelope>,
}

pub struct EventStore {
    pub(crate) catalog: Catalog,
    active: Arc<RwLock<ActivePartition>>,
    root: PathBuf,
    max_payload_bytes: usize,
    rotation: RotationPolicy,
    pool: Arc<DatabasePool>,
}

struct ActivePartition {
    db: Database,
    name: String,
    start_ms: i64,
    suffix: Option<char>,
}

impl EventStore {
    /// Opens a new partitioned event store at the specified root directory.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The root directory path contains invalid UTF-8
    /// - The database cannot be created or opened
    /// - Migration fails
    /// - File system permissions prevent access
    pub async fn open_partitioned(root: &str, rotation: RotationPolicy) -> Result<Self> {
        let root_path = PathBuf::from(root);
        tokio::fs::create_dir_all(&root_path).await?;

        // Create connection pool
        let pool = Arc::new(DatabasePool::new(&root_path)?);

        let catalog = Catalog::open_with_pool(&root_path, Arc::clone(&pool)).await?;

        // Initialize or get active partition
        let active = Self::initialize_active_partition(&catalog, &root_path, &rotation).await?;

        Ok(Self {
            catalog,
            active: Arc::new(RwLock::new(active)),
            root: root_path,
            max_payload_bytes: 1024 * 1024, // 1MB default
            rotation,
            pool,
        })
    }

    async fn initialize_active_partition(
        catalog: &Catalog,
        root: &Path,
        rotation: &RotationPolicy,
    ) -> Result<ActivePartition> {
        // Check if there's an active partition
        let Some(active_ref) = catalog.get_active_partition().await? else {
            return Self::create_new_active_partition(catalog, root, rotation).await;
        };

        let partition_path = root.join(&active_ref.path);
        let partition_path_str = partition_path.to_str().ok_or_else(|| {
            EsError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", partition_path))
        })?;
        let db = turso::Builder::new_local(partition_path_str)
            .build()
            .await?;
        configure_database(&db).await?;

        Ok(ActivePartition {
            db,
            name: active_ref.name,
            start_ms: active_ref.start_ms,
            suffix: None, // We'll parse this from the name if needed
        })
    }

    async fn create_new_active_partition(
        catalog: &Catalog,
        root: &Path,
        rotation: &RotationPolicy,
    ) -> Result<ActivePartition> {
        let now_ms = time::OffsetDateTime::now_utc().unix_timestamp();
        let start_ms = floor_to_window_ms(now_ms * 1000, rotation.window()); // Convert to ms for floor function
        let name = generate_partition_name(start_ms, rotation.window(), None)?;
        let partition_path = root.join(&name);
        let partition_path_str = partition_path.to_str().ok_or_else(|| {
            EsError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", partition_path))
        })?;

        // Create partition database
        let db = turso::Builder::new_local(partition_path_str)
            .build()
            .await?;
        configure_database(&db).await?;

        // Run migrations on new partition
        {
            let conn = db.connect()?;
            configure_connection(&conn).await?;
            partition_migrations().run(&conn).await?;
        }

        // Register partition in catalog
        let partition_ref = crate::catalog::PartitionRef {
            name: name.clone(),
            path: name.clone(),
            start_ms,
            end_ms: None,
            sealed: false,
        };
        catalog.create_partition(&partition_ref).await?;

        Ok(ActivePartition {
            db,
            name,
            start_ms,
            suffix: None,
        })
    }

    /// Checks if the current partition needs rotation and performs it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File metadata cannot be read
    /// - Partition rotation fails
    /// - Database operations fail
    pub async fn maybe_rotate(&self) -> Result<()> {
        let active = self.active.read().await;
        let file_path = self.root.join(&active.name);
        let file_size = tokio::fs::metadata(&file_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        // Check if rotation is needed
        if !should_rotate(&active.name, &self.rotation, file_size, active.start_ms)? {
            return Ok(());
        }

        // Capture timestamp for consistent rotation
        let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        drop(active); // Release read lock
        self.rotate_partition(now_ms).await
    }

    async fn rotate_partition(&self, now_ms: i64) -> Result<()> {
        let mut active = self.active.write().await;

        // Seal current partition
        let end_ms = now_ms / 1000; // Convert from ms to seconds
        self.catalog.seal_partition(&active.name, end_ms).await?;

        let (new_name, new_start_ms, new_suffix) =
            self.determine_new_partition_info(active.start_ms, active.suffix, now_ms)?;

        let new_db = self
            .create_and_register_new_partition(&new_name, new_start_ms)
            .await?;

        // Swap active partition
        active.db = new_db;
        active.name = new_name;
        active.start_ms = new_start_ms;
        active.suffix = new_suffix;

        tracing::info!("Rotated to new partition: {}", active.name);
        Ok(())
    }

    fn determine_new_partition_info(
        &self,
        current_start_ms: i64,
        current_suffix: Option<char>,
        now_ms: i64,
    ) -> Result<(String, i64, Option<char>)> {
        let new_start_ms = floor_to_window_ms(now_ms, self.rotation.window());

        if new_start_ms == current_start_ms {
            // Same time window, add suffix
            let suffix = get_next_suffix(current_suffix).ok_or_else(|| {
                EsError::Migration("Exhausted suffixes for current time window".to_string())
            })?;
            let new_name =
                generate_partition_name(new_start_ms, self.rotation.window(), Some(suffix))?;
            return Ok((new_name, new_start_ms, Some(suffix)));
        }
        let new_name = generate_partition_name(new_start_ms, self.rotation.window(), None)?;
        Ok((new_name, new_start_ms, None))
    }

    async fn create_and_register_new_partition(
        &self,
        name: &str,
        start_ms: i64,
    ) -> Result<Database> {
        let new_partition_path = self.root.join(name);
        let new_partition_path_str = new_partition_path.to_str().ok_or_else(|| {
            EsError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", new_partition_path))
        })?;
        let new_db = turso::Builder::new_local(new_partition_path_str)
            .build()
            .await?;
        configure_database(&new_db).await?;

        {
            let conn = new_db.connect()?;
            configure_connection(&conn).await?;
            partition_migrations().run(&conn).await?;
        }

        // Register new partition
        let partition_ref = crate::catalog::PartitionRef {
            name: name.to_string(),
            path: name.to_string(),
            start_ms,
            end_ms: None,
            sealed: false,
        };
        self.catalog.create_partition(&partition_ref).await?;

        Ok(new_db)
    }

    /// Appends one or more events to a stream with optimistic concurrency control.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Event payload exceeds maximum size limit
    /// - Optimistic concurrency check fails (wrong expected version)
    /// - Database operations fail
    /// - Event serialization fails
    /// - Partition rotation fails
    pub async fn append(
        &self,
        stream_id: &str,
        expected: ExpectedVersion,
        events: impl IntoIterator<Item = NewEvent>,
    ) -> Result<AppendResult> {
        // Check rotation first
        self.maybe_rotate().await?;

        let active = self.active.read().await;
        let events: Vec<NewEvent> = events.into_iter().collect();

        if events.is_empty() {
            return Ok(AppendResult {
                version: 0,
                events: vec![],
            });
        }

        self.validate_payload_sizes(&events)?;
        let current_version = self
            .get_current_version_and_validate_expected(stream_id, &expected)
            .await?;

        // Drop read lock and acquire write lock; ensure partition window matches event timestamps
        drop(active);

        // Capture time once and use it consistently for both rotation checking and event creation
        let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let base_time =
            time::OffsetDateTime::from_unix_timestamp_nanos(now_ms as i128 * 1_000_000)?;

        // Ensure we are operating on a partition whose window matches current time
        // to keep created_at within the partition time window.
        loop {
            {
                let active = self.active.read().await;
                let floored = floor_to_window_ms(now_ms, self.rotation.window());
                if floored == active.start_ms {
                    break;
                }
            }
            // Rotate to align with the current time window
            self.rotate_partition(now_ms).await?;
        }

        let active = self.active.write().await;
        let conn = self.pool.get_connection(&active.name).await?;

        // Begin transaction to ensure atomic batch append within the partition DB
        conn.execute("BEGIN", ()).await?;

        // Insert all events; rollback on any failure
        let result_events = match self
            .insert_events(&conn, stream_id, events, current_version, base_time)
            .await
        {
            Ok(evts) => evts,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await; // best-effort rollback
                return Err(e);
            }
        };

        // Commit inserted events
        if let Err(e) = conn.execute("COMMIT", ()).await {
            // Try to rollback if commit fails
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e.into());
        }

        drop(conn);

        // Update stream head in catalog (separate DB; cannot be part of same transaction)
        // Retry on transient database errors
        self.update_stream_head_with_retry(stream_id, &active.name, &result_events)
            .await?;

        let final_version = result_events
            .last()
            .ok_or_else(|| EsError::Migration("Result events vector is empty".to_string()))?
            .version;

        Ok(AppendResult {
            version: final_version,
            events: result_events,
        })
    }

    fn validate_payload_sizes(&self, events: &[NewEvent]) -> Result<()> {
        for event in events {
            let payload_str = serde_json::to_string(&event.payload)?;
            if payload_str.len() > self.max_payload_bytes {
                return Err(EsError::PayloadTooLarge {
                    size: payload_str.len(),
                    max: self.max_payload_bytes,
                });
            }
        }
        Ok(())
    }

    async fn get_current_version_and_validate_expected(
        &self,
        stream_id: &str,
        expected: &ExpectedVersion,
    ) -> Result<i64> {
        let stream_head = self.catalog.get_stream_head(stream_id).await?;
        let current_version = stream_head.as_ref().map(|h| h.version).unwrap_or(0);

        // Validate expected version with early returns
        match expected {
            ExpectedVersion::NoStream if current_version != 0 => {
                return Err(EsError::Concurrency {
                    expected: -1,
                    actual: current_version,
                    stream_id: stream_id.to_string(),
                });
            }
            ExpectedVersion::Exact(expected) if *expected != current_version => {
                return Err(EsError::Concurrency {
                    expected: *expected,
                    actual: current_version,
                    stream_id: stream_id.to_string(),
                });
            }
            ExpectedVersion::Any | ExpectedVersion::NoStream | ExpectedVersion::Exact(_) => {
                // Version is valid, continue
            }
        }
        Ok(current_version)
    }

    async fn insert_events(
        &self,
        conn: &turso::Connection,
        stream_id: &str,
        events: Vec<NewEvent>,
        current_version: i64,
        base_time: time::OffsetDateTime,
    ) -> Result<Vec<EventEnvelope>> {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        // Extract trace ID and span ID from current span context once for the batch
        let (trace_id, span_id) = {
            let span = tracing::Span::current();
            let context = span.context();
            let otel_span = context.span();
            let span_context = otel_span.span_context();

            let trace_id = {
                let id = span_context.trace_id();
                if id == opentelemetry::trace::TraceId::INVALID {
                    None
                } else {
                    Some(id.to_string())
                }
            };

            let span_id = {
                let id = span_context.span_id();
                if id == opentelemetry::trace::SpanId::INVALID {
                    None
                } else {
                    Some(id.to_string())
                }
            };

            (trace_id, span_id)
        };

        let mut result_events = Vec::new();

        for (i, event) in events.into_iter().enumerate() {
            let id = uuid::Uuid::new_v4();
            let version = current_version + 1 + i as i64;
            // Use the same base timestamp for the whole batch to avoid crossing window boundaries
            let created_at = base_time;

            let envelope = EventEnvelope {
                id,
                stream_id: stream_id.to_string(),
                r#type: event.r#type,
                payload: event.payload,
                version,
                created_at,
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
                request_id: event.request_id.clone(),
                actor_id: event.actor_id.clone(),
                actor_type: event.actor_type,
            };

            conn.execute(
                "INSERT INTO events (id, stream_id, type, payload, version, created_at, trace_id, span_id, request_id, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                (
                    id.to_string(),
                    stream_id,
                    envelope.r#type.clone(),
                    serde_json::to_string(&envelope.payload)?,
                    version,
                    (created_at.unix_timestamp_nanos() / 1_000_000) as i64,
                    turso::Value::from(trace_id.as_deref()),
                    turso::Value::from(span_id.as_deref()),
                    turso::Value::from(event.request_id.as_deref()),
                    event.actor_id.as_str(),
                    event.actor_type.as_str(),
                ),
            ).await?;

            result_events.push(envelope);
        }
        Ok(result_events)
    }

    async fn update_stream_head(
        &self,
        stream_id: &str,
        partition_name: &str,
        result_events: &[EventEnvelope],
    ) -> Result<()> {
        let last_event = result_events
            .last()
            .ok_or_else(|| EsError::Migration("Result events vector is empty".to_string()))?;

        self.catalog
            .update_stream_head(
                stream_id,
                last_event.version,
                (last_event.created_at.unix_timestamp_nanos() / 1_000_000) as i64,
                &last_event.id,
                partition_name,
            )
            .await?;
        Ok(())
    }

    async fn update_stream_head_with_retry(
        &self,
        stream_id: &str,
        partition_name: &str,
        result_events: &[EventEnvelope],
    ) -> Result<()> {
        let mut attempt = 0;
        let max_attempts = 3;

        loop {
            match self
                .update_stream_head(stream_id, partition_name, result_events)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    // Only retry on database errors; immediately fail on others
                    match e {
                        EsError::Db(_) if attempt < max_attempts => {
                            let backoff_ms = 50_u64.saturating_mul(1 << (attempt - 1));
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            continue;
                        }
                        other => return Err(other),
                    }
                }
            }
        }
    }

    /// Loads all events from a specific stream in version order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database operations fail
    /// - Event deserialization fails
    /// - Partition files are missing or corrupted
    pub async fn load(&self, stream_id: &str) -> Result<Vec<EventEnvelope>> {
        let partitions = self.catalog.get_all_partitions().await?;
        let mut all_events = Vec::new();

        for partition in partitions {
            if !self.partition_exists(&partition.path) {
                continue;
            }

            let partition_events = self
                .load_events_from_partition(&partition.path, stream_id)
                .await?;
            all_events.extend(partition_events);
        }

        // Sort by version (should already be sorted, but ensure consistency)
        all_events.sort_by_key(|e| e.version);
        Ok(all_events)
    }

    fn partition_exists(&self, db_path: &str) -> bool {
        let full_path = self.root.join(db_path);
        full_path.exists()
    }

    async fn load_events_from_partition(
        &self,
        db_path: &str,
        stream_id: &str,
    ) -> Result<Vec<EventEnvelope>> {
        let conn = self.pool.get_connection(db_path).await?;
        let mut rows = conn.query(
            "SELECT id, stream_id, type, payload, version, created_at, trace_id, span_id, request_id, actor_id, actor_type FROM events WHERE stream_id = ?1 ORDER BY version",
            (stream_id,),
        ).await?;

        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            let envelope = self.create_envelope_from_row(&row)?;
            events.push(envelope);
        }
        Ok(events)
    }

    fn create_envelope_from_row(&self, row: &turso::Row) -> Result<EventEnvelope> {
        // Column indices match SELECT query order:
        // 0: id, 1: stream_id, 2: type, 3: payload, 4: version, 5: created_at,
        // 6: trace_id, 7: span_id, 8: request_id, 9: actor_id, 10: actor_type
        let id_str = get_text_safe(row, 0)?;
        let created_at = get_integer_safe(row, 5)?;
        let payload_str = get_text_safe(row, 3)?;
        // trace_id may be NULL
        let trace_id = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        // span_id may be NULL
        let span_id = row
            .get_value(7)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        // request_id may be NULL
        let request_id = row
            .get_value(8)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        // actor_id is NOT NULL
        let actor_id = get_text_safe(row, 9)?;
        // actor_type is NOT NULL
        let actor_type_str = get_text_safe(row, 10)?;
        let actor_type = actor_type_str.parse::<ActorType>().map_err(|e| {
            EsError::Migration(format!("Invalid actor_type '{}': {}", actor_type_str, e))
        })?;

        Ok(EventEnvelope {
            id: uuid::Uuid::parse_str(&id_str)?,
            stream_id: get_text_safe(row, 1)?,
            r#type: get_text_safe(row, 2)?,
            payload: serde_json::from_str(&payload_str)?,
            version: get_integer_safe(row, 4)?,
            created_at: time::OffsetDateTime::from_unix_timestamp_nanos(
                (created_at as i128) * 1_000_000,
            )?,
            trace_id,
            span_id,
            request_id,
            actor_id,
            actor_type,
        })
    }

    /// Reads events globally from all partitions starting from a cursor position.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database operations fail
    /// - Event deserialization fails
    /// - Partition files are missing or corrupted
    /// - Invalid cursor position
    pub async fn all_since(
        &self,
        cursor: PartitionedCursor,
        limit: i64,
    ) -> Result<(Vec<EventEnvelope>, PartitionedCursor)> {
        let mut events = Vec::new();
        let mut current_partition = cursor.partition.clone();
        let mut current_created_at = cursor.created_at_ms;
        let mut current_event_id = cursor.event_id;

        loop {
            // Skip to next partition if current one doesn't exist
            if !self.partition_exists(&current_partition) {
                let moved = self
                    .try_move_to_next_partition(
                        &mut current_partition,
                        &mut current_created_at,
                        &mut current_event_id,
                    )
                    .await?;

                if !moved {
                    break; // No more partitions
                }
                continue;
            }

            let partition_events = self
                .query_partition_events(
                    &current_partition,
                    current_created_at,
                    current_event_id,
                    limit,
                )
                .await?;

            // Move to next partition if no events found
            if partition_events.is_empty() {
                let moved = self
                    .try_move_to_next_partition_if_sealed(
                        &mut current_partition,
                        &mut current_created_at,
                        &mut current_event_id,
                    )
                    .await?;

                if !moved {
                    break; // No more events in this partition
                }
                continue;
            }

            // Add all events from this partition
            events.extend(partition_events);

            // Update cursor to last event
            if let Some(last_event) = events.last() {
                current_created_at =
                    (last_event.created_at.unix_timestamp_nanos() / 1_000_000) as i64;
                current_event_id = last_event.id;
            }

            if events.len() >= limit as usize {
                break;
            }
        }

        let next_cursor = if events.is_empty() {
            cursor
        } else {
            PartitionedCursor {
                partition: current_partition,
                created_at_ms: current_created_at,
                event_id: current_event_id,
            }
        };

        Ok((events, next_cursor))
    }

    async fn try_move_to_next_partition(
        &self,
        current_partition: &mut String,
        current_created_at: &mut i64,
        current_event_id: &mut uuid::Uuid,
    ) -> Result<bool> {
        let next_partition = self.catalog.get_next_partition(current_partition).await?;

        if let Some(next_partition) = next_partition {
            *current_partition = next_partition.name;
            *current_created_at = 0;
            *current_event_id = uuid::Uuid::new_v4(); // Reset for new partition
            Ok(true)
        } else {
            Ok(false) // No more partitions
        }
    }

    async fn try_move_to_next_partition_if_sealed(
        &self,
        current_partition: &mut String,
        current_created_at: &mut i64,
        current_event_id: &mut uuid::Uuid,
    ) -> Result<bool> {
        let partitions = self.catalog.get_all_partitions().await?;

        // Find the current partition and check if it's sealed
        let Some(partition) = partitions.iter().find(|p| p.name == *current_partition) else {
            return Ok(false); // Partition not found
        };

        if !partition.sealed {
            return Ok(false); // Current partition is not sealed
        }

        // Move to next partition since current is sealed
        self.try_move_to_next_partition(current_partition, current_created_at, current_event_id)
            .await
    }

    async fn query_partition_events(
        &self,
        db_path: &str,
        current_created_at: i64,
        current_event_id: uuid::Uuid,
        limit: i64,
    ) -> Result<Vec<EventEnvelope>> {
        let conn = self.pool.get_connection(db_path).await?;

        let mut rows = conn
            .query(
                r#"
            SELECT id, stream_id, type, payload, version, created_at, trace_id, span_id, request_id, actor_id, actor_type
            FROM events
            WHERE (created_at > ?1 OR (created_at = ?1 AND id > ?2))
            ORDER BY created_at, id
            LIMIT ?3
            "#,
                (current_created_at, current_event_id.to_string(), limit),
            )
            .await?;

        let mut partition_events = Vec::new();
        while let Some(row) = rows.next().await? {
            let envelope = self.create_envelope_from_row(&row)?;
            partition_events.push(envelope);
        }
        Ok(partition_events)
    }

    /// Gets the current version of a stream (0 if stream doesn't exist).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database operations fail
    /// - Catalog is corrupted
    pub async fn get_stream_version(&self, stream_id: &str) -> Result<i64> {
        let Some(head) = self.catalog.get_stream_head(stream_id).await? else {
            return Ok(0);
        };
        Ok(head.version)
    }

    /// Gets the name of the currently active partition.
    pub async fn get_active_partition_name(&self) -> Result<String> {
        let active = self.active.read().await;
        Ok(active.name.clone())
    }

    /// Gets statistics about the connection pool.
    ///
    /// This method provides insights into the pool's performance and usage,
    /// including the number of cached databases and active connections.
    pub async fn pool_stats(&self) -> crate::pool::PoolStats {
        self.pool.stats().await
    }
}
