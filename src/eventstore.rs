use crate::{
    actor::ActorType,
    catalog::{Catalog, PartitionedCursor, StreamHead},
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
    /// Monotonic sequence number within a partition for deterministic global ordering.
    pub sequence: i64,
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

/// An event paired with its cursor position within a partition.
/// Used internally by `run_with_handler` to track per-event positions
/// within a batch without exposing partition info on `EventEnvelope`.
#[derive(Debug, Clone)]
pub(crate) struct PositionedEvent {
    pub event: EventEnvelope,
    pub cursor: PartitionedCursor,
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

        let store = Self {
            catalog,
            active: Arc::new(RwLock::new(active)),
            root: root_path,
            max_payload_bytes: 1024 * 1024, // 1MB default
            rotation,
            pool,
        };

        // Recover streams in the last active partition whose catalog head is stale
        // due to a prior crash (events committed but stream_heads not updated).
        // Only scans the active partition — cost is proportional to streams in that
        // partition, not total dataset. Safe to run here: no concurrent writers exist yet.
        store.recover_active_partition_heads().await?;

        Ok(store)
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

        let (_, suffix) = crate::rotation::parse_partition_name(&active_ref.name)?;

        Ok(ActivePartition {
            db,
            name: active_ref.name,
            start_ms: active_ref.start_ms,
            suffix,
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

        let events: Vec<NewEvent> = events.into_iter().collect();

        if events.is_empty() {
            return Ok(AppendResult {
                version: 0,
                events: vec![],
            });
        }

        self.validate_payload_sizes(&events)?;

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

        // OCC check happens inside the write lock to prevent stale reads
        let current_version = self
            .get_current_version_and_validate_expected(stream_id, &expected)
            .await?;

        let conn = self.pool.get_connection(&active.name).await?;

        // Begin transaction to ensure atomic batch append within the partition DB
        conn.execute("BEGIN IMMEDIATE", ()).await?;

        // Insert all events; rollback on any failure.
        // Map uniqueness violations to Concurrency errors — this is the safety net
        // for the rare case where the catalog head is stale.
        let result_events = match self
            .insert_events(&conn, stream_id, events, current_version, base_time)
            .await
        {
            Ok(evts) => evts,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await; // best-effort rollback
                return Err(Self::map_uniqueness_to_concurrency(
                    e,
                    &conn,
                    stream_id,
                    current_version,
                )
                .await);
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
        // Retry on transient database errors.
        // The version guard ensures older writes cannot overwrite newer ones.
        //
        // IMPORTANT: The write lock must be held until the catalog update completes.
        // Releasing it earlier would allow another writer to read a stale head,
        // rotate to a new partition, and insert a duplicate version — the per-partition
        // uniqueness constraint would not catch this cross-partition race.
        let final_version = result_events
            .last()
            .ok_or_else(|| EsError::Migration("Result events vector is empty".to_string()))?
            .version;

        if let Err(e) = self
            .update_stream_head_with_retry(stream_id, &active.name, &result_events)
            .await
        {
            return Err(EsError::CatalogDrift {
                stream_id: stream_id.to_string(),
                committed_version: final_version,
                source: Box::new(e),
            });
        }

        drop(active);

        Ok(AppendResult {
            version: final_version,
            events: result_events,
        })
    }

    /// Maps a DB uniqueness constraint violation on `events(stream_id, version)` to
    /// `EsError::Concurrency`, querying the partition DB for the actual max version.
    /// All other errors pass through unchanged.
    async fn map_uniqueness_to_concurrency(
        err: EsError,
        conn: &turso::Connection,
        stream_id: &str,
        stale_version: i64,
    ) -> EsError {
        if let EsError::Db(ref db_err) = err {
            let msg = db_err.to_string();
            if msg.contains("UNIQUE constraint failed") && msg.contains("stream_id, version") {
                // Query the partition directly for the real max version —
                // the catalog may itself be stale, so it's not a reliable source here.
                let actual = match conn
                    .query(
                        "SELECT MAX(version) FROM events WHERE stream_id = ?1",
                        (stream_id,),
                    )
                    .await
                {
                    Ok(mut rows) => match rows.next().await {
                        Ok(Some(row)) => row
                            .get_value(0)
                            .ok()
                            .and_then(|v| v.as_integer().copied())
                            .unwrap_or(stale_version),
                        _ => stale_version,
                    },
                    Err(_) => stale_version,
                };
                return EsError::Concurrency {
                    expected: stale_version,
                    actual,
                    stream_id: stream_id.to_string(),
                };
            }
        }
        err
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

        // Get the current max sequence in this partition (inside BEGIN IMMEDIATE)
        let base_sequence = {
            let mut rows = conn
                .query("SELECT COALESCE(MAX(sequence), 0) FROM events", ())
                .await?;
            match rows.next().await? {
                Some(row) => get_integer_safe(&row, 0)?,
                None => 0,
            }
        };

        let mut result_events = Vec::new();

        for (i, event) in events.into_iter().enumerate() {
            let id = uuid::Uuid::new_v4();
            let version = current_version + 1 + i as i64;
            let sequence = base_sequence + 1 + i as i64;
            // Use the same base timestamp for the whole batch to avoid crossing window boundaries
            let created_at = base_time;

            let envelope = EventEnvelope {
                id,
                stream_id: stream_id.to_string(),
                r#type: event.r#type,
                payload: event.payload,
                version,
                created_at,
                sequence,
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
                request_id: event.request_id.clone(),
                actor_id: event.actor_id.clone(),
                actor_type: event.actor_type,
            };

            conn.execute(
                "INSERT INTO events (id, stream_id, type, payload, version, created_at, sequence, trace_id, span_id, request_id, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                (
                    id.to_string(),
                    stream_id,
                    envelope.r#type.clone(),
                    serde_json::to_string(&envelope.payload)?,
                    version,
                    (created_at.unix_timestamp_nanos() / 1_000_000) as i64,
                    sequence,
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
    ) -> Result<bool> {
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
            .await
    }

    async fn update_stream_head_with_retry(
        &self,
        stream_id: &str,
        partition_name: &str,
        result_events: &[EventEnvelope],
    ) -> Result<bool> {
        let mut attempt = 0;
        let max_attempts = 3;

        loop {
            match self
                .update_stream_head(stream_id, partition_name, result_events)
                .await
            {
                Ok(updated) => return Ok(updated),
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

    /// Loads events from a specific stream starting from a given event ID (inclusive).
    ///
    /// This is used for workflow recovery - given the workflow start event ID,
    /// load all events from that point onwards to derive current state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database operations fail
    /// - Event deserialization fails
    /// - Partition files are missing or corrupted
    /// - The starting event is not found
    pub async fn load_since_event(
        &self,
        stream_id: &str,
        from_event_id: uuid::Uuid,
    ) -> Result<Vec<EventEnvelope>> {
        // First, load all events for the stream
        let all_events = self.load(stream_id).await?;

        // Find the starting event and return from that point
        let start_idx = all_events
            .iter()
            .position(|e| e.id == from_event_id)
            .ok_or_else(|| {
                EsError::Cursor(format!(
                    "Workflow start event {} not found in stream {}",
                    from_event_id, stream_id
                ))
            })?;

        Ok(all_events[start_idx..].to_vec())
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
            "SELECT id, stream_id, type, payload, version, created_at, sequence, trace_id, span_id, request_id, actor_id, actor_type FROM events WHERE stream_id = ?1 ORDER BY version",
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
        // 6: sequence, 7: trace_id, 8: span_id, 9: request_id, 10: actor_id, 11: actor_type
        let id_str = get_text_safe(row, 0)?;
        let created_at = get_integer_safe(row, 5)?;
        let sequence = get_integer_safe(row, 6)?;
        let payload_str = get_text_safe(row, 3)?;
        // trace_id may be NULL
        let trace_id = row
            .get_value(7)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        // span_id may be NULL
        let span_id = row
            .get_value(8)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        // request_id may be NULL
        let request_id = row
            .get_value(9)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));
        // actor_id is NOT NULL
        let actor_id = get_text_safe(row, 10)?;
        // actor_type is NOT NULL
        let actor_type_str = get_text_safe(row, 11)?;
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
            sequence,
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
        let positioned = self.all_since_with_positions(cursor.clone(), limit).await?;

        if positioned.is_empty() {
            return Ok((vec![], cursor));
        }

        let next_cursor = positioned.last().unwrap().cursor.clone();
        let events = positioned.into_iter().map(|p| p.event).collect();
        Ok((events, next_cursor))
    }

    /// Like `all_since`, but returns each event paired with its own cursor
    /// position so callers can checkpoint at any point within the batch.
    pub(crate) async fn all_since_with_positions(
        &self,
        cursor: PartitionedCursor,
        limit: i64,
    ) -> Result<Vec<PositionedEvent>> {
        let mut positioned = Vec::new();
        let mut current_partition = cursor.partition.clone();
        let mut current_sequence = cursor.sequence;

        loop {
            if !self.partition_exists(&current_partition) {
                let moved = self
                    .try_move_to_next_partition(&mut current_partition, &mut current_sequence)
                    .await?;
                if !moved {
                    break;
                }
                continue;
            }

            let remaining = limit - positioned.len() as i64;
            let partition_events = self
                .query_partition_events(&current_partition, current_sequence, remaining)
                .await?;

            if partition_events.is_empty() {
                let moved = self
                    .try_move_to_next_partition_if_sealed(
                        &mut current_partition,
                        &mut current_sequence,
                    )
                    .await?;
                if !moved {
                    break;
                }
                continue;
            }

            for event in partition_events {
                let event_cursor = PartitionedCursor {
                    partition: current_partition.clone(),
                    created_at_ms: (event.created_at.unix_timestamp_nanos() / 1_000_000) as i64,
                    sequence: event.sequence,
                };
                current_sequence = event.sequence;
                positioned.push(PositionedEvent {
                    event,
                    cursor: event_cursor,
                });
            }

            if positioned.len() >= limit as usize {
                break;
            }
        }

        Ok(positioned)
    }

    async fn try_move_to_next_partition(
        &self,
        current_partition: &mut String,
        current_sequence: &mut i64,
    ) -> Result<bool> {
        let next_partition = self.catalog.get_next_partition(current_partition).await?;

        if let Some(next_partition) = next_partition {
            *current_partition = next_partition.name;
            *current_sequence = 0;
            Ok(true)
        } else {
            Ok(false) // No more partitions
        }
    }

    async fn try_move_to_next_partition_if_sealed(
        &self,
        current_partition: &mut String,
        current_sequence: &mut i64,
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
        self.try_move_to_next_partition(current_partition, current_sequence)
            .await
    }

    async fn query_partition_events(
        &self,
        db_path: &str,
        current_sequence: i64,
        limit: i64,
    ) -> Result<Vec<EventEnvelope>> {
        let conn = self.pool.get_connection(db_path).await?;

        let mut rows = conn
            .query(
                r#"
            SELECT id, stream_id, type, payload, version, created_at, sequence, trace_id, span_id, request_id, actor_id, actor_type
            FROM events
            WHERE sequence > ?1
            ORDER BY sequence
            LIMIT ?2
            "#,
                (current_sequence, limit),
            )
            .await?;

        let mut partition_events = Vec::new();
        while let Some(row) = rows.next().await? {
            let envelope = self.create_envelope_from_row(&row)?;
            partition_events.push(envelope);
        }
        Ok(partition_events)
    }

    /// Reconciles the catalog stream head with the actual maximum version
    /// found in partition databases. Call this to repair catalog drift after
    /// a crash or failed catalog update.
    ///
    /// **Must be called when no concurrent appends are in progress** (e.g.,
    /// during startup recovery before the store is shared with writers).
    ///
    /// Returns the reconciled version (0 if no events exist for the stream).
    pub async fn reconcile_stream_head(&self, stream_id: &str) -> Result<i64> {
        let partitions = self.catalog.get_all_partitions().await?;

        let mut max_version: i64 = 0;
        let mut max_event_id = None;
        let mut max_created_at_ms: i64 = 0;
        let mut max_partition = String::new();

        for partition in &partitions {
            if !self.partition_exists(&partition.path) {
                continue;
            }
            let conn = self.pool.get_connection(&partition.path).await?;
            let mut rows = conn
                .query(
                    "SELECT version, id, created_at FROM events WHERE stream_id = ?1 ORDER BY version DESC LIMIT 1",
                    (stream_id,),
                )
                .await?;

            if let Some(row) = rows.next().await? {
                let version = get_integer_safe(&row, 0)?;
                if version > max_version {
                    max_version = version;
                    max_event_id = Some(uuid::Uuid::parse_str(&get_text_safe(&row, 1)?)?);
                    max_created_at_ms = get_integer_safe(&row, 2)?;
                    max_partition = partition.name.clone();
                }
            }
        }

        if max_version > 0 {
            if let Some(event_id) = max_event_id {
                self.catalog
                    .update_stream_head(
                        stream_id,
                        max_version,
                        max_created_at_ms,
                        &event_id,
                        &max_partition,
                    )
                    .await?;
            }
        }

        Ok(max_version)
    }

    /// Recovers streams in the current active partition whose catalog head is
    /// stale. Called during startup to handle the common crash scenario: events
    /// committed to the active partition but `stream_heads` not updated.
    ///
    /// Cost is proportional to the number of distinct streams in the active
    /// partition, not the total dataset.
    async fn recover_active_partition_heads(&self) -> Result<()> {
        let active = self.active.read().await;
        let partition_name = active.name.clone();
        let partition_path = active.name.clone();
        drop(active);

        if !self.partition_exists(&partition_path) {
            return Ok(());
        }

        self.recover_partition_heads(&partition_name, &partition_path)
            .await
    }

    /// Scans a single partition for streams whose max version exceeds the
    /// catalog head, and repairs the catalog via the version-guarded upsert.
    async fn recover_partition_heads(
        &self,
        partition_name: &str,
        partition_path: &str,
    ) -> Result<()> {
        let conn = self.pool.get_connection(partition_path).await?;
        let mut rows = conn
            .query(
                r#"
                SELECT e.stream_id, e.version, e.id, e.created_at
                FROM events e
                INNER JOIN (
                    SELECT stream_id, MAX(version) as max_ver
                    FROM events
                    GROUP BY stream_id
                ) m ON e.stream_id = m.stream_id AND e.version = m.max_ver
                "#,
                (),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let stream_id = get_text_safe(&row, 0)?;
            let version = get_integer_safe(&row, 1)?;
            let event_id = uuid::Uuid::parse_str(&get_text_safe(&row, 2)?)?;
            let created_at_ms = get_integer_safe(&row, 3)?;

            // The version guard in update_stream_head ensures this is a no-op
            // when the catalog is already at or beyond this version.
            self.catalog
                .update_stream_head(
                    &stream_id,
                    version,
                    created_at_ms,
                    &event_id,
                    partition_name,
                )
                .await?;
        }

        Ok(())
    }

    /// Scans **all** partitions for streams whose actual max version exceeds the
    /// catalog head, and repairs the catalog. Use this as an explicit admin/maintenance
    /// operation for full dataset reconciliation.
    ///
    /// **Must be called when no concurrent appends are in progress.**
    ///
    /// Cost is proportional to total stream cardinality across all partitions.
    pub async fn recover_all_stale_heads(&self) -> Result<()> {
        let partitions = self.catalog.get_all_partitions().await?;

        for partition in &partitions {
            if !self.partition_exists(&partition.path) {
                continue;
            }
            self.recover_partition_heads(&partition.name, &partition.path)
                .await?;
        }

        Ok(())
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

    /// Returns the full stream head metadata, or `None` if the stream has no events.
    pub async fn get_stream_head(&self, stream_id: &str) -> Result<Option<StreamHead>> {
        self.catalog.get_stream_head(stream_id).await
    }

    /// Lists stream IDs known to the catalog whose IDs start with `prefix`.
    ///
    /// This is intended for startup catch-up supervisors that need to resume
    /// work for domain-scoped streams without scanning partition tables.
    pub async fn stream_ids_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.catalog.stream_ids_with_prefix(prefix).await
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

    /// Checkpoints all WAL files, flushing pages to the main db files.
    ///
    /// Must be called with no active connections (e.g. after all tasks have
    /// stopped). The `EventStore` itself must also be the sole remaining owner
    /// — do not hold any `PooledConnection` or cloned `Arc<DatabasePool>` at
    /// the call site. The active partition's `Database` handle remains open
    /// during the checkpoint; this is safe only because no connections are
    /// active on it.
    pub async fn checkpoint(&self) -> Result<()> {
        // Drain all pool-managed handles so no open connections remain when
        // we open fresh handles for the TRUNCATE checkpoint.
        self.pool.clear_cache().await;

        let mut read_dir = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("db") {
                continue;
            }
            let db_path = path.to_str().ok_or_else(|| {
                EsError::InvalidPath("Partition path contains invalid UTF-8".to_string())
            })?;
            let db = turso::Builder::new_local(db_path)
                .build()
                .await
                .map_err(EsError::Db)?;
            let conn = db.connect().map_err(EsError::Db)?;
            let mut rows = conn
                .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
                .await
                .map_err(EsError::Db)?;
            if let Some(row) = rows.next().await.map_err(EsError::Db)? {
                let busy = row
                    .get_value(0)
                    .ok()
                    .and_then(|v| v.as_integer().copied())
                    .unwrap_or(-1);
                if busy > 0 {
                    tracing::warn!(
                        path = db_path,
                        busy_frames = busy,
                        "WAL checkpoint completed with busy frames; WAL not fully truncated"
                    );
                }
            }
        }
        Ok(())
    }
}
