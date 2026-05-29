use crate::{
    actor::ActorType,
    catalog::{Catalog, EventFileRange},
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

const MAX_READ_LIMIT: usize = 10_000;

fn get_text_safe(row: &turso::Row, index: usize) -> Result<String> {
    row.get_value(index)?
        .as_text()
        .ok_or_else(|| EsError::Migration(format!("Expected text value at column {}", index)))
        .map(|s| s.to_string())
}

fn get_integer_safe(row: &turso::Row, index: usize) -> Result<i64> {
    row.get_value(index)?
        .as_integer()
        .ok_or_else(|| EsError::Migration(format!("Expected integer value at column {}", index)))
        .copied()
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventStreamVersion(i64);

impl EventStreamVersion {
    pub fn start() -> Self {
        Self(0)
    }

    pub fn new(value: i64) -> Result<Self> {
        if value <= 0 {
            return Err(EsError::InvalidVersion(format!(
                "event stream versions must be positive, got {value}"
            )));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> i64 {
        self.0
    }

    pub fn is_start(self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for EventStreamVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowRef {
    None,
    StartsThisWorkflow,
    Continues { started_by_event_id: uuid::Uuid },
}

impl Default for WorkflowRef {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEvent {
    pub r#type: String,
    pub payload: serde_json::Value,
    pub workflow_kind: Option<String>,
    #[serde(skip)]
    pub workflow: WorkflowRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub actor_id: String,
    pub actor_type: ActorType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: uuid::Uuid,
    pub r#type: String,
    pub payload: serde_json::Value,
    pub version: EventStreamVersion,
    pub created_at: time::OffsetDateTime,
    pub sequence: i64,
    pub workflow_kind: Option<String>,
    pub workflow_started_by_event_id: Option<uuid::Uuid>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub request_id: Option<String>,
    pub actor_id: String,
    pub actor_type: ActorType,
}

#[derive(Debug, Clone)]
pub enum ExpectedVersion {
    NoStream,
    Any,
    Exact(EventStreamVersion),
}

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub first_version: EventStreamVersion,
    pub last_version: EventStreamVersion,
    pub events: Vec<EventEnvelope>,
}

#[derive(Clone)]
pub struct EventStream {
    inner: Arc<EventStreamInner>,
}

struct EventStreamInner {
    catalog: Catalog,
    active: RwLock<ActivePartition>,
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

struct EventFileAppendHead {
    current_version: i64,
}

struct EventFileSummary {
    name: String,
    first_version: i64,
    current_version: i64,
}

impl EventStream {
    pub(crate) async fn open_partitioned(
        root: impl AsRef<Path>,
        rotation: RotationPolicy,
    ) -> Result<Self> {
        let root_path = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root_path).await?;

        let pool = Arc::new(DatabasePool::new(&root_path)?);
        let catalog = Catalog::open_with_pool(&root_path, Arc::clone(&pool)).await?;
        let active = Self::initialize_active_partition(&catalog, &root_path, &rotation).await?;

        Ok(Self {
            inner: Arc::new(EventStreamInner {
                catalog,
                active: RwLock::new(active),
                root: root_path,
                max_payload_bytes: 1024 * 1024,
                rotation,
                pool,
            }),
        })
    }

    async fn initialize_active_partition(
        catalog: &Catalog,
        root: &Path,
        rotation: &RotationPolicy,
    ) -> Result<ActivePartition> {
        let mut ranges = catalog.get_all_event_file_ranges().await?;
        let unsealed_count = ranges.iter().filter(|range| !range.sealed).count();
        if unsealed_count != 1 {
            ranges = Self::rebuild_event_file_ranges(catalog, root).await?;
        }

        if let Some(active_ref) = ranges
            .iter()
            .filter(|range| !range.sealed)
            .max_by(|a, b| {
                a.first_version
                    .cmp(&b.first_version)
                    .then_with(|| a.name.cmp(&b.name))
            })
            .cloned()
        {
            let (start_ms, suffix) = crate::rotation::parse_partition_name(&active_ref.name)?;
            let db = Self::open_partition_db(root, &active_ref.path).await?;
            return Ok(ActivePartition {
                db,
                name: active_ref.name,
                start_ms,
                suffix,
            });
        }

        Self::create_new_active_partition(catalog, root, rotation, 1).await
    }

    async fn rebuild_event_file_ranges(
        catalog: &Catalog,
        root: &Path,
    ) -> Result<Vec<EventFileRange>> {
        let mut summaries = Self::discover_event_file_summaries(root).await?;
        if summaries.is_empty() {
            catalog.replace_event_file_ranges(&[]).await?;
            return Ok(Vec::new());
        }

        summaries.sort_by(|a, b| {
            a.first_version
                .cmp(&b.first_version)
                .then_with(|| a.name.cmp(&b.name))
        });
        let active_index = summaries.len() - 1;
        let ranges: Vec<EventFileRange> = summaries
            .iter()
            .enumerate()
            .map(|(index, summary)| EventFileRange {
                name: summary.name.clone(),
                path: summary.name.clone(),
                first_version: summary.first_version,
                last_version: (index != active_index).then_some(summary.current_version),
                sealed: index != active_index,
            })
            .collect();

        catalog.replace_event_file_ranges(&ranges).await?;
        Ok(ranges)
    }

    async fn discover_event_file_summaries(root: &Path) -> Result<Vec<EventFileSummary>> {
        let mut read_dir = match tokio::fs::read_dir(root).await {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err.into()),
        };
        let mut summaries = Vec::new();

        while let Some(entry) = read_dir.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if crate::rotation::parse_partition_name(&name).is_err() {
                continue;
            };

            let db = Self::open_partition_db(root, &name).await?;
            let conn = db.connect()?;
            configure_connection(&conn).await?;
            let head = Self::read_event_file_append_head(&conn).await?;
            let first_version = Self::read_first_event_version(&conn)
                .await?
                .unwrap_or(head.current_version + 1);

            summaries.push(EventFileSummary {
                name,
                first_version,
                current_version: head.current_version,
            });
        }

        Ok(summaries)
    }

    async fn create_new_active_partition(
        catalog: &Catalog,
        root: &Path,
        rotation: &RotationPolicy,
        first_version: i64,
    ) -> Result<ActivePartition> {
        let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let start_ms = floor_to_window_ms(now_ms, rotation.window());
        let name = generate_partition_name(start_ms, rotation.window(), None)?;
        let db = Self::create_partition_db(root, &name, first_version - 1, None).await?;

        catalog
            .create_event_file_range(&EventFileRange {
                name: name.clone(),
                path: name.clone(),
                first_version,
                last_version: None,
                sealed: false,
            })
            .await?;

        Ok(ActivePartition {
            db,
            name,
            start_ms,
            suffix: None,
        })
    }

    async fn open_partition_db(root: &Path, db_path: &str) -> Result<Database> {
        let full_path = root.join(db_path);
        let path_str = full_path.to_str().ok_or_else(|| {
            EsError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", full_path))
        })?;
        let db = turso::Builder::new_local(path_str).build().await?;
        configure_database(&db).await?;
        Ok(db)
    }

    async fn create_partition_db(
        root: &Path,
        db_path: &str,
        current_version: i64,
        last_event_id: Option<uuid::Uuid>,
    ) -> Result<Database> {
        let db = Self::open_partition_db(root, db_path).await?;
        let conn = db.connect()?;
        configure_connection(&conn).await?;
        partition_migrations().run(&conn).await?;
        Self::initialize_event_file_append_head(&conn, current_version, last_event_id).await?;
        Ok(db)
    }

    pub async fn append(
        &self,
        expected: ExpectedVersion,
        events: impl IntoIterator<Item = NewEvent>,
    ) -> Result<AppendResult> {
        self.maybe_rotate().await?;

        let events: Vec<NewEvent> = events.into_iter().collect();
        if events.is_empty() {
            return Ok(AppendResult {
                first_version: EventStreamVersion::start(),
                last_version: EventStreamVersion::start(),
                events: Vec::new(),
            });
        }

        self.validate_events(&events)?;

        let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let base_time =
            time::OffsetDateTime::from_unix_timestamp_nanos(now_ms as i128 * 1_000_000)?;

        self.rotate_until_current_window(now_ms).await?;

        let active = self.inner.active.write().await;
        let conn = self.inner.pool.get_connection(&active.name).await?;
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        let current_version = match Self::read_event_file_append_head(&conn).await {
            Ok(head) => {
                match Self::validate_expected_version_against(&expected, head.current_version) {
                    Ok(current_version) => current_version,
                    Err(e) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(e);
            }
        };

        let result_events = match self
            .insert_events(&conn, events, current_version, base_time)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(self
                    .map_uniqueness_to_incorrect_event_version(e, current_version)
                    .await);
            }
        };

        let last_event = result_events
            .last()
            .ok_or_else(|| EsError::Migration("append result was unexpectedly empty".into()))?;
        if let Err(e) = Self::update_event_file_append_head(
            &conn,
            last_event.version.get(),
            Some(last_event.id),
        )
        .await
        {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e);
        }

        if let Err(e) = conn.execute("COMMIT", ()).await {
            let _ = conn.execute("ROLLBACK", ()).await;
            return Err(e.into());
        }

        let first_version = result_events[0].version;
        let last_version = last_event.version;
        Ok(AppendResult {
            first_version,
            last_version,
            events: result_events,
        })
    }

    pub async fn load_after_version(
        &self,
        cursor: EventStreamVersion,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        self.validate_read_limit(limit)?;
        let mut events = Vec::new();
        let ranges = self
            .inner
            .catalog
            .get_event_file_ranges_after_version(cursor.get())
            .await?;

        for range in ranges {
            let remaining = limit - events.len();
            if remaining == 0 {
                break;
            }
            let mut partition_events = self
                .query_partition_events_after_version(&range.path, cursor.get(), remaining)
                .await?;
            events.append(&mut partition_events);
        }

        events.sort_by_key(|event| event.version);
        events.truncate(limit);
        Ok(events)
    }

    pub async fn load_workflow_after_version(
        &self,
        workflow_started_by_event_id: uuid::Uuid,
        cursor: EventStreamVersion,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        self.validate_read_limit(limit)?;
        let mut events = Vec::new();
        let ranges = self
            .inner
            .catalog
            .get_event_file_ranges_after_version(cursor.get())
            .await?;

        for range in ranges {
            let remaining = limit - events.len();
            if remaining == 0 {
                break;
            }
            let mut partition_events = self
                .query_partition_workflow_events_after_version(
                    &range.path,
                    workflow_started_by_event_id,
                    cursor.get(),
                    remaining,
                )
                .await?;
            events.append(&mut partition_events);
        }

        events.sort_by_key(|event| event.version);
        events.truncate(limit);
        Ok(events)
    }

    pub async fn current_version(&self) -> Result<EventStreamVersion> {
        let active = self.inner.active.read().await;
        let conn = self.inner.pool.get_connection(&active.name).await?;
        let head = Self::read_event_file_append_head(&conn).await?;
        if head.current_version == 0 {
            Ok(EventStreamVersion::start())
        } else {
            EventStreamVersion::new(head.current_version)
        }
    }

    pub async fn maybe_rotate(&self) -> Result<()> {
        let active = self.inner.active.read().await;
        let file_path = self.inner.root.join(&active.name);
        let file_size = tokio::fs::metadata(&file_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        if !should_rotate(
            &active.name,
            &self.inner.rotation,
            file_size,
            active.start_ms,
        )? {
            return Ok(());
        }

        let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        drop(active);
        self.rotate_partition(now_ms).await
    }

    pub async fn pool_stats(&self) -> crate::pool::PoolStats {
        self.inner.pool.stats().await
    }

    async fn rotate_until_current_window(&self, now_ms: i64) -> Result<()> {
        loop {
            let active = self.inner.active.read().await;
            let floored = floor_to_window_ms(now_ms, self.inner.rotation.window());
            if floored == active.start_ms {
                return Ok(());
            }
            drop(active);
            self.rotate_partition(now_ms).await?;
        }
    }

    async fn rotate_partition(&self, now_ms: i64) -> Result<()> {
        let mut active = self.inner.active.write().await;
        let conn = self.inner.pool.get_connection(&active.name).await?;
        let head = Self::read_event_file_append_head(&conn).await?;
        self.inner
            .catalog
            .seal_event_file_range(&active.name, head.current_version)
            .await?;

        let (new_name, new_start_ms, new_suffix) =
            self.determine_new_partition_info(active.start_ms, active.suffix, now_ms)?;
        let new_db =
            Self::create_partition_db(&self.inner.root, &new_name, head.current_version, None)
                .await?;
        self.inner
            .catalog
            .create_event_file_range(&EventFileRange {
                name: new_name.clone(),
                path: new_name.clone(),
                first_version: head.current_version + 1,
                last_version: None,
                sealed: false,
            })
            .await?;

        active.db = new_db;
        active.name = new_name;
        active.start_ms = new_start_ms;
        active.suffix = new_suffix;
        Ok(())
    }

    fn determine_new_partition_info(
        &self,
        current_start_ms: i64,
        current_suffix: Option<char>,
        now_ms: i64,
    ) -> Result<(String, i64, Option<char>)> {
        let new_start_ms = floor_to_window_ms(now_ms, self.inner.rotation.window());

        if new_start_ms == current_start_ms {
            let suffix = get_next_suffix(current_suffix).ok_or_else(|| {
                EsError::Migration("Exhausted suffixes for current time window".to_string())
            })?;
            let new_name =
                generate_partition_name(new_start_ms, self.inner.rotation.window(), Some(suffix))?;
            return Ok((new_name, new_start_ms, Some(suffix)));
        }

        let new_name = generate_partition_name(new_start_ms, self.inner.rotation.window(), None)?;
        Ok((new_name, new_start_ms, None))
    }

    fn validate_expected_version_against(
        expected: &ExpectedVersion,
        current_version: i64,
    ) -> Result<i64> {
        match expected {
            ExpectedVersion::NoStream if current_version != 0 => {
                Err(EsError::IncorrectEventVersion {
                    expected: 0,
                    actual: current_version,
                })
            }
            ExpectedVersion::Exact(version) if version.is_start() => Err(EsError::InvalidVersion(
                "ExpectedVersion::Exact cannot use EventStreamVersion::start()".to_string(),
            )),
            ExpectedVersion::Exact(version) if version.get() != current_version => {
                Err(EsError::IncorrectEventVersion {
                    expected: version.get(),
                    actual: current_version,
                })
            }
            ExpectedVersion::Any | ExpectedVersion::NoStream | ExpectedVersion::Exact(_) => {
                Ok(current_version)
            }
        }
    }

    async fn initialize_event_file_append_head(
        conn: &turso::Connection,
        current_version: i64,
        last_event_id: Option<uuid::Uuid>,
    ) -> Result<()> {
        conn.execute(
            r#"
            INSERT INTO event_file_append_head (id, current_version, last_event_id)
            VALUES (1, ?1, ?2)
            ON CONFLICT(id) DO UPDATE SET
                current_version = excluded.current_version,
                last_event_id = excluded.last_event_id
            "#,
            (
                current_version,
                turso::Value::from(last_event_id.map(|id| id.to_string()).as_deref()),
            ),
        )
        .await?;
        Ok(())
    }

    async fn read_event_file_append_head(conn: &turso::Connection) -> Result<EventFileAppendHead> {
        let mut rows = conn
            .query(
                "SELECT current_version, last_event_id FROM event_file_append_head WHERE id = 1",
                (),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Err(EsError::Migration(
                "event_file_append_head is missing its singleton row".to_string(),
            ));
        };

        let _last_event_id = optional_text(&row, 1)?
            .map(|s| uuid::Uuid::parse_str(&s))
            .transpose()?;

        Ok(EventFileAppendHead {
            current_version: get_integer_safe(&row, 0)?,
        })
    }

    async fn read_first_event_version(conn: &turso::Connection) -> Result<Option<i64>> {
        let mut rows = conn.query("SELECT MIN(version) FROM events", ()).await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(row.get_value(0)?.as_integer().copied())
    }

    async fn update_event_file_append_head(
        conn: &turso::Connection,
        current_version: i64,
        last_event_id: Option<uuid::Uuid>,
    ) -> Result<()> {
        conn.execute(
            r#"
            UPDATE event_file_append_head
            SET current_version = ?1,
                last_event_id = ?2
            WHERE id = 1 AND ?1 >= current_version
            "#,
            (
                current_version,
                turso::Value::from(last_event_id.map(|id| id.to_string()).as_deref()),
            ),
        )
        .await?;
        Ok(())
    }

    async fn insert_events(
        &self,
        conn: &turso::Connection,
        events: Vec<NewEvent>,
        current_version: i64,
        base_time: time::OffsetDateTime,
    ) -> Result<Vec<EventEnvelope>> {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let (trace_id, span_id) = {
            let span = tracing::Span::current();
            let context = span.context();
            let otel_span = context.span();
            let span_context = otel_span.span_context();
            let trace_id = if span_context.trace_id() == opentelemetry::trace::TraceId::INVALID {
                None
            } else {
                Some(span_context.trace_id().to_string())
            };
            let span_id = if span_context.span_id() == opentelemetry::trace::SpanId::INVALID {
                None
            } else {
                Some(span_context.span_id().to_string())
            };
            (trace_id, span_id)
        };

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
            let version = EventStreamVersion::new(current_version + 1 + i as i64)?;
            let sequence = base_sequence + 1 + i as i64;
            let workflow_started_by_event_id = match event.workflow {
                WorkflowRef::None => None,
                WorkflowRef::StartsThisWorkflow => Some(id),
                WorkflowRef::Continues {
                    started_by_event_id,
                } => Some(started_by_event_id),
            };

            let envelope = EventEnvelope {
                id,
                r#type: event.r#type,
                payload: event.payload,
                version,
                created_at: base_time,
                sequence,
                workflow_kind: event.workflow_kind,
                workflow_started_by_event_id,
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
                request_id: event.request_id,
                actor_id: event.actor_id,
                actor_type: event.actor_type,
            };

            conn.execute(
                r#"
                INSERT INTO events
                    (id, type, payload, version, created_at, sequence, workflow_kind,
                     workflow_started_by_event_id, trace_id, span_id, request_id, actor_id, actor_type)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                "#,
                (
                    envelope.id.to_string(),
                    envelope.r#type.clone(),
                    serde_json::to_string(&envelope.payload)?,
                    envelope.version.get(),
                    (envelope.created_at.unix_timestamp_nanos() / 1_000_000) as i64,
                    envelope.sequence,
                    turso::Value::from(envelope.workflow_kind.as_deref()),
                    turso::Value::from(
                        envelope
                            .workflow_started_by_event_id
                            .as_ref()
                            .map(uuid::Uuid::to_string)
                            .as_deref(),
                    ),
                    turso::Value::from(envelope.trace_id.as_deref()),
                    turso::Value::from(envelope.span_id.as_deref()),
                    turso::Value::from(envelope.request_id.as_deref()),
                    envelope.actor_id.as_str(),
                    envelope.actor_type.as_str(),
                ),
            )
            .await?;

            result_events.push(envelope);
        }

        Ok(result_events)
    }

    async fn map_uniqueness_to_incorrect_event_version(
        &self,
        err: EsError,
        stale_version: i64,
    ) -> EsError {
        if let EsError::Db(ref db_err) = err {
            let msg = db_err.to_string();
            if msg.contains("UNIQUE constraint failed") && msg.contains("version") {
                return EsError::IncorrectEventVersion {
                    expected: stale_version,
                    actual: stale_version,
                };
            }
        }
        err
    }

    fn validate_events(&self, events: &[NewEvent]) -> Result<()> {
        for event in events {
            let payload_str = serde_json::to_string(&event.payload)?;
            if payload_str.len() > self.inner.max_payload_bytes {
                return Err(EsError::PayloadTooLarge {
                    size: payload_str.len(),
                    max: self.inner.max_payload_bytes,
                });
            }

            match (&event.workflow, &event.workflow_kind) {
                (WorkflowRef::None, None) => {}
                (WorkflowRef::None, Some(_)) => {
                    return Err(EsError::InvalidWorkflowMetadata(
                        "workflow_kind must be None when workflow is None".to_string(),
                    ));
                }
                (WorkflowRef::StartsThisWorkflow | WorkflowRef::Continues { .. }, Some(kind)) => {
                    validate_safe_label("workflow_kind", kind)?;
                }
                (WorkflowRef::StartsThisWorkflow | WorkflowRef::Continues { .. }, None) => {
                    return Err(EsError::InvalidWorkflowMetadata(
                        "workflow_kind is required when workflow is set".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_read_limit(&self, limit: usize) -> Result<()> {
        if limit == 0 || limit > MAX_READ_LIMIT {
            return Err(EsError::InvalidReadLimit {
                limit,
                max: MAX_READ_LIMIT,
            });
        }
        Ok(())
    }

    async fn query_partition_events_after_version(
        &self,
        db_path: &str,
        cursor: i64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let conn = self.inner.pool.get_connection(db_path).await?;
        let mut rows = conn
            .query(
                r#"
                SELECT id, type, payload, version, created_at, sequence, workflow_kind,
                       workflow_started_by_event_id, trace_id, span_id, request_id, actor_id, actor_type
                FROM events
                WHERE version > ?1
                ORDER BY version
                LIMIT ?2
                "#,
                (cursor, limit as i64),
            )
            .await?;
        self.collect_events_from_rows(&mut rows).await
    }

    async fn query_partition_workflow_events_after_version(
        &self,
        db_path: &str,
        workflow_started_by_event_id: uuid::Uuid,
        cursor: i64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let conn = self.inner.pool.get_connection(db_path).await?;
        let mut rows = conn
            .query(
                r#"
                SELECT id, type, payload, version, created_at, sequence, workflow_kind,
                       workflow_started_by_event_id, trace_id, span_id, request_id, actor_id, actor_type
                FROM events
                WHERE workflow_started_by_event_id = ?1 AND version > ?2
                ORDER BY version
                LIMIT ?3
                "#,
                (
                    workflow_started_by_event_id.to_string(),
                    cursor,
                    limit as i64,
                ),
            )
            .await?;
        self.collect_events_from_rows(&mut rows).await
    }

    async fn collect_events_from_rows(&self, rows: &mut turso::Rows) -> Result<Vec<EventEnvelope>> {
        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            events.push(Self::event_from_row(&row)?);
        }
        Ok(events)
    }

    fn event_from_row(row: &turso::Row) -> Result<EventEnvelope> {
        let id = uuid::Uuid::parse_str(&get_text_safe(row, 0)?)?;
        let payload = serde_json::from_str(&get_text_safe(row, 2)?)?;
        let created_at_ms = get_integer_safe(row, 4)?;
        let workflow_started_by_event_id = optional_text(row, 7)?
            .map(|value| uuid::Uuid::parse_str(&value))
            .transpose()?;
        let actor_type_str = get_text_safe(row, 12)?;
        let actor_type = actor_type_str.parse::<ActorType>().map_err(|e| {
            EsError::Migration(format!("Invalid actor_type '{}': {}", actor_type_str, e))
        })?;

        Ok(EventEnvelope {
            id,
            r#type: get_text_safe(row, 1)?,
            payload,
            version: EventStreamVersion::new(get_integer_safe(row, 3)?)?,
            created_at: time::OffsetDateTime::from_unix_timestamp_nanos(
                created_at_ms as i128 * 1_000_000,
            )?,
            sequence: get_integer_safe(row, 5)?,
            workflow_kind: optional_text(row, 6)?,
            workflow_started_by_event_id,
            trace_id: optional_text(row, 8)?,
            span_id: optional_text(row, 9)?,
            request_id: optional_text(row, 10)?,
            actor_id: get_text_safe(row, 11)?,
            actor_type,
        })
    }
}

fn optional_text(row: &turso::Row, index: usize) -> Result<Option<String>> {
    let value = row.get_value(index)?;
    Ok(value.as_text().map(|s| s.to_string()))
}

pub(crate) fn validate_safe_label(field: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        return Err(EsError::InvalidSafeName {
            field: field.to_string(),
            value: value.to_string(),
        });
    }

    let valid = value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');

    if !valid {
        return Err(EsError::InvalidSafeName {
            field: field.to_string(),
            value: value.to_string(),
        });
    }

    Ok(())
}
