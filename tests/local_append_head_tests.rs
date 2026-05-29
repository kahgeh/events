use events::{
    ActorType, EsError, EventNamespaces, EventStreamVersion, ExpectedVersion, NewEvent,
    RotationPolicy, WorkflowRef,
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

fn rotation_policy(max_bytes: Option<u64>) -> RotationPolicy {
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes,
    }
}

fn event(event_type: &str) -> NewEvent {
    NewEvent {
        r#type: event_type.to_string(),
        payload: json!({ "event_type": event_type }),
        workflow_kind: None,
        workflow: WorkflowRef::None,
        request_id: None,
        actor_id: "test-system".to_string(),
        actor_type: ActorType::System,
    }
}

async fn open_db(path: &Path) -> turso::Result<turso::Connection> {
    let db = turso::Builder::new_local(path.to_str().expect("test path is valid UTF-8"))
        .build()
        .await?;
    db.connect()
}

async fn query_i64(conn: &turso::Connection, sql: &str) -> turso::Result<i64> {
    let mut rows = conn.query(sql, ()).await?;
    let row = rows.next().await?.expect("query should return one row");
    Ok(*row
        .get_value(0)?
        .as_integer()
        .expect("query should return an integer"))
}

async fn query_optional_i64(conn: &turso::Connection, sql: &str) -> turso::Result<Option<i64>> {
    let mut rows = conn.query(sql, ()).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(row.get_value(0)?.as_integer().copied())
}

async fn query_text(conn: &turso::Connection, sql: &str) -> turso::Result<String> {
    let mut rows = conn.query(sql, ()).await?;
    let row = rows.next().await?.expect("query should return one row");
    Ok(row
        .get_value(0)?
        .as_text()
        .expect("query should return text")
        .to_string())
}

async fn active_event_file_path(partition_path: &Path) -> turso::Result<PathBuf> {
    let catalog = open_db(&partition_path.join("catalog.db")).await?;
    let active_path = query_text(
        &catalog,
        "SELECT path FROM event_file_ranges WHERE sealed = 0 ORDER BY first_version DESC, name DESC LIMIT 1",
    )
    .await?;
    Ok(partition_path.join(active_path))
}

async fn stale_old_catalog_head(partition_path: &Path, version: i64) -> turso::Result<()> {
    let catalog = open_db(&partition_path.join("catalog.db")).await?;
    let result = catalog
        .execute(
            "UPDATE event_stream_head SET current_version = ?1 WHERE id = 1",
            (version,),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) if err.to_string().contains("no such table") => Ok(()),
        Err(err) => Err(err),
    }
}

async fn delete_catalog_ranges(partition_path: &Path) -> turso::Result<()> {
    let catalog = open_db(&partition_path.join("catalog.db")).await?;
    catalog.execute("DELETE FROM event_file_ranges", ()).await?;
    Ok(())
}

async fn mark_all_catalog_ranges_unsealed(partition_path: &Path) -> turso::Result<()> {
    let catalog = open_db(&partition_path.join("catalog.db")).await?;
    catalog
        .execute(
            "UPDATE event_file_ranges SET sealed = 0, last_version = NULL",
            (),
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn append_writes_event_file_local_head() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    let result = stream
        .append(ExpectedVersion::NoStream, [event("UserCreated")])
        .await?;

    let active_path = active_event_file_path(partition.descriptor().path.as_path()).await?;
    let active = open_db(&active_path).await?;

    assert_eq!(
        query_i64(
            &active,
            "SELECT current_version FROM event_file_append_head WHERE id = 1"
        )
        .await?,
        1
    );
    assert_eq!(
        query_text(
            &active,
            "SELECT last_event_id FROM event_file_append_head WHERE id = 1"
        )
        .await?,
        result.events[0].id.to_string()
    );
    Ok(())
}

#[tokio::test]
async fn stale_catalog_head_does_not_control_current_version_or_append() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    let first = stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    assert_eq!(first.last_version, EventStreamVersion::new(1)?);

    stale_old_catalog_head(partition.descriptor().path.as_path(), 0).await?;

    let reopened_namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let reopened_users = reopened_namespaces.ensure_namespace("users").await?;
    let reopened = reopened_users
        .ensure_partition("user-123")
        .await?
        .open()
        .await?;

    assert_eq!(
        reopened.current_version().await?,
        EventStreamVersion::new(1)?
    );

    let second = reopened
        .append(
            ExpectedVersion::Exact(EventStreamVersion::new(1)?),
            [event("Second")],
        )
        .await?;
    assert_eq!(second.last_version, EventStreamVersion::new(2)?);

    let events = reopened
        .load_after_version(EventStreamVersion::start(), 100)
        .await?;
    assert_eq!(
        events
            .iter()
            .map(|event| event.r#type.as_str())
            .collect::<Vec<_>>(),
        vec!["First", "Second"]
    );
    Ok(())
}

#[tokio::test]
async fn rotation_initializes_next_event_file_local_head() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    let first = stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    stream.maybe_rotate().await?;

    let active_path = active_event_file_path(partition.descriptor().path.as_path()).await?;
    let active = open_db(&active_path).await?;
    assert_eq!(
        query_i64(
            &active,
            "SELECT current_version FROM event_file_append_head WHERE id = 1"
        )
        .await?,
        first.last_version.get()
    );

    let second = stream
        .append(
            ExpectedVersion::Exact(first.last_version),
            [event("Second")],
        )
        .await?;
    assert_eq!(second.last_version, EventStreamVersion::new(2)?);
    Ok(())
}

#[tokio::test]
async fn catalog_schema_does_not_keep_event_stream_head() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let _stream = partition.open().await?;

    let catalog = open_db(&partition.descriptor().path.join("catalog.db")).await?;
    assert_eq!(
        query_optional_i64(
            &catalog,
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'event_stream_head'"
        )
        .await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn missing_catalog_ranges_are_rebuilt_without_reusing_versions() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    let first = stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    assert_eq!(first.last_version, EventStreamVersion::new(1)?);

    delete_catalog_ranges(partition.descriptor().path.as_path()).await?;

    let reopened_namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let reopened_users = reopened_namespaces.ensure_namespace("users").await?;
    let reopened = reopened_users
        .ensure_partition("user-123")
        .await?
        .open()
        .await?;

    assert_eq!(
        reopened.current_version().await?,
        EventStreamVersion::new(1)?
    );

    let second = reopened
        .append(
            ExpectedVersion::Exact(EventStreamVersion::new(1)?),
            [event("Second")],
        )
        .await?;
    assert_eq!(second.last_version, EventStreamVersion::new(2)?);

    let events = reopened
        .load_after_version(EventStreamVersion::start(), 100)
        .await?;
    assert_eq!(
        events
            .iter()
            .map(|event| event.r#type.as_str())
            .collect::<Vec<_>>(),
        vec!["First", "Second"]
    );
    Ok(())
}

#[tokio::test]
async fn missing_local_head_row_prevents_partial_event_insert() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    let first = stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;

    let active_path = active_event_file_path(partition.descriptor().path.as_path()).await?;
    let active = open_db(&active_path).await?;
    active
        .execute("DELETE FROM event_file_append_head WHERE id = 1", ())
        .await?;

    let failed = stream
        .append(
            ExpectedVersion::Exact(first.last_version),
            [event("Second")],
        )
        .await;
    assert!(failed.is_err());

    assert_eq!(query_i64(&active, "SELECT COUNT(*) FROM events").await?, 1);
    assert_eq!(
        query_optional_i64(
            &active,
            "SELECT current_version FROM event_file_append_head WHERE id = 1"
        )
        .await?,
        None
    );
    Ok(())
}

#[tokio::test]
async fn multiple_unsealed_catalog_ranges_are_rebuilt_without_reusing_versions(
) -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    let first = stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    stream.maybe_rotate().await?;
    let second = stream
        .append(
            ExpectedVersion::Exact(first.last_version),
            [event("Second")],
        )
        .await?;
    assert_eq!(second.last_version, EventStreamVersion::new(2)?);

    mark_all_catalog_ranges_unsealed(partition.descriptor().path.as_path()).await?;

    let reopened_namespaces =
        EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let reopened_users = reopened_namespaces.ensure_namespace("users").await?;
    let reopened = reopened_users
        .ensure_partition("user-123")
        .await?
        .open()
        .await?;

    assert_eq!(
        reopened.current_version().await?,
        EventStreamVersion::new(2)?
    );
    let third = reopened
        .append(
            ExpectedVersion::Exact(EventStreamVersion::new(2)?),
            [event("Third")],
        )
        .await?;
    assert_eq!(third.last_version, EventStreamVersion::new(3)?);

    let events = reopened
        .load_after_version(EventStreamVersion::start(), 100)
        .await?;
    assert_eq!(
        events
            .iter()
            .map(|event| event.r#type.as_str())
            .collect::<Vec<_>>(),
        vec!["First", "Second", "Third"]
    );
    Ok(())
}
