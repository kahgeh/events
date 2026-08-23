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

async fn rename_catalog_range(
    partition_path: &Path,
    old_name: &str,
    new_name: &str,
) -> turso::Result<()> {
    let catalog = open_db(&partition_path.join("catalog.db")).await?;
    catalog
        .execute(
            "UPDATE event_file_ranges SET name = ?1, path = ?1 WHERE name = ?2",
            (new_name, old_name),
        )
        .await?;
    Ok(())
}

async fn rename_event_file(
    partition_path: &Path,
    old_name: &str,
    new_name: &str,
) -> std::io::Result<()> {
    tokio::fs::rename(partition_path.join(old_name), partition_path.join(new_name)).await?;
    for suffix in ["-wal", "-shm"] {
        let old_sidecar = partition_path.join(format!("{old_name}{suffix}"));
        if tokio::fs::try_exists(&old_sidecar).await? {
            tokio::fs::rename(
                old_sidecar,
                partition_path.join(format!("{new_name}{suffix}")),
            )
            .await?;
        }
    }
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

#[tokio::test]
async fn sealed_alpha_catalog_range_is_rejected() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    stream.maybe_rotate().await?;

    let catalog = open_db(&partition.descriptor().path.join("catalog.db")).await?;
    catalog
        .execute(
            "UPDATE event_file_ranges SET name = 'events_20241002_a.db' WHERE name = (SELECT name FROM event_file_ranges WHERE sealed = 1 ORDER BY first_version LIMIT 1)",
            (),
        )
        .await?;

    let reopened_namespaces =
        EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let reopened = reopened_namespaces
        .ensure_namespace("users")
        .await?
        .ensure_partition("user-123")
        .await?
        .open()
        .await;
    assert!(matches!(reopened, Err(EsError::InvalidPartition(_))));
    Ok(())
}

#[tokio::test]
async fn alpha_event_file_candidate_is_rejected_during_reconstruction() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    tokio::fs::write(
        partition.descriptor().path.join("events_20241002_a.db"),
        b"legacy alpha candidate",
    )
    .await?;
    delete_catalog_ranges(partition.descriptor().path.as_path()).await?;

    let reopened_namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let reopened = reopened_namespaces
        .ensure_namespace("users")
        .await?
        .ensure_partition("user-123")
        .await?
        .open()
        .await;
    assert!(matches!(reopened, Err(EsError::InvalidPartition(_))));
    Ok(())
}

#[tokio::test]
async fn ordinal_exhaustion_keeps_active_catalog_range_unsealed() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;
    stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;

    let partition_path = partition.descriptor().path.clone();
    let active_path = active_event_file_path(&partition_path).await?;
    let active_name = active_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("test event-file name is valid UTF-8")
        .to_string();
    let active_stem = active_name
        .strip_suffix(".db")
        .expect("event-file name ends in .db");
    let base_stem = match active_stem.rsplit_once('_') {
        Some((base, ordinal))
            if ordinal.len() == 6 && ordinal.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            base
        }
        _ => active_stem,
    };
    let exhausted_name = format!("{base_stem}_999999.db");

    drop(stream);
    drop(partition);
    drop(users);
    drop(namespaces);
    rename_event_file(&partition_path, &active_name, &exhausted_name).await?;
    rename_catalog_range(&partition_path, &active_name, &exhausted_name).await?;

    let reopened_namespaces =
        EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let reopened_partition = reopened_namespaces
        .ensure_namespace("users")
        .await?
        .ensure_partition("user-123")
        .await?;
    let reopened = reopened_partition.open().await?;

    assert!(matches!(
        reopened.maybe_rotate().await,
        Err(EsError::RotationOrdinalExhausted { max: 999_999 })
    ));

    let catalog = open_db(&partition_path.join("catalog.db")).await?;
    assert_eq!(
        query_i64(
            &catalog,
            "SELECT sealed FROM event_file_ranges WHERE name = 'events_20241002_999999.db' OR name LIKE '%_999999.db'"
        )
        .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn numeric_rotation_reopens_after_old_alpha_limit() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;

    for _ in 0..27 {
        stream.maybe_rotate().await?;
    }
    let active_path = active_event_file_path(partition.descriptor().path.as_path()).await?;
    assert!(active_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with("_000027.db")));

    let appended = stream
        .append(ExpectedVersion::NoStream, [event("AfterTwentySix")])
        .await?;
    assert_eq!(appended.last_version, EventStreamVersion::new(1)?);

    drop(stream);
    drop(partition);
    drop(users);
    drop(namespaces);
    let reopened_namespaces =
        EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let reopened = reopened_namespaces
        .ensure_namespace("users")
        .await?
        .ensure_partition("user-123")
        .await?
        .open()
        .await?;
    assert_eq!(
        reopened.current_version().await?,
        EventStreamVersion::new(1)?
    );
    Ok(())
}

#[tokio::test]
async fn exhausted_ordinal_rolls_to_new_time_window_base_file() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition("user-123").await?;
    let stream = partition.open().await?;
    stream
        .append(ExpectedVersion::NoStream, [event("BeforeRollover")])
        .await?;
    let partition_path = partition.descriptor().path.clone();
    let active_path = active_event_file_path(&partition_path).await?;
    let active_name = active_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("test event-file name is valid UTF-8")
        .to_string();
    let old_window_name = "events_20000101T00_999999.db";

    drop(stream);
    drop(partition);
    drop(users);
    drop(namespaces);
    rename_event_file(&partition_path, &active_name, old_window_name).await?;
    rename_catalog_range(&partition_path, &active_name, old_window_name).await?;

    let reopened_namespaces =
        EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let reopened_partition = reopened_namespaces
        .ensure_namespace("users")
        .await?
        .ensure_partition("user-123")
        .await?;
    let reopened = reopened_partition.open().await?;
    reopened.maybe_rotate().await?;

    let new_active = active_event_file_path(&partition_path).await?;
    let new_active_name = new_active
        .file_name()
        .and_then(|name| name.to_str())
        .expect("test event-file name is valid UTF-8");
    assert!(!new_active_name
        .strip_prefix("events_")
        .expect("event-file name has events_ prefix")
        .contains('_'));
    Ok(())
}
