use events::{
    ActorType, EsError, EventNamespaces, EventStreamVersion, ExpectedVersion, NewEvent,
    RotationPolicy, WorkflowRef,
};
use serde_json::json;
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

#[tokio::test]
async fn event_stream_append_and_read() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let partition = users.ensure_partition_exists("user-123").await?;
    let stream = partition.open().await?;

    let result = stream
        .append(ExpectedVersion::NoStream, [event("UserCreated")])
        .await?;

    assert_eq!(result.first_version, EventStreamVersion::new(1)?);
    assert_eq!(result.last_version, EventStreamVersion::new(1)?);
    assert_eq!(result.events[0].version, EventStreamVersion::new(1)?);

    let events = stream
        .load_after_version(EventStreamVersion::start(), 100)
        .await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].r#type, "UserCreated");

    let none = stream
        .load_after_version(EventStreamVersion::new(1)?, 100)
        .await?;
    assert!(none.is_empty());
    Ok(())
}

#[tokio::test]
async fn expected_version_semantics() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let stream = users
        .ensure_partition_exists("user-123")
        .await?
        .open()
        .await?;

    stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;

    let duplicate_create = stream
        .append(ExpectedVersion::NoStream, [event("Duplicate")])
        .await;
    assert!(matches!(
        duplicate_create,
        Err(EsError::IncorrectEventVersion { .. })
    ));

    let exact_start = stream
        .append(
            ExpectedVersion::Exact(EventStreamVersion::start()),
            [event("BadExact")],
        )
        .await;
    assert!(matches!(exact_start, Err(EsError::InvalidVersion(_))));

    let result = stream
        .append(
            ExpectedVersion::Exact(EventStreamVersion::new(1)?),
            [event("Second")],
        )
        .await?;
    assert_eq!(result.last_version, EventStreamVersion::new(2)?);

    let result = stream
        .append(ExpectedVersion::Any, [event("Third")])
        .await?;
    assert_eq!(result.last_version, EventStreamVersion::new(3)?);
    Ok(())
}

#[test]
fn event_stream_version_start_is_not_a_stored_event_version() {
    assert!(EventStreamVersion::start().is_start());
    assert!(EventStreamVersion::new(0).is_err());
}

#[tokio::test]
async fn partition_keys_are_safe_and_listing_is_shallow_sorted() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    users.ensure_partition_exists("user-2").await?;
    users.ensure_partition_exists("user-1").await?;

    tokio::fs::write(
        temp_dir.path().join("users").join("not-a-directory"),
        b"ignored",
    )
    .await?;
    tokio::fs::create_dir_all(temp_dir.path().join("users").join("User-3")).await?;
    tokio::fs::create_dir_all(temp_dir.path().join("users").join("user_4")).await?;

    assert!(users.ensure_partition_exists("User-3").await.is_err());
    assert!(users.ensure_partition_exists("user_4").await.is_err());
    assert!(users.ensure_partition_exists("user.5").await.is_err());

    let listed = users.list_partitions().await?;
    let keys: Vec<_> = listed
        .into_iter()
        .map(|descriptor| descriptor.partition_key)
        .collect();
    assert_eq!(keys, vec!["user-1", "user-2"]);
    Ok(())
}

#[tokio::test]
async fn partitions_are_isolated_event_streams() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let a = users
        .ensure_partition_exists("user-123")
        .await?
        .open()
        .await?;
    let b = users
        .ensure_partition_exists("user-456")
        .await?
        .open()
        .await?;

    a.append(ExpectedVersion::NoStream, [event("A")]).await?;
    b.append(ExpectedVersion::NoStream, [event("B")]).await?;

    assert_eq!(
        a.load_after_version(EventStreamVersion::start(), 100)
            .await?[0]
            .r#type,
        "A"
    );
    assert_eq!(
        b.load_after_version(EventStreamVersion::start(), 100)
            .await?[0]
            .r#type,
        "B"
    );
    Ok(())
}

#[tokio::test]
async fn workflow_metadata_and_filtered_reads() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let clients = namespaces.ensure_namespace("clients").await?;
    let stream = clients
        .ensure_partition_exists("client-123")
        .await?
        .open()
        .await?;

    let invalid_kind_without_workflow = NewEvent {
        workflow_kind: Some("provisioning".to_string()),
        ..event("Invalid")
    };
    let invalid = stream
        .append(ExpectedVersion::NoStream, [invalid_kind_without_workflow])
        .await;
    assert!(matches!(invalid, Err(EsError::InvalidWorkflowMetadata(_))));

    let started = stream
        .append(
            ExpectedVersion::NoStream,
            [NewEvent {
                r#type: "ProvisioningStarted".to_string(),
                payload: json!({}),
                workflow_kind: Some("provisioning".to_string()),
                workflow: WorkflowRef::StartsThisWorkflow,
                request_id: None,
                actor_id: "test-system".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await?;
    let starter_id = started.events[0]
        .workflow_started_by_event_id
        .expect("starter event records its own id as the anchor");
    assert_eq!(starter_id, started.events[0].id);

    stream
        .append(
            ExpectedVersion::Exact(started.last_version),
            [
                event("Unrelated"),
                NewEvent {
                    r#type: "MachineCreated".to_string(),
                    payload: json!({}),
                    workflow_kind: Some("provisioning".to_string()),
                    workflow: WorkflowRef::Continues {
                        started_by_event_id: starter_id,
                    },
                    request_id: None,
                    actor_id: "test-system".to_string(),
                    actor_type: ActorType::System,
                },
            ],
        )
        .await?;

    let workflow_events = stream
        .load_workflow_after_version(starter_id, EventStreamVersion::start(), 100)
        .await?;
    assert_eq!(workflow_events.len(), 2);
    assert_eq!(workflow_events[0].r#type, "ProvisioningStarted");
    assert_eq!(workflow_events[1].r#type, "MachineCreated");

    let after_start = stream
        .load_workflow_after_version(starter_id, started.last_version, 100)
        .await?;
    assert_eq!(after_start.len(), 1);
    assert_eq!(after_start[0].r#type, "MachineCreated");

    let unknown = stream
        .load_workflow_after_version(uuid::Uuid::new_v4(), EventStreamVersion::start(), 100)
        .await?;
    assert!(unknown.is_empty());
    Ok(())
}

#[tokio::test]
async fn workflow_kind_rejects_unsafe_labels() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let clients = namespaces.ensure_namespace("clients").await?;
    let stream = clients
        .ensure_partition_exists("client-123")
        .await?
        .open()
        .await?;

    for bad_kind in ["Provisioning", "provisioning_flow", "provisioning flow"] {
        let bad = stream
            .append(
                ExpectedVersion::NoStream,
                [NewEvent {
                    r#type: "Bad".to_string(),
                    payload: json!({}),
                    workflow_kind: Some(bad_kind.to_string()),
                    workflow: WorkflowRef::StartsThisWorkflow,
                    request_id: None,
                    actor_id: "test-system".to_string(),
                    actor_type: ActorType::System,
                }],
            )
            .await;
        assert!(matches!(bad, Err(EsError::InvalidSafeName { .. })));
    }

    Ok(())
}

#[tokio::test]
async fn read_limits_are_bounded() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(None)).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let stream = users
        .ensure_partition_exists("user-123")
        .await?
        .open()
        .await?;

    assert!(matches!(
        stream
            .load_after_version(EventStreamVersion::start(), 0)
            .await,
        Err(EsError::InvalidReadLimit { .. })
    ));
    assert!(matches!(
        stream
            .load_after_version(EventStreamVersion::start(), 10_001)
            .await,
        Err(EsError::InvalidReadLimit { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn rotated_file_traversal_is_internal() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let namespaces = EventNamespaces::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let users = namespaces.ensure_namespace("users").await?;
    let stream = users
        .ensure_partition_exists("user-123")
        .await?
        .open()
        .await?;

    let first = stream
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    stream.maybe_rotate().await?;
    stream
        .append(
            ExpectedVersion::Exact(first.last_version),
            [event("Second")],
        )
        .await?;

    let events = stream
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
