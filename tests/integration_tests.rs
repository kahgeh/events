use events::{
    ActorType, EsError, EventPartitions, ExpectedVersion, NewEvent, OwnerLogVersion,
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
async fn owner_log_append_and_read() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    let partition = partitions.ensure_exists("users", "user-123").await?;
    let store = partition.open().await?;

    let result = store
        .append(ExpectedVersion::NoStream, [event("UserCreated")])
        .await?;

    assert_eq!(result.first_version, OwnerLogVersion::new(1)?);
    assert_eq!(result.last_version, OwnerLogVersion::new(1)?);
    assert_eq!(result.events[0].version, OwnerLogVersion::new(1)?);

    let events = store
        .load_after_version(OwnerLogVersion::start(), 100)
        .await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].r#type, "UserCreated");

    let none = store
        .load_after_version(OwnerLogVersion::new(1)?, 100)
        .await?;
    assert!(none.is_empty());
    Ok(())
}

#[tokio::test]
async fn expected_version_semantics() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    let store = partitions
        .ensure_exists("users", "user-123")
        .await?
        .open()
        .await?;

    store
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;

    let duplicate_create = store
        .append(ExpectedVersion::NoStream, [event("Duplicate")])
        .await;
    assert!(matches!(duplicate_create, Err(EsError::Concurrency { .. })));

    let exact_start = store
        .append(
            ExpectedVersion::Exact(OwnerLogVersion::start()),
            [event("BadExact")],
        )
        .await;
    assert!(matches!(exact_start, Err(EsError::InvalidVersion(_))));

    let result = store
        .append(
            ExpectedVersion::Exact(OwnerLogVersion::new(1)?),
            [event("Second")],
        )
        .await?;
    assert_eq!(result.last_version, OwnerLogVersion::new(2)?);

    let result = store.append(ExpectedVersion::Any, [event("Third")]).await?;
    assert_eq!(result.last_version, OwnerLogVersion::new(3)?);
    Ok(())
}

#[test]
fn owner_log_version_start_is_not_a_stored_event_version() {
    assert!(OwnerLogVersion::start().is_start());
    assert!(OwnerLogVersion::new(0).is_err());
}

#[tokio::test]
async fn partition_keys_are_safe_and_listing_is_shallow_sorted() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    partitions.ensure_exists("users", "user-2").await?;
    partitions.ensure_exists("users", "user-1").await?;

    tokio::fs::write(
        temp_dir.path().join("users").join("not-a-directory"),
        b"ignored",
    )
    .await?;
    tokio::fs::create_dir_all(temp_dir.path().join("users").join("User-3")).await?;
    tokio::fs::create_dir_all(temp_dir.path().join("users").join("user_4")).await?;

    assert!(partitions.ensure_exists("users", "User-3").await.is_err());
    assert!(partitions.ensure_exists("users", "user_4").await.is_err());
    assert!(partitions.ensure_exists("users", "user.5").await.is_err());

    let listed = partitions.list("users").await?;
    let keys: Vec<_> = listed
        .into_iter()
        .map(|descriptor| descriptor.partition_key)
        .collect();
    assert_eq!(keys, vec!["user-1", "user-2"]);
    Ok(())
}

#[tokio::test]
async fn partitions_are_isolated_owner_logs() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    let a = partitions
        .ensure_exists("users", "user-123")
        .await?
        .open()
        .await?;
    let b = partitions
        .ensure_exists("users", "user-456")
        .await?
        .open()
        .await?;

    a.append(ExpectedVersion::NoStream, [event("A")]).await?;
    b.append(ExpectedVersion::NoStream, [event("B")]).await?;

    assert_eq!(
        a.load_after_version(OwnerLogVersion::start(), 100).await?[0].r#type,
        "A"
    );
    assert_eq!(
        b.load_after_version(OwnerLogVersion::start(), 100).await?[0].r#type,
        "B"
    );
    Ok(())
}

#[tokio::test]
async fn workflow_metadata_and_filtered_reads() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    let store = partitions
        .ensure_exists("clients", "client-123")
        .await?
        .open()
        .await?;

    let invalid_kind_without_workflow = NewEvent {
        workflow_kind: Some("provisioning".to_string()),
        ..event("Invalid")
    };
    let invalid = store
        .append(ExpectedVersion::NoStream, [invalid_kind_without_workflow])
        .await;
    assert!(matches!(invalid, Err(EsError::InvalidWorkflowMetadata(_))));

    let started = store
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
        .expect("starter event stores its own id as the anchor");
    assert_eq!(starter_id, started.events[0].id);

    store
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

    let workflow_events = store
        .load_workflow_after_version(starter_id, OwnerLogVersion::start(), 100)
        .await?;
    assert_eq!(workflow_events.len(), 2);
    assert_eq!(workflow_events[0].r#type, "ProvisioningStarted");
    assert_eq!(workflow_events[1].r#type, "MachineCreated");

    let after_start = store
        .load_workflow_after_version(starter_id, started.last_version, 100)
        .await?;
    assert_eq!(after_start.len(), 1);
    assert_eq!(after_start[0].r#type, "MachineCreated");

    let unknown = store
        .load_workflow_after_version(uuid::Uuid::new_v4(), OwnerLogVersion::start(), 100)
        .await?;
    assert!(unknown.is_empty());
    Ok(())
}

#[tokio::test]
async fn workflow_kind_rejects_unsafe_labels() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    let store = partitions
        .ensure_exists("clients", "client-123")
        .await?
        .open()
        .await?;

    for bad_kind in ["Provisioning", "provisioning_flow", "provisioning flow"] {
        let bad = store
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
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(None)).await?;
    let store = partitions
        .ensure_exists("users", "user-123")
        .await?
        .open()
        .await?;

    assert!(matches!(
        store.load_after_version(OwnerLogVersion::start(), 0).await,
        Err(EsError::InvalidReadLimit { .. })
    ));
    assert!(matches!(
        store
            .load_after_version(OwnerLogVersion::start(), 10_001)
            .await,
        Err(EsError::InvalidReadLimit { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn rotated_file_traversal_is_internal() -> Result<(), EsError> {
    let temp_dir = TempDir::new()?;
    let partitions = EventPartitions::open(temp_dir.path(), rotation_policy(Some(1))).await?;
    let store = partitions
        .ensure_exists("users", "user-123")
        .await?
        .open()
        .await?;

    let first = store
        .append(ExpectedVersion::NoStream, [event("First")])
        .await?;
    store.maybe_rotate().await?;
    store
        .append(
            ExpectedVersion::Exact(first.last_version),
            [event("Second")],
        )
        .await?;

    let events = store
        .load_after_version(OwnerLogVersion::start(), 100)
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
