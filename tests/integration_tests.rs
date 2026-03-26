use events::{
    bootstrap_cursor, create_broadcast_system, migration, ActorType, EsError, EventEnvelope,
    EventStore, ExpectedVersion, NewEvent, PartitionedCursor, Projector, ProjectorHandler,
    ProjectorHandlerError, RotationPolicy, StreamEventSender,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_basic_append_and_load() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "test-stream-1";

    // Append events to empty stream
    let result = store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![
                NewEvent {
                    r#type: "TestEvent1".into(),
                    payload: json!({"data": "test1"}),
                    request_id: None,
                    actor_id: "test:integration".to_string(),
                    actor_type: ActorType::System,
                },
                NewEvent {
                    r#type: "TestEvent2".into(),
                    payload: json!({"data": "test2"}),
                    request_id: None,
                    actor_id: "test:integration".to_string(),
                    actor_type: ActorType::System,
                },
            ],
        )
        .await?;

    assert_eq!(result.version, 2);
    assert_eq!(result.events.len(), 2);

    // Load events
    let events = store.load(stream_id).await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].r#type, "TestEvent1");
    assert_eq!(events[1].r#type, "TestEvent2");

    Ok(())
}

#[tokio::test]
async fn test_optimistic_concurrency_control() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "test-stream-2";

    // First append
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![NewEvent {
                r#type: "TestEvent1".into(),
                payload: json!({"data": "test1"}),
                request_id: None,
                actor_id: "test:integration".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await?;

    // Try to append with wrong expected version
    let result = store
        .append(
            stream_id,
            ExpectedVersion::NoStream, // Wrong - stream now has version 1
            vec![NewEvent {
                r#type: "TestEvent2".into(),
                payload: json!({"data": "test2"}),
                request_id: None,
                actor_id: "test:integration".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await;

    match result {
        Err(EsError::Concurrency {
            expected, actual, ..
        }) => {
            assert_eq!(expected, -1);
            assert_eq!(actual, 1);
        }
        _ => panic!("Expected concurrency error"),
    }

    // Correct version
    let result = store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![NewEvent {
                r#type: "TestEvent2".into(),
                payload: json!({"data": "test2"}),
                request_id: None,
                actor_id: "test:integration".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await?;

    assert_eq!(result.version, 2);

    Ok(())
}

#[tokio::test]
async fn test_time_based_rotation() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    // Use very short time window for testing
    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_millis(100), // Very short window
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "test-stream-3";

    // Append events
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![NewEvent {
                r#type: "TestEvent1".into(),
                payload: json!({"data": "test1"}),
                request_id: None,
                actor_id: "test:integration".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await?;

    // Wait for rotation window
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Trigger rotation
    store.maybe_rotate().await?;

    // Append more events - should go to new partition
    store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![NewEvent {
                r#type: "TestEvent2".into(),
                payload: json!({"data": "test2"}),
                request_id: None,
                actor_id: "test:integration".to_string(),
                actor_type: ActorType::System,
            }],
        )
        .await?;

    // Load events should return all events from both partitions
    let events = store.load(stream_id).await?;
    assert_eq!(events.len(), 2);

    Ok(())
}

#[tokio::test]
async fn test_bootstrap_cursor() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let consumer = "test-consumer";

    // Bootstrap cursor for new consumer
    let cursor = bootstrap_cursor(&store, consumer).await?;

    // Should start at earliest partition
    assert!(!cursor.partition.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_all_since_pagination() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "test-stream-4";

    // Append multiple events
    for i in 1..=5 {
        store
            .append(
                stream_id,
                ExpectedVersion::Any,
                vec![NewEvent {
                    r#type: format!("TestEvent{}", i),
                    payload: json!({"index": i}),
                    request_id: None,
                    actor_id: "test:integration".to_string(),
                    actor_type: ActorType::System,
                }],
            )
            .await?;

        // Small delay to prevent database lock contention
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Allow some time for all operations to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start from beginning
    let cursor = bootstrap_cursor(&store, "test-reader").await?;

    // Get first batch
    let (events1, cursor1) = store.all_since(cursor, 2).await?;
    assert_eq!(events1.len(), 2);
    assert_eq!(events1[0].r#type, "TestEvent1");
    assert_eq!(events1[1].r#type, "TestEvent2");

    // Small delay between batches
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Get next batch
    let (events2, cursor2) = store.all_since(cursor1, 2).await?;
    assert_eq!(events2.len(), 2);
    assert_eq!(events2[0].r#type, "TestEvent3");
    assert_eq!(events2[1].r#type, "TestEvent4");

    // Small delay between batches
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Get final batch
    let (events3, cursor3) = store.all_since(cursor2, 10).await?;
    assert_eq!(events3.len(), 1);
    assert_eq!(events3[0].r#type, "TestEvent5");

    // Small delay before final check
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Should return no more events
    let (events4, _) = store.all_since(cursor3, 10).await?;
    assert_eq!(events4.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_partitioned_cursor() -> Result<(), EsError> {
    let partition_name = "events_20241002.db".to_string();
    let created_at = 1727881200000; // 2024-10-02 12:00:00 UTC
    let sequence = 42;

    let cursor = PartitionedCursor::new(partition_name.clone(), created_at, sequence);

    assert_eq!(cursor.partition, partition_name);
    assert_eq!(cursor.created_at_ms, created_at);
    assert_eq!(cursor.sequence, sequence);

    let (partition, time, seq) = cursor.as_tuple();
    assert_eq!(partition, partition_name.as_str());
    assert_eq!(time, created_at);
    assert_eq!(seq, sequence);

    Ok(())
}

#[tokio::test]
async fn test_actor_fields_persisted() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "test-actor-stream";

    // Append event with actor fields
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![
                NewEvent {
                    r#type: "UserEvent".into(),
                    payload: json!({"action": "create"}),
                    request_id: Some("req-123".to_string()),
                    actor_id: "user_abc123xyz".to_string(),
                    actor_type: ActorType::User,
                },
                NewEvent {
                    r#type: "SystemEvent".into(),
                    payload: json!({"action": "process"}),
                    request_id: None,
                    actor_id: "system:projector".to_string(),
                    actor_type: ActorType::System,
                },
            ],
        )
        .await?;

    // Load and verify actor fields are persisted
    let events = store.load(stream_id).await?;
    assert_eq!(events.len(), 2);

    // Verify user event
    assert_eq!(events[0].r#type, "UserEvent");
    assert_eq!(events[0].actor_id, "user_abc123xyz");
    assert_eq!(events[0].actor_type, ActorType::User);
    assert_eq!(events[0].request_id, Some("req-123".to_string()));

    // Verify system event
    assert_eq!(events[1].r#type, "SystemEvent");
    assert_eq!(events[1].actor_id, "system:projector");
    assert_eq!(events[1].actor_type, ActorType::System);
    assert_eq!(events[1].request_id, None);

    Ok(())
}

#[tokio::test]
async fn test_unique_stream_version_constraint() -> Result<(), EsError> {
    // Create a temporary partition database and run migrations
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = turso::Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let conn = db.connect()?;
    migration::partition_migrations().run(&conn).await?;

    // Insert two events with the same (stream_id, version)
    let stream_id = "dup-stream";
    let created_at_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

    // First insert should succeed
    conn.execute(
        "INSERT INTO events (id, stream_id, type, payload, version, created_at, sequence, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            uuid::Uuid::new_v4().to_string(),
            stream_id,
            "TestEvent",
            "{}",
            1i64,
            created_at_ms,
            1i64,
            "test:constraint",
            "system",
        ),
    ).await?;

    // Second insert with same (stream_id, version) should fail due to unique index
    let result = conn.execute(
        "INSERT INTO events (id, stream_id, type, payload, version, created_at, sequence, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            uuid::Uuid::new_v4().to_string(),
            stream_id,
            "TestEvent",
            "{}",
            1i64, // duplicate version for same stream
            created_at_ms + 1,
            2i64,
            "test:constraint",
            "system",
        ),
    ).await;

    assert!(
        result.is_err(),
        "Expected unique constraint violation on (stream_id, version)"
    );

    Ok(())
}

fn test_event(event_type: &str) -> NewEvent {
    NewEvent {
        r#type: event_type.into(),
        payload: json!({"test": true}),
        request_id: None,
        actor_id: "test:atomicity".to_string(),
        actor_type: ActorType::System,
    }
}

#[tokio::test]
async fn test_concurrent_writers_same_expected_version() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(
        EventStore::open_partitioned(
            temp_dir.path().to_str().unwrap(),
            RotationPolicy::TimeWindow {
                window: Duration::from_secs(3600),
                max_bytes: None,
            },
        )
        .await?,
    );

    let stream_id = "concurrent-occ-stream";

    // Seed the stream with version 1
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![test_event("SeedEvent")],
        )
        .await?;

    // Launch two concurrent writers both expecting version 1
    let store_a = Arc::clone(&store);
    let store_b = Arc::clone(&store);
    let sid = stream_id.to_string();
    let sid2 = stream_id.to_string();

    let handle_a = tokio::spawn(async move {
        store_a
            .append(&sid, ExpectedVersion::Exact(1), vec![test_event("WriterA")])
            .await
    });
    let handle_b = tokio::spawn(async move {
        store_b
            .append(
                &sid2,
                ExpectedVersion::Exact(1),
                vec![test_event("WriterB")],
            )
            .await
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let result_a = result_a.unwrap();
    let result_b = result_b.unwrap();

    // Exactly one must succeed; the other must be a Concurrency error
    let (winner, loser) = match (&result_a, &result_b) {
        (Ok(_), Err(_)) => (result_a, result_b),
        (Err(_), Ok(_)) => (result_b, result_a),
        (Ok(_), Ok(_)) => panic!("Both writers succeeded — OCC violated"),
        (Err(a), Err(b)) => panic!("Both writers failed: {a}, {b}"),
    };

    assert!(winner.is_ok());
    match loser {
        Err(EsError::Concurrency { stream_id, .. }) => {
            assert_eq!(stream_id, "concurrent-occ-stream");
        }
        other => panic!("Expected Concurrency error, got: {:?}", other),
    }

    // Stream should have exactly 2 events
    let events = store.load(stream_id).await?;
    assert_eq!(events.len(), 2);

    Ok(())
}

#[tokio::test]
async fn test_version_guard_prevents_head_regression() -> Result<(), EsError> {
    // Directly test that catalog.update_stream_head won't regress the version
    let temp_dir = TempDir::new().unwrap();
    let catalog = events::Catalog::open(temp_dir.path()).await?;

    let stream_id = "guard-test";
    let event_id_v5 = uuid::Uuid::new_v4();
    let event_id_v3 = uuid::Uuid::new_v4();

    // Set head to version 5
    let updated = catalog
        .update_stream_head(stream_id, 5, 1000, &event_id_v5, "partition_a")
        .await?;
    assert!(updated, "Initial insert should succeed");

    // Try to regress to version 3 — should be a no-op
    let updated = catalog
        .update_stream_head(stream_id, 3, 900, &event_id_v3, "partition_a")
        .await?;
    assert!(!updated, "Regression to lower version should be rejected");

    // Verify head is still at version 5
    let head = catalog.get_stream_head(stream_id).await?.unwrap();
    assert_eq!(head.version, 5);
    assert_eq!(head.last_event_id, event_id_v5);

    // Advancing to version 7 should succeed
    let event_id_v7 = uuid::Uuid::new_v4();
    let updated = catalog
        .update_stream_head(stream_id, 7, 1100, &event_id_v7, "partition_b")
        .await?;
    assert!(updated, "Advancing version should succeed");

    let head = catalog.get_stream_head(stream_id).await?.unwrap();
    assert_eq!(head.version, 7);

    Ok(())
}

#[tokio::test]
async fn test_reconcile_repairs_stale_catalog_head() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_str().unwrap();

    let store = EventStore::open_partitioned(
        root,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "reconcile-stream";

    // Append 3 events normally — catalog head is at version 3
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![
                test_event("Event1"),
                test_event("Event2"),
                test_event("Event3"),
            ],
        )
        .await?;

    assert_eq!(store.get_stream_version(stream_id).await?, 3);

    // Simulate catalog drift: directly regress stream_heads to version 1
    // by writing to the catalog DB, bypassing the version guard.
    let catalog_path = temp_dir.path().join("catalog.db");
    let catalog_db = turso::Builder::new_local(catalog_path.to_str().unwrap())
        .build()
        .await?;
    let catalog_conn = catalog_db.connect()?;
    catalog_conn
        .execute(
            "UPDATE stream_heads SET version = 1 WHERE stream_id = ?1",
            (stream_id,),
        )
        .await?;

    // Confirm the catalog is now stale
    assert_eq!(store.get_stream_version(stream_id).await?, 1);

    // Reconcile should scan partitions and repair the head to version 3
    let reconciled = store.reconcile_stream_head(stream_id).await?;
    assert_eq!(
        reconciled, 3,
        "Reconcile should find version 3 in partition and repair catalog"
    );

    // Catalog head should now be repaired
    assert_eq!(store.get_stream_version(stream_id).await?, 3);

    // Non-existent stream should reconcile to 0
    let reconciled = store.reconcile_stream_head("no-such-stream").await?;
    assert_eq!(reconciled, 0);

    Ok(())
}

#[tokio::test]
async fn test_cross_partition_rotation_preserves_occ() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    // Use very short window so rotation happens during test
    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_millis(100),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "rotation-occ-stream";

    // Append to first partition
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![test_event("BeforeRotation")],
        )
        .await?;

    // Wait for rotation window to pass
    tokio::time::sleep(Duration::from_millis(150)).await;

    // This append triggers rotation and goes to a new partition
    let result = store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![test_event("AfterRotation")],
        )
        .await?;
    assert_eq!(result.version, 2);

    // A stale writer trying version 1 after rotation must get Concurrency
    let stale_result = store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![test_event("StaleWriter")],
        )
        .await;

    match stale_result {
        Err(EsError::Concurrency {
            expected, actual, ..
        }) => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        other => panic!("Expected Concurrency error, got: {:?}", other),
    }

    // Stream should have exactly 2 events across partitions
    let events = store.load(stream_id).await?;
    assert_eq!(events.len(), 2);

    Ok(())
}

#[tokio::test]
async fn test_uniqueness_violation_returns_actual_version_from_partition() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_str().unwrap();

    let store = EventStore::open_partitioned(
        root,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    let stream_id = "uniqueness-safety-net";

    // Append version 1 normally
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![test_event("Event1")],
        )
        .await?;

    // Get the active partition name so we can insert directly
    let partition_name = store.get_active_partition_name().await?;
    let partition_path = temp_dir.path().join(&partition_name);

    // Insert version 2 directly into the partition DB, bypassing the catalog.
    // This simulates a crash where events were committed but stream_heads was not updated.
    let partition_db = turso::Builder::new_local(partition_path.to_str().unwrap())
        .build()
        .await?;
    let partition_conn = partition_db.connect()?;
    let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    partition_conn
        .execute(
            "INSERT INTO events (id, stream_id, type, payload, version, created_at, sequence, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            (
                uuid::Uuid::new_v4().to_string(),
                stream_id,
                "OrphanEvent",
                "{}",
                2i64,
                now_ms,
                2i64,
                "test:orphan",
                "system",
            ),
        )
        .await?;

    // Catalog still thinks head is at version 1 (the direct insert bypassed it).
    assert_eq!(store.get_stream_version(stream_id).await?, 1);

    // Append with ExpectedVersion::Exact(1) — the OCC check passes because catalog
    // says version 1, but the INSERT hits the UNIQUE constraint on (stream_id, version=2).
    // The safety net should map this to Concurrency with actual=2 from the partition.
    let result = store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![test_event("ConflictEvent")],
        )
        .await;

    match result {
        Err(EsError::Concurrency {
            expected,
            actual,
            stream_id: sid,
        }) => {
            assert_eq!(sid, "uniqueness-safety-net");
            assert_eq!(expected, 1, "expected should reflect the stale OCC check");
            assert_eq!(
                actual, 2,
                "actual should come from the partition, not the catalog"
            );
        }
        other => panic!(
            "Expected Concurrency error with actual version from partition, got: {:?}",
            other
        ),
    }

    Ok(())
}

#[tokio::test]
async fn test_stale_head_after_restart_with_rotation_no_duplicate_versions() -> Result<(), EsError>
{
    // Simulate: crash after partition commit but before catalog update,
    // then restart in a later time window that triggers rotation.
    // Without startup recovery, append() would read the stale head and
    // insert a duplicate version into the new partition.
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_str().unwrap();

    // Phase 1: normal append — version 1 lands in partition, catalog updated.
    {
        let store = EventStore::open_partitioned(
            root,
            RotationPolicy::TimeWindow {
                window: Duration::from_millis(100),
                max_bytes: None,
            },
        )
        .await?;

        store
            .append(
                "restart-stream",
                ExpectedVersion::NoStream,
                vec![test_event("Event1")],
            )
            .await?;

        assert_eq!(store.get_stream_version("restart-stream").await?, 1);

        // Inject version 2 directly into the partition (simulating a crash
        // where the partition commit succeeded but stream_heads was not updated).
        let partition_name = store.get_active_partition_name().await?;
        let partition_path = temp_dir.path().join(&partition_name);
        let db = turso::Builder::new_local(partition_path.to_str().unwrap())
            .build()
            .await?;
        let conn = db.connect()?;
        let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        conn.execute(
            "INSERT INTO events (id, stream_id, type, payload, version, created_at, sequence, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            (
                uuid::Uuid::new_v4().to_string(),
                "restart-stream",
                "OrphanEvent",
                "{}",
                2i64,
                now_ms,
                2i64,
                "test:crash-sim",
                "system",
            ),
        ).await?;

        // Catalog still thinks head is at version 1
        assert_eq!(store.get_stream_version("restart-stream").await?, 1);
    }

    // Wait so the next open lands in a new time window (triggers rotation).
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Phase 2: "restart" — open_partitioned runs recovery, then rotation happens.
    let store = EventStore::open_partitioned(
        root,
        RotationPolicy::TimeWindow {
            window: Duration::from_millis(100),
            max_bytes: None,
        },
    )
    .await?;

    // Recovery during open should have repaired the head to version 2.
    assert_eq!(
        store.get_stream_version("restart-stream").await?,
        2,
        "Startup recovery should have repaired the stale catalog head"
    );

    // Appending with Exact(2) should succeed — no duplicate version.
    let result = store
        .append(
            "restart-stream",
            ExpectedVersion::Exact(2),
            vec![test_event("Event3")],
        )
        .await?;
    assert_eq!(result.version, 3);

    // Appending with Exact(1) should fail — the stale version must not be accepted.
    let stale = store
        .append(
            "restart-stream",
            ExpectedVersion::Exact(1),
            vec![test_event("StaleWriter")],
        )
        .await;
    assert!(
        matches!(stale, Err(EsError::Concurrency { .. })),
        "Stale writer should get Concurrency error, got: {:?}",
        stale
    );

    // All 3 events should be loadable
    let events = store.load("restart-stream").await?;
    assert_eq!(events.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_recover_all_stale_heads_repairs_sealed_partition() -> Result<(), EsError> {
    // Verify that the admin repair path (recover_all_stale_heads) finds and
    // fixes stale heads in a sealed historical partition that startup recovery
    // (active-partition-only) would not touch.
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_str().unwrap();

    // Use max_bytes: 1 to force size-based rotation after the first append.
    let store = EventStore::open_partitioned(
        root,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(1),
        },
    )
    .await?;

    let stream_id = "sealed-repair-stream";

    // Append version 1 into the first partition.
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![test_event("Event1")],
        )
        .await?;

    let first_partition = store.get_active_partition_name().await?;

    // Force size-based rotation (partition has data, max_bytes is 1).
    store.maybe_rotate().await?;

    let second_partition = store.get_active_partition_name().await?;
    assert_ne!(
        first_partition, second_partition,
        "Should have rotated to a new partition"
    );

    // Append into the new (rotated) partition.
    store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![test_event("Event2")],
        )
        .await?;

    // Inject an orphan version 3 directly into the *sealed* first partition,
    // simulating a historical crash where the catalog was never updated.
    let first_path = temp_dir.path().join(&first_partition);
    let db = turso::Builder::new_local(first_path.to_str().unwrap())
        .build()
        .await?;
    let conn = db.connect()?;
    let now_ms = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    conn.execute(
        "INSERT INTO events (id, stream_id, type, payload, version, created_at, sequence, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        (
            uuid::Uuid::new_v4().to_string(),
            stream_id,
            "OrphanInSealed",
            "{}",
            3i64,
            now_ms,
            3i64,
            "test:sealed-orphan",
            "system",
        ),
    ).await?;

    // Catalog still thinks head is at version 2 (from the normal append).
    assert_eq!(store.get_stream_version(stream_id).await?, 2);

    // Active-partition startup recovery would NOT catch this because the orphan
    // is in the sealed first partition. The admin path should.
    store.recover_all_stale_heads().await?;

    // Head should now be repaired to version 3.
    assert_eq!(
        store.get_stream_version(stream_id).await?,
        3,
        "recover_all_stale_heads should repair head from sealed partition"
    );

    // Subsequent append should build on version 3.
    let result = store
        .append(
            stream_id,
            ExpectedVersion::Exact(3),
            vec![test_event("Event4")],
        )
        .await?;
    assert_eq!(result.version, 4);

    Ok(())
}

#[tokio::test]
async fn test_size_rotation_traversal_across_same_window_partitions() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    // max_bytes: 1 forces size-based rotation after every append
    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(1),
        },
    )
    .await?;

    let stream_id = "size-rotation-stream";

    // Append into first partition (no suffix)
    store
        .append(
            stream_id,
            ExpectedVersion::NoStream,
            vec![test_event("Event1")],
        )
        .await?;

    let first_partition = store.get_active_partition_name().await?;

    // Force rotation to _a
    store.maybe_rotate().await?;
    let second_partition = store.get_active_partition_name().await?;
    assert_ne!(first_partition, second_partition, "Should have rotated");

    store
        .append(
            stream_id,
            ExpectedVersion::Exact(1),
            vec![test_event("Event2")],
        )
        .await?;

    // Force rotation to _b
    store.maybe_rotate().await?;
    let third_partition = store.get_active_partition_name().await?;
    assert_ne!(
        second_partition, third_partition,
        "Should have rotated again"
    );

    store
        .append(
            stream_id,
            ExpectedVersion::Exact(2),
            vec![test_event("Event3")],
        )
        .await?;

    // A projector should be able to traverse all three same-window partitions
    let cursor = bootstrap_cursor(&store, "size-rotation-reader").await?;
    let (events, _) = store.all_since(cursor, 100).await?;

    assert_eq!(
        events.len(),
        3,
        "Should see all 3 events across same-window partitions"
    );
    assert_eq!(events[0].r#type, "Event1");
    assert_eq!(events[1].r#type, "Event2");
    assert_eq!(events[2].r#type, "Event3");

    Ok(())
}

#[tokio::test]
async fn test_batch_append_ordering_preserved_in_all_since() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    // Append a batch of 5 events in a single call — all get the same created_at
    store
        .append(
            "batch-stream",
            ExpectedVersion::NoStream,
            vec![
                test_event("BatchA"),
                test_event("BatchB"),
                test_event("BatchC"),
                test_event("BatchD"),
                test_event("BatchE"),
            ],
        )
        .await?;

    let cursor = bootstrap_cursor(&store, "batch-reader").await?;
    let (events, _) = store.all_since(cursor, 100).await?;

    assert_eq!(events.len(), 5);
    // Sequence-based ordering preserves original append order
    assert_eq!(events[0].r#type, "BatchA");
    assert_eq!(events[1].r#type, "BatchB");
    assert_eq!(events[2].r#type, "BatchC");
    assert_eq!(events[3].r#type, "BatchD");
    assert_eq!(events[4].r#type, "BatchE");

    // Sequences must be monotonically increasing
    for window in events.windows(2) {
        assert!(
            window[1].sequence > window[0].sequence,
            "Sequence must be monotonically increasing: {} should be > {}",
            window[1].sequence,
            window[0].sequence,
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_same_millisecond_appends_no_event_loss() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await?;

    // Append multiple batches as fast as possible (likely same millisecond)
    for i in 0..5 {
        store
            .append(
                &format!("ms-stream-{}", i),
                ExpectedVersion::NoStream,
                vec![test_event(&format!("Event{}", i))],
            )
            .await?;
    }

    // Read all events and verify none are lost
    let cursor = bootstrap_cursor(&store, "ms-reader").await?;
    let (events, next_cursor) = store.all_since(cursor, 100).await?;

    assert_eq!(events.len(), 5, "All 5 events should be visible");

    // Verify cursor resume doesn't skip anything
    let (events_after, _) = store.all_since(next_cursor, 100).await?;
    assert_eq!(events_after.len(), 0, "No duplicate events after resume");

    Ok(())
}

#[tokio::test]
async fn test_cursor_resume_after_checkpoint() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();

    let store = std::sync::Arc::new(
        EventStore::open_partitioned(
            temp_dir.path().to_str().unwrap(),
            RotationPolicy::TimeWindow {
                window: Duration::from_secs(3600),
                max_bytes: None,
            },
        )
        .await?,
    );

    // Append 3 events
    store
        .append(
            "checkpoint-stream",
            ExpectedVersion::NoStream,
            vec![
                test_event("First"),
                test_event("Second"),
                test_event("Third"),
            ],
        )
        .await?;

    let consumer = "checkpoint-consumer";

    // Read first 2 events and checkpoint
    let cursor = bootstrap_cursor(&store, consumer).await?;
    let (events, next_cursor) = store.all_since(cursor, 2).await?;
    assert_eq!(events.len(), 2);

    events::checkpoint(&store, consumer, &next_cursor, None).await?;

    // Simulate restart: bootstrap from checkpoint
    let resumed_cursor = bootstrap_cursor(&store, consumer).await?;
    assert_eq!(resumed_cursor.sequence, next_cursor.sequence);
    assert_eq!(resumed_cursor.partition, next_cursor.partition);

    // Should get the remaining event
    let (remaining, _) = store.all_since(resumed_cursor, 100).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].r#type, "Third");

    Ok(())
}

#[tokio::test]
async fn test_suffix_restored_after_restart() -> Result<(), EsError> {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_str().unwrap();

    // Phase 1: create store with size rotation, track which suffix we reach
    let last_suffix;
    {
        let store = EventStore::open_partitioned(
            root,
            RotationPolicy::TimeWindow {
                window: Duration::from_secs(3600),
                max_bytes: Some(1),
            },
        )
        .await?;

        // Append triggers maybe_rotate internally. With max_bytes=1 each
        // append after the first will rotate before inserting.
        store
            .append(
                "suffix-stream",
                ExpectedVersion::NoStream,
                vec![test_event("Event1")],
            )
            .await?;

        store
            .append(
                "suffix-stream",
                ExpectedVersion::Exact(1),
                vec![test_event("Event2")],
            )
            .await?;

        last_suffix = store.get_active_partition_name().await?;
    }

    // Phase 2: "restart" — suffix should be restored from partition name
    let store = EventStore::open_partitioned(
        root,
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: Some(1),
        },
    )
    .await?;

    // The active partition should match what we had before "crash"
    let restored = store.get_active_partition_name().await?;
    assert_eq!(
        restored, last_suffix,
        "Active partition should be restored after restart"
    );

    // Appending should trigger another size rotation beyond the restored suffix
    store
        .append(
            "suffix-stream",
            ExpectedVersion::Exact(2),
            vec![test_event("Event3")],
        )
        .await?;

    let after_append = store.get_active_partition_name().await?;
    assert!(
        after_append > last_suffix,
        "After append+rotation, partition name should advance beyond {}: got {}",
        last_suffix,
        after_append
    );

    let events = store.load("suffix-stream").await?;
    assert_eq!(events.len(), 3);

    Ok(())
}

// ---------------------------------------------------------------------------
// Projector handler-failure tests
// ---------------------------------------------------------------------------

/// A test handler that fails on events whose type matches `fail_on_type`.
/// Tracks which event types were successfully processed.
struct FailingHandler {
    fail_on_type: String,
    processed: Arc<Mutex<Vec<String>>>,
    attempts: Arc<Mutex<u32>>,
}

impl ProjectorHandler for FailingHandler {
    async fn handle_event(
        &self,
        event: &EventEnvelope,
        _sender: &StreamEventSender,
    ) -> std::result::Result<(), ProjectorHandlerError> {
        {
            let mut attempts = self.attempts.lock().await;
            *attempts += 1;
        }
        if event.r#type == self.fail_on_type {
            return Err(ProjectorHandlerError::Handler(format!(
                "simulated failure on {}",
                event.r#type
            )));
        }
        let mut processed = self.processed.lock().await;
        processed.push(event.r#type.clone());
        Ok(())
    }
}

/// Helper: create a store with 3 events (Event1, Event2, Event3)
async fn store_with_three_events() -> (Arc<EventStore>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600),
            max_bytes: None,
        },
    )
    .await
    .unwrap();

    store
        .append(
            "s1",
            ExpectedVersion::NoStream,
            vec![
                NewEvent {
                    r#type: "Event1".into(),
                    payload: json!({}),
                    request_id: None,
                    actor_id: "test:projector".into(),
                    actor_type: ActorType::System,
                },
                NewEvent {
                    r#type: "Event2".into(),
                    payload: json!({}),
                    request_id: None,
                    actor_id: "test:projector".into(),
                    actor_type: ActorType::System,
                },
                NewEvent {
                    r#type: "Event3".into(),
                    payload: json!({}),
                    request_id: None,
                    actor_id: "test:projector".into(),
                    actor_type: ActorType::System,
                },
            ],
        )
        .await
        .unwrap();

    (Arc::new(store), temp_dir)
}

#[tokio::test]
async fn test_handler_failure_middle_of_batch_does_not_checkpoint() {
    let (store, _dir) = store_with_three_events().await;
    let (sender, _subscriber, broadcast_loop) = create_broadcast_system();
    tokio::spawn(broadcast_loop.run());

    let processed = Arc::new(Mutex::new(Vec::<String>::new()));
    let attempts = Arc::new(Mutex::new(0u32));
    let handler = FailingHandler {
        fail_on_type: "Event2".into(),
        processed: processed.clone(),
        attempts: attempts.clone(),
    };

    let projector = Projector::new(store.clone(), "test-consumer-mid".into()).with_batch_size(10);

    // Run projector briefly — it will retry the batch multiple times
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        projector.run_with_handler(&handler, &sender),
    )
    .await;

    // Checkpoint should NOT have advanced — bootstrap_cursor returns earliest partition cursor
    // since no checkpoint was stored
    let cursor_after = bootstrap_cursor(&store, "test-consumer-mid").await.unwrap();
    assert_eq!(
        cursor_after.sequence, 0,
        "Checkpoint must not advance past a failed event, cursor sequence should be 0 (earliest)"
    );

    // Event1 was processed on each retry, but Event2 always fails so Event3 is never reached
    let processed = processed.lock().await;
    assert!(
        processed.iter().all(|t| t == "Event1"),
        "Only Event1 should be processed, got: {:?}",
        *processed
    );

    // Multiple retry attempts should have occurred
    let attempts = *attempts.lock().await;
    assert!(
        attempts > 1,
        "Expected multiple attempts due to retries, got {}",
        attempts
    );
}

#[tokio::test]
async fn test_handler_failure_first_event_does_not_checkpoint() {
    let (store, _dir) = store_with_three_events().await;
    let (sender, _subscriber, broadcast_loop) = create_broadcast_system();
    tokio::spawn(broadcast_loop.run());

    let processed = Arc::new(Mutex::new(Vec::<String>::new()));
    let attempts = Arc::new(Mutex::new(0u32));
    let handler = FailingHandler {
        fail_on_type: "Event1".into(),
        processed: processed.clone(),
        attempts: attempts.clone(),
    };

    let projector = Projector::new(store.clone(), "test-consumer-first".into()).with_batch_size(10);

    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        projector.run_with_handler(&handler, &sender),
    )
    .await;

    let cursor_after = bootstrap_cursor(&store, "test-consumer-first")
        .await
        .unwrap();
    assert_eq!(
        cursor_after.sequence, 0,
        "Checkpoint must not advance when first event fails"
    );

    // No events should have been successfully processed
    let processed = processed.lock().await;
    assert!(
        processed.is_empty(),
        "No events should be processed when first event fails, got: {:?}",
        *processed
    );
}

#[tokio::test]
async fn test_handler_failure_last_event_does_not_checkpoint() {
    let (store, _dir) = store_with_three_events().await;
    let (sender, _subscriber, broadcast_loop) = create_broadcast_system();
    tokio::spawn(broadcast_loop.run());

    let processed = Arc::new(Mutex::new(Vec::<String>::new()));
    let attempts = Arc::new(Mutex::new(0u32));
    let handler = FailingHandler {
        fail_on_type: "Event3".into(),
        processed: processed.clone(),
        attempts: attempts.clone(),
    };

    let projector = Projector::new(store.clone(), "test-consumer-last".into()).with_batch_size(10);

    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        projector.run_with_handler(&handler, &sender),
    )
    .await;

    let cursor_after = bootstrap_cursor(&store, "test-consumer-last")
        .await
        .unwrap();
    assert_eq!(
        cursor_after.sequence, 0,
        "Checkpoint must not advance when last event fails"
    );

    // Event1 and Event2 processed on each retry, but batch never checkpoints
    let processed = processed.lock().await;
    assert!(
        processed.iter().all(|t| t == "Event1" || t == "Event2"),
        "Only Event1 and Event2 should be processed, got: {:?}",
        *processed
    );
}

/// A handler that fails for the first N attempts on a given event type,
/// then succeeds. This simulates transient failures with retry recovery.
struct TransientFailHandler {
    fail_on_type: String,
    fail_count: u32,
    attempt_counter: Arc<Mutex<u32>>,
    processed: Arc<Mutex<Vec<String>>>,
}

impl ProjectorHandler for TransientFailHandler {
    async fn handle_event(
        &self,
        event: &EventEnvelope,
        _sender: &StreamEventSender,
    ) -> std::result::Result<(), ProjectorHandlerError> {
        if event.r#type == self.fail_on_type {
            let mut counter = self.attempt_counter.lock().await;
            *counter += 1;
            if *counter <= self.fail_count {
                return Err(ProjectorHandlerError::Handler(format!(
                    "transient failure #{} on {}",
                    *counter, event.r#type
                )));
            }
        }
        let mut processed = self.processed.lock().await;
        processed.push(event.r#type.clone());
        Ok(())
    }
}

#[tokio::test]
async fn test_handler_transient_failure_recovers_on_retry() {
    let (store, _dir) = store_with_three_events().await;
    let (sender, _subscriber, broadcast_loop) = create_broadcast_system();
    tokio::spawn(broadcast_loop.run());

    let attempt_counter = Arc::new(Mutex::new(0u32));
    let processed = Arc::new(Mutex::new(Vec::<String>::new()));
    let handler = TransientFailHandler {
        fail_on_type: "Event2".into(),
        fail_count: 2, // fail first 2 attempts, succeed on 3rd
        attempt_counter: attempt_counter.clone(),
        processed: processed.clone(),
    };

    let projector =
        Projector::new(store.clone(), "test-consumer-transient".into()).with_batch_size(10);

    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        projector.run_with_handler(&handler, &sender),
    )
    .await;

    // After recovery, all 3 events should have been processed
    let processed = processed.lock().await;
    // Event1 is processed on each retry attempt, then all 3 on the successful batch
    assert!(
        processed.contains(&"Event1".to_string())
            && processed.contains(&"Event2".to_string())
            && processed.contains(&"Event3".to_string()),
        "All events should eventually be processed after transient failure recovery, got: {:?}",
        *processed
    );

    // Checkpoint should have advanced after successful batch
    let cursor_after = bootstrap_cursor(&store, "test-consumer-transient")
        .await
        .unwrap();
    assert!(
        cursor_after.sequence > 0,
        "Checkpoint should advance after successful retry, got sequence {}",
        cursor_after.sequence
    );
}
