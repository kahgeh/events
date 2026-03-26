use events::{
    bootstrap_cursor, migration, ActorType, EsError, EventStore, ExpectedVersion, NewEvent,
    PartitionedCursor, RotationPolicy,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

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
    let event_id = Uuid::new_v4();

    let cursor = PartitionedCursor::new(partition_name.clone(), created_at, event_id);

    assert_eq!(cursor.partition, partition_name);
    assert_eq!(cursor.created_at_ms, created_at);
    assert_eq!(cursor.event_id, event_id);

    let (partition, time, id) = cursor.as_tuple();
    assert_eq!(partition, partition_name.as_str());
    assert_eq!(time, created_at);
    assert_eq!(id, &event_id);

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
        "INSERT INTO events (id, stream_id, type, payload, version, created_at, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        (
            Uuid::new_v4().to_string(),
            stream_id,
            "TestEvent",
            "{}",
            1i64,
            created_at_ms,
            "test:constraint",
            "system",
        ),
    ).await?;

    // Second insert with same (stream_id, version) should fail due to unique index
    let result = conn.execute(
        "INSERT INTO events (id, stream_id, type, payload, version, created_at, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        (
            Uuid::new_v4().to_string(),
            stream_id,
            "TestEvent",
            "{}",
            1i64, // duplicate version for same stream
            created_at_ms + 1,
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
    let event_id_v5 = Uuid::new_v4();
    let event_id_v3 = Uuid::new_v4();

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
    let event_id_v7 = Uuid::new_v4();
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
            "INSERT INTO events (id, stream_id, type, payload, version, created_at, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            (
                Uuid::new_v4().to_string(),
                stream_id,
                "OrphanEvent",
                "{}",
                2i64,
                now_ms,
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
            "INSERT INTO events (id, stream_id, type, payload, version, created_at, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            (
                Uuid::new_v4().to_string(),
                "restart-stream",
                "OrphanEvent",
                "{}",
                2i64,
                now_ms,
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
        "INSERT INTO events (id, stream_id, type, payload, version, created_at, actor_id, actor_type) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        (
            Uuid::new_v4().to_string(),
            stream_id,
            "OrphanInSealed",
            "{}",
            3i64,
            now_ms,
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
