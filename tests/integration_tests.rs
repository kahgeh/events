use events::{
    bootstrap_cursor, migration, EsError, EventStore, ExpectedVersion, NewEvent, PartitionedCursor,
    RotationPolicy,
};
use serde_json::json;
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
                },
                NewEvent {
                    r#type: "TestEvent2".into(),
                    payload: json!({"data": "test2"}),
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
        "INSERT INTO events (id, stream_id, type, payload, version, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        (
            Uuid::new_v4().to_string(),
            stream_id,
            "TestEvent",
            "{}",
            1i64,
            created_at_ms,
        ),
    ).await?;

    // Second insert with same (stream_id, version) should fail due to unique index
    let result = conn.execute(
        "INSERT INTO events (id, stream_id, type, payload, version, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        (
            Uuid::new_v4().to_string(),
            stream_id,
            "TestEvent",
            "{}",
            1i64, // duplicate version for same stream
            created_at_ms + 1,
        ),
    ).await;

    assert!(
        result.is_err(),
        "Expected unique constraint violation on (stream_id, version)"
    );

    Ok(())
}
