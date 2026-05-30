use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_record_and_get_progress() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

    let event = StreamEvent::progress(
        "req-123".to_string(),
        "stream-1".to_string(),
        1,
        3,
        "Creating app".to_string(),
    );

    store.record(&event).await.unwrap();

    let result = store.get("req-123").await.unwrap();
    assert!(result.is_some());

    let retrieved = result.unwrap();
    assert_eq!(retrieved.request_id, "req-123");
    assert_eq!(retrieved.stream_id, "stream-1");
    assert_eq!(retrieved.current_step, 1);
    assert_eq!(retrieved.total_steps, 3);
    assert_eq!(retrieved.kind, EventKind::Progress);
}

#[tokio::test]
async fn test_record_and_get_completed() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

    let event = StreamEvent::completed(
        "req-456".to_string(),
        "stream-2".to_string(),
        3,
        Some(serde_json::json!({"machine_id": "m-789"})),
    );

    store.record(&event).await.unwrap();

    let result = store.get("req-456").await.unwrap();
    assert!(result.is_some());

    let retrieved = result.unwrap();
    assert_eq!(retrieved.kind, EventKind::Completed);
    assert!(retrieved.payload.is_some());
    assert_eq!(retrieved.payload.unwrap()["machine_id"], "m-789");
}

#[tokio::test]
async fn test_record_and_get_failed() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

    let event = StreamEvent::failed(
        "req-789".to_string(),
        "stream-3".to_string(),
        2,
        3,
        "Network error".to_string(),
        true,
    );

    store.record(&event).await.unwrap();

    let result = store.get("req-789").await.unwrap();
    assert!(result.is_some());

    let retrieved = result.unwrap();
    assert_eq!(retrieved.kind, EventKind::Failed);
    assert_eq!(retrieved.error_message, Some("Network error".to_string()));
    assert_eq!(retrieved.retriable, Some(true));
}

#[tokio::test]
async fn test_upsert_updates_event() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

    // Record progress event
    let event1 = StreamEvent::progress(
        "req-upsert".to_string(),
        "stream-1".to_string(),
        1,
        3,
        "Step 1".to_string(),
    );
    store.record(&event1).await.unwrap();

    // Record completion event for same request
    let event2 = StreamEvent::completed(
        "req-upsert".to_string(),
        "stream-1".to_string(),
        3,
        Some(serde_json::json!({"result": "success"})),
    );
    store.record(&event2).await.unwrap();

    // Should get the latest (completion) event
    let result = store.get("req-upsert").await.unwrap();
    assert!(result.is_some());

    let retrieved = result.unwrap();
    assert_eq!(retrieved.kind, EventKind::Completed);
    assert_eq!(retrieved.current_step, 3);
}

#[tokio::test]
async fn test_get_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

    let result = store.get("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_ttl_expiration() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::with_ttl(temp_dir.path(), Duration::from_secs(1))
        .await
        .unwrap();

    let event = StreamEvent::progress(
        "req-ttl".to_string(),
        "stream-1".to_string(),
        1,
        1,
        "Testing".to_string(),
    );

    store.record(&event).await.unwrap();

    // Should exist immediately
    assert!(store.get("req-ttl").await.unwrap().is_some());

    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Should not exist after TTL
    assert!(store.get("req-ttl").await.unwrap().is_none());
}

#[tokio::test]
async fn test_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let store = NotificationsStore::with_ttl(temp_dir.path(), Duration::from_secs(1))
        .await
        .unwrap();

    // Record multiple events
    for i in 0..5 {
        let event = StreamEvent::progress(
            format!("req-{}", i),
            "stream-1".to_string(),
            1,
            1,
            "Testing".to_string(),
        );
        store.record(&event).await.unwrap();
    }

    // Wait for TTL
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Cleanup should delete all expired
    let deleted = store.cleanup().await.unwrap();
    assert!(deleted >= 5, "Expected at least 5 deleted, got {}", deleted);

    // Cleanup again should delete nothing
    let deleted = store.cleanup().await.unwrap();
    assert_eq!(deleted, 0);
}
