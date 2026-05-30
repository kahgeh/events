use super::*;
use crate::broadcast::StreamEvent;
use crate::NewEvent;
use tempfile::TempDir;

#[tokio::test]
async fn test_runtime_initialization() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();

    // Verify we can access components
    let _event_namespaces = runtime.event_namespaces();
    let _notifications_store = runtime.notifications_store();
    let _sender = runtime.stream_event_sender();
    let _subscriber = runtime.stream_event_subscriber();
}

#[tokio::test]
async fn test_runtime_event_stream_works() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
    let event_namespaces = runtime.event_namespaces();
    let namespace = event_namespaces.ensure_namespace("runtime").await.unwrap();
    let partition = namespace.ensure_partition("stream-1").await.unwrap();
    let event_stream = partition.open().await.unwrap();

    // Append an event
    let event = NewEvent {
        r#type: "test_event".to_string(),
        payload: serde_json::json!({"key": "value"}),
        workflow_kind: None,
        workflow: crate::WorkflowRef::None,
        request_id: Some("req-123".to_string()),
        actor_id: "test:runtime".to_string(),
        actor_type: crate::ActorType::System,
    };

    let result = event_stream
        .append(crate::ExpectedVersion::Any, [event])
        .await
        .unwrap();

    assert_eq!(result.events.len(), 1);
}

#[tokio::test]
async fn test_runtime_notifications_store_works() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
    let notifications_store = runtime.notifications_store();

    // Record a stream event
    let event = StreamEvent::completed(
        "req-123".to_string(),
        "stream-1".to_string(),
        3,
        Some(serde_json::json!({"result": "ok"})),
    );

    notifications_store.record(&event).await.unwrap();

    // Query it back
    let retrieved = notifications_store.get("req-123").await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().request_id, "req-123");
}

#[tokio::test]
async fn test_runtime_broadcast_works() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let mut runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
    let sender = runtime.stream_event_sender();
    let subscriber = runtime.stream_event_subscriber();

    // Spawn broadcast loop
    let _handle = runtime.spawn_broadcast_loop();

    // Subscribe
    let mut rx = subscriber.subscribe();

    // Send a stream event
    let event = StreamEvent::progress(
        "req-456".to_string(),
        "stream-1".to_string(),
        1,
        3,
        "Creating app".to_string(),
    );
    sender.send(event).await.unwrap();

    // Receive it
    let received = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(received.request_id, "req-456");
    assert_eq!(received.current_step, 1);
}

#[tokio::test]
async fn test_notification_maintenance_worker_stops_on_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let handle = runtime.start_notification_maintenance_worker(
        NotificationMaintenanceOptions::new(std::time::Duration::from_millis(10)).unwrap(),
        shutdown_rx,
    );

    shutdown_tx.send(true).unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_notification_maintenance_worker_stops_when_shutdown_already_true() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    shutdown_tx.send(true).unwrap();

    let handle = runtime.start_notification_maintenance_worker(
        NotificationMaintenanceOptions::new(std::time::Duration::from_secs(60)).unwrap(),
        shutdown_tx.subscribe(),
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn test_notification_maintenance_options_rejects_zero_interval() {
    let error = NotificationMaintenanceOptions::new(std::time::Duration::ZERO).unwrap_err();

    assert!(matches!(
        error,
        EsError::InvalidDuration { ref field, .. }
            if field == "notification maintenance cleanup_interval"
    ));
}
