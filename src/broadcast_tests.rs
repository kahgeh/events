use super::*;
use std::time::Duration;

#[tokio::test]
async fn test_broadcast_single_subscriber() {
    let (sender, subscriber, loop_task) = create_broadcast_system();

    // Start broadcast loop in background
    let handle = tokio::spawn(loop_task.run());

    // Subscribe before sending
    let mut rx = subscriber.subscribe();

    // Send a progress event
    let event = StreamEvent::progress(
        "req-123".to_string(),
        "stream-1".to_string(),
        1,
        3,
        "Creating app".to_string(),
    );
    sender.send(event).await.unwrap();

    // Receive the event
    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(received.request_id, "req-123");
    assert_eq!(received.stream_id, "stream-1");
    assert_eq!(received.current_step, 1);
    assert_eq!(received.total_steps, 3);
    assert_eq!(received.kind, EventKind::Progress);

    // Drop sender to stop loop
    drop(sender);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn test_broadcast_multiple_subscribers() {
    let (sender, subscriber, loop_task) = create_broadcast_system();

    let handle = tokio::spawn(loop_task.run());

    // Multiple subscribers
    let mut rx1 = subscriber.subscribe();
    let mut rx2 = subscriber.subscribe();
    let mut rx3 = subscriber.subscribe();

    assert_eq!(subscriber.subscriber_count(), 3);

    // Send a failure event
    let event = StreamEvent::failed(
        "req-456".to_string(),
        "stream-2".to_string(),
        2,
        3,
        "Network error".to_string(),
        true,
    );
    sender.send(event).await.unwrap();

    // All subscribers should receive it
    let r1 = tokio::time::timeout(Duration::from_secs(1), rx1.recv())
        .await
        .unwrap()
        .unwrap();
    let r2 = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    let r3 = tokio::time::timeout(Duration::from_secs(1), rx3.recv())
        .await
        .unwrap()
        .unwrap();

    // All should have received the same event (Arc means same pointer)
    assert!(Arc::ptr_eq(&r1, &r2));
    assert!(Arc::ptr_eq(&r2, &r3));

    drop(sender);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn test_no_subscribers() {
    let (sender, _subscriber, loop_task) = create_broadcast_system();

    let handle = tokio::spawn(loop_task.run());

    // Send without any subscribers - should not block or error
    let event = StreamEvent::completed("req-789".to_string(), "stream-3".to_string(), 3, None);
    sender.send(event).await.unwrap();

    // Give some time for processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    drop(sender);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn test_sender_clone() {
    let (sender1, subscriber, loop_task) = create_broadcast_system();
    let sender2 = sender1.clone();

    let handle = tokio::spawn(loop_task.run());
    let mut rx = subscriber.subscribe();

    // Send from both senders
    sender1
        .send(StreamEvent::progress(
            "req-1".to_string(),
            "stream-1".to_string(),
            1,
            2,
            "Step 1".to_string(),
        ))
        .await
        .unwrap();

    sender2
        .send(StreamEvent::completed(
            "req-2".to_string(),
            "stream-2".to_string(),
            2,
            None,
        ))
        .await
        .unwrap();

    // Should receive both
    let r1 = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let r2 = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(r1.request_id, "req-1");
    assert_eq!(r2.request_id, "req-2");

    drop(sender1);
    drop(sender2);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}

#[tokio::test]
async fn test_multiple_events_per_request() {
    let (sender, subscriber, loop_task) = create_broadcast_system();

    let handle = tokio::spawn(loop_task.run());
    let mut rx = subscriber.subscribe();

    // Send multiple progress events for the same request
    for step in 1..=3 {
        let event = StreamEvent::progress(
            "req-multi".to_string(),
            "stream-1".to_string(),
            step,
            4,
            format!("Step {}", step),
        );
        sender.send(event).await.unwrap();
    }

    // Send completion
    let event = StreamEvent::completed(
        "req-multi".to_string(),
        "stream-1".to_string(),
        4,
        Some(serde_json::json!({"result": "success"})),
    );
    sender.send(event).await.unwrap();

    // Receive all events
    for step in 1..=3 {
        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.current_step, step);
        assert_eq!(received.kind, EventKind::Progress);
    }

    // Receive completion
    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.kind, EventKind::Completed);
    assert!(received.is_terminal());

    drop(sender);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
}
