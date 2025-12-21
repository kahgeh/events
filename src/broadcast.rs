//! Broadcast infrastructure for stream events
//!
//! This module provides the channel-based infrastructure for broadcasting
//! stream events (progress and completion) from projectors to connected FOH instances.

use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

/// Default capacity for the stream event broadcast channel
pub const BROADCAST_CAPACITY: usize = 1024;

/// Default capacity for the stream event sender channel (projector -> broadcast loop)
pub const SENDER_CAPACITY: usize = 256;

/// Event kind discriminator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// Intermediate progress update
    Progress,
    /// Operation completed successfully
    Completed,
    /// Operation failed
    Failed,
}

impl EventKind {
    /// Returns true if this is a terminal event (Completed or Failed)
    pub fn is_terminal(&self) -> bool {
        matches!(self, EventKind::Completed | EventKind::Failed)
    }
}

/// Status for individual items in batch operations
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ItemStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Progress for individual items in batch operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ItemProgress {
    pub item_id: String,
    pub status: ItemStatus,
    pub message: String,
}

/// A stream event for broadcasting to subscribers
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// The request ID this event is for
    pub request_id: String,
    /// Stream ID where the event originated
    pub stream_id: String,
    /// Unix timestamp of the event
    pub timestamp: i64,
    /// Event kind: Progress, Completed, or Failed
    pub kind: EventKind,
    /// Current step number (1-indexed)
    pub current_step: u32,
    /// Total number of steps
    pub total_steps: u32,
    /// Human-readable step name
    pub step_name: String,
    /// For Completed: optional JSON payload
    pub payload: Option<serde_json::Value>,
    /// For Failed: error message
    pub error_message: Option<String>,
    /// For Failed: whether the operation can be retried
    pub retriable: Option<bool>,
    /// For batch operations: per-item progress
    pub items: Vec<ItemProgress>,
}

impl StreamEvent {
    fn now() -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp()
    }

    /// Create a progress event
    pub fn progress(
        request_id: String,
        stream_id: String,
        current_step: u32,
        total_steps: u32,
        step_name: String,
    ) -> Self {
        Self {
            request_id,
            stream_id,
            timestamp: Self::now(),
            kind: EventKind::Progress,
            current_step,
            total_steps,
            step_name,
            payload: None,
            error_message: None,
            retriable: None,
            items: Vec::new(),
        }
    }

    /// Create a completion event
    pub fn completed(
        request_id: String,
        stream_id: String,
        total_steps: u32,
        payload: Option<serde_json::Value>,
    ) -> Self {
        Self {
            request_id,
            stream_id,
            timestamp: Self::now(),
            kind: EventKind::Completed,
            current_step: total_steps,
            total_steps,
            step_name: "Completed".to_string(),
            payload,
            error_message: None,
            retriable: None,
            items: Vec::new(),
        }
    }

    /// Create a failure event
    pub fn failed(
        request_id: String,
        stream_id: String,
        current_step: u32,
        total_steps: u32,
        error: String,
        retriable: bool,
    ) -> Self {
        Self {
            request_id,
            stream_id,
            timestamp: Self::now(),
            kind: EventKind::Failed,
            current_step,
            total_steps,
            step_name: "Failed".to_string(),
            payload: None,
            error_message: Some(error),
            retriable: Some(retriable),
            items: Vec::new(),
        }
    }

    /// Add item progress for batch operations
    pub fn with_items(mut self, items: Vec<ItemProgress>) -> Self {
        self.items = items;
        self
    }

    /// Returns true if this is a terminal event
    pub fn is_terminal(&self) -> bool {
        self.kind.is_terminal()
    }
}

/// Handle for sending stream events from projectors
///
/// Projectors use this to send stream events to the broadcast loop.
/// Multiple projectors can hold clones of this handle.
#[derive(Clone)]
pub struct StreamEventSender {
    tx: mpsc::Sender<StreamEvent>,
}

impl StreamEventSender {
    /// Send a stream event
    ///
    /// Returns an error if the broadcast loop has been dropped.
    pub async fn send(&self, event: StreamEvent) -> Result<(), StreamEventSendError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| StreamEventSendError::ChannelClosed)
    }

    /// Try to send a stream event without blocking
    ///
    /// Returns an error if the channel is full or closed.
    pub fn try_send(&self, event: StreamEvent) -> Result<(), StreamEventSendError> {
        self.tx.try_send(event).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => StreamEventSendError::ChannelFull,
            mpsc::error::TrySendError::Closed(_) => StreamEventSendError::ChannelClosed,
        })
    }
}

/// Error when sending stream events
#[derive(Debug, thiserror::Error)]
pub enum StreamEventSendError {
    #[error("Stream event channel is closed")]
    ChannelClosed,
    #[error("Stream event channel is full")]
    ChannelFull,
}

/// Handle for subscribing to stream event broadcasts
///
/// FOH instances use this to receive stream events.
#[derive(Clone)]
pub struct StreamEventSubscriber {
    tx: broadcast::Sender<Arc<StreamEvent>>,
}

impl StreamEventSubscriber {
    /// Subscribe to receive stream events
    ///
    /// Returns a receiver that will receive all stream events.
    /// If the receiver falls behind, older events will be dropped.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<StreamEvent>> {
        self.tx.subscribe()
    }

    /// Get the current number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// The broadcast loop that receives stream events and fans them out
///
/// This runs as a background task and:
/// 1. Receives stream events from projectors via mpsc channel
/// 2. Broadcasts them to all connected subscribers via broadcast channel
pub struct StreamEventBroadcastLoop {
    /// Receiver for stream events from projectors
    rx: mpsc::Receiver<StreamEvent>,
    /// Sender for broadcasting to subscribers
    broadcast_tx: broadcast::Sender<Arc<StreamEvent>>,
}

impl StreamEventBroadcastLoop {
    /// Run the broadcast loop
    ///
    /// This method runs until the sender channel is closed (all senders dropped).
    pub async fn run(mut self) {
        tracing::info!("Starting stream event broadcast loop");

        while let Some(event) = self.rx.recv().await {
            let request_id = event.request_id.clone();
            let subscriber_count = self.broadcast_tx.receiver_count();

            if subscriber_count == 0 {
                tracing::debug!(
                    request_id = %request_id,
                    "No subscribers for stream event"
                );
                continue;
            }

            // Wrap in Arc for efficient broadcasting
            let event = Arc::new(event);

            match self.broadcast_tx.send(event) {
                Ok(n) => {
                    tracing::debug!(
                        request_id = %request_id,
                        receivers = n,
                        "Broadcasted stream event"
                    );
                }
                Err(_) => {
                    // All receivers have been dropped
                    tracing::warn!(
                        request_id = %request_id,
                        "Failed to broadcast stream event - no receivers"
                    );
                }
            }
        }

        tracing::info!("Stream event broadcast loop stopped (sender channel closed)");
    }
}

/// Create a new stream event broadcast system
///
/// Returns:
/// - `StreamEventSender`: For projectors to send stream events
/// - `StreamEventSubscriber`: For FOH instances to subscribe
/// - `StreamEventBroadcastLoop`: The background task to run
pub fn create_broadcast_system() -> (
    StreamEventSender,
    StreamEventSubscriber,
    StreamEventBroadcastLoop,
) {
    create_broadcast_system_with_capacity(SENDER_CAPACITY, BROADCAST_CAPACITY)
}

/// Create a new stream event broadcast system with custom capacities
pub fn create_broadcast_system_with_capacity(
    sender_capacity: usize,
    broadcast_capacity: usize,
) -> (
    StreamEventSender,
    StreamEventSubscriber,
    StreamEventBroadcastLoop,
) {
    let (mpsc_tx, mpsc_rx) = mpsc::channel(sender_capacity);
    let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);

    let sender = StreamEventSender { tx: mpsc_tx };
    let subscriber = StreamEventSubscriber {
        tx: broadcast_tx.clone(),
    };
    let loop_task = StreamEventBroadcastLoop {
        rx: mpsc_rx,
        broadcast_tx,
    };

    (sender, subscriber, loop_task)
}

#[cfg(test)]
mod tests {
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
        let event = StreamEvent::completed(
            "req-789".to_string(),
            "stream-3".to_string(),
            3,
            None,
        );
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
}
