//! Broadcast infrastructure for completion events
//!
//! This module provides the channel-based infrastructure for broadcasting
//! completion events from projectors to connected FOH instances.

use crate::completions_store::{Completion, CompletionStatus};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

/// Default capacity for the completion broadcast channel
pub const BROADCAST_CAPACITY: usize = 1024;

/// Default capacity for the completion sender channel (projector -> broadcast loop)
pub const SENDER_CAPACITY: usize = 256;

/// A completion event for broadcasting to subscribers
#[derive(Debug, Clone)]
pub struct CompletionEvent {
    /// The completion data
    pub completion: Completion,
    /// Stream ID where the completion event originated
    pub stream_id: String,
    /// Event type that triggered the completion (e.g., "machine_created", "machine_creation_failed")
    pub event_type: String,
}

impl CompletionEvent {
    /// Create a new success completion event
    pub fn success(
        request_id: String,
        stream_id: String,
        event_type: String,
        payload: Option<serde_json::Value>,
    ) -> Self {
        Self {
            completion: Completion {
                request_id,
                status: CompletionStatus::Success { payload },
            },
            stream_id,
            event_type,
        }
    }

    /// Create a new failure completion event
    pub fn failed(
        request_id: String,
        stream_id: String,
        event_type: String,
        error: String,
        retriable: bool,
    ) -> Self {
        Self {
            completion: Completion {
                request_id,
                status: CompletionStatus::Failed { error, retriable },
            },
            stream_id,
            event_type,
        }
    }
}

/// Handle for sending completions from projectors
///
/// Projectors use this to send completion events to the broadcast loop.
/// Multiple projectors can hold clones of this handle.
#[derive(Clone)]
pub struct CompletionSender {
    tx: mpsc::Sender<CompletionEvent>,
}

impl CompletionSender {
    /// Send a completion event
    ///
    /// Returns an error if the broadcast loop has been dropped.
    pub async fn send(&self, event: CompletionEvent) -> Result<(), CompletionSendError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| CompletionSendError::ChannelClosed)
    }

    /// Try to send a completion event without blocking
    ///
    /// Returns an error if the channel is full or closed.
    pub fn try_send(&self, event: CompletionEvent) -> Result<(), CompletionSendError> {
        self.tx.try_send(event).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => CompletionSendError::ChannelFull,
            mpsc::error::TrySendError::Closed(_) => CompletionSendError::ChannelClosed,
        })
    }
}

/// Error when sending completions
#[derive(Debug, thiserror::Error)]
pub enum CompletionSendError {
    #[error("Completion channel is closed")]
    ChannelClosed,
    #[error("Completion channel is full")]
    ChannelFull,
}

/// Handle for subscribing to completion broadcasts
///
/// FOH instances use this to receive completion events.
#[derive(Clone)]
pub struct CompletionSubscriber {
    tx: broadcast::Sender<Arc<CompletionEvent>>,
}

impl CompletionSubscriber {
    /// Subscribe to receive completion events
    ///
    /// Returns a receiver that will receive all completion events.
    /// If the receiver falls behind, older events will be dropped.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<CompletionEvent>> {
        self.tx.subscribe()
    }

    /// Get the current number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// The broadcast loop that receives completions and fans them out
///
/// This runs as a background task and:
/// 1. Receives completions from projectors via mpsc channel
/// 2. Broadcasts them to all connected subscribers via broadcast channel
pub struct CompletionBroadcastLoop {
    /// Receiver for completions from projectors
    rx: mpsc::Receiver<CompletionEvent>,
    /// Sender for broadcasting to subscribers
    broadcast_tx: broadcast::Sender<Arc<CompletionEvent>>,
}

impl CompletionBroadcastLoop {
    /// Run the broadcast loop
    ///
    /// This method runs until the sender channel is closed (all senders dropped).
    pub async fn run(mut self) {
        tracing::info!("Starting completion broadcast loop");

        while let Some(event) = self.rx.recv().await {
            let request_id = event.completion.request_id.clone();
            let subscriber_count = self.broadcast_tx.receiver_count();

            if subscriber_count == 0 {
                tracing::debug!(
                    request_id = %request_id,
                    "No subscribers for completion event"
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
                        "Broadcasted completion event"
                    );
                }
                Err(_) => {
                    // All receivers have been dropped
                    tracing::warn!(
                        request_id = %request_id,
                        "Failed to broadcast completion - no receivers"
                    );
                }
            }
        }

        tracing::info!("Completion broadcast loop stopped (sender channel closed)");
    }
}

/// Create a new completion broadcast system
///
/// Returns:
/// - `CompletionSender`: For projectors to send completions
/// - `CompletionSubscriber`: For FOH instances to subscribe
/// - `CompletionBroadcastLoop`: The background task to run
pub fn create_broadcast_system() -> (
    CompletionSender,
    CompletionSubscriber,
    CompletionBroadcastLoop,
) {
    create_broadcast_system_with_capacity(SENDER_CAPACITY, BROADCAST_CAPACITY)
}

/// Create a new completion broadcast system with custom capacities
pub fn create_broadcast_system_with_capacity(
    sender_capacity: usize,
    broadcast_capacity: usize,
) -> (
    CompletionSender,
    CompletionSubscriber,
    CompletionBroadcastLoop,
) {
    let (mpsc_tx, mpsc_rx) = mpsc::channel(sender_capacity);
    let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);

    let sender = CompletionSender { tx: mpsc_tx };
    let subscriber = CompletionSubscriber {
        tx: broadcast_tx.clone(),
    };
    let loop_task = CompletionBroadcastLoop {
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

        // Send a completion
        let event = CompletionEvent::success(
            "req-123".to_string(),
            "stream-1".to_string(),
            "machine_created".to_string(),
            Some(serde_json::json!({"machine_id": "m-456"})),
        );
        sender.send(event).await.unwrap();

        // Receive the completion
        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received.completion.request_id, "req-123");
        assert_eq!(received.stream_id, "stream-1");
        assert_eq!(received.event_type, "machine_created");

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

        // Send a completion
        let event = CompletionEvent::failed(
            "req-456".to_string(),
            "stream-2".to_string(),
            "machine_creation_failed".to_string(),
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
        let event = CompletionEvent::success(
            "req-789".to_string(),
            "stream-3".to_string(),
            "app_created".to_string(),
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
            .send(CompletionEvent::success(
                "req-1".to_string(),
                "stream-1".to_string(),
                "event-1".to_string(),
                None,
            ))
            .await
            .unwrap();

        sender2
            .send(CompletionEvent::success(
                "req-2".to_string(),
                "stream-2".to_string(),
                "event-2".to_string(),
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

        assert_eq!(r1.completion.request_id, "req-1");
        assert_eq!(r2.completion.request_id, "req-2");

        drop(sender1);
        drop(sender2);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }
}
