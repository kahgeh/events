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
#[path = "broadcast_tests.rs"]
mod broadcast_tests;
