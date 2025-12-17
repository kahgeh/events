//! EventsRuntime - wires together event store, completions store, and broadcast system
//!
//! This module provides a convenient way to initialize and run the events infrastructure.
//! Services use EventsRuntime to get access to:
//! - EventStore for appending events
//! - CompletionsStore for recording and querying completions
//! - CompletionSender for projectors to send completions
//! - CompletionSubscriber for gRPC streaming service

use crate::broadcast::{
    create_broadcast_system, CompletionBroadcastLoop, CompletionSender, CompletionSubscriber,
};
use crate::completions_store::CompletionsStore;
use crate::rotation::RotationPolicy;
use crate::{EventStore, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Default completions TTL (5 minutes)
pub const DEFAULT_COMPLETIONS_TTL: Duration = Duration::from_secs(300);

/// Default rotation policy (1 hour windows)
pub fn default_rotation_policy() -> RotationPolicy {
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: None,
    }
}

/// Configuration for EventsRuntime
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Data directory for databases
    pub data_dir: String,
    /// Completions TTL (how long to keep completions for reconnection queries)
    pub completions_ttl: Duration,
    /// Rotation policy for event store partitions
    pub rotation_policy: RotationPolicy,
}

impl RuntimeConfig {
    /// Create a new config with the given data directory
    pub fn new(data_dir: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
            completions_ttl: DEFAULT_COMPLETIONS_TTL,
            rotation_policy: default_rotation_policy(),
        }
    }

    /// Set the completions TTL
    pub fn with_completions_ttl(mut self, ttl: Duration) -> Self {
        self.completions_ttl = ttl;
        self
    }

    /// Set the rotation policy
    pub fn with_rotation_policy(mut self, policy: RotationPolicy) -> Self {
        self.rotation_policy = policy;
        self
    }
}

/// The events runtime that wires everything together
pub struct EventsRuntime {
    /// Event store for domain events
    event_store: Arc<EventStore>,
    /// Completions store for request correlation
    completions_store: Arc<CompletionsStore>,
    /// Sender for projectors to send completions
    completion_sender: CompletionSender,
    /// Subscriber for gRPC streaming service
    completion_subscriber: CompletionSubscriber,
    /// Broadcast loop task (runs until dropped)
    broadcast_loop: Option<CompletionBroadcastLoop>,
}

impl EventsRuntime {
    /// Create a new EventsRuntime with the given configuration
    pub async fn new(config: RuntimeConfig) -> Result<Self> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_dir)?;

        // Initialize event store (events/ subdirectory)
        let events_path = format!("{}/events", config.data_dir);
        let event_store =
            EventStore::open_partitioned(&events_path, config.rotation_policy).await?;

        // Initialize completions store (completions/ subdirectory)
        let completions_path = Path::new(&config.data_dir).join("completions");
        std::fs::create_dir_all(&completions_path)?;
        let completions_store =
            CompletionsStore::with_ttl(&completions_path, config.completions_ttl).await?;

        // Create broadcast system
        let (completion_sender, completion_subscriber, broadcast_loop) = create_broadcast_system();

        Ok(Self {
            event_store: Arc::new(event_store),
            completions_store: Arc::new(completions_store),
            completion_sender,
            completion_subscriber,
            broadcast_loop: Some(broadcast_loop),
        })
    }

    /// Create a new EventsRuntime with default configuration
    pub async fn with_data_dir(data_dir: impl Into<String>) -> Result<Self> {
        Self::new(RuntimeConfig::new(data_dir)).await
    }

    /// Get the event store for appending events
    pub fn event_store(&self) -> Arc<EventStore> {
        Arc::clone(&self.event_store)
    }

    /// Get the completions store for recording and querying completions
    pub fn completions_store(&self) -> Arc<CompletionsStore> {
        Arc::clone(&self.completions_store)
    }

    /// Get the completion sender for projectors
    pub fn completion_sender(&self) -> CompletionSender {
        self.completion_sender.clone()
    }

    /// Get the completion subscriber for gRPC streaming service
    pub fn completion_subscriber(&self) -> CompletionSubscriber {
        self.completion_subscriber.clone()
    }

    /// Take the broadcast loop to spawn it
    ///
    /// This consumes the broadcast loop from the runtime.
    /// The loop should be spawned as a background task.
    pub fn take_broadcast_loop(&mut self) -> Option<CompletionBroadcastLoop> {
        self.broadcast_loop.take()
    }

    /// Spawn the broadcast loop and return the join handle
    ///
    /// This is a convenience method that takes and spawns the broadcast loop.
    pub fn spawn_broadcast_loop(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.take_broadcast_loop()
            .map(|loop_task| tokio::spawn(loop_task.run()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::CompletionEvent;
    use crate::completions_store::CompletionStatus;
    use crate::NewEvent;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_runtime_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();

        // Verify we can access components
        let _event_store = runtime.event_store();
        let _completions_store = runtime.completions_store();
        let _sender = runtime.completion_sender();
        let _subscriber = runtime.completion_subscriber();
    }

    #[tokio::test]
    async fn test_runtime_event_store_works() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
        let event_store = runtime.event_store();

        // Append an event
        let event = NewEvent {
            r#type: "test_event".to_string(),
            payload: serde_json::json!({"key": "value"}),
            request_id: Some("req-123".to_string()),
        };

        let result = event_store
            .append("stream-1", crate::ExpectedVersion::Any, [event])
            .await
            .unwrap();

        assert_eq!(result.events.len(), 1);
    }

    #[tokio::test]
    async fn test_runtime_completions_store_works() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
        let completions_store = runtime.completions_store();

        // Record a completion
        let completion = crate::Completion {
            request_id: "req-123".to_string(),
            status: CompletionStatus::Success {
                payload: Some(serde_json::json!({"result": "ok"})),
            },
        };

        completions_store.record(&completion).await.unwrap();

        // Query it back
        let retrieved = completions_store.get("req-123").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().request_id, "req-123");
    }

    #[tokio::test]
    async fn test_runtime_broadcast_works() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let mut runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
        let sender = runtime.completion_sender();
        let subscriber = runtime.completion_subscriber();

        // Spawn broadcast loop
        let _handle = runtime.spawn_broadcast_loop();

        // Subscribe
        let mut rx = subscriber.subscribe();

        // Send a completion
        let event = CompletionEvent::success(
            "req-456".to_string(),
            "stream-1".to_string(),
            "test_event".to_string(),
            Some(serde_json::json!({"data": "test"})),
        );
        sender.send(event).await.unwrap();

        // Receive it
        let received = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received.completion.request_id, "req-456");
    }
}
