//! EventsRuntime - wires together event store, notifications store, and broadcast system
//!
//! This module provides a convenient way to initialize and run the events infrastructure.
//! Services use EventsRuntime to get access to:
//! - EventPartitions for resolving owner event stores
//! - NotificationsStore for recording and querying stream events
//! - StreamEventSender for projectors to send stream events
//! - StreamEventSubscriber for gRPC streaming service

use crate::broadcast::{
    create_broadcast_system, StreamEventBroadcastLoop, StreamEventSender, StreamEventSubscriber,
};
use crate::notifications_store::NotificationsStore;
use crate::rotation::RotationPolicy;
use crate::{EventPartitions, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Default events store TTL (5 minutes)
pub const DEFAULT_EVENTS_STORE_TTL: Duration = Duration::from_secs(300);

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
    /// Events store TTL (how long to keep stream events for reconnection queries)
    pub events_store_ttl: Duration,
    /// Rotation policy for event store partitions
    pub rotation_policy: RotationPolicy,
}

impl RuntimeConfig {
    /// Create a new config with the given data directory
    pub fn new(data_dir: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
            events_store_ttl: DEFAULT_EVENTS_STORE_TTL,
            rotation_policy: default_rotation_policy(),
        }
    }

    /// Set the events store TTL
    pub fn with_events_store_ttl(mut self, ttl: Duration) -> Self {
        self.events_store_ttl = ttl;
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
    /// Partition resolver for domain event stores
    event_partitions: Arc<EventPartitions>,
    /// Notifications store for stream events (progress + completion)
    notifications_store: Arc<NotificationsStore>,
    /// Sender for projectors to send stream events
    stream_event_sender: StreamEventSender,
    /// Subscriber for gRPC streaming service
    stream_event_subscriber: StreamEventSubscriber,
    /// Broadcast loop task (runs until dropped)
    broadcast_loop: Option<StreamEventBroadcastLoop>,
}

impl EventsRuntime {
    /// Create a new EventsRuntime with the given configuration
    pub async fn new(config: RuntimeConfig) -> Result<Self> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_dir)?;

        // Initialize event partitions (events/ subdirectory)
        let events_path = format!("{}/events", config.data_dir);
        let event_partitions = EventPartitions::open(&events_path, config.rotation_policy).await?;

        // Initialize notifications store (stream_events/ subdirectory)
        let stream_events_path = Path::new(&config.data_dir).join("stream_events");
        std::fs::create_dir_all(&stream_events_path)?;
        let notifications_store =
            NotificationsStore::with_ttl(&stream_events_path, config.events_store_ttl).await?;

        // Create broadcast system
        let (stream_event_sender, stream_event_subscriber, broadcast_loop) =
            create_broadcast_system();

        Ok(Self {
            event_partitions: Arc::new(event_partitions),
            notifications_store: Arc::new(notifications_store),
            stream_event_sender,
            stream_event_subscriber,
            broadcast_loop: Some(broadcast_loop),
        })
    }

    /// Create a new EventsRuntime with default configuration
    pub async fn with_data_dir(data_dir: impl Into<String>) -> Result<Self> {
        Self::new(RuntimeConfig::new(data_dir)).await
    }

    /// Get the partition resolver for owner event stores.
    pub fn event_partitions(&self) -> Arc<EventPartitions> {
        Arc::clone(&self.event_partitions)
    }

    /// Get the notifications store for recording and querying stream events
    pub fn notifications_store(&self) -> Arc<NotificationsStore> {
        Arc::clone(&self.notifications_store)
    }

    /// Get the stream event sender for projectors
    pub fn stream_event_sender(&self) -> StreamEventSender {
        self.stream_event_sender.clone()
    }

    /// Get the stream event subscriber for gRPC streaming service
    pub fn stream_event_subscriber(&self) -> StreamEventSubscriber {
        self.stream_event_subscriber.clone()
    }

    /// Take the broadcast loop to spawn it
    ///
    /// This consumes the broadcast loop from the runtime.
    /// The loop should be spawned as a background task.
    pub fn take_broadcast_loop(&mut self) -> Option<StreamEventBroadcastLoop> {
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
    use crate::broadcast::StreamEvent;
    use crate::NewEvent;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_runtime_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();

        // Verify we can access components
        let _event_partitions = runtime.event_partitions();
        let _notifications_store = runtime.notifications_store();
        let _sender = runtime.stream_event_sender();
        let _subscriber = runtime.stream_event_subscriber();
    }

    #[tokio::test]
    async fn test_runtime_event_store_works() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_str().unwrap();

        let runtime = EventsRuntime::with_data_dir(data_dir).await.unwrap();
        let event_partitions = runtime.event_partitions();
        let partition = event_partitions
            .ensure_exists("runtime", "stream-1")
            .await
            .unwrap();
        let event_store = partition.open().await.unwrap();

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

        let result = event_store
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
}
