//! EventsRuntime wires together durable event streams, notifications, and broadcast.
//!
//! This module provides a convenient way to initialize and run the events infrastructure.
//! Services use EventsRuntime to get access to:
//! - EventNamespaces for resolving partitioned event streams
//! - NotificationsStore for recording and querying stream events
//! - StreamEventSender for event handlers to send stream events
//! - StreamEventSubscriber for gRPC streaming services

use crate::broadcast::{
    create_broadcast_system, StreamEventBroadcastLoop, StreamEventSender, StreamEventSubscriber,
};
use crate::notifications_store::NotificationsStore;
use crate::rotation::RotationPolicy;
use crate::{EsError, EventNamespaces, Result};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Default progress notification TTL (5 minutes)
pub const DEFAULT_PROGRESS_NOTIFICATION_TTL: Duration = Duration::from_secs(300);

/// Default rotation policy (1 hour windows)
pub fn default_rotation_policy() -> RotationPolicy {
    RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: None,
    }
}

/// Options for the progress notification maintenance worker.
#[derive(Debug, Clone)]
pub struct NotificationMaintenanceOptions {
    /// How often expired progress notifications should be deleted.
    cleanup_interval: Duration,
}

impl NotificationMaintenanceOptions {
    /// Create options with an explicit cleanup interval.
    pub fn new(cleanup_interval: Duration) -> Result<Self> {
        if cleanup_interval.is_zero() {
            return Err(EsError::InvalidDuration {
                field: "notification maintenance cleanup_interval".to_string(),
                message: "expected a duration greater than zero".to_string(),
            });
        }

        Ok(Self { cleanup_interval })
    }

    /// How often expired progress notifications should be deleted.
    pub fn cleanup_interval(&self) -> Duration {
        self.cleanup_interval
    }
}

/// Configuration for EventsRuntime
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Data directory for databases
    pub data_dir: String,
    /// Progress notification TTL for reconnection queries.
    pub progress_notification_ttl: Duration,
    /// Rotation policy for event stream partitions.
    pub rotation_policy: RotationPolicy,
}

impl RuntimeConfig {
    /// Create a new config with the given data directory
    pub fn new(data_dir: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
            progress_notification_ttl: DEFAULT_PROGRESS_NOTIFICATION_TTL,
            rotation_policy: default_rotation_policy(),
        }
    }

    /// Set the progress notification TTL.
    pub fn with_progress_notification_ttl(mut self, ttl: Duration) -> Self {
        self.progress_notification_ttl = ttl;
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
    /// Namespace resolver for domain event streams
    event_namespaces: Arc<EventNamespaces>,
    /// Notifications store for stream events (progress + completion)
    notifications_store: Arc<NotificationsStore>,
    /// Sender for event handlers to send stream events
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

        // Initialize event namespaces (events/ subdirectory)
        let events_path = format!("{}/events", config.data_dir);
        let event_namespaces = EventNamespaces::open(&events_path, config.rotation_policy).await?;

        // Initialize notifications store (stream_events/ subdirectory)
        let stream_events_path = Path::new(&config.data_dir).join("stream_events");
        std::fs::create_dir_all(&stream_events_path)?;
        let notifications_store =
            NotificationsStore::with_ttl(&stream_events_path, config.progress_notification_ttl)
                .await?;

        // Create broadcast system
        let (stream_event_sender, stream_event_subscriber, broadcast_loop) =
            create_broadcast_system();

        Ok(Self {
            event_namespaces: Arc::new(event_namespaces),
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

    /// Get the namespace resolver for event streams.
    pub fn event_namespaces(&self) -> Arc<EventNamespaces> {
        Arc::clone(&self.event_namespaces)
    }

    /// Get the notifications store for recording and querying stream events
    pub fn notifications_store(&self) -> Arc<NotificationsStore> {
        Arc::clone(&self.notifications_store)
    }

    /// Get the stream event sender for event handlers.
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

    /// Start the progress notification maintenance worker.
    ///
    /// The worker periodically deletes expired records from `NotificationsStore`
    /// until the shutdown signal is set to `true` or all senders are dropped.
    pub fn start_notification_maintenance_worker(
        &self,
        options: NotificationMaintenanceOptions,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let notifications = self.notifications_store();

        tokio::spawn(async move {
            if *shutdown.borrow() {
                tracing::info!("Progress notification maintenance worker shutting down");
                return;
            }

            let mut interval = tokio::time::interval(options.cleanup_interval());

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match notifications.cleanup().await {
                            Ok(deleted) if deleted > 0 => {
                                tracing::debug!(deleted, "Cleaned up expired progress notifications");
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::error!(%error, "Failed to clean up expired progress notifications");
                            }
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            tracing::info!("Progress notification maintenance worker shutting down");
                            break;
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod runtime_tests;
