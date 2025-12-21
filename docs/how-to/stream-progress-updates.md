# Stream Progress Updates

This guide shows how to implement progress streaming for async operations, enabling real-time feedback to users.

## What You'll Learn

- Setting up EventsRuntime
- Sending progress events from projectors
- Subscribing to progress in services
- Handling client reconnection
- Implementing batch operation progress

## Before You Start

- Understand [event projections](implement-projection.md)
- Familiarity with async Rust and Tokio
- Review the [Progress Streaming Architecture](../explanation/progress-streaming.md)

## Setting Up EventsRuntime

EventsRuntime wires together the event store, notifications store, and broadcast system.

### Basic Setup

```rust
use events::{EventsRuntime, RuntimeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create runtime with default configuration
    let mut runtime = EventsRuntime::with_data_dir("./data").await?;

    // Spawn the broadcast loop as a background task
    let _broadcast_handle = runtime.spawn_broadcast_loop();

    // Access components
    let event_store = runtime.event_store();
    let notifications_store = runtime.notifications_store();
    let sender = runtime.stream_event_sender();
    let subscriber = runtime.stream_event_subscriber();

    Ok(())
}
```

### Custom Configuration

```rust
use events::{RuntimeConfig, RotationPolicy};
use std::time::Duration;

let config = RuntimeConfig::new("./data")
    .with_events_store_ttl(Duration::from_secs(600))  // 10 minutes
    .with_rotation_policy(RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: Some(512 * 1024 * 1024),
    });

let mut runtime = EventsRuntime::new(config).await?;
```

## Sending Progress from Projectors

### Basic Progress Events

Send progress events as your projector processes domain events:

```rust
use events::broadcast::{StreamEvent, StreamEventSender};

pub struct ProvisioningProjector {
    sender: StreamEventSender,
}

impl ProvisioningProjector {
    pub async fn process_app_creation_requested(
        &self,
        request_id: &str,
        stream_id: &str,
    ) -> Result<(), EsError> {
        // Step 1: Validating request
        let progress = StreamEvent::progress(
            request_id.to_string(),
            stream_id.to_string(),
            1,  // current step
            4,  // total steps
            "Validating request".to_string(),
        );
        self.sender.send(progress).await?;

        // ... do validation work ...

        // Step 2: Creating app
        let progress = StreamEvent::progress(
            request_id.to_string(),
            stream_id.to_string(),
            2,
            4,
            "Creating application".to_string(),
        );
        self.sender.send(progress).await?;

        // ... create app ...

        Ok(())
    }
}
```

### Completion Events

Send a completion event when the operation succeeds:

```rust
use serde_json::json;

pub async fn process_app_created(
    &self,
    request_id: &str,
    stream_id: &str,
    app_id: &str,
    app_url: &str,
) -> Result<(), EsError> {
    let completion = StreamEvent::completed(
        request_id.to_string(),
        stream_id.to_string(),
        4,  // total_steps
        Some(json!({
            "app_id": app_id,
            "url": app_url,
        })),
    );
    self.sender.send(completion).await?;

    Ok(())
}
```

### Failure Events

Report failures with retry information:

```rust
pub async fn process_app_creation_failed(
    &self,
    request_id: &str,
    stream_id: &str,
    error: &str,
    current_step: u32,
    retriable: bool,
) -> Result<(), EsError> {
    let failure = StreamEvent::failed(
        request_id.to_string(),
        stream_id.to_string(),
        current_step,
        4,
        error.to_string(),
        retriable,
    );
    self.sender.send(failure).await?;

    Ok(())
}
```

## Recording for Reconnection

Always record events to NotificationsStore for clients that reconnect:

```rust
use events::NotificationsStore;
use std::sync::Arc;

pub struct ProvisioningProjector {
    sender: StreamEventSender,
    notifications: Arc<NotificationsStore>,
}

impl ProvisioningProjector {
    pub async fn send_progress(&self, event: StreamEvent) -> Result<(), EsError> {
        // Record to store first (for reconnection support)
        self.notifications.record(&event).await?;

        // Then broadcast to live subscribers
        self.sender.send(event).await?;

        Ok(())
    }
}
```

## Subscribing to Progress

### In a gRPC Streaming Service

```rust
use events::broadcast::StreamEventSubscriber;
use tokio_stream::StreamExt;

pub struct ProgressService {
    subscriber: StreamEventSubscriber,
    notifications: Arc<NotificationsStore>,
}

impl ProgressService {
    pub async fn stream_progress(
        &self,
        request_id: String,
    ) -> impl Stream<Item = StreamEvent> {
        let mut rx = self.subscriber.subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                // Filter for our request
                if event.request_id == request_id {
                    yield (*event).clone();

                    // Stop on terminal events
                    if event.is_terminal() {
                        break;
                    }
                }
            }
        }
    }
}
```

### In an Axum SSE Handler

```rust
use axum::{
    extract::{Path, State},
    response::sse::{Event, Sse},
};
use futures::Stream;

pub async fn sse_progress(
    State(subscriber): State<StreamEventSubscriber>,
    Path(request_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let mut rx = subscriber.subscribe();

    let stream = async_stream::stream! {
        while let Ok(event) = rx.recv().await {
            if event.request_id == request_id {
                let data = serde_json::to_string(&*event).unwrap_or_default();
                yield Ok(Event::default().data(data));

                if event.is_terminal() {
                    break;
                }
            }
        }
    };

    Sse::new(stream)
}
```

## Handling Client Reconnection

When a client reconnects, check NotificationsStore for the latest state:

```rust
pub async fn handle_reconnection(
    &self,
    request_id: &str,
) -> Result<Option<StreamEvent>, EsError> {
    // Check for existing progress
    if let Some(event) = self.notifications.get(request_id).await? {
        // If operation already completed, return immediately
        if event.is_terminal() {
            return Ok(Some(event));
        }

        // Otherwise, return current progress and continue streaming
        return Ok(Some(event));
    }

    // No existing progress found
    Ok(None)
}

pub async fn stream_with_reconnection(
    &self,
    request_id: String,
) -> impl Stream<Item = StreamEvent> {
    let notifications = Arc::clone(&self.notifications);
    let mut rx = self.subscriber.subscribe();

    async_stream::stream! {
        // First, check for existing progress
        if let Ok(Some(event)) = notifications.get(&request_id).await {
            yield event.clone();

            // If already terminal, we're done
            if event.is_terminal() {
                return;
            }
        }

        // Then stream live updates
        while let Ok(event) = rx.recv().await {
            if event.request_id == request_id {
                yield (*event).clone();

                if event.is_terminal() {
                    break;
                }
            }
        }
    }
}
```

## Batch Operation Progress

For operations affecting multiple items, use ItemProgress:

```rust
use events::broadcast::{ItemProgress, ItemStatus, StreamEvent};

pub async fn process_batch_provision(
    &self,
    request_id: &str,
    stream_id: &str,
    machine_ids: &[&str],
) -> Result<(), EsError> {
    let total = machine_ids.len() as u32;

    for (index, machine_id) in machine_ids.iter().enumerate() {
        // Build item progress list
        let items: Vec<ItemProgress> = machine_ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let status = if i < index {
                    ItemStatus::Completed
                } else if i == index {
                    ItemStatus::InProgress
                } else {
                    ItemStatus::Pending
                };

                ItemProgress {
                    item_id: id.to_string(),
                    status,
                    message: match status {
                        ItemStatus::Completed => "Created".to_string(),
                        ItemStatus::InProgress => "Creating...".to_string(),
                        ItemStatus::Pending => "Waiting".to_string(),
                        ItemStatus::Failed => "Failed".to_string(),
                    },
                }
            })
            .collect();

        let progress = StreamEvent::progress(
            request_id.to_string(),
            stream_id.to_string(),
            (index + 1) as u32,
            total,
            format!("Creating machine {}", machine_id),
        )
        .with_items(items);

        self.sender.send(progress).await?;

        // ... create machine ...
    }

    Ok(())
}
```

## Non-blocking Send

For high-throughput scenarios, use `try_send` to avoid blocking:

```rust
use events::broadcast::StreamEventSendError;

pub async fn send_progress_nonblocking(&self, event: StreamEvent) {
    match self.sender.try_send(event) {
        Ok(()) => {}
        Err(StreamEventSendError::ChannelFull) => {
            tracing::warn!("Progress channel full, dropping event");
        }
        Err(StreamEventSendError::ChannelClosed) => {
            tracing::error!("Progress channel closed");
        }
    }
}
```

## Cleanup Expired Events

Run periodic cleanup to remove expired events from NotificationsStore:

```rust
pub async fn run_cleanup_task(notifications: Arc<NotificationsStore>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        match notifications.cleanup().await {
            Ok(deleted) if deleted > 0 => {
                tracing::debug!(deleted = deleted, "Cleaned up expired notifications");
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Failed to cleanup notifications");
            }
        }
    }
}
```

## Complete Example

Here's a complete example tying everything together:

```rust
use events::{
    broadcast::{StreamEvent, StreamEventSender},
    EventsRuntime, NotificationsStore,
};
use std::sync::Arc;
use std::time::Duration;

pub struct AppService {
    runtime: EventsRuntime,
}

impl AppService {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut runtime = EventsRuntime::with_data_dir("./data").await?;

        // Start broadcast loop
        runtime.spawn_broadcast_loop();

        // Start cleanup task
        let notifications = runtime.notifications_store();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let _ = notifications.cleanup().await;
            }
        });

        Ok(Self { runtime })
    }

    pub fn sender(&self) -> StreamEventSender {
        self.runtime.stream_event_sender()
    }

    pub fn subscriber(&self) -> StreamEventSubscriber {
        self.runtime.stream_event_subscriber()
    }

    pub fn notifications(&self) -> Arc<NotificationsStore> {
        self.runtime.notifications_store()
    }
}
```

## Best Practices

1. **Always record to NotificationsStore**: Clients may reconnect at any time
2. **Use meaningful step names**: They appear in the UI
3. **Include retry information**: Help users know if they can retry
4. **Keep payloads small**: Large payloads impact memory and network
5. **Handle channel errors gracefully**: Log and continue, don't crash
6. **Set appropriate TTL**: Balance between storage and reconnection window
7. **Clean up periodically**: Run cleanup task to prevent unbounded growth

## Troubleshooting

### Events Not Received

1. Verify broadcast loop is running
2. Check subscriber count: `subscriber.subscriber_count()`
3. Verify request_id filtering matches

### Reconnection Returns None

1. Check TTL hasn't expired
2. Verify event was recorded to NotificationsStore
3. Check request_id matches exactly

### Channel Full Errors

1. Increase channel capacity
2. Use `try_send` for non-critical updates
3. Check for slow subscribers

## Next Steps

- [Progress Streaming Architecture](../explanation/progress-streaming.md) - Understand the design
- [API Reference](../reference/api.md) - Complete type documentation
- [Handle Concurrency](handle-concurrency.md) - Concurrent event processing
