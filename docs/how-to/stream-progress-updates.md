# Stream Progress Updates

This guide shows how to send live progress updates for a request while keeping durable recovery in the event stream and application read models.

## What You'll Learn

- Setting up `EventsRuntime`
- Sending progress events from event handlers
- Recording notifications for reconnecting clients
- Subscribing to progress in streaming services
- Reporting batch operation progress

## Before You Start

- Review [Progress Streaming Architecture](../explanation/progress-streaming.md).
- Have a request ID that should be observed by the UI.
- Append durable domain events to an `EventStream`.
- Keep durable recovery state in the application database.

## Prerequisites

- A running broadcast loop. The examples below use `EventsRuntime::spawn_broadcast_loop()`.
- A notification maintenance worker for deleting expired reconnect state.
- A client-facing streaming transport such as gRPC or SSE.

## Setting Up EventsRuntime

`EventsRuntime` wires together the event namespace resolver, notification store, broadcast loop, and notification maintenance worker.

### Basic Setup

```rust
let mut runtime = EventsRuntime::with_data_dir("./data").await?;
let _broadcast_handle = runtime.spawn_broadcast_loop();
let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
let _maintenance_handle = runtime.start_notification_maintenance_worker(
    NotificationMaintenanceOptions::new(Duration::from_secs(60))?,
    shutdown_rx,
);

let namespaces = runtime.event_namespaces();
let notifications = runtime.notifications_store();
let sender = runtime.stream_event_sender();
let subscriber = runtime.stream_event_subscriber();
```

### Custom Configuration

```rust
let config = RuntimeConfig::new("./data")
    .with_progress_notification_ttl(Duration::from_secs(600))
    .with_rotation_policy(RotationPolicy::TimeWindow {
        window: Duration::from_secs(3600),
        max_bytes: Some(512 * 1024 * 1024),
    });

let mut runtime = EventsRuntime::new(config).await?;
```

## Sending Progress From Event Handlers

### Basic Progress Events

Append the durable event first. Put the request ID on the event so background
work can correlate progress notifications with the caller.

```rust
let clients = namespaces.ensure_namespace("clients").await?;
let partition = clients.ensure_partition("client-a").await?;
let stream = partition.open().await?;

stream
    .append(ExpectedVersion::Any, [NewEvent {
        r#type: "ClientImportRequested".to_string(),
        payload: serde_json::json!({ "client_id": "client-a" }),
        workflow_kind: None,
        workflow: WorkflowRef::None,
        request_id: Some(request_id.clone()),
        actor_id: "user-42".to_string(),
        actor_type: ActorType::User,
    }])
    .await?;
```

Then send progress as work advances:

```rust
let progress = StreamEvent::progress(
    request_id.clone(),
    "clients/client-a".to_string(),
    1,
    3,
    "Validating client import".to_string(),
);

notifications.record(&progress).await?;
sender.send(progress).await?;
```

The `stream_id` argument on `StreamEvent::progress` is a context string for the progress update. It is commonly the namespace and partition key, such as `clients/client-a`, and a workflow handler can choose a value that also names the workflow run. It is not the durable event ID; durable event IDs live on `EventEnvelope::id`.

### Completion Events

```rust
let completed = StreamEvent::completed(
    request_id.clone(),
    "clients/client-a".to_string(),
    3,
    Some(serde_json::json!({ "status": "queued" })),
);

notifications.record(&completed).await?;
sender.send(completed).await?;
```

### Failure Events

```rust
let failed = StreamEvent::failed(
    request_id.clone(),
    "clients/client-a".to_string(),
    2,
    3,
    "Client import failed".to_string(),
    true,
);

notifications.record(&failed).await?;
sender.send(failed).await?;
```

## Recording for Reconnection

The next examples sit in two different application components. Event handlers or workers publish progress by recording the latest status and sending a live event. Client-facing transports such as gRPC or SSE subscribe to those live events and forward only the matching request ID. For the full component flow, see [Progress Streaming Architecture](../explanation/progress-streaming.md).

Record before broadcasting. That gives reconnecting clients a latest known status even if the live connection drops immediately after the update.

```rust
async fn publish_progress(
    notifications: &NotificationsStore,
    sender: &StreamEventSender,
    event: StreamEvent,
) -> Result<(), EsError> {
    notifications.record(&event).await?;
    sender.send(event).await?;
    Ok(())
}
```

## Subscribing to Progress

Subscription code belongs in the client-facing transport layer, not in the event handler. It receives events from the broadcast loop and filters them for the request being streamed to the client.

### In a gRPC Streaming Service

```rust
let mut rx = subscriber.subscribe();

while let Ok(event) = rx.recv().await {
    if event.request_id != request_id {
        continue;
    }

    send_to_client(&event).await?;

    if event.is_terminal() {
        break;
    }
}
```

### In an Axum SSE Handler

The SSE handler follows the same model: subscribe, filter by request ID, and close after a terminal event.

```rust
let mut rx = subscriber.subscribe();

async_stream::stream! {
    while let Ok(event) = rx.recv().await {
        if event.request_id == request_id {
            yield sse_event(&event);
            if event.is_terminal() {
                break;
            }
        }
    }
}
```

## Handling Client Reconnection

On reconnect, send the latest recorded notification before subscribing to live updates:

```rust
if let Some(last_seen) = notifications.get(&request_id).await? {
    send_to_client(&last_seen).await?;
}

let mut rx = subscriber.subscribe();
```

If no notification exists, recover durable state from the event stream and
application read model. Do not treat progress notifications as workflow state.

## Batch Operation Progress

For batch work, attach item progress to the request-level event:

```rust
let progress = StreamEvent::progress(
    request_id.clone(),
    "imports/import-42".to_string(),
    2,
    4,
    "Processing rows".to_string(),
)
.with_items(vec![
    ItemProgress {
        item_id: "row-1".to_string(),
        status: ItemStatus::Completed,
        message: "Imported".to_string(),
    },
    ItemProgress {
        item_id: "row-2".to_string(),
        status: ItemStatus::InProgress,
        message: "Validating".to_string(),
    },
]);
```

## Non-blocking Send

Use `try_send` for low-priority intermediate updates when the worker should not wait for channel capacity:

```rust
match sender.try_send(progress) {
    Ok(()) => {}
    Err(StreamEventSendError::ChannelFull) => {
        tracing::debug!("dropping intermediate progress update");
    }
    Err(StreamEventSendError::ChannelClosed) => {
        tracing::warn!("progress broadcast loop is not running");
    }
}
```

## Cleanup Expired Events

Expired progress notifications are removed by the notification maintenance worker, not by the broadcast loop. Use a TTL long enough for normal client reconnect windows and short enough that request-progress storage remains bounded.

```rust
let options = NotificationMaintenanceOptions::new(Duration::from_secs(60))?;

let maintenance_handle =
    runtime.start_notification_maintenance_worker(options, shutdown_tx.subscribe());
```

## Complete Example

```rust
async fn publish_client_import_progress(
    runtime: &EventsRuntime,
    request_id: String,
) -> Result<(), EsError> {
    let progress = StreamEvent::progress(
        request_id,
        "clients/client-a".to_string(),
        1,
        3,
        "Validating client import".to_string(),
    );

    runtime.notifications_store().record(&progress).await?;
    runtime.stream_event_sender().send(progress).await?;
    Ok(())
}
```

## Best Practices

- Record terminal events before broadcasting them.
- Keep durable recovery in event-stream events and application read models.
- Use request IDs for client correlation.
- Treat `stream_id` in `StreamEvent` as context metadata for the progress update, such as the partition key or workflow run it relates to.
- Use bounded progress detail for batch work.

## Troubleshooting

### Events Not Received

Check that the broadcast loop is running and that the subscriber filters by the same request ID the worker sends.

### Reconnection Returns None

Check the notification TTL and confirm the worker records events before broadcasting.

### Channel Full Errors

Increase channel capacity or use `try_send` only for updates that can be safely
dropped.

## Verification

Verify both paths:

- Live subscribers receive progress and terminal events for the request ID.
- A reconnecting client can read the most recent notification.
- Restarting the worker recovers durable work from event-stream events and
  application read-model state.
- Terminal events are recorded before broadcast.

## Next Steps

- [Progress Streaming Architecture](../explanation/progress-streaming.md)
- [Implement event handlers](implement-event-handlers.md)
