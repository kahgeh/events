# Progress Streaming Architecture

Progress streaming gives clients real-time feedback for asynchronous work while
keeping the durable event log focused on facts. It is a notification layer, not a
second event store.

## Overview

Long-running commands often append a durable event quickly and finish later in a
projector or worker. Users still need to know what is happening: whether work
started, which step is running, whether it completed, and whether a failed step
can be retried.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Progress Streaming Flow                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐   mpsc    ┌──────────────┐  broadcast  ┌───────────────┐  │
│  │  Projector   │──────────▶│  Broadcast   │────────────▶│  Subscribing  │  │
│  │  or Worker   │           │    Loop      │             │    Client     │  │
│  └──────────────┘           └──────────────┘             └───────────────┘  │
│         │                                                                   │
│         │ record latest request status                                      │
│         ▼                                                                   │
│  ┌──────────────┐                                                           │
│  │ Notifications│◀─────────────────────────────────────────┐                │
│  │    Store     │                                get()     │                │
│  │  (TTL-based) │                                          │                │
│  └──────────────┘                                 ┌────────┴──────┐         │
│                                                   │  Subscribing  │         │
│                                                   │    Client     │         │
│                                                   │ (reconnecting)│         │
│                                                   └───────────────┘         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Why Progress Streaming?

### The Problem

Without progress streaming, a slow operation creates several product and
operational problems:

1. Users cannot tell whether the request is still running.
2. Users may retry and create duplicate work.
3. A reconnecting browser has no current operation state.
4. Multi-step work is opaque, so support cannot tell where it failed.

The durable event log should not solve all of that. Durable events are facts
that must be retained. Progress updates are transient request state.

### The Solution

Progress streaming separates request feedback from durable event storage:

- `EventLog` stores durable facts.
- `NotificationsStore` stores the latest request status for reconnect windows.
- `StreamEventBroadcastLoop` fans out live notifications.
- Application read models remain the source of durable recovery.

## Core Components

### StreamEvent

`StreamEvent` is the request-progress message sent to clients:

```rust
let event = StreamEvent::progress(
    request_id,
    "orders/order-123".to_string(),
    2,
    4,
    "Authorizing payment".to_string(),
);
```

It carries a request ID, display/correlation metadata, current step, total step
count, optional payload/error detail, and optional per-item batch progress.

### EventKind

`EventKind` identifies whether the update is intermediate or terminal:

- `Progress`
- `Completed`
- `Failed`

`Completed` and `Failed` are terminal.

### Two-Channel Design

Progress streaming uses two channels:

1. an `mpsc` channel from workers/projectors into the broadcast loop
2. a `broadcast` channel from the loop to subscribers

This keeps producer backpressure separate from subscriber fan-out.

### NotificationsStore

`NotificationsStore` records the latest event per request ID:

```rust
notifications.record(&event).await?;
```

A reconnecting client can query:

```rust
if let Some(last_seen) = notifications.get(&request_id).await? {
    send_to_client(last_seen).await?;
}
```

Notifications expire by TTL. They are not projection checkpoints.

### EventsRuntime

`EventsRuntime` wires together:

- `EventNamespaces`
- `NotificationsStore`
- `StreamEventSender`
- `StreamEventSubscriber`
- `StreamEventBroadcastLoop`

## Data Flow

### Live Progress Updates

```
1. Command appends a durable event to the selected partition store.
2. Projector or worker handles the event.
3. Worker records the latest request status in NotificationsStore.
4. Worker sends StreamEvent::progress(...).
5. Broadcast loop forwards the event to live subscribers.
6. Client renders the current step.
```

### Reconnection Recovery

```
1. Client reconnects with request_id.
2. Service queries NotificationsStore::get(request_id).
3. If an unexpired record exists, service returns the latest status.
4. Client subscribes for future broadcast updates.
```

### Completion Flow

Terminal events should be recorded before they are broadcast:

```rust
let completed = StreamEvent::completed(request_id, context, total_steps, payload);
notifications.record(&completed).await?;
sender.send(completed).await?;
```

## Design Decisions

### Arc-wrapped Events

Broadcast events are wrapped in `Arc` internally so multiple subscribers can
receive the same event without copying large payloads.

### No Subscriber = Drop Event

Live broadcast is best-effort. If no subscribers are connected, the live message
does not need to be retained by the broadcast channel. Reconnect uses
`NotificationsStore`.

### Single Record per Request

The store keeps the latest event per request ID. Clients need current operation
state, not a permanent progress history.

### TTL-based Expiration

Progress records expire after the configured TTL. Durable recovery comes from
event-log events and application read models.

### Channel Capacity Choices

`create_broadcast_system()` uses bounded capacities. Use
`create_broadcast_system_with_capacity(sender, broadcast)` when a service needs
different backpressure behavior.

## Batch Operation Support

Batch work can attach item-level progress to the request-level event:

```rust
let event = StreamEvent::progress(request_id, context, 2, 4, "Processing rows".into())
    .with_items(vec![
        ItemProgress {
            item_id: "row-1".into(),
            status: ItemStatus::Completed,
            message: "Imported".into(),
        },
    ]);
```

## Integration Points

### With Projectors

Projectors can send progress while draining event-log events. Their durable
offsets still belong in the application database.

### With gRPC Services

gRPC streaming services can subscribe to the broadcast channel, filter by
request ID, and stop when a terminal event arrives.

### With SSE Handlers

SSE handlers follow the same pattern: replay the latest notification on
reconnect, then subscribe and filter live events by request ID.

## Error Handling

### Channel Errors

`StreamEventSendError::ChannelFull` means non-blocking send could not enqueue the
event. `ChannelClosed` means the broadcast loop has stopped.

### Store Errors

`NotificationsStore` errors should be handled according to the product contract.
For user-visible operations, record-before-broadcast gives reconnecting clients a
consistent latest status.

## Performance Considerations

### Memory Usage

Broadcast capacity bounds retained live messages. Notification TTL bounds
reconnect storage.

### Throughput

Use non-blocking sends for low-priority progress updates when producer latency is
more important than every intermediate notification.

### Latency

Progress streaming is in-process and channel-based. Durable recovery is not on
the live broadcast path.

## Related Documentation

- [Stream Progress Updates](../how-to/stream-progress-updates.md)
- [Implement Robust Event Projections](../how-to/implement-projection.md)
- [API Reference](../reference/api.md)
