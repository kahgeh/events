# Progress Streaming Architecture

This document explains the architecture and design decisions behind the progress streaming system, which enables real-time feedback for async operations.

## Overview

The progress streaming system provides a way to communicate operation progress from backend projectors to frontend clients. It solves the fundamental challenge of async operations: keeping users informed about what's happening when operations take time.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Progress Streaming Flow                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    mpsc     ┌──────────────┐  broadcast  ┌──────────────┐ │
│  │  Projector   │────────────▶│  Broadcast   │────────────▶│  Subscribing │ │
│  │  (Producer)  │             │    Loop      │             │    Client    │ │
│  └──────────────┘             └──────────────┘             │  (e.g. SSE)  │ │
│         │                                                  └──────────────┘ │
│         │ record                                                             │
│         ▼                                                                    │
│  ┌──────────────┐                                                            │
│  │ Notifications│◀──────────────────────────────────────────┐                │
│  │    Store     │                                 get()     │                │
│  │  (TTL-based) │                                           │                │
│  └──────────────┘                                  ┌────────┴─────────────┐  │
│                                                    │ Client (on reconnect) │  │
│                                                    └──────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Why Progress Streaming?

### The Problem

When a user initiates a long-running operation (e.g., provisioning a cloud resource), several challenges arise:

1. **User Uncertainty**: Without feedback, users don't know if the operation is working
2. **Retry Storms**: Users may retry operations, causing duplicate work
3. **Disconnection Recovery**: If the client disconnects, they lose all progress context
4. **Multi-step Visibility**: Complex operations have multiple stages users should see

### The Solution

Progress streaming addresses these challenges through:

- **Real-time Updates**: Stream progress as it happens via Server-Sent Events (SSE)
- **Reconnection Support**: Store recent progress for clients that reconnect
- **Structured Events**: Typed events with step counts, names, and completion status
- **Batch Operations**: Track individual item progress within batch operations

## Core Components

### StreamEvent

The fundamental unit of progress communication:

```rust
pub struct StreamEvent {
    pub request_id: String,      // Correlates with the original request
    pub stream_id: String,       // Domain event stream (e.g., "user:123")
    pub timestamp: i64,          // Unix timestamp
    pub kind: EventKind,         // Progress, Completed, or Failed
    pub current_step: u32,       // 1-indexed step number
    pub total_steps: u32,        // Total steps in operation
    pub step_name: String,       // Human-readable step name
    pub payload: Option<Value>,  // Completion payload (for Completed)
    pub error_message: Option<String>,  // Error details (for Failed)
    pub retriable: Option<bool>, // Can the operation be retried?
    pub items: Vec<ItemProgress>, // Per-item progress (for batches)
}
```

### EventKind

Three terminal states for operations:

| Kind | Meaning | Terminal? |
|------|---------|-----------|
| `Progress` | Intermediate update | No |
| `Completed` | Operation succeeded | Yes |
| `Failed` | Operation failed | Yes |

### Two-Channel Design

The system uses two separate channels for different purposes:

#### 1. mpsc Channel (Projector → Broadcast Loop)

- **Type**: `tokio::sync::mpsc`
- **Purpose**: Collect events from multiple projectors
- **Capacity**: 256 events (configurable)
- **Sender**: `StreamEventSender` (cloneable, used by projectors)

#### 2. broadcast Channel (Broadcast Loop → Subscribers)

- **Type**: `tokio::sync::broadcast`
- **Purpose**: Fan out events to all connected clients
- **Capacity**: 1024 events (configurable)
- **Receiver**: Created via `StreamEventSubscriber::subscribe()`

**Why two channels?**

- mpsc is efficient for many-to-one collection
- broadcast handles one-to-many distribution
- Separation allows independent capacity tuning
- broadcast receivers can be dropped without affecting others

### NotificationsStore

A separate database for transient progress events:

```rust
pub struct NotificationsStore {
    db: Database,
    ttl: Duration,  // Default: 5 minutes
}
```

**Key characteristics:**

- **TTL-based expiration**: Events auto-expire after configurable duration
- **UPSERT semantics**: Newer events for same request_id replace older ones
- **Single record per request**: Only stores the latest event
- **Separate from EventStore**: Not part of the permanent event sourcing journal

**Why separate storage?**

1. **Different lifecycles**: Progress events are transient; domain events are permanent
2. **Different query patterns**: Progress is queried by request_id; events by stream_id
3. **Different retention**: Progress expires quickly; events are kept indefinitely
4. **Performance isolation**: High-frequency progress updates don't affect event store

### EventsRuntime

The orchestrator that wires everything together:

```rust
pub struct EventsRuntime {
    event_store: Arc<EventStore>,           // Domain events
    notifications_store: Arc<NotificationsStore>,  // Progress events
    stream_event_sender: StreamEventSender,  // For projectors
    stream_event_subscriber: StreamEventSubscriber,  // For services
    broadcast_loop: Option<StreamEventBroadcastLoop>,
}
```

## Data Flow

### Live Progress Updates

```
1. Projector processes domain event
2. Projector creates StreamEvent::progress(...)
3. StreamEventSender.send(event) → mpsc channel
4. Broadcast loop receives event
5. Broadcast loop wraps in Arc and broadcasts
6. All subscribers receive Arc<StreamEvent>
7. Service sends to client (e.g., SSE)
```

### Reconnection Recovery

```
1. Client disconnects during operation
2. Client reconnects with request_id
3. Service queries NotificationsStore.get(request_id)
4. If event exists and not expired, return it
5. Client catches up on missed progress
6. Client subscribes for future updates
```

### Completion Flow

```
1. Projector processes final domain event
2. Projector creates StreamEvent::completed(...) with payload
3. Event sent through both channels:
   a. Broadcast for live subscribers
   b. NotificationsStore for reconnection queries
4. Subscribers receive completion, close connection
```

## Design Decisions

### Arc-wrapped Events

Events are wrapped in `Arc<StreamEvent>` before broadcasting:

```rust
let event = Arc::new(event);
self.broadcast_tx.send(event);
```

**Rationale**: Multiple subscribers receive the same event. Arc prevents copying the event data for each subscriber, especially important for events with large payloads.

### No Subscriber = Drop Event

When there are no subscribers, events are logged and dropped:

```rust
if subscriber_count == 0 {
    tracing::debug!("No subscribers for stream event");
    continue;
}
```

**Rationale**: Progress events are ephemeral. If no one is listening, there's no point storing them in the broadcast buffer. The NotificationsStore provides persistence for reconnection.

### Single Record per Request

NotificationsStore only keeps the latest event per request_id:

```sql
ON CONFLICT(request_id) DO UPDATE SET ...
```

**Rationale**:
- Clients only need the current state, not history
- Reduces storage requirements
- Simplifies reconnection logic

### TTL-based Expiration

Events expire after a configurable duration (default 5 minutes):

```rust
let expires_at = now + self.ttl.as_secs() as i64;
```

**Rationale**:
- Progress events lose value quickly after operation completes
- Prevents unbounded storage growth
- Aligns with typical session/reconnection windows

### Channel Capacity Choices

| Channel | Default Capacity | Rationale |
|---------|-----------------|-----------|
| mpsc (sender) | 256 | Balance between memory and burst handling |
| broadcast | 1024 | Larger buffer for slow subscribers |

**Backpressure behavior**:
- mpsc: `send()` awaits if full, `try_send()` returns error
- broadcast: Oldest events dropped when full (lagging receivers)

## Batch Operation Support

For operations affecting multiple items:

```rust
pub struct ItemProgress {
    pub item_id: String,
    pub status: ItemStatus,  // Pending, InProgress, Completed, Failed
    pub message: String,
}
```

This enables UI patterns like:

```
Provisioning 3 machines...
  ✓ machine-1: Created
  ⟳ machine-2: Starting
  ○ machine-3: Pending
```

## Integration Points

### With Projectors

Projectors send progress events during domain event processing:

```rust
async fn process_user_app_provisioned(&self, event: &EventEnvelope) {
    // ... process event ...

    let progress = StreamEvent::progress(
        request_id,
        stream_id,
        2, 3,
        "Creating machine".to_string(),
    );
    self.sender.send(progress).await?;
}
```

### With gRPC Services

Services use the subscriber for streaming responses:

```rust
async fn stream_progress(
    &self,
    request: Request<StreamRequest>,
) -> Result<Response<Self::StreamProgressStream>, Status> {
    let mut rx = self.subscriber.subscribe();
    let request_id = request.into_inner().request_id;

    let stream = async_stream::stream! {
        while let Ok(event) = rx.recv().await {
            if event.request_id == request_id {
                yield Ok(to_proto(&event));
                if event.is_terminal() {
                    break;
                }
            }
        }
    };

    Ok(Response::new(Box::pin(stream)))
}
```

### With SSE Handlers

Web application handlers use subscriber for Server-Sent Events:

```rust
async fn sse_progress(
    State(subscriber): State<StreamEventSubscriber>,
    Path(request_id): Path<String>,
) -> Sse<impl Stream<Item = Event>> {
    let mut rx = subscriber.subscribe();

    let stream = async_stream::stream! {
        while let Ok(event) = rx.recv().await {
            if event.request_id == request_id {
                yield Event::default().json_data(&event);
                if event.is_terminal() {
                    break;
                }
            }
        }
    };

    Sse::new(stream)
}
```

## Error Handling

### Channel Errors

| Error | Cause | Recovery |
|-------|-------|----------|
| `ChannelClosed` | Broadcast loop stopped | System error, restart required |
| `ChannelFull` | Buffer overflow | Drop event, log warning |
| `RecvError::Lagged` | Subscriber too slow | Skip missed events, continue |

### Store Errors

NotificationsStore operations can fail due to:
- Database I/O errors
- Serialization errors (malformed JSON)
- TTL expiration (treated as "not found")

## Performance Considerations

### Memory Usage

- Each StreamEvent: ~200-500 bytes (depending on payload)
- Arc overhead: 16 bytes per reference
- Broadcast buffer: capacity × event size

### Throughput

- mpsc channel: millions of messages per second
- broadcast channel: depends on subscriber count
- NotificationsStore: bounded by Turso write throughput

### Latency

- Channel operations: microseconds
- Store operations: milliseconds
- End-to-end (projector → client): ~1-10ms typical

## Related Documentation

- [How to Stream Progress Updates](../how-to/stream-progress-updates.md) - Implementation guide
- [API Reference](../reference/api.md) - Complete type documentation
- [Architecture Overview](architecture.md) - General system architecture
