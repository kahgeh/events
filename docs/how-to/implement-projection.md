# Implement Robust Event Projections

Projections transform event-stream events into queryable read models. This
guide shows how to build reliable application-owned projections with bounded
reads from `EventStream`.

## What You'll Learn

- Designing read models for event streams
- Storing projection offsets in the application database
- Processing events idempotently
- Handling retries and crash recovery
- Scaling projection workers by partition key

## Before You Start

Complete [Building Projections](../tutorial/building-projections.md) first. You
should already have:

- an `EventNamespaces` resolver
- a partition key to project
- an application database for the read model

## Projection Design Patterns

Use one of these patterns, or combine them, before writing the processing loop.

### 1. Materialized View Pattern

Create denormalized tables optimized for queries:

```rust
async fn process_order_created(
    conn: &turso::Connection,
    event: &EventEnvelope,
) -> Result<(), EsError> {
    let payload: OrderCreated = serde_json::from_value(event.payload.clone())?;

    conn.execute(
        r#"
        INSERT INTO order_status_view
            (order_id, status, total_amount, last_event_version)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(order_id) DO UPDATE SET
            status = excluded.status,
            total_amount = excluded.total_amount,
            last_event_version = excluded.last_event_version
        "#,
        (
            payload.order_id,
            "created",
            payload.total_amount,
            event.version.get(),
        ),
    )
    .await?;

    Ok(())
}
```

### 2. Aggregate Pattern

Maintain running totals:

```rust
async fn process_payment_authorized(
    conn: &turso::Connection,
    event: &EventEnvelope,
) -> Result<(), EsError> {
    let payload: PaymentAuthorized = serde_json::from_value(event.payload.clone())?;

    conn.execute(
        r#"
        INSERT INTO daily_sales (day, order_count, total_amount)
        VALUES (?1, 1, ?2)
        ON CONFLICT(day) DO UPDATE SET
            order_count = order_count + 1,
            total_amount = total_amount + excluded.total_amount
        "#,
        (payload.day, payload.amount),
    )
    .await?;

    Ok(())
}
```

### 3. Lookup Table Pattern

Create fast lookup tables for relationships:

```rust
async fn process_client_linked(
    conn: &turso::Connection,
    event: &EventEnvelope,
) -> Result<(), EsError> {
    let payload: ClientLinked = serde_json::from_value(event.payload.clone())?;

    conn.execute(
        "INSERT INTO client_lookup (client_id, display_name) VALUES (?1, ?2)",
        (payload.client_id, payload.display_name),
    )
    .await?;

    Ok(())
}
```

## Idempotent Processing

### Event Tracking Table

If handlers perform external side effects, use an idempotency table:

```sql
CREATE TABLE processed_events (
    projection_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (projection_name, event_id)
);
```

Check this table before side effects and insert into it in the same transaction
as read-model changes when possible.

### Idempotent Processing

Projection handlers should be safe to retry. A process can crash after applying
an event but before saving the offset, so the next run may see that event again.

Use event IDs, workflow starter IDs, or natural read-model keys to make repeated
handling safe.

## Checkpoint Management

### Storing Checkpoints

Projection checkpoints belong in the application database. Store the last
successfully committed `EventStreamVersion` per partition key and projection name:

```sql
CREATE TABLE projection_offsets (
    projection_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_projected_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (projection_name, namespace, partition_key)
);
```

Use `0` in the database to mean `EventStreamVersion::start()`.

### Bootstrap from Checkpoint

Load the offset before each drain pass:

```rust
loop {
    let offset = load_offset(&app_db, projection, namespace, partition_key).await?;
    let cursor = match offset {
        0 => EventStreamVersion::start(),
        version => EventStreamVersion::new(version)?,
    };

    let events = stream.load_after_version(cursor, 500).await?;
    if events.is_empty() {
        break;
    }

    let last_version = events.last().expect("non-empty batch").version;

    app_db.execute("BEGIN IMMEDIATE", ()).await?;
    for event in &events {
        apply_event(&app_db, event).await?;
    }
    save_offset(&app_db, projection, namespace, partition_key, last_version).await?;
    app_db.execute("COMMIT", ()).await?;
}
```

## Error Handling and Recovery

### Handler Error Contract

- If event handling fails before the application transaction commits, do not
  advance the offset.
- If the process crashes after committing, the saved offset prevents duplicate
  read-model changes.
- If side effects happen outside the transaction, make the side effect
  idempotent by event ID or workflow starter ID.

### Custom Retry Mechanism

Retry by leaving the saved offset unchanged, fixing the underlying problem, and
draining the same partition again.

### Dead Letter Queue

If a projection cannot process a valid event after retry, write a diagnostic row
to an application-owned dead-letter table and alert an operator. Do not advance
the projection offset unless the application intentionally skips that event.

## Performance Optimization

### Batch Processing

Read bounded batches:

```rust
let events = stream.load_after_version(cursor, 500).await?;
```

The crate rejects zero and oversized limits. Choose a batch size that keeps
handler memory use bounded.

### Concurrent Processing

Use one active consumer per projection and partition key, and process that
partition's events serially in `EventStreamVersion` order. Bound the global
number of workers. Dirty keys received while a consumer is already active should
be marked pending and consumed again after the current pass.

See [Worker Pool Over Per-Partition Stores](worker-pool-over-per-partition-store.md).

## Schema Management

### Migration System

Read-model tables, projection offsets, and active workflow state belong to the
application database. Migrate them with the application schema.

## Monitoring and Observability

### Metrics Collection

Track:

- last projected version per partition key
- batch size and drain duration
- handler failures
- pending dirty partition keys
- worker count and queue depth

## Best Practices

- Save read-model changes and projection offsets in the same transaction.
- Keep handlers idempotent.
- Use one active consumer per projection and partition key.
- Process each partition key serially in event-stream version order.
- Keep progress notifications separate from projection offsets.

## Verification

Test these cases:

1. A clean projection reads from `EventStreamVersion::start()`.
2. A second run starts after the committed offset.
3. A handler failure does not advance the offset.
4. A crash after committing the offset does not process committed events again.
5. Two partition keys can be projected independently.

## Next Steps

- [Worker Pool Over Per-Partition Stores](worker-pool-over-per-partition-store.md)
