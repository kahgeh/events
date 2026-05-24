# Scale Consumers for High-Volume Event Streams

Scale projection by scheduling work per partition store. Choosing partition keys
that match owners, accounts, clients, or other independent units is one scaling
strategy.

## What You'll Learn

- How to scale by partition key
- How to schedule one active consumer per projection and partition
- How to use bounded event-stream reads
- How to monitor and recover consumers

## Scaling Patterns

### 1. Partition-based Scaling

Discover partition keys with shallow listing:

```rust
let users = namespaces.ensure_namespace("users").await?;
let descriptors = users.list_partitions().await?;
```

Schedule one active consumer per projection and partition key. The consumer
reads bounded batches:

```rust
let events = stream.load_after_version(last_projected, 500).await?;
```

Commit read-model changes and the partition offset in the application database.

### 2. Event-type Based Scaling

Filter by event type inside the projection handler when the same event stream
feeds multiple read models. Keep the offset per projection name and partition
key.

### 3. Stream-based Scaling

Use partition keys as the scheduling boundary. A plain application can use one
stable key; high-volume systems can choose keys that match owners, accounts,
clients, or other independent units of work.

## Load Balancing Strategies

### Consistent Hashing

For multiple workers, assign partition keys deterministically. Worker placement
is operational; correctness comes from keeping one active consumer for each
projection and partition key, then handling that partition's events in
`EventStreamVersion` order.

### Dynamic Load Balancing

Maintain a queue of dirty partition keys. If a key is already active, mark it
pending and drain again after the current pass.

## Performance Optimization

### Batch Processing

Use bounded reads and commit after each batch:

```rust
let events = stream.load_after_version(cursor, batch_size).await?;
```

The crate rejects zero and oversized limits.

### Concurrent Processing

Run consumers concurrently across partition keys, not within one partition key.
Events for one partition key are ordered by `EventStreamVersion` and should be
handled serially.

## Error Handling and Recovery

### Dead Letter Queue

If a valid event cannot be projected after retry, write a diagnostic row to an
application-owned dead-letter table. Do not advance the offset unless the
application intentionally skips that event.

## Monitoring and Observability

### Consumer Metrics

Track:

- active partition keys
- pending partition keys
- last projected version
- projection lag
- batch duration
- retry count
- last error

## Best Practices

### 1. Consumer Design

Keep handlers idempotent and save offsets with read-model changes.

### 2. Load Balancing

Bound total active workers and keep one active consumer per projection and
partition key.

### 3. Performance Optimization

Tune batch size by handler memory use and transaction duration.

### 4. Reliability

Failed workers must not advance offsets. Dirty keys received while active should
be processed after the current drain.

## Verification

The pool should prove:

- no more than one active consumer per projection and partition key
- events for one partition key are handled in event-stream version order
- bounded global worker count
- dirty keys received while active are processed after the current drain
- failed workers do not advance the application offset

## Next Steps

- [Worker Pool Over Per-Partition Stores](worker-pool-over-per-partition-store.md)
- [Implement Robust Event Projections](implement-projection.md)
