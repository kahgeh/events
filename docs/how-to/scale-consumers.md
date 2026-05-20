# Scale Consumers for High-Volume Event Streams

Scale projection by scheduling work per partition store. Owner partitioning is
one way to choose those partition keys.

## What You'll Learn

- How to scale by partition key
- How to schedule one active worker per partition
- How to use bounded owner-log reads
- How to monitor and recover consumers

## Scaling Patterns

### 1. Partition-based Scaling

Discover partition keys with shallow listing:

```rust
let descriptors = partitions.list("users").await?;
```

Schedule one active worker per partition key. The worker drains bounded batches:

```rust
let events = store.load_after_version(last_projected, 500).await?;
```

Commit read-model changes and the partition offset in the application database.

### 2. Event-type Based Scaling

Filter by event type inside the projection handler when the same partition log
feeds multiple read models. Keep the offset per projection name and partition
key.

### 3. Stream-based Scaling

Use partition keys as the scheduling boundary. A plain application can use one
stable key; high-volume systems can choose keys that match owners, accounts,
clients, or other independent units of work.

## Load Balancing Strategies

### Consistent Hashing

For multiple worker processes, assign partition keys to processes with a stable
hash. Keep per-key processing single-threaded inside the assigned process.

### Dynamic Load Balancing

Maintain a queue of dirty partition keys. If a key is already active, mark it
pending and drain again after the current pass.

## Performance Optimization

### Batch Processing

Use bounded reads and commit after each batch:

```rust
let events = store.load_after_version(cursor, batch_size).await?;
```

The crate rejects zero and oversized limits.

### Parallel Processing

Parallelize across partition keys, not within one partition key, unless the
projection is explicitly order-insensitive.

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

Bound total active workers and keep one active worker per partition key.

### 3. Performance Optimization

Tune batch size by handler memory use and transaction duration.

### 4. Reliability

Failed workers must not advance offsets. Dirty keys received while active should
be processed after the current drain.

## Verification

The pool should prove:

- no more than one active worker per partition key
- bounded global worker count
- dirty keys received while active are processed after the current drain
- failed workers do not advance the application offset

## Next Steps

- [Worker Pool Over Per-Partition Stores](worker-pool-over-per-partition-store.md)
- [Implement Robust Event Projections](implement-projection.md)
