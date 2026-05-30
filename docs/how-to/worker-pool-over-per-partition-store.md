# Worker Pool Over Per-Partition Stores

Use `EventNamespaces` as the resolver and keep the worker pool in application
code.

```rust
let namespaces = EventNamespaces::open(root, rotation).await?;
let owners = namespaces.ensure_namespace("owners").await?;
let partition = owners.ensure_partition("acme").await?;
let stream = partition.open().await?;
let cursor = match last_processed_event_version {
    0 => EventStreamVersion::start(),
    version => EventStreamVersion::new(version)?,
};
let events = stream.load_after_version(cursor, 100).await?;
```

The application pool usually tracks:

- active partition keys
- pending partition keys
- application-owned `last_processed_event` rows by partition key
- retry/backoff state

Bound total active workers so a burst of partition keys with newly appended events cannot create unbounded work. Add handler throughput by running event handlers concurrently across partition keys, not by splitting one partition stream across workers.

When a partition key may have new events, start an event handler if one is not already active for that key. If new events arrive while a handler is active, record the key as pending and run another handler pass after the current pass exits.

Each handler pass:

1. Opens the partition store.
2. Reads a bounded batch after `last_processed_event`.
3. Applies read-model changes.
4. Commits the read model and `last_processed_event` in the application database.
5. Exits when a bounded read returns no events.

When one event handler updates multiple read models, filter by event type inside
that handler and commit all affected read models with the same `last_processed_event`
row.

Track:

- active partition keys
- pending partition keys
- last processed event version
- projection lag
- batch duration
- retry count
- last error

The pool should prove:

- no more than one active handler per partition key
- events for one partition key are handled in event-stream version order
- bounded global worker count
- partition keys marked pending while active are processed after the current drain
- failed workers do not advance `last_processed_event`

See `examples/partition_worker_pool.rs` for a minimal on-demand worker-pool
example.
