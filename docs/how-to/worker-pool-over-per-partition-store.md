# Worker Pool Over Per-Partition Stores

Use `EventNamespaces` as the resolver and keep the worker pool in application
code.

```rust
let namespaces = EventNamespaces::open(root, rotation).await?;
let owners = namespaces.ensure_namespace("owners").await?;
let partition = owners.ensure_partition_exists("acme").await?;
let stream = partition.open().await?;
let events = stream.load_after_version(last_projected_version, 100).await?;
```

The application pool usually tracks:

- active partition keys
- pending partition keys
- application-owned offsets by partition key
- retry/backoff state

Bound total active workers so a burst of dirty partition keys cannot create
unbounded work. Add projection throughput by running consumers concurrently
across partition keys, not by splitting one partition stream across workers.

When a key is dirty, start a consumer for the projection if one is not already
active for that key. If a key is marked dirty while active, record it as pending
and run another consumer pass after the current consumer exits.

Each consumer:

1. Opens the partition store.
2. Reads a bounded batch after the application offset.
3. Applies read-model changes.
4. Commits the read model and offset in the application database.
5. Exits when a bounded read returns no events.

When one event stream feeds multiple read models, filter by event type inside
each projection handler. That filtering is handler behavior, not another
scheduling boundary. Keep the offset per projection name and partition key.

Track:

- active partition keys
- pending partition keys
- last projected version
- projection lag
- batch duration
- retry count
- last error

The pool should prove:

- no more than one active consumer per projection and partition key
- events for one partition key are handled in event-stream version order
- bounded global worker count
- dirty keys received while active are processed after the current drain
- failed workers do not advance the application offset

See `examples/partition_worker_pool.rs` for a minimal on-demand worker-pool
example.
