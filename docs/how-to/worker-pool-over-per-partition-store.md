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

When a key is dirty, start a worker if one is not already active. If a key is
marked dirty while active, record it as pending and run another drain pass after
the current worker exits.

Each worker:

1. Opens the partition store.
2. Reads a bounded batch after the application offset.
3. Applies read-model changes.
4. Commits the read model and offset in the application database.
5. Exits when a bounded read returns no events.

See `examples/partition_worker_pool.rs` for a minimal on-demand worker-pool
example.
