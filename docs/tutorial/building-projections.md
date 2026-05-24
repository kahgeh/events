# Building Projections

This tutorial builds a simple projection loop for one partition key.

## 1. Store Projection Offset

Create an application-owned offset record:

```text
owner_key = "user-123"
last_projected_version = 0
```

Represent `0` in Rust as `EventStreamVersion::start()`.

## 2. Open The Partition Store

```rust
let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition_exists(owner_key).await?;
let stream = partition.open().await?;
```

## 3. Read A Batch

```rust
let events = stream.load_after_version(last_projected, 100).await?;
```

## 4. Apply And Commit

Apply events to the read model, then commit the read-model changes and new
offset in one application database transaction.

```rust
if let Some(last) = events.last() {
    save_projection_offset(owner_key, last.version).await?;
}
```

## 5. Repeat

Keep reading until `events.is_empty()`. For multiple partition keys, put this
loop inside an application worker pool. See
[worker pool over per-partition stores](../how-to/worker-pool-over-per-partition-store.md).
