# First Event Store

An event store is opened for one partition key. That key may represent an owner
when you are using owner partitioning as a scaling strategy, but the storage
primitive is simply one ordered log per partition store.

## Create The Store

```rust
let partitions = EventPartitions::open(root, rotation).await?;
let partition = partitions.ensure_exists("orders", "order-123").await?;
let store = partition.open().await?;
```

## Append With Optimistic Concurrency

Use `ExpectedVersion::NoStream` for the first append.

```rust
let created = store
    .append(ExpectedVersion::NoStream, [order_created])
    .await?;
```

Use the returned `last_version` for a later exact append.

```rust
let packed = store
    .append(ExpectedVersion::Exact(created.last_version), [order_packed])
    .await?;
```

## Read Bounded Batches

```rust
let events = store
    .load_after_version(OwnerLogVersion::start(), 100)
    .await?;
```

Projection code should persist the last processed `OwnerLogVersion` in the
application database.

## Add Workflow Metadata

For retryable workflows, mark the starter event:

```rust
workflow_kind: Some("fulfilment".into()),
workflow: WorkflowRef::StartsThisWorkflow,
```

The returned envelope contains the generated starter ID in
`workflow_started_by_event_id`.
