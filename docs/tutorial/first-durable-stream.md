# First Durable Stream

An event stream is opened for one partition key. That key may represent an owner,
account, client, or another independent unit, but the storage primitive is
simply one ordered event stream per partition store.

## Create The Partition Store

```rust
let namespaces = EventNamespaces::open(root, rotation).await?;
let orders = namespaces.ensure_namespace("orders").await?;
let partition = orders.ensure_partition("order-123").await?;
let stream = partition.open().await?;
```

## Append With Optimistic Concurrency

Use `ExpectedVersion::NoStream` for the first append.

```rust
let created = stream
    .append(ExpectedVersion::NoStream, [order_created])
    .await?;
```

Use the returned `last_version` for a later exact append.

```rust
let packed = stream
    .append(ExpectedVersion::Exact(created.last_version), [order_packed])
    .await?;
```

## Read Bounded Batches

```rust
let events = stream
    .load_after_version(EventStreamVersion::start(), 100)
    .await?;
```

Projection code should persist the last processed `EventStreamVersion` in the
application database.

## Add Workflow Metadata

For retryable workflows, mark the starter event:

```rust
workflow_kind: Some("fulfilment".into()),
workflow: WorkflowRef::StartsThisWorkflow,
```

The returned envelope contains the generated starter ID in
`workflow_started_by_event_id`.
