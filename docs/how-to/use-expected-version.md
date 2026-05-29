# Use ExpectedVersion

Pass `ExpectedVersion` to say what version this event stream must currently be at before the append is allowed.

## What You'll Learn

- How `ExpectedVersion` protects command decisions
- When to use `NoStream`, `Exact`, and `Any`
- How `ExpectedVersion` fits with one active worker per partition key
- Why `EsError::IncorrectEventVersion` should be unusual under serial partition processing
- How to test version-guard behavior

## Understand Expected Versions

### The Consistency Guard

In the recommended worker model, one active worker owns a partition key at a time and appends decisions for that partition in order. `ExpectedVersion` still stays on each append as a consistency guard: if the stream head is not what the worker expected, the append is rejected before stale or duplicate events enter the stream.

## Choose An ExpectedVersion

```rust
pub enum ExpectedVersion {
    NoStream,
    Any,
    Exact(EventStreamVersion),
}
```

### When to Use Each Type

| Type             | Use when                                    |
| ---------------- | ------------------------------------------- |
| `NoStream`       | creating the first event in an event stream |
| `Exact(version)` | command decision was based on loaded state  |
| `Any`            | blind append is domain-correct              |

## Use It With Serial Partition Processing

### One Active Consumer Per Projection And Partition

Projection scheduling belongs to the application worker pool. For each projection and partition key, use one active consumer and process events serially in `EventStreamVersion` order. Commit read-model changes with the projection offset.

Serial partition processing should make `IncorrectEventVersion` unusual, but it does not remove the need for the check. Commands still use `ExpectedVersion` to protect state-dependent writes before events enter the ordered stream.

## Handle An Unexpected Mismatch

### Unexpected Version Mismatch

```rust
match stream.append(ExpectedVersion::Exact(current_version), new_events).await {
    Ok(result) => Ok(result),
    Err(EsError::IncorrectEventVersion { actual, .. }) => {
        let head = EventStreamVersion::new(actual)?;
        let new_events = stream.load_after_version(current_version, 100).await?;
        decide_retry_merge_or_reject(head, new_events).await
    }
    Err(err) => Err(err),
}
```

### Recovery Options

Common strategies:

- reload and retry when the new events do not invalidate the command
- reject when the decision is stale
- investigate duplicate commands or unexpected extra writers
- surface mismatch information to the caller

## Test Version Guards

### Append Guard Tests

Tests should cover:

- `NoStream` succeeds for the first append
- `NoStream` fails after the first append
- `Exact(EventStreamVersion::start())` is rejected
- stale `Exact(version)` fails
- `Any` appends after the current head

## Best Practices for Expected Versions

### 1. Treat Mismatches As Unexpected

Treat `EsError::IncorrectEventVersion` as an unexpected consistency signal, not a storage failure.

### 2. Use Appropriate Retry Strategies

Retry only after reloading state and re-running domain rules.

### 3. Design for Idempotency

Projection handlers and external side effects should be idempotent by event ID or workflow starter ID.

### 4. Keep Downstream Side Effects Idempotent

`ExpectedVersion` only decides whether events can be appended. Emails, webhooks, payments, notifications, and other side effects still need their own idempotency keys or delivery records.

## Common Pitfalls

### 1. Lost Updates

Using `ExpectedVersion::Any` for state-dependent commands can hide lost updates.

### 2. More Than One Consumer For One Projection And Partition

Running two consumers for the same projection and partition key can duplicate read-model work or apply events out of order.

### 3. Inconsistent Read Models

Saving offsets separately from read-model changes can duplicate or miss read-model work after a crash.

## Next Steps

- [Implement Robust Event Projections](implement-projection.md)
