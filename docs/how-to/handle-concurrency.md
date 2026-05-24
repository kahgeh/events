# Handle Append Concurrency

Use `ExpectedVersion` to express the append precondition for one event stream.

## What You'll Learn

- How `ExpectedVersion` protects command decisions
- When to use `NoStream`, `Exact`, and `Any`
- How to handle `EsError::Concurrency`
- How to test concurrent append scenarios

## Understanding Append Concurrency

### The Problem Scenario

Two handlers can read the same event-stream head and both decide to append. The
first append moves the head; the second must reload or reject rather than commit
a stale decision.

## ExpectedVersion Types

```rust
pub enum ExpectedVersion {
    NoStream,
    Any,
    Exact(EventStreamVersion),
}
```

### When to Use Each Type

| Type | Use when |
| --- | --- |
| `NoStream` | creating the first event in an event stream |
| `Exact(version)` | command decision was based on loaded state |
| `Any` | blind append is domain-correct |

## Handling Concurrency Conflicts

### Basic Conflict Handling

```rust
match stream.append(ExpectedVersion::Exact(current_version), new_events).await {
    Ok(result) => Ok(result),
    Err(EsError::Concurrency { actual, .. }) => {
        let head = EventStreamVersion::new(actual)?;
        let new_events = stream.load_after_version(current_version, 100).await?;
        decide_retry_merge_or_reject(head, new_events).await
    }
    Err(err) => Err(err),
}
```

### Advanced Conflict Resolution

Common strategies:

- reload and retry when the new events do not invalidate the command
- merge when concurrent facts are compatible
- reject when the decision is stale
- surface conflict information to the caller

## Serial Partition Processing

### One Active Consumer Per Projection And Partition

Projection scheduling belongs to the application worker pool. For each
projection and partition key, use one active consumer and process events
serially in `EventStreamVersion` order. Commit read-model changes with the
projection offset.

This does not replace append concurrency control. Commands still use
`ExpectedVersion` to protect state-dependent writes before events enter the
ordered stream.

## Testing Concurrent Scenarios

### Concurrent Append Tests

Tests should cover:

- `NoStream` succeeds for the first append
- `NoStream` fails after the first append
- `Exact(EventStreamVersion::start())` is rejected
- stale `Exact(version)` fails
- `Any` appends after the current head

## Best Practices for Concurrency

### 1. Always Handle Concurrency Errors

Treat `EsError::Concurrency` as a domain decision point, not a storage failure.

### 2. Use Appropriate Retry Strategies

Retry only after reloading state and re-running domain rules.

### 3. Design for Idempotency

Projection handlers and external side effects should be idempotent by event ID
or workflow starter ID.

### 4. Use Proper Idempotency

Do not rely on append concurrency to protect downstream side effects.

## Common Concurrency Pitfalls

### 1. Lost Updates

Using `ExpectedVersion::Any` for state-dependent commands can hide lost updates.

### 2. Concurrent Consumers For One Projection And Partition

Running two consumers for the same projection and partition key can duplicate
read-model work or apply events out of order.

### 3. Inconsistent Read Models

Saving offsets separately from read-model changes can duplicate or miss
read-model work after a crash.

## Next Steps

- [Concurrency Control](../explanation/concurrency-control.md)
- [Implement Robust Event Projections](implement-projection.md)
