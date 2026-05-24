# Handle Append Concurrency

Use `ExpectedVersion` to express the append precondition for one event log.

## What You'll Learn

- How `ExpectedVersion` protects command decisions
- When to use `NoStream`, `Exact`, and `Any`
- How to handle `EsError::Concurrency`
- How to test concurrent append scenarios

## Understanding Append Concurrency

### The Problem Scenario

Two handlers can read the same event-log head and both decide to append. The
first append moves the head; the second must reload or reject rather than commit
a stale decision.

## ExpectedVersion Types

```rust
pub enum ExpectedVersion {
    NoStream,
    Any,
    Exact(EventLogVersion),
}
```

### When to Use Each Type

| Type | Use when |
| --- | --- |
| `NoStream` | creating the first event in an event log |
| `Exact(version)` | command decision was based on loaded state |
| `Any` | blind append is domain-correct |

## Handling Concurrency Conflicts

### Basic Conflict Handling

```rust
match store.append(ExpectedVersion::Exact(current_version), new_events).await {
    Ok(result) => Ok(result),
    Err(EsError::Concurrency { actual, .. }) => {
        let head = EventLogVersion::new(actual)?;
        let new_events = store.load_after_version(current_version, 100).await?;
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

## Concurrent Projections

### Lock-Based Projection Processing

Projection concurrency belongs to the application worker pool. Use one active
worker per partition key and commit read-model changes with the projection
offset.

## Testing Concurrent Scenarios

### Concurrent Append Tests

Tests should cover:

- `NoStream` succeeds for the first append
- `NoStream` fails after the first append
- `Exact(EventLogVersion::start())` is rejected
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

### 2. Race Conditions in Projections

Running two workers for the same partition key can duplicate read-model work.

### 3. Inconsistent Read Models

Saving offsets separately from read-model changes can duplicate or miss
read-model work after a crash.

## Next Steps

- [Concurrency Control](../explanation/concurrency-control.md)
- [Implement Robust Event Projections](implement-projection.md)
