# Concurrency Control

Concurrency control protects decisions made from a read event-stream state.
The crate uses optimistic concurrency: commands state what version they expect,
and appends fail if the event-stream head has moved.

## The Concurrency Problem

Two command handlers can read the same state and make conflicting decisions:

```
Handler A reads head v5
Handler B reads head v5
Handler A appends event at v6
Handler B tries to append based on v5
```

The second append must not silently succeed if the domain decision depended on
the v5 state.

## Optimistic Concurrency Control

### How It Works

Commands choose an `ExpectedVersion`:

| Expected version | Meaning | Use when |
| --- | --- | --- |
| `NoStream` | the event stream must be empty | creating the first event in a partition |
| `Exact(version)` | the head must equal `version` | command was based on loaded state |
| `Any` | append after current head | blind append is domain-correct |

`EventStream::append` checks the current event-stream head before assigning new
versions.

```
append(...)
  ├─ read catalog head under store write path
  ├─ validate ExpectedVersion
  ├─ insert events with UNIQUE(version)
  ├─ commit event DB transaction
  └─ update catalog head
```

## Conflict Detection and Resolution

### Types of Conflicts

- `NoStream` conflict: the event stream already has events.
- `Exact(version)` conflict: the current head differs from the expected version.
- Invalid exact start: `Exact(EventStreamVersion::start())` is rejected.

### Resolution Strategies

On `EsError::Concurrency`, reload state and choose a domain response:

```rust
match stream.append(ExpectedVersion::Exact(seen), events).await {
    Ok(result) => Ok(result),
    Err(EsError::Concurrency { actual, .. }) => {
        let head = EventStreamVersion::new(actual)?;
        let new_events = stream.load_after_version(seen, 100).await?;
        decide_retry_merge_or_reject(head, new_events).await
    }
    Err(err) => Err(err),
}
```

Common responses:

- retry the command against the new state
- merge when the events are compatible
- reject when the decision is stale
- surface a conflict to the caller

## Database-Level Concurrency

### Write Lock and Transaction Isolation

Appends are serialized for one `EventStream`. The event table enforces
`UNIQUE(version)` as a storage-level guard.

### Connection Pool Management

The resolver cache and database pool reduce reopen cost. They do not change the
domain concurrency rule: expected versions are checked per event stream.

## Cross-Partition Concurrency

### Partition Head Coordination

Each partition store has its own event-stream head. A version from one partition
store is not meaningful as an expected version for another store.

If a command spans multiple partition keys, coordinate that at the application
level. The events crate provides partition-local append safety.

## Performance Considerations

### Contention Points

Contention is concentrated on the selected partition store. A single `app/default`
partition is simple but has one write-concurrency boundary.

### Mitigation Strategies

- choose partition keys that match independent units of work
- keep append batches bounded
- use `ExpectedVersion::Any` only for events where blind append is correct
- keep cross-partition coordination in application workflows

## Testing Concurrency

### Concurrent Write Test

Test two command handlers using the same loaded head. One should succeed and the
other should receive `EsError::Concurrency`.

### Version Conflict Test

Test:

- `NoStream` succeeds for the first append
- `NoStream` fails after the first append
- stale `Exact(version)` fails
- `Exact(current_head)` succeeds
- `Any` appends after the current head

## Best Practices

### 1. Always Use Expected Version

Use `Exact(version)` when a command decision depends on loaded state.

### 2. Implement Retry Logic

Retries should reload state and re-run domain decision logic. Do not reuse the
same event batch blindly after a conflict.

### 3. Design for Idempotency

Projection handlers and external side effects should use event IDs or workflow
starter IDs for idempotency. Concurrency control prevents stale appends; it does
not make downstream side effects automatically safe.
