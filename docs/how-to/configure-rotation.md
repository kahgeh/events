# Configure Partition Rotation

Use `RotationPolicy` when opening `EventNamespaces`. The policy applies to every partition store opened through that resolver.

## Choose A Policy

Start with time-window rotation:

```rust
let rotation = RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(512 * 1024 * 1024),
};
```

Use one resolver per rotation policy:

```rust
let namespaces = EventNamespaces::open("./data/events", rotation).await?;
let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition("user-123").await?;
let stream = partition.open().await?;
```

Rotation is checked before append. You can also trigger the check explicitly:

```rust
stream.maybe_rotate().await?;
```

## Pick Window And Size Bounds

Choose the window from the append volume of one partition store, not from global application volume.

| Per-partition volume | Starting window | Size limit |
| --- | --- | --- |
| High | 15 minutes | Use a bound |
| Medium | 1 hour | Use a bound when file maintenance needs it |
| Low | 1 day | Usually optional |

Use `max_bytes: None` when time windows alone keep files manageable. Same-window overflow files use suffixes such as `events_20260521T10_a.db`.

## Verify Rotation

Use a temporary directory, a short window or small size limit, append enough events to rotate, then verify logical order:

```rust
let events = stream.load_after_version(EventStreamVersion::start(), 1000).await?;
assert!(events.windows(2).all(|w| w[0].version < w[1].version));
```

## Troubleshoot

- Files grow too large: reduce the window or set `max_bytes`.
- Too many small files: increase the window or remove an unnecessary size limit.
- Rotation errors: check append traffic, active file size, and the result from `maybe_rotate()`.

## Related Pages

- [Configuration reference](../reference/configuration.md)
- [Performance reference](../reference/performance.md)
- [Partitioning strategy](../explanation/partitioning-strategy.md)
