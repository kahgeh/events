# Configure Partition Rotation

Use `RotationPolicy` when creating `EventNamespaces`. The same policy applies to
every partition store opened through that resolver.

## What You'll Learn

- How to configure time-window rotation
- How to choose window sizes and file size limits
- How rotation affects partition-store reads
- How to monitor and test rotation behavior

## Understanding Rotation Policies

Rotation is physical file management inside one partition store. It does not
create new logical streams and it does not change the public cursor:
applications still read with `EventStreamVersion`.

## Basic Configuration

```rust
let rotation = RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(512 * 1024 * 1024),
};

let namespaces = EventNamespaces::open("./data/events", rotation).await?;
let users = namespaces.ensure_namespace("users").await?;
let partition = users.ensure_partition_exists("user-123").await?;
let stream = partition.open().await?;
```

Rotation is checked before append and can also be triggered explicitly:

```rust
stream.maybe_rotate().await?;
```

## Choosing Time Windows

### High Volume Systems (>10,000 events/second)

Use shorter windows, such as 15 minutes, to keep active files and indexes small.
Pair the window with a size limit.

### Medium Volume Systems (100-10,000 events/second)

One-hour windows are a good default. They keep file count predictable while still
bounding maintenance units.

### Low Volume Systems (<100 events/second)

Daily windows are often enough. Use size limits only when operational tooling
needs strict file-size bounds.

## Size Limits Configuration

### Size-based Rotation Within Time Windows

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(256 * 1024 * 1024),
}
```

Same-window overflow files use suffixes:

```text
events_20260521T10.db
events_20260521T10_a.db
events_20260521T10_b.db
```

### No Size Limits

Use `max_bytes: None` when time windows alone create manageable files.

## Advanced Configuration Patterns

### Tiered Rotation Strategy

Run separate `EventNamespaces` roots when different domains need very different
rotation policies. Keep one policy per resolver.

### Event-type Based Rotation

The crate does not rotate by event type. If event types need independent
operational treatment, choose partition keys or namespaces that reflect that
boundary.

## Monitoring Rotation Performance

### Track Partition Statistics

Monitor:

- active file size
- files per partition store
- append latency around rotation
- open-store cache pressure

### Automatic Rotation Monitoring

Record the active event file name and event-stream head periodically. Reads
should continue in event-stream order across file boundaries.

## Partition Lifecycle Management

### Automated Archival Strategy

Sealed files can be copied or archived independently. Keep catalog metadata with
the partition store so version ranges remain available for reads.

### Partition Compaction

Compaction is an operational task for sealed files. Do not mutate event rows or
version ranges while a store is actively writing.

## Performance Optimization

### Connection Pool Tuning

Use `EventNamespaces` cache knobs to avoid unbounded idle store retention:

```rust
let namespaces = EventNamespaces::open(root, rotation)
    .await?
    .with_max_open_stores(128)?
    .with_idle_store_ttl(Duration::from_secs(300))?;
```

### Batch Size Optimization

Projection batch size is separate from rotation. Use bounded reads:

```rust
let events = stream.load_after_version(cursor, 500).await?;
```

### WAL Mode Configuration

Connection setup is handled by the crate. Monitor append latency and file growth
instead of tuning storage pragmas from application code.

## Troubleshooting Common Issues

### Problem: Partitions Growing Too Large

Reduce the time window or set `max_bytes`.

### Problem: Too Many Small Partitions

Increase the window or remove an unnecessary size limit.

### Problem: Rotation Delays

Check append traffic, active file size, and whether `maybe_rotate()` is returning
errors.

## Testing Rotation Configuration

### Load Testing Script

Use a temporary directory, a small window or size limit, append enough events to
rotate, then verify logical order:

```rust
let events = stream.load_after_version(EventStreamVersion::start(), 1000).await?;
assert!(events.windows(2).all(|w| w[0].version < w[1].version));
```

## Best Practices Summary

- Keep one rotation policy per `EventNamespaces` resolver.
- Choose windows based on per-partition volume.
- Use size limits when file maintenance needs bounds.
- Verify reads by event-stream version, not file order.

## Next Steps

- [Partitioning Strategy](../explanation/partitioning-strategy.md)
- [Performance Reference](../reference/performance.md)
