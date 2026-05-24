# Configuration Reference

The crate has two durable event stream configuration surfaces: rotation policy
and resolver cache policy.

Configuration is deliberately small. Partition routing is chosen by application
code through `EventNamespaces`; file rotation and cache behavior are the crate's
only public tuning knobs.

## RotationPolicy

```rust
RotationPolicy::TimeWindow {
    window: Duration,
    max_bytes: Option<u64>,
}
```

`window` controls the time bucket used for physical event database files.

`max_bytes` optionally rotates within the same time window when the active file
reaches the configured size. Same-window rotations use suffixes in the generated
file name.

### Window Selection

| Window | Typical use | Operational trade-off |
| --- | --- | --- |
| 15 minutes | high-volume partitions | more files, smaller active indexes |
| 1 hour | general purpose default | balanced file count and maintenance size |
| 1 day | low-volume partitions | fewer files, larger maintenance units |

The window affects physical file names and rotation cadence. It does not change
the public read cursor; callers still use `EventLogVersion`.

### Size Limit

`max_bytes: Some(limit)` bounds active file size. If the active file reaches the
limit before the time window changes, the store creates a same-window overflow
file:

```text
events_20260521T10.db
events_20260521T10_a.db
events_20260521T10_b.db
```

Use a size limit when maintenance, backup, or file-copy operations need bounded
database files. Leave it as `None` when time windows alone are sufficient.

## EventNamespaces Cache

```rust
let namespaces = EventNamespaces::open(root, rotation)
    .await?
    .with_max_open_stores(128)?
    .with_idle_store_ttl(Duration::from_secs(300))?;
```

`with_max_open_stores` must be greater than zero.

`with_idle_store_ttl` must be greater than zero.

The cache only controls idle opened stores held by the resolver. Cloned
`EventLog` handles remain valid even if the resolver evicts its cached
entry.

### Cache Defaults

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_open_stores` | `50` | maximum idle stores retained by the resolver |
| `idle_store_ttl` | `300s` | how long an idle cached store is retained |

The cache is an implementation detail of `EventNamespaces`. It avoids reopening
hot partition stores repeatedly, but it is not a public worker-pool API and it
does not own worker lifecycle.

## Safe Path Segments

Namespaces and partition keys must use lowercase ASCII letters, digits, and
`-`, with length `1..=128`.

Invalid examples:

```text
User-123
user_123
user.123
user/123
```

Valid examples:

```text
users
user-123
client-2026
default
```

The safe-name rule keeps partition directories portable and prevents partition
keys from smuggling path separators into the storage layout.

## RuntimeConfig

`EventsRuntime` combines event namespaces, progress notification storage, and
broadcast wiring:

```rust
let config = RuntimeConfig::new("./data")
    .with_progress_notification_ttl(Duration::from_secs(600))
    .with_rotation_policy(rotation);
```

| Field | Meaning |
| --- | --- |
| `data_dir` | root directory for runtime-managed event and notification data |
| `progress_notification_ttl` | retention period for request progress notifications |
| `rotation_policy` | rotation policy used by the event partition resolver |

`progress_notification_ttl` applies to progress notifications, not durable
domain events.
