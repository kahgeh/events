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

`max_bytes` optionally rotates within the same time window when the active file reaches the configured size. Same-window rotations use six-digit overflow ordinals in the generated file name.

### Window Selection

Choose a duration that keeps file counts and file sizes manageable for your operational environment. The window affects physical file names and rotation cadence. It does not change the public read cursor; callers still use `EventStreamVersion`.

### Size Limit

`max_bytes: Some(limit)` bounds active file size. If the active file reaches the
limit before the time window changes, the store creates a same-window overflow
file:

```text
events_20260521T10.db
events_20260521T10_000001.db
events_20260521T10_000002.db
```

Valid overflow ordinals are `000001..999999`. Reaching `999999` within the same window returns `EsError::RotationOrdinalExhausted` before the active catalog range is sealed. Entering a new time window still creates the new window's unsuffixed base file.

Alpha-suffixed files from older versions are unsupported. Reset the partition store before opening it with this version; the crate does not rename or migrate legacy files.

Use a size limit when maintenance, backup, or file-copy operations need bounded database files. Leave it as `None` when time windows alone are sufficient.

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
`EventStream` handles remain valid even if the resolver evicts its cached
entry.

### Cache Defaults

| Setting           | Default | Meaning                                      |
| ----------------- | ------- | -------------------------------------------- |
| `max_open_stores` | `50`    | maximum idle stores retained by the resolver |
| `idle_store_ttl`  | `300s`  | how long an idle cached store is retained    |

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

| Field                       | Meaning                                                        |
| --------------------------- | -------------------------------------------------------------- |
| `data_dir`                  | root directory for runtime-managed event and notification data |
| `progress_notification_ttl` | retention period for request progress notifications            |
| `rotation_policy`           | rotation policy used by the event partition resolver           |

`progress_notification_ttl` applies to progress notifications, not durable
domain events.

## NotificationMaintenanceOptions

`NotificationMaintenanceOptions` controls the background worker that deletes expired progress notifications from `NotificationsStore`:

```rust
let options = NotificationMaintenanceOptions::new(Duration::from_secs(60))?;
```

| Field              | Meaning                                               |
| ------------------ | ----------------------------------------------------- |
| `cleanup_interval()` | how often expired progress notification rows are deleted |

The worker is started with `EventsRuntime::start_notification_maintenance_worker(options, shutdown_rx)`. It is separate from the broadcast loop.
The cleanup interval must be greater than zero.
