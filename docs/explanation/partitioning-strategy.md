# Partitioning Strategy

Understanding partitioning helps you choose the right ordered stream boundary and
operate rotated event files without exposing file details to application code.

## What is Partitioning?

Partitioning has two different meanings in this crate:

- **Application partitioning** chooses which `EventStream` a command uses.
- **Physical rotation** splits one `EventStream` across time-window database
  files.

### Plain Partition Store

```
data/events/
└── app/
    └── default/
        ├── catalog.db
        └── events_20260521T10.db
```

A small service can choose one stable partition key and treat it as its plain
event stream:

```rust
let app = namespaces.ensure_namespace("app").await?;
let partition = app.ensure_partition("default").await?;
let stream = partition.open().await?;
```

### Partitioning By Owner Or Account

```
data/events/
├── users/
│   ├── user-123/
│   └── user-456/
└── clients/
    └── client-123/
```

Partitioning by owner, account, client, or another independent unit is a scaling
strategy layered on top of the plain partition-store model.

## Why Partitioning Matters

### 1. Performance Optimization

Each partition store has its own ordered event stream and active writer path. Good
partition keys distribute independent command decisions and read-model projection work.

### 2. Operational Benefits

The directory shape is inspectable and bounded by safe path segments:

```rust
let users = namespaces.ensure_namespace("users").await?;
users.ensure_partition("user-123").await?;

let clients = namespaces.ensure_namespace("clients").await?;
clients.ensure_partition("client-123").await?;

let orders = namespaces.ensure_namespace("orders").await?;
orders.ensure_partition("order-456").await?;
```

Keys must be lowercase ASCII letters, digits, and `-`, length `1..=128`.

### 3. Scalability Patterns

Use owner/account/client-style partition keys when:

- append concurrency should be scoped to that unit
- event handlers should drain that unit independently
- worker pools need one active handler per unit
- operational inspection benefits from separate directories

## Partition Naming Convention

Physical event files are named from their time window:

```text
events_20260521T10.db
events_20260521T10_a.db
events_20260521T10_b.db
```

Suffixes represent same-window overflow files. The public cursor remains
`EventStreamVersion`; applications do not store these file names as offsets.

## Rotation Policies

### Time-Based Rotation

```rust
RotationPolicy::TimeWindow {
    window: Duration::from_secs(3600),
    max_bytes: Some(512 * 1024 * 1024),
}
```

The window controls the physical file bucket.

### Size-Based Rotation

`max_bytes` creates a new same-window file when the active file reaches the size
limit. This keeps maintenance units bounded without changing logical ordering.

## Partition Lifecycle

### 1. Creation

`EventNamespaces::ensure_namespace(namespace)` validates and creates a namespace
directory. `EventNamespace::ensure_partition(key)` validates and creates
the partition directory.

### 2. Active Phase

`Partition::open()` returns an `EventStream`. Appends write to the current
active event file and advance that file's local append head in the same
transaction as the event rows.

### 3. Sealing

When rotation criteria are met, the current file is sealed by catalog metadata
and a new active file is created.

### 4. Archival

Sealed event files can be copied, backed up, or inspected independently. The
catalog keeps version ranges so reads continue through `EventStreamVersion`.

## Query Patterns with Partitioning

### Event-Stream Reads

```rust
let events = stream
    .load_after_version(EventStreamVersion::start(), 500)
    .await?;
```

Reads are exclusive and bounded.

### Workflow Reads

```rust
let events = stream
    .load_workflow_after_version(started_by_event_id, cursor, 100)
    .await?;
```

Workflow reads filter within the selected event stream.

## Catalog Database Role

### Partition Registry

The catalog stores rotated file paths and their event-stream version ranges.

### Event-Stream Head Tracking

The active event file stores the current append head. Expected-version checks
compare against this local head, while the catalog remains routing metadata.

### Query Planning

Reads use catalog ranges to open only the event files that may contain versions
after the cursor.

## Performance Implications

### Write Performance

Writes are scoped to one partition store. More partition keys can reduce write
contention when those keys match independent work.

### Read Performance

Event handlers read bounded batches from one partition store at a time.
Application worker pools should bound global worker count.

## Best Practices

### 1. Choose Appropriate Time Windows

Use shorter windows for high-volume partitions and longer windows for low-volume
partitions.

### 2. Monitor Partition Health

Monitor active file size, catalog drift errors, open store cache pressure, and
projection lag by partition key.

### 3. Plan for Data Growth

Partition count grows with namespace/key count and rotation cadence. Choose
partition keys that reflect real operational boundaries.

## Trade-offs and Considerations

### Advantages

- independent append contention by partition key
- stable logical cursors across rotated files
- inspectable storage layout
- application-owned handler state

### Considerations

- more partition keys create more directories and catalogs
- worker pools need bounded scheduling
- application code must choose the partition key consistently

### When to Use Partitioning

Use a single stable partition key for simple applications. Add owner/account/client
style partitioning when contention, handler scheduling, or operational
isolation needs it.
