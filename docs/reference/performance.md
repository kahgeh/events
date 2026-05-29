# Performance Reference

Performance is shaped by partition-key choice, batch size, rotation policy, and
application worker scheduling.

This page is a reference for choosing limits and diagnosing bottlenecks. The
crate does not hide a global worker pool behind the API; it gives applications
the store boundaries needed to build one.

## Write Path

Each `EventStream` serializes appends for one event stream. The storage
table enforces `UNIQUE(version)` as the stream-local safety guard.

Good partition keys distribute independent write paths. Poor partition keys
concentrate unrelated work into one stream.

| Choice                      | Effect                                          |
| --------------------------- | ----------------------------------------------- |
| one `app/default` partition | simplest model, one writer bottleneck           |
| partition by owner/account  | independent event streams and worker scheduling |
| partition too finely        | more directories, catalogs, and cache churn     |

## Read Path

Reads are bounded:

```rust
stream.load_after_version(cursor, limit).await?;
stream.load_workflow_after_version(starter_id, cursor, limit).await?;
```

The crate rejects zero and oversized limits. Keep batch sizes large enough to
reduce overhead but small enough that handler memory use stays predictable.

The hard read limit is `10_000` events per call. Projection loops should commit
their application offset after applying a bounded batch, then continue until a
read returns an empty batch.

## Rotation

Rotation is physical. Public reads use `EventStreamVersion`; they do not expose file
cursors. The catalog maps version ranges to files.

Too-small size limits create many files and more catalog traversal. Too-large
files can make maintenance and checkpointing heavier. Choose limits based on
append volume and operational needs.

Rotation does not provide horizontal scaling by itself. It bounds files inside
one `EventStream`. Use partition keys when independent write paths or
independent projection scheduling are needed.

## Worker Pools

For each projection, use one active consumer per partition key and a bounded
global worker count. Store offsets in the application database so worker
restarts do not depend on event crate checkpoint state.

The consumer for a partition key should process events serially in
`EventStreamVersion` order. Add projection throughput through concurrency across
partition keys, not by splitting one partition stream across workers.

Recommended shape:

```text
dirty partition key ─▶ scheduler ─▶ one active consumer per projection/key
                                      │
                                      ▼
                         load_after_version(offset, limit)
                                      │
                                      ▼
                         apply read model + save offset
```

Avoid unbounded worker creation. If many partition keys become dirty at once,
queue keys and run only up to the application's configured worker limit.

## Notification Streaming

Progress notifications use bounded channels:

| Channel                  | Default capacity |
| ------------------------ | ---------------- |
| projector-to-loop sender | `256`            |
| broadcast fan-out        | `1024`           |

If a live receiver falls behind, older broadcast messages can be dropped. The
latest request status should also be recorded in `NotificationsStore` so clients
can reconnect by request ID.
