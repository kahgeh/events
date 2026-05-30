# Monitor Production Event Streams

Monitor the application boundary around partition stores: appends, event handlers, storage health, and progress notifications.

## Emit Core Metrics

Track append outcomes by `EsError` variant, append latency, partition store open failures, migration failures, cached database count, and progress notification send failures.

Projection health belongs in the application database:

```text
namespace
partition_key
last_processed_event_version
last_seen_event_stream_version
lag = last_seen - last_processed
```

## Add Health Checks

Expose health endpoints from the application. A useful endpoint checks:

- event partition root is writable
- a representative partition store can open
- `last_processed_event` storage is reachable
- worker queue depth is below the alert threshold

## Alert On Operational Risk

Alert on:

- append latency above target
- projection lag above target
- worker drain duration above target
- repeated `IncorrectEventVersion` errors for the same command class
- migration failures
- disk pressure near the event data root
- unexpected growth in files per partition store

## Include Correlation Fields

Configure logging at the application boundary so command handlers, event handlers, and worker-pool scheduling share fields:

- namespace
- partition key
- event-stream version
- workflow kind
- workflow starter event ID
- request ID
- actor type

## Production Checklist

- Append success and error metrics are emitted.
- Last processed event versions and projection lag are visible.
- Worker-pool queue depth and active worker count are visible.
- Batch sizes are bounded.
- Rotation file counts are monitored.
- Store cache size is monitored.
- Migration failures alert operators.
- Worker-pool max concurrency is bounded.

## Related Pages

- [Performance reference](../reference/performance.md)
- [Worker pool over per-partition stores](worker-pool-over-per-partition-store.md)
