# Monitor Production Event Streams

Monitor partition stores at the resolver, storage, and application-worker
boundaries.

## What You'll Monitor

- append success and error rates
- projection lag per partition key
- worker-pool activity and failures
- storage health and migration failures
- progress notification delivery health

## Key Metrics to Track

### 1. Event Stream Metrics

Track append outcomes by `EsError` variant:

- `IncorrectEventVersion`: stale command state or duplicate creation
- `CatalogDrift`: events committed but catalog head update failed
- `InvalidSafeName`: invalid namespace, partition key, or workflow kind
- `InvalidReadLimit`: caller requested an empty or too-large batch

Track event-stream head per hot partition key where useful.

### 2. Database Connection Monitoring

Monitor:

- partition store open failures
- migration failures
- cached database count
- active connection count
- open latency for hot stores

### 3. Projection Health Monitoring

Projection health belongs in the application database:

```text
namespace
partition_key
projection_name
last_projected_version
last_seen_event_stream_head_version
lag = last_seen - last_projected
```

## Health Check Endpoints

### HTTP Health Check Server

Expose health endpoints from the application, not the events crate. A useful
endpoint checks:

- event partition root is writable
- representative partition store can open
- projection offset table is reachable
- worker queue is below alert threshold

## Alerting Strategies

### 1. Performance Alerts

Alert on:

- append latency above target
- projection lag above target
- worker drain duration above target
- repeated `IncorrectEventVersion` errors for the same command class

### 2. Storage Monitoring

Alert on:

- `CatalogDrift`
- migration failures
- disk pressure near the event data root
- unexpected growth in files per partition store

## Logging Strategy

### Structured Logging

Include:

- namespace
- partition key
- event-stream version
- workflow kind
- workflow starter event ID
- request ID
- actor type

### Configuration

Configure logging at the application boundary so command handlers, projectors,
and worker-pool scheduling share correlation fields.

## Dashboard Examples

### Grafana Dashboard Queries

Useful panels:

- append latency by namespace
- error count by `EsError` variant
- projection lag by partition key
- active worker count
- pending dirty partition keys
- progress notification send failures

## Production Readiness Checklist

### 1. Monitoring Setup

- Append success/error metrics are emitted.
- Projection offsets are visible.
- Worker-pool queue depth is visible.

### 2. Performance Monitoring

- Batch sizes are bounded.
- Rotation file counts are monitored.
- Store cache size is monitored.

### 3. Error Handling

- `CatalogDrift` stops writes to the affected store.
- `IncorrectEventVersion` is handled as a domain conflict.
- Migration failures alert operators.

### 4. Capacity Planning

- Partition-key count is understood.
- Rotation cadence is sized for expected volume.
- Worker-pool max concurrency is bounded.

## Troubleshooting Common Issues

### High Append Latency

Check partition hot spots, file size, disk pressure, and store open latency.

### Projection Lag

Check worker failures, batch duration, pending dirty keys, and application
database transaction time.

### Connection Pool Exhaustion

Reduce active worker count or tune `EventNamespaces` idle-store cache settings.

## Best Practices

- Store projection offsets in the application database.
- Alert differently for domain conflicts and storage safety errors.
- Keep one active consumer per projection and partition key.
- Monitor partitioning by owner or account as a scaling strategy, not as a required domain
  model.

## Next Steps

- [Performance Reference](../reference/performance.md)
- [Scale Consumers](scale-consumers.md)
