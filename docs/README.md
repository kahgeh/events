# Events Documentation

The events crate stores one ordered log per partition store. Applications
resolve partition stores with `EventNamespaces`, append and read through
`EventLog`, and keep projection offsets plus active workflow state in
their own database. Partitioning by owner or account is one scaling strategy layered on this
plain partition-store model.

## Tutorials

- [Getting started](tutorial/getting-started.md)
- [First event store](tutorial/first-event-store.md)
- [Building projections](tutorial/building-projections.md)

## How-Tos

- [Configure rotation](how-to/configure-rotation.md)
- [Handle concurrency](how-to/handle-concurrency.md)
- [Implement projection](how-to/implement-projection.md)
- [Migrate schema](how-to/migrate-schema.md)
- [Monitor production](how-to/monitor-production.md)
- [Recover workflows](how-to/recover-workflows.md)
- [Scale consumers](how-to/scale-consumers.md)
- [Stream progress updates](how-to/stream-progress-updates.md)
- [Worker pool over per-partition stores](how-to/worker-pool-over-per-partition-store.md)

## Explanation

- [Architecture](explanation/architecture.md)
- [Concurrency control](explanation/concurrency-control.md)
- [Cursor mechanism](explanation/cursor-mechanism.md)
- [Partitioning strategy](explanation/partitioning-strategy.md)
- [Progress streaming](explanation/progress-streaming.md)

## Reference

- [API reference](reference/api.md)
- [Configuration](reference/configuration.md)
- [Error types](reference/error-types.md)
- [Performance](reference/performance.md)
- [SQL schema](reference/sql-schema.md)
