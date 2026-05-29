# Events Documentation

The events crate stores one ordered event stream per partition store. Applications resolve partition stores with `EventNamespaces`, append and read through `EventStream`, and keep projection offsets plus active workflow state in their own database. Partitioning by an application-defined group, such as owner or account, is one scaling strategy layered on this plain partition-store model.

## Tutorials

- [Getting started](tutorial/getting-started.md)
- [First durable stream](tutorial/first-durable-stream.md)
- [Building projections](tutorial/building-projections.md)

## How-Tos

- [Configure rotation](how-to/configure-rotation.md)
- [Use ExpectedVersion](how-to/use-expected-version.md)
- [Implement projection](how-to/implement-projection.md)
- [Migrate schema](how-to/migrate-schema.md)
- [Monitor production](how-to/monitor-production.md)
- [Recover workflows](how-to/recover-workflows.md)
- [Stream progress updates](how-to/stream-progress-updates.md)
- [Worker pool over per-partition stores](how-to/worker-pool-over-per-partition-store.md)

## Explanation

- [Architecture](explanation/architecture.md)
- [Cursor mechanism](explanation/cursor-mechanism.md)
- [Partitioning strategy](explanation/partitioning-strategy.md)
- [Progress streaming](explanation/progress-streaming.md)

## Reference

- [API reference](reference/api.md)
- [Configuration](reference/configuration.md)
- [Error types](reference/error-types.md)
- [Performance](reference/performance.md)
