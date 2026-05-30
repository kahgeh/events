# Events Documentation

Use this documentation by starting with the path that matches your current job.

- New to the crate: start with [Getting started](tutorial/getting-started.md), then use [Implement event handlers](how-to/implement-event-handlers.md) when you need read-model projections or resilient workflows.
- Implementing event handlers: use [Implement event handlers](how-to/implement-event-handlers.md) for expected-version appends, projections, and resilient workflows.
- Scaling or operating production: use [Worker pool over per-partition stores](how-to/worker-pool-over-per-partition-store.md), [Configure rotation](how-to/configure-rotation.md), [Monitor production](how-to/monitor-production.md), and [Stream progress updates](how-to/stream-progress-updates.md).
- Looking up facts: use the API, configuration, error, and performance reference pages.

Application-owned SQL snippets in these docs are abridged. Use `events_dev_cli schema app` to generate the example SQL for the application tables used by event handlers.

## Tutorials

- [Getting started](tutorial/getting-started.md)

## How-Tos

- [Configure rotation](how-to/configure-rotation.md)
- [Implement event handlers](how-to/implement-event-handlers.md)
- [Monitor production](how-to/monitor-production.md)
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
