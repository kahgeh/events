# Events Crate Documentation

Welcome to the documentation for the Events crate: an embedded Rust event store
for append-only streams, optimistic concurrency, partitioned storage, and
durable projector checkpoints.

## 📚 Documentation Structure

This documentation follows the **Diátaxis framework**, organizing content by user needs:

### 🎓 [Tutorials](tutorial/) - Learning for Beginners

Step-by-step lessons that guide you through learning durable event streams from scratch.

- **[Getting Started](tutorial/getting-started.md)** - Your first event store and basic concepts
- **[First Project](tutorial/first-event-store.md)** - Build a complete e-commerce order system
- **[Building Projections](tutorial/building-projections.md)** - Create read models from events

### 🎯 [How-to Guides](how-to/) - Solutions for Specific Goals

Practical guides that show you how to solve specific problems and implement common patterns.

- **[Configure Rotation](how-to/configure-rotation.md)** - Set up partition rotation strategies
- **[Implement Projections](how-to/implement-projection.md)** - Build robust event processing
- **[Stream Progress Updates](how-to/stream-progress-updates.md)** - Real-time feedback for async operations
- **[Recover Workflows](how-to/recover-workflows.md)** - Handle incomplete workflows after crashes
- **[Handle Concurrency](how-to/handle-concurrency.md)** - Manage concurrent access and conflicts
- **[Partition by Tenant](how-to/partition-by-tenant.md)** - Run independent stores and projectors per tenant or shard
- **[Migrate Schema](how-to/migrate-schema.md)** - Handle database schema changes
- **[Monitor Production](how-to/monitor-production.md)** - Production monitoring and alerting
- **[Scale Consumers](how-to/scale-consumers.md)** - Handle high-volume event streams

### 📖 [Reference](reference/) - Detailed Information

Comprehensive technical reference for all APIs, configuration options, and concepts.

- **[API Reference](reference/api.md)** - Complete API documentation
- **[Configuration](reference/configuration.md)** - All configuration options
- **[Error Types](reference/error-types.md)** - Error handling reference
- **[SQL Schema](reference/sql-schema.md)** - Database schema documentation
- **[Performance](reference/performance.md)** - Performance characteristics and tuning

### 💡 [Explanation](explanation/) - Understanding the System

In-depth discussions of how and why the system works the way it does.

- **[Architecture](explanation/architecture.md)** - System design and rationale
- **[Progress Streaming](explanation/progress-streaming.md)** - Real-time progress feedback architecture
- **[Partitioning Strategy](explanation/partitioning-strategy.md)** - Why partitioning matters
- **[Concurrency Control](explanation/concurrency-control.md)** - Optimistic concurrency details
- **[Cursor Mechanism](explanation/cursor-mechanism.md)** - Cross-partition navigation


## 🚀 Quick Start

If you're new to the Events crate, start with the **[Getting Started tutorial](tutorial/getting-started.md)**.

```rust
use events::{EventStore, ExpectedVersion, NewEvent, RotationPolicy};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), events::EsError> {
    let store = EventStore::open_partitioned(
        "./data",
        RotationPolicy::TimeWindow {
            window: Duration::from_secs(3600), // 1 hour
            max_bytes: Some(512 * 1024 * 1024), // 512MB
        },
    ).await?;

    let result = store.append(
        "order-123",
        ExpectedVersion::NoStream,
        vec![NewEvent {
            r#type: "OrderCreated".into(),
            payload: json!({"total": 9999}),
        }],
    ).await?;

    println!("Appended events, new version: {}", result.version);
    Ok(())
}
```

## 🎯 Finding What You Need

### I'm New to the Events Crate
👉 Start with **[Getting Started](tutorial/getting-started.md)**

### I Need to Build Something Specific
👉 Browse the **[How-to Guides](how-to/)**

### I Need Detailed Technical Information
👉 Check the **[Reference Documentation](reference/)**

### I Want to Understand the Design Decisions
👉 Read the **[Explanation Articles](explanation/)**

## 🏗️ Key Concepts

### Event Store
The core component that stores and retrieves append-only event streams from
embedded Turso DB files with automatic rotation.

### Projections
Read models built by processing event streams, optimized for querying and reporting.

### Partitioning
Time-based organization of event data into separate files for performance and
maintainability. Applications can also partition by tenant or shard by opening
independent store roots.

### Concurrency Control
Optimistic concurrency using version numbers to prevent conflicting updates.

### Cursors
Position markers that enable reading events across multiple partitions.

### Progress Streaming
Real-time feedback system for async operations with reconnection support.

### Workflow Recovery
Track active workflows during checkpoints to enable recovery of incomplete multi-step operations after crashes.

## 📊 Features

- **💾 Embedded Storage**: Durable Turso DB files managed by the crate
- **🧾 Append-only Streams**: Immutable events grouped by stream ID
- **🔄 Automatic Rotation**: Time-based partition rotation with size limits
- **🔒 Concurrency Safe**: Optimistic concurrency control prevents data corruption
- **📍 Durable Checkpoints**: Projectors resume from persisted cursors
- **🧩 Application Partitioning**: Run independent stores/projectors per tenant or shard
- **🔧 Configurable**: Flexible rotation policies and performance tuning

## 🚧 Non-goals

- Distributed consumer coordination
- Cluster membership, replication, or shard rebalancing
- A full read-model or event-sourcing framework
- Multiple simultaneous owners for the same store instance

## 🛠️ Common Use Cases

### E-commerce Platforms
Track orders, payments, and inventory with complete audit trails.

### Financial Systems
Record transactions with immutable logs and regulatory compliance.

### IoT Data Ingestion
Handle high-volume sensor data with automatic partitioning.

### Audit Logging
Maintain tamper-proof logs for compliance and debugging.

### Service-local Event Logs
Keep a durable event history inside a Rust service without operating a separate
event-store service.

## 🔗 External Resources

- [CQRS Pattern](https://martinfowler.com/bliki/CQRS.html) - Command Query Responsibility Segregation
- [Turso Documentation](https://docs.turso.tech/) - Database engine documentation

## 🤝 Contributing

This documentation is a living resource. If you find:

- **Missing Information**: Please open an issue describing what you need
- **Errors**: Please report any inaccuracies or bugs
- **Improvements**: Suggestions for better examples or explanations

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Choose your path above or return to the [main README](../README.md)** for project overview.
