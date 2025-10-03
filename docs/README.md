# Events Crate Documentation

Welcome to the comprehensive documentation for the Events crate, a production-ready partitioned event store implementation in Rust.

## 📚 Documentation Structure

This documentation follows the **Diátaxis framework**, organizing content by user needs:

### 🎓 [Tutorials](tutorial/) - Learning for Beginners

Step-by-step lessons that guide you through learning event sourcing from scratch.

- **[Getting Started](tutorial/getting-started.md)** - Your first event store and basic concepts
- **[First Project](tutorial/first-event-store.md)** - Build a complete e-commerce order system
- **[Building Projections](tutorial/building-projections.md)** - Create read models from events

### 🎯 [How-to Guides](how-to/) - Solutions for Specific Goals

Practical guides that show you how to solve specific problems and implement common patterns.

- **[Configure Rotation](how-to/configure-rotation.md)** - Set up partition rotation strategies
- **[Implement Projections](how-to/implement-projection.md)** - Build robust event processing
- **[Handle Concurrency](how-to/handle-concurrency.md)** - Manage concurrent access and conflicts
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
- **[Partitioning Strategy](explanation/partitioning-strategy.md)** - Why partitioning matters
- **[Concurrency Control](explanation/concurrency-control.md)** - Optimistic concurrency details
- **[Cursor Mechanism](explanation/cursor-mechanism.md)** - Cross-partition navigation
- **[Lease Management](explanation/lease-management.md)** - Consumer coordination

## 🚀 Quick Start

If you're new to event sourcing, start with the **[Getting Started tutorial](tutorial/getting-started.md)**.

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

### I'm New to Event Sourcing
👉 Start with **[Getting Started](tutorial/getting-started.md)**

### I Need to Build Something Specific
👉 Browse the **[How-to Guides](how-to/)**

### I Need Detailed Technical Information
👉 Check the **[Reference Documentation](reference/)**

### I Want to Understand the Design Decisions
👉 Read the **[Explanation Articles](explanation/)**

## 🏗️ Key Concepts

### Event Store
The core component that stores and retrieves events from partitioned files with automatic rotation.

### Projections
Read models built by processing event streams, optimized for querying and reporting.

### Partitioning
Time-based organization of event data into separate files for performance and maintainability.

### Concurrency Control
Optimistic concurrency using version numbers to prevent conflicting updates.

### Cursors
Position markers that enable reading events across multiple partitions.

## 📊 Features

- **✅ Production-ready**: Comprehensive error handling and testing
- **⚡ High Performance**: Optimized Turso with WAL mode and connection pooling
- **🔄 Automatic Rotation**: Time-based partition rotation with size limits
- **🔒 Concurrency Safe**: Optimistic concurrency control prevents data corruption
- **📈 Scalable**: Horizontal scaling through partitioning and consumer coordination
- **🛡️ Reliable**: ACID compliance and comprehensive error recovery
- **🔧 Configurable**: Flexible rotation policies and performance tuning

## 🛠️ Common Use Cases

### E-commerce Platforms
Track orders, payments, and inventory with complete audit trails.

### Financial Systems
Record transactions with immutable logs and regulatory compliance.

### IoT Data Ingestion
Handle high-volume sensor data with automatic partitioning.

### Audit Logging
Maintain tamper-proof logs for compliance and debugging.

### Event-driven Architecture
Build decoupled systems with reliable event communication.

## 🔗 External Resources

- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html) - Martin Fowler
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