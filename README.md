# Events Crate

A durable event store for CQRS in Rust — append-only streams with time-based partition rotation, optimistic concurrency control, and projector utilities.

Planned with ChatGPT 5 ( reviewed by Sonnet 4.5 and GLM 4.6 )
Coded and documented by GLM 4.6

## Quick Links

**Getting started?** Start with our [tutorial series](docs/tutorial/).

**Looking for specific solutions?** Browse our [how-to guides](docs/how-to/).

**Need detailed information?** Check our [reference documentation](docs/reference/).

**Want to understand the design?** Read our [explanation articles](docs/explanation/).

## Key Features

- **Time-based Partitioning**: Automatic rotation of event files based on configurable time windows
- **Optimistic Concurrency Control**: Prevents concurrent modifications using version numbers
- **Cross-partition Cursors**: Seamless event replay across multiple partitions
- **Single-owner Processing**: Checkpoint-based consumer progression

## Documentation

### 📚 [Tutorials](docs/tutorial/) - Learning for Beginners

- [Getting Started](docs/tutorial/getting-started.md) - Your first event store
- [First Project](docs/tutorial/first-event-store.md) - Complete example application
- [Building Projections](docs/tutorial/building-projections.md) - Creating read models

### 🎯 [How-to Guides](docs/how-to/) - Solutions for Specific Goals

- [Configure Rotation](docs/how-to/configure-rotation.md) - Set up partition rotation
- [Implement Projections](docs/how-to/implement-projection.md) - Build event processors
- [Handle Concurrency](docs/how-to/handle-concurrency.md) - Manage concurrent access
- [Monitor Production](docs/how-to/monitor-production.md) - Production monitoring

### 📖 [Reference](docs/reference/) - Detailed Information

- [API Reference](docs/reference/api.md) - Complete API documentation
- [Configuration](docs/reference/configuration.md) - All configuration options
- [Error Types](docs/reference/error-types.md) - Error handling reference
- [SQL Schema](docs/reference/sql-schema.md) - Database schema

### 💡 [Explanation](docs/explanation/) - Understanding the System

- [Architecture](docs/explanation/architecture.md) - System design and rationale
- [Partitioning Strategy](docs/explanation/partitioning-strategy.md) - Why partitioning matters
- [Concurrency Control](docs/explanation/concurrency-control.md) - Optimistic concurrency details

## Quick Start

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
        vec![
            NewEvent {
                r#type: "OrderCreated".into(),
                payload: json!({"sku": "ABC", "qty": 1}),
            },
        ],
    ).await?;

    println!("Appended {} events", result.events.len());
    Ok(())
}
```

For detailed installation and usage instructions, see the [Getting Started tutorial](docs/tutorial/getting-started.md).

## License

This project is licensed under the MIT License.
