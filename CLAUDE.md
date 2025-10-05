# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust crate (`events`) that implements a production-ready, partitioned event store with time-based partition rotation and optimistic concurrency control. The crate provides event sourcing functionality using Turso DB as the embedded database.

## Common Development Commands

**Build**:

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo check              # Quick syntax/type checking
```

**Test**:

```bash
cargo test               # Run all tests
cargo test --no-run      # Compile tests only
```

**Development**:

```bash
cargo fmt               # Format code
cargo clippy            # Lint code
```

**Run Examples**:

```bash
cargo run --example basic_usage
```

## Architecture Overview

The codebase follows a modular architecture with the following core components:

- **EventStore** (`src/eventstore.rs`) - Main API for event operations (append, read, subscribe)
- **Catalog** (`src/catalog.rs`) - Partition metadata and cursor management
- **Validation** (`src/validation.rs`) - Event validation and business rules
- **Projector** (`src/projector.rs`) - Event projection and consumer coordination
- **Rotation** (`src/rotation.rs`) - Time-based partition rotation logic
- **Pool** (`src/pool.rs`) - Database connection pooling for Turso
- **Migration** (`src/migration.rs`) - Database schema migrations

Key architectural patterns:

- **Time-based Partitioning**: Events are automatically partitioned by configurable time windows
- **Optimistic Concurrency Control**: Uses version numbers to prevent concurrent modifications
- **Lease-based Consumers**: Multiple consumer instances coordinated through database leases
- **Cross-partition Cursors**: Seamless event replay across partition boundaries

## Database Storage

The crate uses Turso DB embedded databases stored in a `./data` directory by default. Each stream gets its own database file with automatic schema migrations.

## Key Dependencies

- `turso = "0.2.0-pre.14"` - Embedded Turso DB database
- `tokio` - Async runtime (full features)
- `serde/serde_json` - Serialization for event payloads
- `uuid` - Unique identifier generation
- `time` - Time handling for partitioning
- `tracing` - Structured logging

## Testing Strategy

- Unit tests are co-located with source code
- Integration tests in `/tests/integration_tests.rs`
- Uses `tempfile` for isolated test databases
- Example code in `/examples/basic_usage.rs`

## Event Sourcing Concepts

This crate implements classic event sourcing patterns:

- **Events** are immutable facts with types and JSON payloads
- **Streams** are sequences of events identified by stream IDs
- **Projections** build read models from event streams
- **ExpectedVersion** provides optimistic concurrency control
- **PartitionedCursor** enables cross-partition event replay

## Configuration

The system is configured through `RotationPolicy` which determines:

- Time window size for automatic partition rotation
- Maximum partition size limits
- Partition naming conventions

# Memorize

## Turso DB is a Rust database rewrite of SQLite, NEVER refer to it as libSQL or SQLite. Always verify against /Users/kahgeh/Dev/xn/turso codebase, to make sure the api is available

