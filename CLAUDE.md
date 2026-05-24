# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust crate (`events`) that implements durable event streams for CQRS-style Rust services. It stores one ordered event log per partition store, with resilient appends, bounded reads, time-based file rotation, optimistic concurrency control, and application-owned projection support. It uses Turso DB as the embedded database.

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

- **EventLog** (`src/event_log.rs`) - Main append/read API for one ordered event log
- **Partitions** (`src/partitions.rs`) - Namespace and partition-store resolution
- **Catalog** (`src/catalog.rs`) - Partition metadata and cursor management
- **Validation** (`src/validation.rs`) - Event validation and business rules
- **Projector** (`src/projector.rs`) - Application-owned projection guidance
- **Rotation** (`src/rotation.rs`) - Time-based partition rotation logic
- **Pool** (`src/pool.rs`) - Database connection pooling for Turso
- **Migration** (`src/migration.rs`) - Database schema migrations

Key architectural patterns:

- **Partition Store Resolution**: Applications choose namespace and partition keys explicitly
- **Physical Rotation**: Event files rotate by configurable time windows inside one partition store
- **Optimistic Concurrency Control**: Uses version numbers to prevent concurrent modifications
- **Application-owned Projection State**: Projection offsets and active workflow state stay in the application database
- **Progress Notification Separation**: Request progress notifications are separate from durable domain events

## Database Storage

The crate uses Turso DB embedded databases stored under a data directory. Each partition store has its own catalog and rotated event files with automatic schema migrations.

## Key Dependencies

- `turso = "0.5.1"` - Embedded Turso DB database
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

## Core Concepts

- **Events** are immutable facts with types and JSON payloads
- **Partition stores** are durable storage directories selected by namespace and partition key
- **EventLog** is the append/read handle for one ordered event log inside a partition store
- **Projectors** build read models by consuming events in order
- **ExpectedVersion** provides optimistic concurrency control
- **EventLogVersion** is the cursor and event version inside one opened event log

## Configuration

The system is configured through `RotationPolicy` which determines:

- Time window size for automatic partition rotation
- Maximum partition size limits
- Partition naming conventions

# Memorize

## Turso DB is a Rust database rewrite of SQLite, NEVER refer to it as libSQL or SQLite. Always verify against /Users/kahgeh/Dev/xn/turso codebase, to make sure the api is available
