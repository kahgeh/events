# Migrate Database Schema

Database schema migrations are essential for evolving your event store structure without data loss. This guide shows how to handle schema changes safely in production.

## What You'll Learn

- How migrations work in partitioned event stores
- Writing backward-compatible migrations
- Handling partition metadata changes
- Rolling back failed migrations
- Best practices for production deployments

## Understanding Migration Architecture

The Events crate uses a dual-database architecture:

1. **Catalog Database**: Stores partition metadata and consumer offsets
2. **Partition Databases**: Individual files containing event data

Each database has its own migration system that tracks applied migrations via checksums.

### Migration Tables

Both databases maintain a `_migrations` table:

```sql
CREATE TABLE _migrations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);
```

## Writing Migrations

### Basic Migration Structure

```rust
use events::migration::{Migration, MigrationRunner};

let migration = Migration {
    name: "003_add_event_metadata".into(),
    sql: r#"
    ALTER TABLE events ADD COLUMN metadata TEXT;
    CREATE INDEX IF NOT EXISTS idx_events_metadata ON events(metadata);
    "#,
};
```

### Partition Database Migrations

These migrations apply to individual partition files containing event data.

```rust
use events::migration::partition_migrations;

pub fn my_partition_migrations() -> MigrationRunner {
    partition_migrations()
        .with_migration(Migration {
            name: "003_add_event_metadata".into(),
            sql: r#"
            ALTER TABLE events ADD COLUMN metadata TEXT;
            CREATE INDEX IF NOT EXISTS idx_events_metadata ON events(metadata);
            "#,
        })
        .with_migration(Migration {
            name: "004_add_event_source".into(),
            sql: r#"
            ALTER TABLE events ADD COLUMN source TEXT;
            "#,
        })
}
```

### Catalog Database Migrations

These migrations apply to the central catalog database.

```rust
use events::migration::catalog_migrations;

pub fn my_catalog_migrations() -> MigrationRunner {
    catalog_migrations()
        .with_migration(Migration {
            name: "005_add_partition_tags".into(),
            sql: r#"
            ALTER TABLE partitions ADD COLUMN tags TEXT;
            "#,
        })
}
```

## Migration Best Practices

### 1. Use Descriptive Names

```rust
// Good
Migration { name: "003_add_event_metadata_index".into(), ... }

// Avoid
Migration { name: "003_new_index".into(), ... }
```

### 2. Make Changes Additive

```sql
-- Good: Add new column
ALTER TABLE events ADD COLUMN metadata TEXT;

-- Avoid: Rename existing column
ALTER TABLE events RENAME COLUMN payload TO data;
```

### 3. Use IF EXISTS/IF NOT EXISTS

```sql
-- Good
CREATE INDEX IF NOT EXISTS idx_events_metadata ON events(metadata);
ALTER TABLE events ADD COLUMN IF NOT EXISTS metadata TEXT;

-- Risky
CREATE INDEX idx_events_metadata ON events(metadata);
```

### 4. Consider Performance Impact

```sql
-- For large tables, add index after column
ALTER TABLE events ADD COLUMN metadata TEXT;
-- Index creation can be done separately
CREATE INDEX IF NOT EXISTS idx_events_metadata ON events(metadata);
```

## Common Migration Scenarios

### Adding Event Fields

```rust
Migration {
    name: "006_add_event_correlation_id".into(),
    sql: r#"
    ALTER TABLE events ADD COLUMN correlation_id TEXT;
    CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events(correlation_id);
    "#,
}
```

### Adding Partition Metadata

```rust
Migration {
    name: "007_add_partition_size".into(),
    sql: r#"
    ALTER TABLE partitions ADD COLUMN size_bytes INTEGER DEFAULT 0;
    "#,
}
```

### Optimizing Queries

```rust
Migration {
    name: "008_optimize_stream_queries".into(),
    sql: r#"
    CREATE INDEX IF NOT EXISTS idx_events_stream_type ON events(stream_id, type, version);
    "#,
}
```

## Running Migrations

### Automatic Migration

```rust
use events::{EventStore, migration::partition_migrations};

let store = EventStore::open_partitioned_with_migrations(
    "./data",
    rotation_policy,
    my_partition_migrations(),
    my_catalog_migrations(),
).await?;
```

### Manual Migration

```rust
use events::migration::MigrationRunner;
use turso::Database;

let db = Database::open("./data/catalog.db").await?;
let runner = my_catalog_migrations();
runner.run(&db).await?;
```

## Handling Migration Failures

### Checkpoint Strategy

Migrations are transactional per-database. If a migration fails:

1. **Catalog DB**: Rollback completely, system remains unusable until fixed
2. **Partition DB**: Only affected partition fails, others continue working

### Recovery Process

```rust
// Fix the failed migration
let fixed_migration = Migration {
    name: "003_add_event_metadata".into(),
    sql: "ALTER TABLE events ADD COLUMN metadata TEXT DEFAULT '{}';", // Fixed SQL
};

// Update migration runner and retry
let runner = my_partition_migrations();
runner.run(&db).await?;
```

## Production Deployment

### Blue-Green Migration Strategy

1. **Prepare**: Write and test migrations thoroughly
2. **Backup**: Create full backup of catalog and active partitions
3. **Stage**: Deploy to staging environment first
4. **Migrate**: Apply migrations during maintenance window
5. **Verify**: Run health checks and validation queries
6. **Monitor**: Watch for performance issues post-migration

### Zero-Downtime Migration

For some changes, you can avoid downtime:

```rust
// Step 1: Add new column (nullable)
ALTER TABLE events ADD COLUMN new_field TEXT;

// Step 2: Update application to write both old and new fields
// Deploy application code

// Step 3: Backfill data for existing events
UPDATE events SET new_field = extract_from_payload(payload) WHERE new_field IS NULL;

// Step 4: Update application to use only new field
// Deploy application code

// Step 5: Remove old field (optional, in next migration)
```

## Testing Migrations

### Unit Tests

```rust
#[tokio::test]
async fn test_add_metadata_migration() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open(&temp_dir.path().join("test.db")).await.unwrap();

    let migration = Migration {
        name: "001_test_migration".into(),
        sql: "CREATE TABLE test (id INTEGER); ALTER TABLE test ADD COLUMN name TEXT;",
    };

    let runner = MigrationRunner::new().with_migration(migration);
    runner.run(&db).await.unwrap();

    // Verify migration worked
    let result = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='test'", ()).await.unwrap();
    assert!(result.next().await.unwrap().is_some());
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_full_migration_flow() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create store with initial migrations
    let store1 = EventStore::open_partitioned(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::default(),
    ).await.unwrap();

    // Close store
    drop(store1);

    // Create new store with additional migrations
    let store2 = EventStore::open_partitioned_with_migrations(
        temp_dir.path().to_str().unwrap(),
        RotationPolicy::default(),
        enhanced_partition_migrations(),
        enhanced_catalog_migrations(),
    ).await.unwrap();

    // Verify new schema works
    let result = store2.append("test", ExpectedVersion::NoStream, vec![]).await;
    assert!(result.is_ok());
}
```

## Troubleshooting

### Common Issues

1. **Checksum Mismatch**: Migration SQL changed after being applied
2. **Failed Migration**: Syntax error or constraint violation
3. **Locked Database**: Another process has exclusive access
4. **Disk Space**: Insufficient space for migration operation

### Diagnostic Queries

```sql
-- Check applied migrations
SELECT name, checksum, applied_at FROM _migrations ORDER BY id;

-- Check table schema
PRAGMA table_info(events);

-- Check indexes
PRAGMA index_list(events);

-- Check database integrity
PRAGMA integrity_check;
```

## Summary

- Use descriptive, additive migrations with IF EXISTS/IF NOT EXISTS
- Test migrations thoroughly in staging environments
- Plan for rollback strategies and backup procedures
- Consider zero-downtime migration techniques for production systems
- Monitor system performance after applying migrations

Proper migration management ensures your event store can evolve safely while maintaining data integrity and system availability.