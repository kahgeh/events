# Migrate Database Schema

Schema migration recreates crate-owned event and catalog tables for the
partition-log schema. Back up any event data that must be preserved before
running the service against an existing data directory.

## What You'll Learn

- How event and catalog migrations are organized
- What schema state to verify for partition logs
- How to test migrations before production use

## Understanding Migration Architecture

The crate manages two schema families:

- partition event databases
- per-partition catalog databases

### Migration Tables

Migration records are maintained by the crate migration runner. Application
read-model tables and projection offsets should be migrated by the application.

## Writing Migrations

### Basic Migration Structure

Migration names should be descriptive and ordered. The partition-log schema uses:

```text
002_reset_owner_log_events_schema
002_reset_owner_log_catalog_schema
```

### Partition Database Migrations

Partition event databases store event rows with owner-log versions and workflow
metadata:

```text
events.version
events.workflow_kind
events.workflow_started_by_event_id
events.actor_id
events.actor_type
```

### Catalog Database Migrations

Catalog databases store owner-log head and rotated file ranges:

```text
owner_log
partition_refs
```

## Migration Best Practices

### 1. Use Descriptive Names

Migration names should make the schema target clear.

### 2. Make Changes Additive

Use additive migrations for normal application schemas. For crate-owned event
schemas, follow the crate migration contract for this release.

### 3. Use IF EXISTS/IF NOT EXISTS

Use defensive SQL where migrations may run against partially initialized data
directories.

### 4. Consider Performance Impact

Run migrations against a copy of production data before rollout.

## Common Migration Scenarios

### Adding Event Fields

Event schema changes belong in crate migrations. Application-specific data should
usually live in event payloads or read-model tables.

### Adding Partition Metadata

Partition metadata belongs in catalog migrations.

### Optimizing Queries

Add indexes only when read paths require them. Owner-log reads are driven by
version ranges and event version ordering.

## Running Migrations

### Automatic Migration

Opening a partition store applies the required crate migrations.

### Manual Migration

For production rollout, run the service against a copy of the data directory and
verify the schema before pointing workers at the updated crate.

## Handling Migration Failures

### Checkpoint Strategy

Projection offsets and active workflow state are application-owned. Keep them in
the application database and back them up with the read model.

### Recovery Process

If migration fails, stop the service, restore from backup if needed, and inspect
the affected partition store before retrying.

## Production Deployment

### Blue-Green Migration Strategy

Test against a copied data directory, then switch traffic after successful append
and read verification.

### Zero-Downtime Migration

Do not assume zero-downtime rollout for destructive event/catalog schema changes.
Plan a maintenance window when existing data must be preserved or transformed.

## Testing Migrations

### Unit Tests

Test migration SQL with temporary directories.

### Integration Tests

Open a partition store, append an event, and confirm:

- `owner_log.current_version` advances
- event rows have `version`
- workflow rows can be queried by `workflow_started_by_event_id`

## Troubleshooting

### Common Issues

- missing backup before running against existing data
- application offsets left in the event database
- schema inspected in the wrong partition directory

### Diagnostic Queries

Inspect `owner_log`, `partition_refs`, and `events` for the affected partition
store.

## Summary

Migrate crate-owned event/catalog schema through the events crate. Keep
application read models, projection offsets, and active workflow state in the
application database.
