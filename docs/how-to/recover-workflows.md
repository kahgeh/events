# How to Recover Workflows After Crash

Workflow recovery lets an application resume or clean up multi-step work after a
projector or worker restarts. The events crate stores workflow metadata on
events; the application stores active workflow state.

## Overview

A workflow may span several events and external side effects:

```text
ProvisioningStarted -> MachineCreated -> VolumeAttached -> Provisioned
```

If the process stops after `MachineCreated`, reading the whole event stream is too
broad and a single workflow kind is not enough. The same partition can run
the same workflow kind many times.

Each workflow run is identified by the event ID that started it.

## When to Use Workflow Tracking

Use workflow tracking when work:

- spans more than one durable event
- needs crash recovery or cleanup
- can run more than once for the same partition key
- has external side effects that must be resumed carefully

Do not use workflow metadata as a replacement for normal projection offsets.

## Implementation Pattern

### 1. Define Your Workflow Events

Start the workflow with `WorkflowRef::StartsThisWorkflow`:

```rust
NewEvent {
    r#type: "ProvisioningStarted".into(),
    payload,
    workflow_kind: Some("provisioning".into()),
    workflow: WorkflowRef::StartsThisWorkflow,
    request_id,
    actor_id,
    actor_type,
}
```

The returned envelope stores the generated event ID in
`workflow_started_by_event_id`.

### 2. Check for Active Workflow on Startup

Active workflow state belongs in the application database:

```sql
CREATE TABLE active_workflows (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    workflow_kind TEXT NOT NULL,
    workflow_started_by_event_id TEXT NOT NULL,
    last_projected_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace, partition_key, workflow_kind, workflow_started_by_event_id)
);
```

On startup, load active workflow rows and open the relevant partition stores.

### 3. Derive Workflow State from Events

Read only the events for the workflow run:

```rust
let events = stream
    .load_workflow_after_version(
        workflow_started_by_event_id,
        last_projected_version,
        100,
    )
    .await?;
```

Unknown starter IDs return an empty batch. That is useful for permissive
recovery flows; strict applications can validate starter existence before
persisting active workflow state.

### 4. Track Active Workflow During Processing

Continue the workflow with the stored starter ID:

```rust
NewEvent {
    r#type: "MachineCreated".into(),
    payload,
    workflow_kind: Some("provisioning".into()),
    workflow: WorkflowRef::Continues {
        started_by_event_id,
    },
    request_id,
    actor_id,
    actor_type,
}
```

The crate shape-validates this metadata. It does not prove that the starter ID
exists or that it belongs to the same kind. Add domain validation when that
matters.

### 5. Clear Workflow on Success, Preserve on Failure

When the workflow reaches a terminal successful state, remove the active
workflow row in the same transaction as read-model changes and projection
offsets.

When recovery needs another retry, keep the active workflow row and update
diagnostic state in the application database.

## Complete Example

```rust
async fn recover_active_workflow(
    namespaces: &EventNamespaces,
    row: ActiveWorkflowRow,
    app_db: &turso::Connection,
) -> Result<(), EsError> {
    let namespace = namespaces.ensure_namespace(&row.namespace).await?;
    let partition = namespace.ensure_partition_exists(&row.partition_key).await?;
    let stream = partition.open().await?;

    let cursor = match row.last_projected_version {
        0 => EventStreamVersion::start(),
        version => EventStreamVersion::new(version)?,
    };

    let events = stream
        .load_workflow_after_version(row.workflow_started_by_event_id, cursor, 100)
        .await?;

    if events.is_empty() {
        return Ok(());
    }

    app_db.execute("BEGIN IMMEDIATE", ()).await?;
    for event in &events {
        apply_workflow_event(app_db, event).await?;
    }

    let last_version = events.last().expect("non-empty batch").version;
    save_workflow_offset(app_db, &row, last_version).await?;
    app_db.execute("COMMIT", ()).await?;

    Ok(())
}
```

### Key Points

- `workflow_kind` names the process type.
- `workflow_started_by_event_id` names one run.
- Application state decides whether to resume, retry, mark failed, or clean up.
- Projection offsets and active workflow state should commit together.

## Best Practices

### 1. Set Workflow Before Handler, Clear on Success

Persist active workflow state before starting external work. Clear it only after
the terminal durable event and read-model state are committed.

### 2. Make Recovery Idempotent

Recovery may run more than once. Use event IDs or workflow starter IDs to guard
external side effects.

### 3. Record Recovery Actions

Record the namespace, partition key, workflow kind, starter event ID, and last
projected version for every recovery attempt.

### 4. Design Idempotent Handlers

Handlers should tolerate repeated events after crashes. The exclusive cursor
keeps normal progress simple, but crash timing can still repeat an event whose
side effect partially completed.

### 5. Handle Recovery Failures

If recovery fails, keep the active workflow row and record enough diagnostic
state for an operator or retry scheduler.

## Verification

Test recovery with:

1. a workflow start event
2. unrelated interleaved events
3. a continued workflow event
4. a restart from the application-stored version

The recovered batch should include only events with the stored
`workflow_started_by_event_id` and version greater than the exclusive cursor.

## Related Documentation

- [Cursor Mechanism](../explanation/cursor-mechanism.md)
- [Implement Robust Event Projections](implement-projection.md)
- [API Reference](../reference/api.md)
