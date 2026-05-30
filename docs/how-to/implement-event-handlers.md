# Implement Event Handlers

Implement an event handler to process ordered events from one partition store. A handler can project a new read model, advance a resilient workflow, or do both in the same committed application transaction boundary.

## Before You Start

Complete [Getting started](../tutorial/getting-started.md) first. You should already have:

- an `EventNamespaces` resolver
- a namespace and partition key to handle
- an application database for handler state

## Store The Last Processed Event

Store one row per namespace and partition key in the application database:

```sql
-- Abridged. Generate the example application SQL with:
-- events_dev_cli schema app
CREATE TABLE last_processed_event (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    last_processed_event_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (namespace, partition_key)
);
```

Treat a missing row, or a row with `last_processed_event_version = 0`, as `EventStreamVersion::start()`. This is the default model: one serial handler owns all work for a partition key and saves `last_processed_event_version` only after read-model changes, workflow event handling, and side-effect outbox or idempotency state commit.

If workflows can fail in a way that requires an operator or admin retry, store that diagnostic state in `workflow_failures`:

```sql
-- Abridged. Generate the example application SQL with:
-- events_dev_cli schema app
CREATE TABLE workflow_failures (
    namespace TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    workflow_kind TEXT NOT NULL,
    workflow_started_by_event_id TEXT NOT NULL,
    failed_event_version INTEGER NOT NULL,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    is_retriable INTEGER NOT NULL,
    failed_at_ms INTEGER NOT NULL,
    failure_count INTEGER NOT NULL DEFAULT 1,
    reset_at_ms INTEGER,
    PRIMARY KEY (namespace, partition_key, workflow_kind, workflow_started_by_event_id)
);
```

This table is similar in purpose to a dead-letter mechanism, but it is application state: it records failed workflow runs and retry policy, not generic unhandled events. The handler reads only events after `last_processed_event`, and it updates that checkpoint in the same application transaction as its read-model or workflow state changes. After a restart, already checkpointed events are not handled again.

Use `is_retriable` as the current retry eligibility flag and `reset_at_ms` as the time that eligibility was reset. `is_retriable` answers "can this recorded failure be retried now?" while `reset_at_ms` records "when did a human explicitly reopen retry for this particular failure?"

`reset_at_ms` exists for failures that have already exhausted the workflow's retry policy. A well-written workflow should retry a failed step only the fixed number of times allowed by its retry policy, then record the workflow failure as not retriable. If the underlying issue is later fixed, for example by releasing corrected application code or restoring a required resource, an operator can reset the failure to retriable; `reset_at_ms` records when that reset happened. That makes the recorded failure retriable again for the user or system.

| State                                            | Meaning                                                                                                               |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `is_retriable = 1`, `reset_at_ms = NULL`         | The application classified the failure as safe to retry.                                                              |
| `is_retriable = 0`, `reset_at_ms = NULL`         | The application classified the failure as not safe to retry without intervention.                                     |
| `is_retriable = 1`, `reset_at_ms > failed_at_ms` | An operator reset the failure after it happened, so retry is allowed again.                                           |
| New failure after reset                          | Update `failed_at_ms`, increment `failure_count`, and clear `reset_at_ms` so the new failure is evaluated on its own. |

The retry check is:

```rust
let can_retry = is_retriable;
```

## Append With Expected Versions

Pass `ExpectedVersion` on every append so command decisions are checked against the current event-stream head:

```rust
let result = stream
    .append(ExpectedVersion::Exact(current_version), new_events)
    .await?;
```

Use `NoStream` for the first event in a stream, `Exact(version)` when the command decision was based on loaded state, and `Any` only when blind append is domain-correct. Treat `EsError::IncorrectEventVersion` as an unexpected consistency signal: reload state, re-run domain rules, then retry, merge, reject, or investigate duplicate writers.

## Build A Read-Model Projection

A projection turns event-stream events into an application read model. Start with one partition key and the saved `last_processed_event` row if it exists:

```text
partition_key = "user-123"
last_processed_event_version = missing or 0
```

Represent a missing row or `0` in Rust as `EventStreamVersion::start()`. Open the partition store, read a bounded batch after the saved event version, apply each event to the read model, and save the last processed event version.

## Drain A Handler Batch

Load `last_processed_event`, read after it, apply the batch, and save the new version in the same transaction as the read-model changes:

```rust
let last_processed = load_last_processed_event(&app_db, namespace, partition_key)
    .await?
    .unwrap_or(0);
let cursor = match last_processed {
    0 => EventStreamVersion::start(),
    version => EventStreamVersion::new(version)?,
};

let events = stream.load_after_version(cursor, 500).await?;
if events.is_empty() {
    return Ok(());
}

let last_version = events.last().expect("non-empty batch").version;

app_db.execute("BEGIN IMMEDIATE", ()).await?;
for event in &events {
    apply_event_to_read_model(&app_db, event).await?;
}
save_last_processed_event(&app_db, namespace, partition_key, last_version).await?;
app_db.execute("COMMIT", ()).await?;
```

Repeat the drain until a bounded read returns no events.

## Resume A Workflow Run

Start a workflow with `WorkflowRef::StartsThisWorkflow`:

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

The returned envelope stores the generated event ID in `workflow_started_by_event_id`. Continue the same run with `WorkflowRef::Continues { started_by_event_id }`. The workflow's durable step state is the event stream itself.

On restart, resume from `last_processed_event`. When the handler sees a workflow event, read only the events for that run and derive the next step from those events:

```rust
let workflow_cursor = match last_processed_event_version {
    0 => EventStreamVersion::start(),
    version => EventStreamVersion::new(version)?,
};
let events = stream
    .load_workflow_after_version(
        workflow_started_by_event_id,
        workflow_cursor,
        100,
    )
    .await?;
```

When the workflow reaches a terminal successful state, clear any matching `workflow_failures` row in the same transaction as read-model changes and `last_processed_event`. When the workflow reaches a terminal failed state that needs operator visibility, upsert `workflow_failures` and then save `last_processed_event` if the application has intentionally handled that event as a terminal failure.

## Handle Retry And Failure

- If event handling fails before the application transaction commits, leave `last_processed_event` unchanged.
- If the process crashes after committing, the saved offset prevents the same committed batch from being applied again.
- If external side effects are part of handling an event, make them idempotent or write an application-owned outbox entry in the same transaction as `last_processed_event`, then let a separate sender perform the side effect.
- If a valid workflow event cannot be processed after retry, write or update `workflow_failures` and alert an operator. Do not advance `last_processed_event` unless the application intentionally records that event as handled, for example by turning it into a terminal failed workflow state.

## Scale Across Partition Keys

Use one active handler per partition key. Add throughput by running handlers concurrently across independent partition keys, with a bounded global worker count.

For the scheduling pattern, see [Worker pool over per-partition stores](worker-pool-over-per-partition-store.md). For cursor semantics, see [Cursor mechanism](../explanation/cursor-mechanism.md).

## Verify The Handler

Test these cases:

1. `ExpectedVersion::NoStream` succeeds for the first append and fails after the stream exists.
2. Stale `ExpectedVersion::Exact(version)` fails with `EsError::IncorrectEventVersion`.
3. A clean read-model projection starts from `EventStreamVersion::start()`.
4. Handler failure does not advance the saved offset.
5. A crash after committing the offset does not re-apply committed read-model changes.
6. Workflow recovery reads only events with the stored `workflow_started_by_event_id`.
7. Two partition keys can be handled independently.

## Related Pages

- [API reference](../reference/api.md)
- [Cursor mechanism](../explanation/cursor-mechanism.md)
- [Worker pool over per-partition stores](worker-pool-over-per-partition-store.md)
