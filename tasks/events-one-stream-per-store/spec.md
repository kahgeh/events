# Events One Stream Per Store

## Status

Spec draft. This document is a pre-implementation spec for the `events` crate and must be checked against the final code before being migrated into permanent docs.

## Problem Framing

The current `events` crate supports many logical streams inside one physical event store. Each event row carries `stream_id`, the partition table enforces `UNIQUE(stream_id, version)`, and the catalog tracks many stream heads in one `stream_heads` table. That model is flexible, but it means unrelated streams still share the same active partition database writer.

The design discussion started from database contention, then brought the workflow concept back into scope. Removing row-level `stream_id` by itself is not enough if retryable business processes still need a durable identity. The better model is:

```text
physical event store = one aggregate owner's ordered event log
workflow_kind = optional business process type inside that log
workflow_started_by_event_id = event ID that identifies one workflow run
```

Concrete questions this task should answer:

- How does the crate create a new event store when the first event arrives for an aggregate owner?
- How does append/load/replay work when one store contains one ordered aggregate log?
- How does the crate represent workflow retry/recovery without using `stream_id` as the workflow identity?
- How should applications run projection workers over many aggregate-owned stores without exposing low-level batch-drain APIs?
- What documentation and examples should replace the current tenant projector material?

Resolved decision:

- Partition management is a first-class crate API boundary. Applications should not hand-build event-store paths and call `EventStore::open_partitioned(...)` directly for normal owner-partition use. A crate-owned partition manager should validate namespace/partition keys, derive readable safe storage paths, explicitly ensure partition storage exists, open partition stores, and provide discovery/listing hooks for worker pools.
- `ensure_exists(namespace, partition_key)` should return a `Partition` reference. `Partition::open()` returns the per-owner public `OwnerEventStore`. No compatibility layer is required for keeping `EventStore` as the public handle name.
- Namespace and partition-key path handling uses strict literal path segments. Applications generate stable filesystem-safe partition keys up front, and the partition manager rejects invalid namespace or partition-key segments before creating storage.
- No root-level partition registry is needed for the initial design. Because partition keys are readable single safe path segments, listing partition stores can scan the immediate child directories of `<root>/<namespace>/` and return descriptors from directory names without opening or migrating each store. Files and invalid child names are skipped rather than failing the whole listing. Returned descriptors are sorted by partition key ascending.
- Namespaces and partition keys use the same safe-segment rule: lowercase ASCII letters, ASCII digits, and `-`; length `1..=128`; reject anything else.
- One `EventPartitions` partition manager uses one `RotationPolicy` for every namespace and owner store it manages. Do not add per-namespace or per-owner rotation policy configuration initially.
- `NewEvent` and per-event `EventEnvelope` do not carry owner identity. The owner is selected by opening an `OwnerEventStore` from a `Partition`; callers that need owner identity for logs or projection context should get it from the partition/store handle or a once-per-batch context/result, not from each event.
- The partition manager should cache open stores with a bounded internal cache. The initial public knobs are `max_open_stores` and `idle_store_ttl`; active handles keep their store alive, idle stores may be evicted least-recently-used, and callers do not acquire/release explicit leases.
- Workflow metadata is event-level metadata on `NewEvent`, not batch-level append context. A single batch may contain workflow and non-workflow events, or events for different workflow runs, while still appending atomically to one owner log.
- A workflow can run multiple times for the same owner, so workflow run identity is the starter event ID. Use `workflow_started_by_event_id` to group one run; use `workflow_kind` to describe what process is running.
- Workflow-starting events should use an explicit "this event starts the workflow" marker. Append generates the event ID and writes that same ID into `workflow_started_by_event_id` for the starting row. Later workflow events use the concrete starter event ID returned by append.
- Workflow metadata must be internally consistent: `WorkflowRef::None` requires `workflow_kind = None`, and `WorkflowRef::StartsThisWorkflow` or `WorkflowRef::Continues` requires `workflow_kind = Some(...)`.
- `workflow_kind` is a stable machine label, not a path segment: lowercase ASCII letters, digits, and `-` only.
- `WorkflowRef::Continues` is shape-validated only. Append does not verify that the starter event exists or that it has matching workflow semantics.
- `ProjectorBatchOutcome` and `process_next_batch_with_handler` should be removed rather than wrapped. The `events` crate should not expose a public finite-batch outcome surface; applications that need worker-per-partition pools can own scheduling, batching, retry, and drain state using bounded owner-log reads and application-owned offsets.
- Active workflow state belongs in the application DB for the new design. The application should commit active workflow state, read-model changes, and projection offsets in its own transaction. The `events` crate provides workflow metadata and read APIs, not application workflow checkpoint ownership.
- Events-owned consumer checkpoints should be removed from the target design. There is no compatibility requirement for `consumer_offsets`, `workflow_stream_id`, or `workflow_event_id`.
- Public owner-log cursors should be owner-log versions, not `PartitionedCursor`. The partition store catalog maps owner-log version ranges to rotated DB files internally.
- Cross-owner/global cursor APIs should be removed from the target public surface. Do not keep `all_since`, `all_since_with_positions`, `PartitionedCursor`, `bootstrap_cursor`, or events-owned `checkpoint`; applications should project each owner log with owner-local version cursors and application-owned offsets.
- Owner-log versions should be exposed as a public newtype such as `OwnerLogVersion`, not plain `i64`, so expected-version checks, append results, and read cursors cannot be confused with unrelated counters or row IDs.
- `OwnerLogVersion::start()` should be the typed public cursor for "before the first event". Stored event versions still start at `1`.
- `OwnerLogVersion::start()` is valid only for read/projection cursors. It must not be accepted as `ExpectedVersion::Exact`; first append uses `ExpectedVersion::NoStream`.
- `OwnerLogVersion::new(0)` should be invalid. `OwnerLogVersion::start()` is the only public way to construct the before-first cursor.
- Keep `ExpectedVersion::Any` in the target API. It means blind append after the current owner-log head with no caller-supplied head check; it is not a synonym for `Exact(head)`.
- `load_after_version` and `load_workflow_after_version` use exclusive cursors: the event at the cursor version is not returned.
- Bounded read limits must be at least `1`. A returned empty batch means there are no matching events after the cursor, not that the caller requested zero events.
- Bounded read limits must also have a fixed internal crate-enforced maximum, for example `MAX_READ_LIMIT`, so a caller cannot accidentally load an unbounded number of events into memory. The same maximum applies to owner-log reads and workflow-filtered reads. Do not add a public read-limit configuration knob initially, and do not export the maximum as public API.

Terminology:

- `owner partition`: the application-chosen aggregate owner that gets one physical event-store lineage, such as `user-123`, `client-123`, or `shard-07`.
- `partition store`: the physical event-store directory and rotated DB lineage for one owner partition.
- `partition manager`: the crate-owned API that validates partition keys, ensures partition storage exists, opens partition stores, and lists partitions.
- `partition`: a public reference to an existing partition store path. It can be opened into an `OwnerEventStore`, but it is not itself an append/read handle.
- `owner log`: the ordered version sequence inside one partition store.
- `owner log version`: a monotonic event position inside one owner log, exposed publicly as `OwnerLogVersion` and stored internally as an integer. `OwnerLogVersion::start()` is the typed cursor before event version `1`; `OwnerLogVersion::new(n)` only accepts stored event versions `n >= 1`.
- `workflow_kind`: an optional business process type inside an owner log, such as `provisioning` or `portal-request`.
- `workflow_started_by_event_id`: the event ID that identifies one retryable workflow run. For the starting event, this is the starting event's own ID.

Avoid using `tenant` as the primary example term. It made the scaling pattern look multi-tenant specific, when the core pattern is projection over independently owned event stores.

## Goal

Redesign the `events` crate around one ordered event log per physical partition store. Applications choose the owner partition key, explicitly ensure the partition exists, then open an `OwnerEventStore` for append/read operations.

The design must also make workflow retry/recovery explicit. Workflow run identity should be separate from storage identity: an event may belong to an owner log and also carry `workflow_started_by_event_id` that groups one retryable business process run inside that log. Active workflow recovery state belongs to the application DB, where it can be committed with application state and projection offsets.

## Non-Goals

- Do not collapse unrelated business workflows into one workflow just because they share an owner log.
- Do not provide cross-owner global ordering as a native guarantee.
- Do not move transient progress/completion notifications into the durable event log. `NotificationsStore` remains separate.
- Do not design user/client-specific Greenfields portal behavior in this task beyond using it as a motivating caller.
- Do not keep tenant-specific example naming as the primary documentation path.
- Do not preserve branch-local intermediate schemas as legacy compatibility if no production data exists.
- Do not preserve events-owned consumer checkpoint compatibility.
- Do not preserve cross-owner/global cursor APIs in the target public surface.
- Do not preserve `EventStore` as the public per-owner handle name for compatibility.

## Update Type

- Primary update type: public API, storage contract, and documentation contract change.
- Secondary update type: architecture/design decision and example replacement.
- Likely permanent documentation impact: reference docs for event store layout and workflow metadata; how-to docs for worker-pool-over-per-partition-store; explanation docs for owner logs versus workflows.
- Existing docs likely affected:
  - `events/docs/how-to/partition-by-tenant.md`
  - `events/examples/tenant_projectors.rs`
  - `events/README.md`
  - `events/docs/README.md`
  - `events/docs/reference/api.md`
  - `events/docs/reference/sql-schema.md`
  - `events/docs/explanation/architecture.md`
  - `events/docs/explanation/concurrency-control.md`
  - `events/docs/how-to/recover-workflows.md`

## Current Context

Current storage model:

- `EventStore::append(stream_id, expected, events)` routes by `stream_id`.
- `EventEnvelope` includes `stream_id`.
- Partition DB `events` table stores `stream_id`.
- Partition DB `events` table enforces `UNIQUE(stream_id, version)`.
- Catalog DB stores `stream_heads(stream_id, version, ...)`.
- `Projector` consumes global event-store order using partition cursor state.

Current workflow model:

- `ActiveWorkflow` stores `stream_id` and `event_id`.
- `consumer_offsets` stores `workflow_stream_id` and `workflow_event_id`.
- `load_since_event(stream_id, event_id)` exists for workflow recovery.
- In `alir-platform`, provisioning uses active workflow recovery more directly than Greenfields currently does.
- Target design removes events-owned consumer offsets and moves active workflow recovery state to the application DB.

Current example/doc model:

- `events/examples/tenant_projectors.rs` demonstrates application-level partitioning by opening one event store per tenant.
- `events/docs/how-to/partition-by-tenant.md` documents tenant/shard partitioning.
- That example exposes `ProjectorBatchOutcome` directly and teaches readers to build an on-demand drain loop. The redesign should remove that public batch outcome surface.

Current notifications model:

```text
<data_dir>/
  events/
    durable domain event stores

  stream_events/
    notifications_store.db
    transient progress/completion notifications
```

Notifications are separate and should stay separate.

## Behavior Contract

Happy path:

- A caller ensures an owner partition exists, for example `namespace = "users"`, `partition_key = "user-123"`.
- If the partition store does not exist, `ensure_exists` creates the namespace/partition directory and minimal partition metadata, then returns a `Partition`.
- The caller opens the `Partition` into an `OwnerEventStore`.
- Append loads the owner log head and applies `ExpectedVersion`.
- Versions are monotonic within the owner log.
- Append returns the stored event envelopes, including generated event IDs, assigned owner-log versions, and resolved `workflow_started_by_event_id` values.
- Events may include workflow metadata when they belong to a retryable business process run.
- Projectors can load bounded owner-log batches, or bounded batches for one workflow run, using owner-log version cursors.

State changes and side effects:

- Each partition store owns its catalog, partitions, and owner-log head. Application projection offsets and active workflow state are application-owned.
- Event rows no longer need row-level `stream_id` for routing.
- Event rows should support optional workflow metadata:

```sql
workflow_kind TEXT,
workflow_started_by_event_id TEXT,
```

- Application workflow recovery should be able to persist in the application DB:

```text
workflow_kind
owner partition identity
workflow_started_by_event_id
last_projected_version
```

- Projection offsets for application read models are owned by the application database, not by the `events` crate. An application that updates a read model should commit the read-model mutation and the projected offset in the same application DB transaction.

- Example application-owned projection offset table:

```sql
CREATE TABLE client_workflow_projection_offsets (
    client_id TEXT PRIMARY KEY,
    last_projected_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
```

For a user-owned owner log, the same shape would use the owner key selected by the application:

```sql
CREATE TABLE user_projection_offsets (
    user_id TEXT PRIMARY KEY,
    last_projected_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
```

- Rotated DB partitions should include an index that makes workflow recovery practical:

```sql
CREATE INDEX idx_events_workflow_started_version
ON events(workflow_started_by_event_id, version);
```

Error and recovery behavior:

- Invalid namespace or partition key is rejected before any storage is created.
- Concurrent ensure/open/first append for the same owner partition creates at most one valid store and one version `1`; losing first appends get a clean expected-version/concurrency error.
- Store creation must not leave corrupt half-created state that later appends cannot recover from.
- A workflow failure records enough identity to retry from the workflow start or active step.
- Retryable handler failures do not advance projection offsets past the failed event.
- Non-retryable workflow events can be explicitly recorded as failed/skipped and have application-owned offsets advanced according to handler policy.

Existing behavior that must not regress:

- Batch append remains atomic within one owner log.
- `ExpectedVersion::NoStream`, `Any`, and `Exact(OwnerLogVersion)` semantics remain per owner log. `Any` appends after the current head without checking a caller-supplied version. `Exact` must reject or make unrepresentable `OwnerLogVersion::start()`.
- Actor fields, request IDs, trace/span IDs, payload validation, and rotation remain supported.
- `NotificationsStore` continues to support transient progress/completion by request ID.

## Contract Surface

Public or cross-module contract changes are required.

### Data Contracts

| Name | Kind | Fields / shape | Validation rules | Ownership / lifetime | Compatibility notes |
| ---- | ---- | -------------- | ---------------- | -------------------- | ------------------- |
| Partition store path | Storage layout | `<root>/<namespace>/<partition_key>/catalog.db` and rotated event DBs | Namespace and partition key must be strict safe path segments | Owned by `events`; explicitly created by `ensure_exists` | Replaces one shared store containing many row-level streams |
| Partition descriptor | Public metadata | Namespace, partition key, storage path | Namespace and partition key validated by the partition manager | Owned by `events`; returned to applications for listing/supervision | Derived from directory names during listing; does not prove the store opens or migrations are current |
| Partition | Public existing-partition reference | Namespace, partition key, storage path | Created by `ensure_exists` or derived from a valid listed descriptor | Owned by `events`; can be opened into `OwnerEventStore` | Represents storage existence, not an open DB handle |
| Partition manager rotation policy | Partition manager configuration | One `RotationPolicy` passed to `EventPartitions::open(root, rotation_policy)` | Applies to all owner stores opened by that partition manager | Owned by the partition manager | Avoids per-namespace/per-owner rotation policy surface initially |
| Event row | Partition DB row | `id`, `version`, `type`, `payload`, optional `workflow_kind`, optional `workflow_started_by_event_id`, timestamps, trace/span/request/actor fields | `version` unique in the owner log; payload size enforced; workflow-starting rows set `workflow_started_by_event_id = id`; workflow kind is present exactly when workflow ref is not `None`; workflow kind uses lowercase ASCII letters, digits, and `-` only | Durable owner event log | `stream_id` removed as routing state |
| Owner log version | Public scalar / storage integer | `OwnerLogVersion` wrapping an integer storage value | Stored event versions are positive and monotonic within one owner log; `OwnerLogVersion::new(0)` is invalid; `OwnerLogVersion::start()` is allowed only as a read/projection cursor before the first event and is invalid for `ExpectedVersion::Exact` | Stored as integer; exposed as newtype | Prevents confusing event-log cursors with row IDs or unrelated counters |
| Event envelope | Public returned event | `id`, `version: OwnerLogVersion`, `type`, `payload`, optional `workflow_kind`, optional `workflow_started_by_event_id`, timestamps, trace/span/request/actor fields | Mirrors stored row; no owner identity fields per event | Returned from append/read APIs | Lets callers persist starter event IDs and projection versions without reloading |
| Append result | Public append response | `first_version: OwnerLogVersion`, `last_version: OwnerLogVersion`, stored event envelopes, optional once-per-result owner context | Versions cover exactly the appended batch; envelopes include generated IDs and resolved workflow anchors | Returned from append API | Needed so `WorkflowRef::StartsThisWorkflow` callers can persist the generated starter event ID |
| Owner log head | Catalog state | Current `OwnerLogVersion`, last event ID, last partition | Head only advances | Owned by one partition store | Replaces catalog `stream_heads` keyed by many stream IDs |
| Rotated file range | Catalog state | Partition DB name/path, `first_version`, optional `last_version`, sealed flag | Ranges are contiguous and non-overlapping inside one owner log | Owned by one partition store | Lets `load_after_version` cross rotated DB files without exposing file cursors |
| Application projection offset | Application DB table | Owner key, last projected owner-log version, updated time | Version only advances; update in same transaction as read-model mutation | Owned by the application, not `events` | Example: `client_workflow_projection_offsets(client_id, last_projected_version, updated_at_ms)` |
| Active workflow state | Application DB metadata | Owner key, `workflow_kind`, `workflow_started_by_event_id`, last projected version/current step, updated time | `workflow_started_by_event_id` identifies one run inside the owner log | Owned by the application; commit with read-model state and projection offset | Replaces `workflow_stream_id` / `workflow_event_id` in events-owned consumer offsets |

### Callable Contracts

| Function / method / command | Caller | Inputs | Output / result | Error variants | Side effects | Compatibility notes |
| --------------------------- | ------ | ------ | --------------- | -------------- | ------------ | ------------------- |
| Ensure partition exists | Command handlers/services | Namespace, partition key | `Partition` reference | Invalid key, create/migration-prep failure | Creates namespace/partition directory and minimal partition metadata if missing | Explicit creation boundary; replaces hidden lazy creation in a generic resolve call |
| Open partition | Command handlers/projectors | `Partition` reference | `OwnerEventStore` handle for one partition store | Open/migration failure | Opens/migrates one owner store and may enter the partition manager cache | First-class crate API replacing caller-provided `stream_id` on every call and application-side `EventStore::open_partitioned(...)` path construction; no public `EventStore` compatibility alias required |
| Partition cache settings | Services and supervisors | `max_open_stores`, `idle_store_ttl` | Resolver cache policy | Invalid zero capacity or invalid duration | Bounds open idle store handles | Keep public API small; do not expose leases, warmup, min-idle, eviction strategy selection, or waiter controls initially |
| Append API | Command handlers/projectors | `ExpectedVersion`, events with optional event-level workflow metadata | `AppendResult` with `first_version`, `last_version`, and stored envelopes | OCC failure, payload too large, invalid expected version, DB/migration failure | Writes to one owner log | `ExpectedVersion::Any` blind-appends after the current head; `Exact` should use `OwnerLogVersion` but must not accept `OwnerLogVersion::start()`; workflow metadata is not batch-global; workflow-start markers are resolved during append and returned in envelopes |
| Load owner log API | Projectors/recovery | `OwnerLogVersion` cursor and limit | Events from one owner log after the version cursor, up to limit | Validation error for invalid `limit`, DB/migration failure | Read-only | Cursor is exclusive; `limit` must satisfy `1 <= limit <= MAX_READ_LIMIT`; replaces loading/filtering by stream ID and supports application-owned batch processing across rotated files |
| Load workflow API | Workflow recovery | `workflow_started_by_event_id`, `OwnerLogVersion` cursor, limit | Ordered events for that workflow run after the version cursor, up to limit | Validation error for invalid `limit`, DB/migration failure | Read-only | Cursor is exclusive; `limit` must satisfy `1 <= limit <= MAX_READ_LIMIT`; implement as indexed query over rotated partitions; unknown anchors return an empty batch unless a separate strict helper is later added |
| List partition stores API | Startup catch-up/supervisors | Namespace | Partition keys/descriptors from immediate child directories under `<root>/<namespace>/`, sorted by partition key ascending | Invalid namespace, directory read failure | Read-only directory scan only | Shallow metadata-only listing; files and invalid child names are skipped; does not open/migrate each owner store; replaces prefix-based stream discovery; no root registry table required initially |
| Worker-per-partition pool example | Applications | Partition key, max workers, poll interval, max idle iterations, projection batch size | Buzzes or starts workers for partition stores with bounded concurrency | Worker failure, pool exhaustion, handler failure | Starts/stops application-owned worker tasks | Replaces tenant-specific example and removes public `ProjectorBatchOutcome` dependency |

Removed public APIs from the target model:

- `all_since`
- `all_since_with_positions`
- `PartitionedCursor`
- `bootstrap_cursor`
- events-owned `checkpoint`

These are artifacts of a shared event-store/global-cursor model. The replacement surface is owner-local: ensure/open one partition store, read after an owner-log version, and persist the projected version in the application database.

## Failure Modes

| Failure mode / trigger | Description | Expected behavior | State or persistence effect | User/operator feedback | Verification |
| ---------------------- | ----------- | ----------------- | --------------------------- | ---------------------- | ------------ |
| Invalid partition key | Caller passes an empty key or path-like key. | Reject before storage creation. | No directory or DB created. | Structured error naming namespace/key. | Unit tests. |
| Invalid child during listing | Namespace directory contains a stray file or child directory whose name is not a valid partition key. | Skip files and invalid child directory names, then continue listing valid partition descriptors. | No store opened or modified. | Debug/trace log if useful. | Listing test with mixed valid/invalid children. |
| Invalid workflow metadata | Caller provides workflow kind without workflow ref, or workflow ref without workflow kind. | Reject before append. | No event row created. | Structured validation error naming workflow field mismatch. | Unit tests. |
| Invalid workflow kind | Caller passes display text, uppercase, spaces, underscores, or punctuation in `workflow_kind`. | Reject before append. | No event row created. | Structured validation error naming invalid workflow kind. | Unit tests. |
| Unknown workflow starter | Caller continues a workflow using a starter event ID that is not present in the owner log. | Append still succeeds if metadata shape is valid. Recovery by that anchor returns the events that used it. | Event row stores the provided starter anchor. | Domain/application validation may log or reject earlier if it needs stricter guarantees. | Documented behavior; optional application-level test. |
| Concurrent ensure/open/first append | Two callers concurrently ensure/open and append the first event for the same owner. | `ensure_exists` races cleanly; one append wins version `1`; the other gets OCC behavior when using `NoStream`. | One valid partition store and at most one version `1` event. | Caller-visible concurrency error on the losing append. | Integration/concurrency test. |
| Concurrent append to same owner | Two writers append to one owner log. | Preserve one ordered version sequence. | No duplicate versions. | OCC error when expected version is stale. | Integration/concurrency test. |
| Concurrent append to different owners | Two writers append to unrelated owner partitions. | They use separate active partition DBs. | Independent store heads advance. | Logs identify owner partition. | Integration or stress test. |
| Workflow events interleaved | One owner has multiple active workflow runs and unrelated events. | Physical order remains interleaved; workflow queries filter/group by `workflow_started_by_event_id`. | Owner log keeps one version sequence. | No user-facing issue. | Workflow recovery test. |
| Workflow retry after restart | Projector stops while a workflow is active. | Application reads active workflow state from its DB, then loads the next bounded workflow batch from the owner log by starter event ID and last projected version. | Application DB remains source of truth for active workflow and projection offset. | Operator logs workflow kind, started-by event ID, owner key, and last projected version. | Integration test with simulated restart. |
| Projection side effect succeeds but offset write fails | Handler writes read model but application-owned offset write fails. | Whole application DB transaction should roll back when read-model mutation and offset are local. External side effects still require idempotency. | No false projected offset. | Warning/error log. | Application-level transactional tests. |
| Application offset write fails | Application read-model mutation and offset advancement are attempted together. | Whole application DB transaction rolls back. | Neither read-model mutation nor offset commits. | Retry warning from application projector. | Application repository integration test. |
| Store catalog corruption | One owner partition catalog is corrupt. | That owner fails to open; unrelated owners still operate. | Fault isolated to one partition store. | Operator log names owner partition. | Fault-injection test if practical. |
| Worker pool exhausted | More dirty owner partitions arrive than worker capacity. | Queue pending partitions or wait for capacity; warn about delay. | No event loss; projection offset does not advance for delayed partitions. | Warning includes owner key and pool size. | Worker-pool example test. |
| Idle worker race | Dirty signal arrives while worker is about to retire. | Worker consumes final buzz before retirement or caller requeues pending partition. | No missed event; worker either continues or a pending key remains. | Debug log if useful. | Worker-pool example test. |
| Projector handler failure | Handler fails while projecting an assigned partition. | Do not advance application-owned offset past failed event; retry according to worker polling/backoff. | Owner log unchanged; projection offset remains at last successful event. | Error log includes owner key, event ID, and workflow metadata if present. | Projector retry test. |
| Notification store failure | Progress notification cannot be recorded. | Domain event append/projection semantics are unchanged. | Notification missing or stale only. | Operator log. | Existing notifications tests. |

## Test Theories

| Theory / behavior | Case | Description | Starting state | Input / action | Expected response | Expected state / side effects |
| ----------------- | ---- | ----------- | -------------- | -------------- | ----------------- | ----------------------------- |
| Explicit store creation | Ensure then first append | `ensure_exists` creates a missing owner partition store before append. | Empty root | Ensure partition, open it, append with `ExpectedVersion::NoStream` | Version 1 appended | Catalog and first partition DB exist |
| Owner-local OCC | Stale expected version | OCC applies to one owner log. | Owner at version 1 | Append with stale expected version | Concurrency error | No new row |
| Cross-owner isolation | Different owners | Independent owners do not share active partition DBs. | Empty root | Append to two owners | Both version 1 | Two store lineages |
| Workflow grouping | Interleaved workflows | Recovery can read one workflow run from interleaved owner log. | Owner has events with two workflow started-by event IDs | Load workflow batch after version cursor | Only matching workflow events after the cursor are returned in owner order, up to the limit | Unrelated events ignored |
| Workflow restart | Active workflow | Active workflow metadata survives restart. | Application DB has active workflow ref | Reopen and recover | Workflow ref returned from application DB | Recovery can load the next bounded workflow batch from the stored version |
| Application-owned offsets | Transactional projection | Application read model and offset commit together. | Event version N exists, offset at N-1 | Project event N | Success | Read model and offset both reflect N |
| Worker pool correctness | Dirty queue | Bounded workers project many owner partitions without lost wakeups. | Pool max below dirty partition count | Buzz many dirty owners | All eventually projected | Pending set empty at idle |
| Example replacement | Documentation runnable example | New example demonstrates application-owned worker-per-partition projection without exposing outcome enum matching as the lesson. | Clean checkout | Run new example | Success output | Old tenant example no longer referenced |

## Proposed Design

### Storage Layout

```text
<data_dir>/events/
  users/
    user-123/
      catalog.db
      events_2026_05_20T10.db
      events_2026_05_20T11.db

    user-456/
      catalog.db
      events_2026_05_20T10.db

  clients/
    client-123/
      catalog.db
      events_2026_05_20T10.db

<data_dir>/stream_events/
  notifications_store.db
```

The crate should not hard-code `users` or `clients`. Those are namespaces selected by applications. The reusable shape is namespace plus partition key.

### Event Row Shape

Final exact SQL may differ, but the intended event row shape is:

```sql
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL UNIQUE,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    workflow_kind TEXT,
    workflow_started_by_event_id TEXT,
    created_at INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    trace_id TEXT,
    span_id TEXT,
    request_id TEXT,
    actor_id TEXT NOT NULL,
    actor_type TEXT NOT NULL
);

CREATE INDEX idx_events_created_sequence
ON events(created_at, sequence);

CREATE INDEX idx_events_workflow_started_version
ON events(workflow_started_by_event_id, version);
```

`stream_id` is removed from row-level routing. Per-event envelopes should not repeat owner identity fields. If callers need owner identity for logs or progress notifications, expose it from the `OwnerEventStore` handle or once in an append/read batch result, derived from the partition/store handle rather than read as routing state from the event row.

### Rotated File Ranges

Applications should use owner-log versions as projection cursors. The crate maps versions to rotated DB files through partition-store catalog metadata:

Public APIs should use a newtype for owner-log versions:

```rust
pub struct OwnerLogVersion(i64);
```

The storage schema can still store event versions as integers beginning at `1`. The public newtype prevents accidental mixing of owner-log versions with database row IDs, counts, or unrelated application counters.

Constructors should keep the sentinel explicit:

```rust
OwnerLogVersion::start(); // before event 1, read/projection cursor only
OwnerLogVersion::new(1)?; // first stored event version
OwnerLogVersion::new(0)?; // invalid
```

Use `OwnerLogVersion::start()` as the initial read cursor:

```rust
let events = store
    .load_after_version(OwnerLogVersion::start(), batch_size)
    .await?;
```

Read cursors are exclusive:

```text
load_after_version(OwnerLogVersion::start()) -> versions 1, 2, ...
load_after_version(OwnerLogVersion::new(4)) -> versions 5, 6, ...
```

This matches application offsets: if `last_projected_version` is `4`, the next read starts after `4`.

Bounded reads validate limits:

```text
load_after_version(cursor, 0) -> invalid limit
load_after_version(cursor, 1) -> at most one event
load_after_version(cursor, MAX_READ_LIMIT + 1) -> invalid limit
```

That keeps an empty returned batch meaningful: no events matched after the cursor. The maximum exists to prevent accidental unbounded memory use.

`MAX_READ_LIMIT` should be a fixed internal crate constant for the initial implementation, not another partition-manager or store configuration value and not an exported public constant. The same ceiling applies to `load_after_version` and `load_workflow_after_version` because both return event batches into memory. Pick a conservative value during implementation and make it configurable only after a real caller needs a different ceiling.

Do not use `OwnerLogVersion::start()` as an append expectation. First append uses `ExpectedVersion::NoStream`; later exact appends use a real stored event version.

`ExpectedVersion::Any` remains available for blind append:

```rust
store
    .append(ExpectedVersion::Any, [event])
    .await?;
```

This means "append after whatever the current owner-log head is." It does not mean "I expect the head to equal a value"; that remains `ExpectedVersion::Exact(version)`.

```sql
CREATE TABLE partitions (
    name TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    first_version INTEGER NOT NULL,
    last_version INTEGER,
    sealed INTEGER NOT NULL DEFAULT 0
);
```

Example:

```text
events_a.db  first_version = 1     last_version = 1000
events_b.db  first_version = 1001  last_version = 2000
events_c.db  first_version = 2001  last_version = NULL
```

For `load_after_version(OwnerLogVersion::new(1500), 100)`, the crate selects rotated files whose range can contain versions greater than `1500`, starts with `events_b.db`, and continues at version `1501` until it returns `100` events or reaches the end of the owner log.

On rotation, the writer seals the old active file with its final owner-log version and creates the new active file with `first_version = previous_last_version + 1`.

### API Shape

Partition management is a first-class crate API contract. Creation/existence is explicit, and the opened handle is an `OwnerEventStore`, representing one owner's ordered event log:

```rust
let partition = runtime
    .event_partitions()
    .ensure_exists("users", "user-123")
    .await?;

let store: OwnerEventStore = partition.open().await?;

store
    .append(
        ExpectedVersion::NoStream,
        [NewEvent {
            r#type: "provisioning_requested".to_string(),
            workflow_kind: Some("provisioning".to_string()),
            workflow: WorkflowRef::StartsThisWorkflow,
            // ...
        }],
    )
    .await?;
```

The key point is that callers bind to an `OwnerEventStore` before append/load. They do not pass `stream_id` into every store operation. The old public `EventStore` name does not need a compatibility alias in the target design.

Applications choose the namespace and partition key; the crate owns validation, explicit storage creation, opening, and partition-store listing for worker supervisors. Both namespace and partition key should be strict lowercase literal path segments so the storage layout remains readable. Business identifiers that are not filesystem-safe should be normalized by the application into stable partition keys before calling the partition manager.

The partition manager should not introduce a root-level partition registry unless later metadata requirements justify it. Shallow directory listing is enough while each partition key is exactly one directory name. Listing a namespace should inspect only immediate child directories, should not include files, should not recursively scan nested directories, and should not open or migrate each owner store. Invalid child names should be skipped so one stray directory or file does not block startup catch-up. Returned descriptors should be sorted by partition key ascending for deterministic startup catch-up and tests. Workers open and migrate the specific `OwnerEventStore` when they actually drain that partition.

The partition manager should also own the store lookup/cache boundary. Callers should not need to know whether opening a partition store reuses an existing handle or creates a new cached handle:

```rust
let partition = partitions.ensure_exists("users", "user-123").await?;
let store = partition.open().await?;

store
    .append(
        ExpectedVersion::NoStream,
        [NewEvent {
            r#type: "provisioning_requested".to_string(),
            workflow_kind: Some("provisioning".to_string()),
            workflow: WorkflowRef::StartsThisWorkflow,
            // payload/request/actor fields...
        }],
    )
    .await?;
```

Owner context belongs to the `store` handle. `NewEvent` and per-event `EventEnvelope` should not repeat it. `AppendResult` or read batch results may expose owner context once as derived metadata when that context is useful to projectors, logs, or notifications.

Workflow metadata belongs on each `NewEvent`, because workflow membership is an event-level fact inside the owner log:

```rust
store
    .append(
        ExpectedVersion::Exact(OwnerLogVersion::new(4)),
        [
            NewEvent {
                r#type: "profile_updated".to_string(),
                workflow_kind: None,
                workflow: WorkflowRef::None,
                // payload/request/actor fields...
            },
            NewEvent {
                r#type: "domain_provisioning_started".to_string(),
                workflow_kind: Some("provisioning".to_string()),
                workflow: WorkflowRef::StartsThisWorkflow,
                // payload/request/actor fields...
            },
        ],
    )
    .await?;
```

The append layer resolves workflow start markers after it generates event IDs:

```rust
pub enum WorkflowRef {
    None,
    StartsThisWorkflow,
    Continues {
        started_by_event_id: uuid::Uuid,
    },
}
```

For `StartsThisWorkflow`, the stored row uses the generated event ID as `workflow_started_by_event_id`. For `Continues`, the stored row uses the provided starter event ID.

Append returns the stored envelopes so callers can persist generated identities without a second read:

```rust
pub struct AppendResult {
    pub first_version: OwnerLogVersion,
    pub last_version: OwnerLogVersion,
    pub events: Vec<EventEnvelope>,
}
```

For a `WorkflowRef::StartsThisWorkflow` event, the corresponding returned envelope includes both the generated `id` and `workflow_started_by_event_id = id`. Callers use that returned starter event ID for later `WorkflowRef::Continues` appends or for application-owned active workflow state.

Append does not verify that a `Continues` starter event exists. That would require an owner-log lookup and still would not prove the domain semantics are correct. The crate guarantees grouping by the stored starter anchor; applications that need stricter checks should validate them before append.

Workflow metadata validation:

```text
workflow = WorkflowRef::None
  -> workflow_kind must be None

workflow = WorkflowRef::StartsThisWorkflow
  -> workflow_kind must be Some(...)

workflow = WorkflowRef::Continues { ... }
  -> workflow_kind must be Some(...)
```

`workflow_kind` validation is intentionally not a filesystem rule. It is a stable-label rule:

```text
valid:   provisioning
valid:   portal-request
valid:   tax-return-review
valid:   workflow2

invalid: Provisioning
invalid: portal_request
invalid: domain provisioning
invalid: provisioning.
invalid: provisioning:
```

The partition manager cache should expose only the policy values the first implementation needs:

```rust
let partitions = EventPartitions::open(root, rotation)
    .with_max_open_stores(128)
    .with_idle_store_ttl(Duration::from_secs(300));
```

The `rotation` value applies to every namespace and owner store managed by this partition manager. Applications that need materially different rotation behavior can create a separate manager boundary later; the initial API should not add per-namespace rotation settings.

Do not add read-limit configuration here initially; read APIs use the fixed internal crate `MAX_READ_LIMIT`.

Internal defaults:

```text
eviction policy: least-recently-used idle stores
active handles: never evicted
acquire timeout: none initially; open/create returns normal IO/DB errors
min idle stores: 0
warmup: none
manual release/leasing: none; handle drop owns lifetime
```

Safe segment examples:

```text
valid:   users
valid:   clients
valid:   shard-07
valid:   user-123
valid:   client-acme
valid:   01jwd4ehd8b8k7k9v7v6zrqzsa

invalid: ""
invalid: .hidden
invalid: ..
invalid: client/acme
invalid: ACN 123 456 789
invalid: person@example.com
invalid: client:123
invalid: user_123
```

### Workflow Recovery

An active workflow should be represented in the application DB as a business-process reference, not as a stream name:

```rust
pub struct ActiveWorkflowRecord {
    pub owner_namespace: String,
    pub owner_partition_key: String,
    pub workflow_kind: String,
    pub workflow_started_by_event_id: uuid::Uuid,
    pub last_projected_version: OwnerLogVersion,
}
```

The application DB record carries the owner identity because recovery starts outside a specific partition-store handle:

```text
owner namespace = users
owner partition key = user-123
workflow_kind = provisioning
workflow_started_by_event_id = event uuid
last_projected_version = 42
```

Recovery options:

- Load bounded workflow-run batches by `workflow_started_by_event_id` and owner-log version cursor across rotated partition DBs.
- Filter or report by `workflow_kind` when an application needs process-type grouping.

The target API should mirror owner-log projection reads:

```rust
let events = store
    .load_workflow_after_version(workflow_started_by_event_id, last_projected_version, batch_size)
    .await?;
```

Unknown workflow anchors return an empty batch. Append already shape-validates workflow metadata without verifying starter existence, so read behavior should stay consistent unless a separate strict helper is added later.

The spec prefers indexed workflow-run loading, because user/client owner logs may contain interleaved workflow runs. `workflow_kind` is not the run identity; the same kind may run many times for one owner.

### Worker-Per-Partition Pool

The main example should teach this pattern:

```text
append event for partition users/user-123
  -> application buzzes worker pool with PartitionKey(users, user-123)
  -> pool starts one worker if none is active for that partition
  -> worker opens that partition store through the partition manager
  -> worker loads unprojected owner-log events using an application-owned offset
  -> worker updates read model and projection offset in the application DB
  -> worker polls briefly, then retires after configured idle iterations
```

This is application-owned scheduling. The `events` crate should provide the partition manager, owner-log reads, event metadata, and durable event storage; it should not expose `ProjectorBatchOutcome` or require applications to use a crate-owned on-demand supervisor.

The current `practice-management` implementation has the right shape to generalize: startup catch-up discovers client streams, appends buzz a client worker, the pool tracks active and pending clients, idle replaceable workers can be retired at capacity, and workers stop after `max_idle_iterations`.

Application-owned pool state:

```text
workers: partition keys currently assigned to worker handles
pending: dirty partition keys waiting for capacity or retry
max_workers: configured worker cap
poll_interval: retry and poll interval
max_idle_iterations: idle retirement threshold
batch_size: maximum owner-log events to process per drain iteration
```

`batch_size` should be configured as a positive value no larger than the crate read maximum. A worker treats an empty returned batch as idle/no more matching events, not as a successful zero-sized request.

This pattern replaces the tenant example because the reusable concept is not tenancy. It is bounded application-owned projection work over independent owner stores without exposing `ProjectorBatchOutcome`.

Batch processing stays application-owned:

```text
worker wakes for owner partition
  -> read application-owned last_projected_version
  -> load owner-log events after that version, up to batch_size
  -> apply each event to the read model
  -> commit read-model changes and projected offset in the application DB
  -> repeat until no more events or a projection error occurs
```

The crate API needed for this pattern is a bounded owner-log read, not a projector batch outcome enum:

```rust
let events = store
    .load_after_version(last_projected_version, batch_size)
    .await?;
```

`load_after_version` must traverse rotated DB files internally by consulting the owner store catalog. Applications should not store or pass rotated-file cursors.

Because `load_after_version` is exclusive, application projection code can persist the last successfully projected event version directly and pass that same value into the next read.

The target API should not expose a global `all_since` cursor or `PartitionedCursor`. Startup catch-up should list owner partitions for a namespace, then drain each owner log independently using the application-owned `last_projected_version`.

### Application-Owned Projection Offsets

The `events` crate owns durable event append, owner-log head tracking, partition metadata, and owner-log read APIs. It should not own application read-model offsets or active workflow state.

When a projector updates an application read model, the application database should own the offset that says which owner-log version has been projected:

```text
BEGIN application DB transaction
  apply read-model mutation
  update application projection offset to owner-log version N
COMMIT
```

This keeps the invariant local to the application database:

```text
if offset says version N is projected,
then read-model effects through version N are also present
```

For Greenfields client workflow projection, the table shape should be:

```sql
CREATE TABLE client_workflow_projection_offsets (
    client_id TEXT PRIMARY KEY,
    last_projected_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
```

There is no `stream_id` column because the application owner key selects the partition store. The events crate should expose enough event metadata for the application to know the owner-log version it is committing.

### Documentation And Example Replacement Plan

Replace:

```text
events/docs/how-to/partition-by-tenant.md
events/examples/tenant_projectors.rs
```

With:

```text
events/docs/how-to/worker-pool-over-per-partition-store.md
events/examples/partition_worker_pool.rs
```

Update references in:

```text
events/README.md
events/docs/README.md
events/tasks/tighten-value-prop/todo.md, if kept as historical task notes
```

The new example should:

- Use neutral partition keys, not tenants.
- Demonstrate append creating/opening partition stores.
- Demonstrate an application-owned worker-per-partition pool with active/pending tracking, capacity handling, and idle retirement.
- Use application-owned projection offsets committed with read-model changes.
- Remove `ProjectorBatchOutcome` from the public API and examples.
- Include workflow metadata in at least one event to show interleaved workflow-run grouping.

### Projector API Direction

Remove `ProjectorBatchOutcome` and the public `process_next_batch_with_handler` surface. They are not needed by the application-owned worker pool pattern.

The primary crate API should be bounded owner-log reads plus application-owned offsets. Existing continuous projector helpers may remain if useful, but the worker-per-partition example should not depend on a public batch outcome enum.

```rust
let events = store
    .load_after_version(last_projected_version, batch_size)
    .await?;
```

If tests need finite progress inside the crate, use test-only hooks or lower-level private helpers rather than preserving a public batch outcome enum.

## Acceptance Criteria

- `ensure_exists` for a missing owner partition creates the partition store, and the first append writes version `1`.
- Appends to different owner partitions do not write to the same active partition DB.
- Event row routing no longer depends on `stream_id`.
- Workflow identity is explicit and separate from owner partition/storage identity.
- Application read-model projection offsets are owned by the application DB and documented with an example table design.
- Active workflow recovery no longer depends on a field named `workflow_stream_id`.
- Workflow events can be recovered from an interleaved owner log by `workflow_started_by_event_id`.
- `NotificationsStore` remains separate and keeps existing behavior.
- `events/docs/how-to/partition-by-tenant.md` is replaced by `events/docs/how-to/worker-pool-over-per-partition-store.md`.
- `events/examples/tenant_projectors.rs` is replaced by a neutral partition worker-pool example.
- `ProjectorBatchOutcome` and public finite-batch projector APIs are removed.
- Primary docs/examples teach application-owned worker-per-partition projection over partition stores.
- Application-owned batching uses bounded owner-log reads, not `ProjectorBatchOutcome`.
- Existing integration tests are updated for the new storage and workflow model.

## Verification Plan

- `cargo fmt`
- `cargo test -p events`
- `cargo run -p events --example partition_worker_pool`, or the exact workspace command after implementation
- `cargo test -p practice-management`
- `cargo check --workspace --features "greenfields-of-cambridge/design-system"`
- `make test`
- Add or update tests for:
  - explicit partition store creation via `ensure_exists`
  - invalid namespace/partition key rejection
  - concurrent ensure/open/first append race
  - owner-local expected-version behavior
  - cross-owner append isolation
  - workflow metadata persistence
  - workflow recovery from interleaved owner log
  - application-owned projection offset table and transactional update
  - bounded owner-log read after version
  - worker pool active/pending behavior
  - idle retirement race handling
  - projector retry behavior without advancing application-owned offsets past failed events

## Documentation Notes

Likely durable documentation type after implementation:

- Reference: storage layout, event row schema, workflow metadata, and owner-log read APIs.
- How-to: worker pool over per-partition stores.
- Explanation: why owner partition, workflow run identity, and notification store are separate concepts.

Final docs should answer:

- What key should an application use as an owner partition?
- When should an event have workflow metadata?
- How is a partition store created?
- How does retry/recovery find workflow events in an interleaved owner log?
- How does a bounded application-owned worker pool avoid permanent workers per owner?
- Why is `NotificationsStore` separate from durable domain events?

Do not migrate obsolete tenant wording into final docs except as a short note that tenant is only one possible partition key.

## Risks And Approved Assumptions

- Risk: many owner partitions create many small DB lineages. The implementation needs bounded connection/cache behavior.
- Risk: workflow-indexed recovery across rotated partitions can be inefficient if the index or partition scan API is not designed carefully.
- Risk: removing row-level `stream_id` is a broad public API and storage contract change.
- Risk: removing `ProjectorBatchOutcome` may require test-only hooks or private helpers for finite projector verification.
- Approved assumption from the design discussion: owner logs may have interleaved events; `workflow_started_by_event_id` solves retry grouping, not physical interleaving.
- Approved assumption from the design discussion: rotation keeps individual DB files bounded, so one owner log can remain practical if workflow queries are indexed.
- Approved assumption from the design discussion, later refined: creating storage for a previously unseen owner partition is explicit through `ensure_exists`, while the first append writes event version `1`.
- Approved assumption from the design discussion: notifications/progress remain separate from durable domain events.
