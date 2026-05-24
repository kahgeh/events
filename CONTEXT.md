# Events

This context defines the domain language for the `events` crate's partition-store
event log and workflow recovery model.

## Language

**EventNamespaces**:
The root manager for all event namespaces under one storage root. It owns the
root path and shared rotation policy.
_Avoid_: partition manager, tenant opener, path helper

**EventNamespace**:
A named grouping of partitions, such as `clients`, `users`, or `orders`.
_Avoid_: root manager

**Partition**:
A public reference to one selected partition inside an **EventNamespace**. It can
be opened into an **EventLog**, but it is not itself an append/read handle.
_Avoid_: partition store, stream handle

**Partition Store**:
The durable storage behind a **Partition**. One partition store contains one
ordered **EventLog** and hides its physical event files from callers.
_Avoid_: tenant store, stream store

**EventLog**:
The public append/read API for one ordered event log. It abstracts catalog and
rotation details, so callers use event-log versions rather than physical file
names.
_Avoid_: owner log, stream store

**EventLogVersion**:
The position of an event inside one **EventLog**. This is also the read cursor
and the value used by exact expected-version checks.
_Avoid_: event version

**Workflow Kind**:
The type of retryable business process represented by workflow events.
_Avoid_: workflow id

**Workflow Started-By Event ID**:
The event ID that identifies one run of a retryable workflow.
_Avoid_: workflow id, workflow instance id

## Relationships

- One **EventNamespaces** root contains many **EventNamespace** values.
- One **EventNamespace** can ensure a **Partition** exists for a partition key.
- One **Partition** opens into one **EventLog**.
- One **EventLog** hides catalog and physical file rotation.
- One **EventLog** may contain events for many **Workflow Kinds**.
- One **Workflow Kind** may run many times in the same **EventLog**.
- One **Workflow Started-By Event ID** identifies exactly one workflow run inside an **EventLog**.

## Example dialogue

> **Dev:** "If a user runs provisioning twice, do both runs have the same **Workflow Kind**?"
> **Domain expert:** "Yes, but each run has a different **Workflow Started-By Event ID**."

## Flagged ambiguities

- "workflow id" was ambiguous between process type and process run. Resolved: use **Workflow Kind** for the process type and **Workflow Started-By Event ID** for the run identity.
- "tenant" was too specific for the general partitioning model. Resolved:
  explain the root as **EventNamespaces**, the grouping as **EventNamespace**,
  and the append/read API as **EventLog**.
