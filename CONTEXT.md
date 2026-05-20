# Events

This context defines the domain language for the `events` crate's partition-store
event log and workflow recovery model.

## Language

**Partition Store**:
The storage primitive: one application-chosen partition key receives one ordered
event log. This is the plain model; domain-specific scaling can map partition
keys to owners, accounts, clients, or other independent units of work.
_Avoid_: tenant store, stream store

**Owner Partition**:
A scaling/routing strategy where the application chooses partition keys that map
to owners, accounts, clients, or other independent units of work.
_Avoid_: tenant, stream

**Partition Manager**:
The crate-owned boundary that validates partition keys, ensures partition storage exists, opens partition stores, and lists partitions.
_Avoid_: path helper, tenant opener, generic resolver

**Partition**:
A public reference to an existing **Partition Store**. It can be opened into an **Owner Event Store**, but it is not itself an append/read handle.
_Avoid_: stream handle

**Owner Event Store**:
The current public opened handle type for appending to and reading from one
ordered partition log. The name does not require the partition key to represent
an owner.
_Avoid_: EventStore as the public per-owner handle, stream store

**Owner Log**:
The current public version/cursor terminology for the ordered sequence of events
inside one **Partition Store**. Conceptually this is the partition log.
_Avoid_: stream

**Workflow Kind**:
The type of retryable business process represented by workflow events.
_Avoid_: workflow id

**Workflow Started-By Event ID**:
The event ID that identifies one run of a retryable workflow.
_Avoid_: workflow id, workflow instance id

## Relationships

- One **Partition Store** contains exactly one ordered partition log.
- One **Partition Manager** can ensure a **Partition** exists for a partition key.
- One **Partition** opens into one **Owner Event Store**.
- One partition log may contain events for many **Workflow Kinds**.
- One **Workflow Kind** may run many times in the same partition log.
- One **Workflow Started-By Event ID** identifies exactly one workflow run inside a partition log.

## Example dialogue

> **Dev:** "If a user runs provisioning twice, do both runs have the same **Workflow Kind**?"
> **Domain expert:** "Yes, but each run has a different **Workflow Started-By Event ID**."

## Flagged ambiguities

- "workflow id" was ambiguous between process type and process run. Resolved: use **Workflow Kind** for the process type and **Workflow Started-By Event ID** for the run identity.
- "tenant" was too specific for the general partitioning model. Resolved: explain the primitive as a **Partition Store**; use **Owner Partition** only when describing the scaling strategy where partition keys map to owners.
