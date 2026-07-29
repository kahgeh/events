# Task: Fail Closed on Head-Publish Failure and Replace Scan Recovery with `pending_appends`

## Summary

The current append protocol is recoverable, but it still has an unsafe live-service window: events can commit to the active partition database, the `stream_heads` publish can still fail after retries, and the store then returns to normal operation with a stale catalog head. Recovery currently infers incomplete writes by scanning the active partition on startup.

This task makes that state explicit and bounded:

- if post-commit head publication fails, the store must become write-unhealthy and reject further appends
- incomplete appends must be tracked in an explicit `pending_appends` intent journal in the catalog
- recovery must reconcile unresolved intents directly instead of scanning partitions heuristically

## Severity

High

## Why This Matters

- The current write path is still a cross-database protocol, not a true atomic commit.
- A stale `stream_heads` row can remain live after a committed append if head publication exhausts retries.
- Once the writer lock is released, later rotation can move the next append into a new partition while the catalog is still stale.
- Partition scanning is a conservative repair strategy, but it is indirect, costlier than necessary, and not as auditable as an explicit intent log.

## Evidence

- `src/eventstore.rs:400` commits the partition transaction before catalog head publication.
- `src/eventstore.rs:417` retries `stream_heads` publication, but a final failure still bubbles out of `append()`.
- `src/eventstore.rs:420` releases writer coordination after that step returns.
- `src/eventstore.rs:1007` to `src/eventstore.rs:1018` recovers by scanning the active partition rather than resolving explicit incomplete writes.

## Failure Mode

1. Writer commits events to the active partition DB.
2. `stream_heads` publication fails repeatedly and `append()` returns `Err`.
3. The store remains writable because no unhealthy latch is set.
4. A later append can run with a stale catalog head.
5. If rotation moves that later append to a new partition, cross-partition duplicate versions become possible again.

## Desired Outcome

- A post-commit failure to publish `stream_heads` must put the store into a durable write-unhealthy state.
- While unhealthy, all append attempts must fail fast with a dedicated error until reconciliation succeeds.
- The catalog must contain an explicit `pending_appends` journal so recovery knows which appends were in-flight.
- Recovery must resolve pending intents precisely instead of scanning whole partitions for drift.

## Suggested Direction

- Add a `pending_appends` table in the catalog with one row per append attempt.
- Persist an intent before inserting events into the partition DB. The intent should include enough data to reconcile or finalize the append:
  - `append_id`
  - `stream_id`
  - `expected_version`
  - `target_partition`
  - `first_version`
  - `last_version`
  - event IDs or a stable append batch identifier
  - status fields such as `prepared`, `committed`, `published`, `failed`
  - timestamps and last error
- Update the append protocol so that:
  1. create `pending_appends` intent in catalog
  2. write events to the active partition DB
  3. in one catalog transaction, advance `stream_heads` and mark the intent resolved
  4. if step 3 fails after step 2 committed, mark the store unhealthy
- On startup or admin repair, resolve unresolved intents by checking the target partition for the recorded append batch and then either:
  - publish the missing `stream_heads` update and clear the intent, or
  - mark the intent failed if the partition write never landed
- Keep full partition scanning as a last-resort maintenance tool, not the primary recovery mechanism.

## Acceptance Criteria

- If `stream_heads` publication fails after partition commit, subsequent appends fail fast until recovery clears the unhealthy state.
- Recovery uses `pending_appends` as the primary source of truth for incomplete writes.
- No startup path needs to scan whole partitions to detect the common incomplete-append case.
- Tests cover:
  - post-commit head publication failure sets unhealthy state
  - append attempts while unhealthy are rejected
  - unresolved intent survives restart and is reconciled
  - rotation is blocked or rejected while the store is unhealthy
  - recovery clears the unhealthy state only after the intent is fully resolved

## Diagram

See `diagram.html` for the current recoverable-but-open write path and the target fail-closed intent-journal protocol.
