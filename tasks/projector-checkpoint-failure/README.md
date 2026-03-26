# Task: Stop `run_with_handler()` from Checkpointing Past Failed Events

## Summary

`Projector::run_with_handler()` logs per-event handler failures and still advances the batch checkpoint to `next_cursor`. A failed event is therefore skipped permanently on the next polling cycle or restart.

## Severity

Critical

## Why This Matters

- Projectors are the core of CQRS read-model correctness.
- Swallowing an error and then checkpointing past it creates silent divergence.
- Operators may see logs, but the projection state is already wrong and the failed event is gone from replay.

## Evidence

- `src/projector.rs:375` iterates events and logs handler failures.
- `src/projector.rs:388` checkpoints the full batch cursor regardless of those failures.

## Failure Mode

1. Read a batch `[e1, e2, e3]`.
2. `e2` fails in `handle_event`.
3. The loop logs the failure and continues to `e3`.
4. The projector checkpoints `next_cursor`, which points after `e3`.
5. On the next loop, `e2` is no longer reachable.

## Desired Outcome

- A handler failure must not advance the durable cursor beyond the failed event.
- The projector should either:
  - stop the batch on first failure and retry later, or
  - checkpoint only up to the last successful event.

## Suggested Direction

- Decide whether the handler API is fail-fast or partial-success aware.
- Move checkpointing inside a success-only boundary.
- Add tests for single-event failure in the middle of a batch and restart recovery.

## Acceptance Criteria

- A failed event is retried on the next loop or after restart.
- Checkpoint position never moves beyond an unhandled event.
- Tests cover:
  - middle-of-batch failure
  - first-event failure
  - last-event failure
  - restart after failure

## Diagram

See `diagram.html` for the current skip behavior and the target replay-safe flow.
