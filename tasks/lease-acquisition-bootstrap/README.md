# Task: Fix Lease Acquisition for New and Unlocked Consumers

## Summary

The public lease helpers cannot successfully acquire a lease for a new consumer or a checkpointed-but-unlocked consumer. Both `acquire_lease()` and `renew_lease()` call the same `UPDATE`, and that `UPDATE` only matches rows owned by the same caller or rows whose `lease_expires_at` is less than now. Missing rows and `NULL` lease fields both miss the predicate.

## Severity

High

## Why This Matters

- The lease helpers are part of the public projector coordination surface.
- Documented examples imply that a consumer can try to acquire a lease from a cold start.
- In practice, the helper returns `false` forever unless a compatible row already exists.

## Evidence

- `src/projector.rs:191` implements `acquire_lease()` by delegating to `renew_lease()`.
- `src/projector.rs:203` does the same for explicit renewals.
- `src/catalog.rs:355` performs an `UPDATE ... WHERE lease_owner = ?1 OR lease_expires_at < now`.
- `src/projector.rs:172` shows checkpoint rows being created without lease fields.

## Failure Mode

1. Start a new projector with no `consumer_offsets` row.
2. Call `acquire_lease()`.
3. The `UPDATE` affects zero rows because the row does not exist.
4. After checkpointing, try `acquire_lease()` again.
5. The row exists but `lease_expires_at` is `NULL`, so `lease_expires_at < now` is not true and the lease still cannot be acquired.

## Desired Outcome

- Cold-start consumers can acquire a lease without a preexisting row.
- Unlocked rows are treated as acquirable.
- Renewals remain owner-safe and do not steal active leases.

## Suggested Direction

- Split acquire and renew semantics.
- Use insert-or-upsert behavior for acquisition.
- Treat `(lease_owner IS NULL AND lease_expires_at IS NULL)` as available.
- Add tests for:
  - brand-new consumer
  - checkpointed unlocked consumer
  - expired lease
  - active lease held by another owner

## Acceptance Criteria

- `acquire_lease()` succeeds for a fresh consumer when no competing lease exists.
- `acquire_lease()` succeeds for an unlocked existing row.
- `renew_lease()` only succeeds for the current owner or an expired lease, depending on the intended contract.
- Tests cover cold start, unlocked state, expiry, and conflict.

## Diagram

See `diagram.html` for the current dead-end update path and the target acquisition flow.
