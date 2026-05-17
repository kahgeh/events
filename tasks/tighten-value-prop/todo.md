# Tighten Value Proposition

## Plan

- [x] Reword README and docs index around the embedded append-only event store value proposition.
- [x] Add non-goals so the crate does not imply distributed consumer coordination or a full framework.
- [x] Add a tenant/shard partitioning example with async demand-retained projectors.
- [x] Link the new guide from the docs index.
- [x] Verify formatting, tests, clippy, runnable example, and stale wording.
- [ ] Request completion reviewer before reporting done.

## Review

- `cargo fmt --check` passed.
- `cargo test` passed: 36 unit tests, 28 integration tests, 3 doc-tests.
- `cargo clippy --all-targets --all-features` passed.
- `cargo run --example tenant_projectors` passed and showed `acme`/`beta` projectors draining independently.
- Value-prop grep found only intentional non-goal mentions for distributed coordination, cluster membership, replication, and read-model framework scope.
- Completion review found a lost-wakeup race in the tenant supervisor. Fixed by tracking `active` and `pending` tenants separately; dirty notifications received while a tenant is active now cause an immediate rerun after the current drain exits.
- Completion review also found replica roadmap wording that conflicted with the tightened scope. Removed the replica example/roadmap language and kept replica/routing as outside the crate.
- Follow-up warning about unverified `PRAGMA cache_status` was fixed by removing that metric from the touched performance docs.
