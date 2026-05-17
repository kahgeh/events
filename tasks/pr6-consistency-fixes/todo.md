# PR 6 Consistency Fixes

## Plan

- [x] Align Mode 2 parallel projector documentation with the all-or-nothing batch checkpoint sample.
- [x] Remove live documentation/source wording that refers to Turso as SQLite/SQLite-compatible or positions the crate as event sourcing.
- [x] Update PR description so it describes the final diff, not superseded history.
- [x] Verify with formatter, tests, clippy, and stale-reference grep.
- [ ] Request completion reviewer subagent before reporting done.

## Review

- `cargo fmt --check` passed.
- `cargo test` passed: 36 unit tests, 25 integration tests, 3 doc-tests.
- `cargo clippy --all-targets --all-features` passed.
- Broad stale-reference grep found no hits for lease APIs/fields, event-sourcing wording, `SQLite`, `libSQL`, or avoidable `sqlite*` internal names in `docs`, `README.md`, `src`, `tests`, or `examples`.
- PR #6 description was updated to describe the final diff and explicitly explain why the branch-local lease tests are removed.
- Follow-up test-history check found that the lease tests were introduced by earlier PR-branch commits and removed by the lease-removal commit. They targeted removed lease APIs/schema fields, so keeping them would be wrong for the final contract.
