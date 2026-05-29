# Ensure Partition Name

- [x] Find the public API, internal helper, docs, examples, and tests that use `ensure_partition_exists`.
- [x] Rename `ensure_partition_exists` to `ensure_partition` consistently.
- [x] Run formatting and tests.
- [x] Record verification results.

## Verification

- `rg -n "ensure_partition_exists" src examples docs tests README.md` found no remaining live references.
- `cargo fmt --check` passed.
- `cargo test` passed.
- `cargo check --examples` passed.
- `cargo clippy --all-targets -- -D warnings` passed.
- `git diff --check` passed.
- Completion reviewer found no missed rename references; follow-up README grammar and stale task-note findings were addressed.
