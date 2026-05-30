# Split inline tests

- [x] Move each inline `#[cfg(test)] mod tests` block under `src/` into a sibling `*_tests.rs` file.
- [x] Keep test visibility and imports compiling without changing behavior.
- [x] Run formatting and test verification.
- [x] Commit the branch update.
- [x] Push the branch update.

Verification:

- `cargo fmt --check`
- `cargo test`
- `git diff --check`

Review:

- Completion reviewer `019e7317-fb31-7281-b59d-2ffcda263db5` reported no blocking issues and gave a thumbs-up.
- Reviewer also ran `cargo fmt --check`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `git diff --check HEAD^..HEAD`.

Follow-up review:

- Removed `src/application_schema_tests.rs` because it only asserted hard-coded SQL substrings.
- Removed `src/actor_tests.rs` because it only asserted hard-coded enum/string mappings.
- Audited the remaining `src/*_tests.rs` and `tests/*.rs`; the remaining tests exercise behavior such as broadcast delivery, notification persistence and expiry, pool connection accounting, rotation parsing/formatting, runtime wiring, validation boundaries, and event-stream persistence.
