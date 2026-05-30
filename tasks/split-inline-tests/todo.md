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
