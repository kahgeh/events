# Simplify ExpectedVersion Wording

- [x] Replace the confusing opening sentence in the expected-version how-to.
- [x] Verify the wording renders cleanly in context.
- [x] Record review result.
- [x] Restate the page so expected-version mismatches are described as an unexpected consistency signal under serial partition processing.
- [x] Rename and reorient the how-to as `docs/how-to/use-expected-version.md`.
- [x] Clarify that `ExpectedVersion` does not protect downstream side effects.

## Verification

- Read `docs/how-to/use-expected-version.md` around the opening section after the edit.
- Checked the scoped diff for `docs/how-to/use-expected-version.md`.
- Grepped `docs/how-to/use-expected-version.md` for remaining `concurr` language after restating the page.
- Verified `docs/how-to/use-expected-version.md` exists and the old how-to filenames do not.
- Grepped docs and task notes for stale old how-to path references after the rename.
- Marked the new how-to with intent-to-add so `git diff` includes the renamed replacement file.

## Review

- Completion reviewer approved the scoped docs change with no findings.
- Completion reviewer approved the restated consistency-guard framing with no findings.
- Completion reviewer rejected the rename pass because the new file was untracked in `git diff`; fixed by marking the new file with intent-to-add.
