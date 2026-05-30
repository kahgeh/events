# Docs Index Intro

- [x] Confirm the current docs index intro repeats the crate description.
- [x] Rewrite the intro to explain how to use the documentation.
- [x] Verify the diff is scoped and markdown links remain unchanged.

## Verification

- `git diff -- docs/README.md tasks/docs-index-intro/todo.md` confirmed the docs index change is limited to the intro paragraph.
- `rg -n "\]\(" docs/README.md` confirmed the existing index links remain present.
