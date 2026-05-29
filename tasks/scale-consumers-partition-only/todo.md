# Remove Scale Consumers Page

- [x] Confirm the misleading scaling headings are limited to `docs/how-to/scale-consumers.md`.
- [x] Consolidate useful consumer-scaling guidance into `docs/how-to/worker-pool-over-per-partition-store.md`.
- [x] Delete `docs/how-to/scale-consumers.md`.
- [x] Remove links to the deleted page from the docs index and next-step sections.
- [x] Keep filtering by event type framed as projection-handler behavior, not a scaling boundary.
- [x] Verify the remaining worker-pool page still teaches one active consumer per projection and partition key.
- [x] Record completion review result.

## Verification

- `rg -n "scale-consumers|Scale Consumers|Scale consumers" docs README.md` returned no matches.
- `rg -n "Event-type Based Scaling|Stream-based Scaling|event-type based|stream-based scaling|Scaling Patterns" docs README.md` returned no matches.

## Review

- Completion reviewer approved the consolidation with no findings.
