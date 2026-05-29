# Docs Content Refresh

## Goal

Keep the existing documentation structure, but refresh the content so it matches the current one-log-per-partition-store implementation. Work through documents one at a time, starting with the architecture and partitioning explanations because those define the vocabulary used by the rest of the docs.

## Evidence To Check

- [x] Inventory current docs and task notes.
- [x] Verify terminology against `CONTEXT.md`, current public API, and storage schema for the architecture pass.
- [x] Verify rotation/file naming details against implementation.
- [x] Verify examples compile after affected doc changes when examples are used as evidence.

## Document Pass Order

- [x] `docs/explanation/architecture.md`
- [ ] `docs/explanation/partitioning-strategy.md`
- [ ] `docs/explanation/cursor-mechanism.md`
- [ ] `docs/explanation/concurrency-control.md`
- [ ] `docs/how-to/worker-pool-over-per-partition-store.md`
- [ ] Remaining how-to/tutorial/reference docs that repeat partition/rotation/workflow terminology.

## Review Notes

- Keep the current Diataxis-style structure and file locations.
- Rename the public root manager to `EventNamespaces`.
- Add `EventNamespace` as the named namespace scope under the root manager.
- Keep `Partition` as the public reference to one selected partition.
- Rename the opened append/read API to `EventLog`.
- Rename the public cursor/version type to `EventLogVersion`.
- Rename internal catalog concepts from event-log language to event-log language: `event_log_head` and `event_file_ranges`.
- Make `EventLog` mean the application-facing ordered log API that abstracts physical rotation.
- Make "rotation" mean physical event database file rollover behind an `EventLog`.
- Make "workflow" identity separate from partition identity and file rotation.
- Avoid returning to tenant-specific language as the primary teaching model.
- Update `CONTEXT.md` to match the final vocabulary once code and docs are aligned.

## Verification

- [x] Run markdown/link/text consistency checks after edits.
- [x] Run `cargo test --no-run` or a narrower compile check if examples or public API references are changed materially.
- [x] Record final review and verification result here before reporting done.

### Architecture Pass

- Source vocabulary checked against `CONTEXT.md`.
- Public API and schema checked against `src/partitions.rs`, `src/eventstore.rs`, and `src/migration.rs`.
- Updated stale architecture wording so partition store, owner partition strategy, event log, rotation, and workflow started-by event ID are separated.
- Added missing `trace_id` and `span_id` fields to the architecture SQL snippet.
- Verification: `git diff --check -- docs/explanation/architecture.md tasks/docs-content-refresh/todo.md` passed; stale-term scan for tenant/stream/workflow-id phrasing returned no matches in `docs/explanation/architecture.md`.

### Naming Alignment Pass

- Renamed public API types:
  - `EventPartitions` to `EventNamespaces`
  - added `EventNamespace`
  - `OwnerEventStore` to `EventLog`
  - `OwnerLogVersion` to `EventLogVersion`
- Renamed catalog-facing concepts:
  - `OwnerLogHead` to `EventLogHead`
  - `PartitionRef` to `EventFileRange`
  - catalog table `owner_log` to `event_log_head`
  - catalog table `partitions` to `event_file_ranges`
- Updated `CONTEXT.md`, README, docs, examples, tests, and runtime accessors to the new vocabulary.
- Verification:
  - `cargo check`
  - `cargo test --no-run`
  - `cargo check --examples`
  - `cargo test`
  - `cargo clippy --all-targets --all-features`
  - `git diff --check -- README.md CONTEXT.md docs src examples tests tasks/docs-content-refresh/todo.md`
  - stale-name scan for `EventPartitions`, `OwnerEventStore`, `OwnerLogVersion`, `OwnerLogHead`, `PartitionRef`, `ensure_exists(`, and `event_partitions(`
  - markdown link existence script over README and docs

### Notification Architecture Correction

- Added the missing progress-notification capability to the core architecture
  diagram.
- Kept durable event logs and progress notifications separate:
  - `EventLog` remains the durable append/read API.
  - `NotificationsStore`, `StreamEventSender`, `StreamEventSubscriber`, and
    `StreamEventBroadcastLoop` describe transient request-status delivery.
- Added a data-flow subsection for progress notifications so reconnect storage
  and live broadcast are visible without treating notifications as durable
  events.
- Verification:
  - `git diff --check -- docs/explanation/architecture.md tasks/docs-content-refresh/todo.md tasks/lessons.md`
  - Mermaid C4 syntax checked against Mermaid C4 docs
  - markdown link existence script over README and docs
  - stale wording scan for `client_a`, `orders.ensure`, `physical physical`,
    `application db`, and `without changing public cursors`

### Core Architecture Diagram Grouping

- Replaced the ASCII core architecture diagram with a Mermaid flowchart after C4 relationship labels overlapped component text.
- Grouped durable event-log components under `Event Core`.
- Grouped progress-notification components under `Progress Notifications`.
- Kept `EventsRuntime` as the boundary that opens both capabilities.
- Kept line routing simple: runtime fans into the two grouped capabilities, and each group has its own top-to-bottom path.

### Architecture Layer Grouping

- Reworked the core architecture diagram to keep the original top-level capability split:
  - `Event Core`
  - `Progress Notifications`
- Nested `API`, `Implementation`, and `Storage` sections inside each capability group.
- Kept component-level boxes inside the layers instead of collapsing each layer into a summary box.
- Verified the rendered SVG places `Event Core` on the left and `Progress Notifications` on the right.
- Replaced the Mermaid source with a checked-in SVG because Mermaid could not reliably preserve both left/right capability groups and horizontal component rows inside nested layers.
- Expanded SVG role descriptions and widened boxes so the text stands alone and stays inside the component boxes.
- Added `draw-svg-architecture-diagram` skill with a reusable SVG text-fit checker, then used it to fix all current text overflow in `architecture-core.svg`.
- Moved `RotationPolicy` into the Event Core API row as a public data contract and kept `Rotation engine` under Internal Components.
- Allowed Event Core API boxes to wrap onto a second row so component labels remain readable.
- Refined the Event Core API layout so component boxes share equal row padding while the smaller `RotationPolicy` data-contract box aligns under `EventLog`.
- Extended the SVG checker to validate row gaps, left/right padding, and layer top/bottom padding in addition to text fit.
- Applied explicit `data-fit-box` wrappers to the current diagram so the checker uses declared box ownership instead of guessing from coordinates.
- Updated the checker to measure layer and group padding from the visual bottom of the title text, which catches real bottom-padding problems without requiring excessive title spacing.
- Verification:
  - `python3 ~/.codex/skills/draw-svg-architecture-diagram/scripts/check_svg_text_fit.py docs/explanation/architecture-core.svg`
  - `python3 -m py_compile ~/.codex/skills/draw-svg-architecture-diagram/scripts/check_svg_text_fit.py`
  - SVG XML parse check
  - markdown link/image check over README and docs
  - `git diff --check -- docs/explanation/architecture.md docs/explanation/architecture-core.svg tasks/docs-content-refresh/todo.md tasks/lessons.md`
