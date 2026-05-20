# Lessons

- When a PR description appears stale, verify the claim against both the final PR diff and the commit history before editing it. A claim can be false as a final-diff summary but still describe an intermediate commit.
- Grep checks for forbidden terminology should include lowercase/internal-name variants when the project rule is about public wording, then consciously decide whether each hit is a necessary literal name or removable documentation drift.
- When changing the event store partitioning model, distinguish the primitive partition store, the ordered event log inside it, owner partition as a scaling/routing strategy, and workflow/retry identity.
- When proposing partition-store path design, prefer readable filesystem-safe application keys before introducing encoding or registries. Encoding adds indirection and should only appear when caller-generated safe keys are not acceptable.
- When modeling workflow recovery, do not assume a workflow identifier names a one-time execution. Some workflow kinds can run multiple times for the same owner, so distinguish workflow kind from workflow instance/run identity.
- Do not preserve low-level projector APIs only because application-owned worker pools need batching. On-demand pools can use bounded owner-log reads and application-owned drain state; the crate does not need a public finite batch outcome surface for that.
- When introducing a typed cursor sentinel, define exactly which API positions accept it. A "before first" read cursor should not silently become a valid append expected version.
- Do not export internal guardrail constants only so applications can prevalidate config. Prefer API-level validation unless the value is deliberately part of the public contract.
- Do not spend repeated clarification questions on low-impact guardrail details once the broad contract is clear. Record a simple default and move back to the architecture-level decisions.
- When the user constrains filesystem-safe keys to lowercase letters, digits, and hyphen only, update examples as well as prose; underscores and uppercase examples quietly undermine the rule.
- Do not rename a settled user-facing domain term just because a technical intermediate type appears. Keep lower-level storage terms for mechanics and preserve the clearer domain handle name when the user prefers it.
- Do not replace stale documentation with placeholder pages in a PR-ready change. If old docs are too stale to preserve, either migrate their useful content to the new model or remove/consolidate the page with explicit links from the docs index.
- Do not overstate "owner" as the only domain model for partition stores. Present the primitive as one ordered log per partition store; describe owner partition as a scaling feature layered on top.
- When updating established docs, preserve the original Diataxis document shape, context, problem statements, and diagrams unless the page is intentionally removed. Do not collapse rich docs into brief summaries just because the API changed.
- In live documentation, describe the current model directly; avoid mentioning previous designs or implementation history unless the page is explicitly a migration note.
- When updating docs for an API change, preserve each document's local structure where practical instead of normalizing every page into the same heading template. Put current content into the existing reader flow.
