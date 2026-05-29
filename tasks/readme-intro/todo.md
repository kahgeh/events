# README intro rewrite

- [x] Inspect the current README opening and project vocabulary.
- [x] Keep the rewrite scoped to the README introduction.
- [x] Verify the diff for terminology and formatting.
- [x] Request completion review and address findings.
- [x] Record review and verification notes here.

## Review notes

- Completion review rejected the first draft because it overclaimed
  service-bus behavior, application read-model rebuild ownership, workflow
  resumption ownership, and bounded resource guarantees.
- Follow-up rewrite narrows the intro to durable event-stream and CQRS-style
  workflows, and states that applications use partition keys, bounded reads,
  and application-owned projection offsets to build those behaviors.
- Second completion review approved the narrowed wording. It had one readability
  suggestion, changing "durable event-stream" to "durable event streams", which
  was applied.
- Verification: `git diff --check -- README.md tasks/readme-intro/todo.md`
  passed.
