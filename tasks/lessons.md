# Lessons

- When a PR description appears stale, verify the claim against both the final PR diff and the commit history before editing it. A claim can be false as a final-diff summary but still describe an intermediate commit.
- Grep checks for forbidden terminology should include lowercase/internal-name variants when the project rule is about public wording, then consciously decide whether each hit is a necessary literal name or removable documentation drift.
