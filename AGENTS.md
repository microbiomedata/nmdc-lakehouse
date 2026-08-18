# Repository instructions for coding agents

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the documentation relevant to a
change before editing. Treat the contributor guide as the source of truth for
scope, complexity, documentation, and pull request expectations.

- Work in issue-sized slices. Keep non-goals explicit and move side quests to
  separate issues unless they are required for correctness, security, data
  safety, validation, or accurate documentation.
- Prefer existing package boundaries and the smallest sufficient design.
  Justify any new dependency, abstraction, wrapper, workflow, service, or
  configuration layer with a current need.
- Verify documentation claims against code and configuration. Update affected
  commands, defaults, paths, schemas, data formats, and operational guidance in
  the same change. Check that the relevant guide is complete enough to perform
  the supported task. Distinguish maintained, prototype, manual, and planned
  behavior.
- Preserve technical precision. Do not weaken prose to satisfy Vale; add a
  narrow vocabulary entry or justified exception for a correct project term.
- Use `just` recipes for repository tasks. Run the checks proportionate to the
  change and report exactly what ran; use `just check` before publishing a
  broadly affecting change.
- Inspect all review feedback, including collapsed or suppressed automated
  comments. After fixing or declining feedback, reply with the decision and
  supporting evidence. Leave pull request merging to a human maintainer.
- Before publishing or recommending merge, check that the title is concise,
  specific, and imperative and that the description covers rationale, behavior,
  validation, documentation impact, and justified follow-up work.
- Preserve unrelated user changes and never expose credentials, connection
  strings, or production data in code, tests, logs, comments, or commits.
