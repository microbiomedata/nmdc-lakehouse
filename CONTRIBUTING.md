# Contributing

Thank you for improving `nmdc-lakehouse`. This project favors small,
evidence-backed changes that keep its data behavior and operational boundaries
clear.

## Set up and validate a checkout

Follow the [development setup guide](docs/development-setup.md), then use the
repository tasks rather than recreating their commands:

```bash
just bootstrap
just check
```

Tests that contact live services are opt-in. A normal contribution must remain
testable without credentials, tunnels, or production data unless its issue
explicitly defines an integration test.

## Keep changes focused

- Solve the linked issue and its stated acceptance criteria. State important
  non-goals when the boundary is not obvious.
- Include review-driven work needed for correctness, security, preventing data
  loss, validating the change, or keeping its documentation accurate.
- Record unrelated improvements as follow-up issues instead of expanding the
  pull request.
- Prefer the smallest design that satisfies a demonstrated current need. A new
  dependency, abstraction, wrapper, workflow, service, or configuration layer
  must identify the concrete problem it solves and why existing components are
  insufficient.
- Do not add flexibility only for a hypothetical future use. Document the
  limitation and open a focused issue when future work is plausible but not yet
  required.

## Keep documentation truthful

Review documentation whenever a change affects commands, configuration,
setup, schemas, data formats, output locations, safety properties, or operating
assumptions. Update the relevant documentation in the same pull request.

Check documented commands, paths, defaults, examples, and capability claims
against the implementation. Clearly label prototypes, planned behavior, and
manual procedures; do not describe them as maintained capabilities. Preserve
useful technical detail when editing prose. Operational guidance should include
the prerequisites, safety boundaries, output location, and recovery information
a contributor needs to complete the supported task.

Vale is a mechanical spelling and style check, not an authority on meaning. Do
not make documentation less precise merely to satisfy it. When a correct
project term triggers Vale, add a narrow entry to the repository vocabulary or
use a justified local exception.

## Describe and review the change

Use a concise, specific, imperative pull request title. The description should
explain:

- why the change is needed and which issue it closes;
- what behavior or contract changed;
- how the result was validated;
- which documentation was reviewed or why no update is needed; and
- any justified complexity or follow-up work.

Keep objective requirements in automated checks: tests, type and dependency
analysis, formatting, workflow validation, prose linting, and link checking.
Scope, design complexity, and semantic documentation accuracy require author
and reviewer judgment; they are not reliable pass/fail lint rules.
