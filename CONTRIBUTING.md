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

Rules carry different levels and only errors fail CI. `Mark.Jargon` and
`Mark.ThroatClearing` are errors because the repository has none of them.
`Mark.EmDash` is a warning while a backlog is cleared, `Mark.BareRef` is a
suggestion until it can exclude references that already carry a URL, and
`Mark.Undefined` is a warning that flags an acronym never expanded in the same
file. For that last one, either expand the term on first use or add it to the
NMDC vocabulary, which is a deliberate statement that this audience needs no
expansion. Vale matches text, so it cannot tell a term being used from a term
being discussed; a bare-reference or undefined-term alert on a document about
those rules is expected.

`MinAlertLevel` in `.vale.ini` must stay at `error`, and a non-blocking rule
works by Vale not emitting it rather than by CI ignoring it.

The Vale step in `.github/workflows/ci.yml` sets `filter_mode: nofilter` and
`fail_on_error: true`. With the `github-pr-annotations` reporter that step uses,
those resolve to fail-on-any: the build fails on one alert of any severity. The
error-only behaviour people expect from `fail_on_error` belongs to the
`github-check` and `github-pr-check` reporters, which this workflow does not
use. The action also passes no `--minAlertLevel` to Vale, so `MinAlertLevel`
above decides what Vale prints, and everything Vale prints can fail the build.

Lowering `MinAlertLevel` therefore makes every warning a failed build. Observed
rather than inferred: a push with `MinAlertLevel = suggestion` failed the
`check` job with 196 annotations, none of them error severity.

There is deliberately no `level:` input on that step. At the pinned action SHA
it is never read; the action derives reviewdog's `-level` from Vale's own exit
code. It was present and inert, and its presence convinced two readers that a
severity threshold existed.

Run `just prose-lint` rather than `vale` directly. The recipe sets `HOME` to a
scratch directory, which is what stops a personal Vale configuration on the
machine leaking in; without it a local run can report problems CI will not, or
miss the vocabulary CI uses. It lints the same file set as CI, and passes
`--minAlertLevel=suggestion` so warnings and suggestions stay visible to you
without reaching CI.

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
