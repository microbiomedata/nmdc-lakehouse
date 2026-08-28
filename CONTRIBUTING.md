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

Rules carry different levels and only errors fail CI. `Mark.Jargon`,
`Mark.ThroatClearing` and `Mark.EmDash` are errors because the maintained prose
Vale checks has none of them. That is a claim about the linted set, not the
repository: 149 em dash characters, on 142 lines, remain in 31 files Vale never
reads, mostly notebooks, scripts and Python source. Count occurrences rather
than lines, because `grep -c` counts matching lines and seven of those files
carry two on one line:

```bash
git ls-files -z | xargs -0 grep -o $'\u2014' | wc -l   # bash or zsh
```

The escape rather than the character itself is deliberate. Vale would allow a
literal em dash there, since it skips fenced blocks and inline code spans, but a
file that documents this count must not add to it: with the character written
out, the command returns 150 here instead of 149. `Mark.EmDash` was a warning until the prose backlog was
cleared on 2026-08-24, which is the sequence a new rule should follow here: land
it at warning, clear what it finds, then promote, so it never starts blocking
work its author did not cause. `Mark.BareRef` is a
suggestion until it can exclude references that already carry a URL, and
`Mark.Undefined` is a warning that flags an acronym never expanded in the same
file. For that last one, either expand the term on first use or add it to the
NMDC vocabulary, which is a deliberate statement that this audience needs no
expansion. Vale matches text, so it cannot tell a term being used from a term
being discussed; a bare-reference or undefined-term alert on a document about
those rules is expected.

`MinAlertLevel` in `.vale.ini` must stay at `error`, and a non-blocking rule
works by Vale not emitting it rather than by CI ignoring it.

The Vale step in `.github/workflows/ci.yml` sets `fail_on_error: true`. The
action installs reviewdog 0.17.0, and at that version `-fail-on-error` returns 1
if any result is reported, with no severity threshold: its own flag
documentation reads "Returns 1 as exit code if any errors/warnings found in
input". Later reviewdog versions branch on the reporter; 0.17.0 does not, so
pin the version when repeating this. The action also passes no
`--minAlertLevel` to Vale, so `MinAlertLevel` above decides what Vale prints,
and everything Vale prints can fail the build.

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

## Say whether a documented procedure has been run

A fenced block under `docs/` in a language that runs something must carry a marker
on the line above it. `just doc-procedures` fails without one, and CI runs it.

```
<!-- verified: 2026-08-24 staged 53 tables against the nmdc tenant -->
<!-- unverified: needs a pod terminal, tracked in <issue URL> -->
```

Both pass. Running the procedure is not the requirement, because most of these
cannot be run from a workstation. Saying which is. A procedure nobody has run, reading
like a tested one, is what this prevents: `docs/berdl-upload.md` exists because a
Spark write to a local path silently produced no backup, and five rounds of review
on the change that documented it were spent on claims nobody had checked.

Mark the block you changed. There is no exemption list and nothing to regenerate:
every runnable block under `docs/` carries a marker, so the only way to satisfy
the check is to say which kind your block is.

A `verified` marker must carry a date, because that is the claim worth holding to
a format. "verified: ok" asserts something with nothing behind it, which is the
failure this exists to prevent. An `unverified` marker concedes rather than
claims, so it needs only a reason and either somewhere it is tracked or a
statement that nothing tracks it:

```
<!-- unverified: needs a pod terminal, tracked in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/136 -->

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```

The second form says something about the marker rather than about the world, on
purpose. An earlier wording claimed nothing tracked the procedure, which was
wrong for five markers whose work is tracked by an open issue. If you know the
tracker, name it; if you have not checked, say only that none is named here.

The second form exists because a tracker does not always exist. The rule once
required one unconditionally, and 80 markers ended up naming an issue that had
closed when a different piece of work finished, so the pointer looked live and
was not. Inventing an issue so the pointer resolves is worse than saying there is
none: it sends the reader somewhere that will not help them.

If the issue a marker names has since closed, the marker needs changing: point at
a live issue, or say that nothing tracks the work. Saying "now closed" beside the
old reference is not accepted, because deciding from prose whether an issue is
settled cannot be done reliably. "#1 is still open and expected to be closed
later" defeated three attempts at it.

`just doc-references-issues` reports markers pointing at a closed issue, and it
is not part of `just check` because it queries GitHub.

Most of the existing markers say `unverified`, which is the honest state: nobody
has recorded running those procedures. Changing one to `verified` means you ran
it and wrote down what came back.

A block with no language, or a data language such as `json` or `text`, is inert
and needs nothing. Any other language is treated as runnable, including one nobody
here has used yet, because a language this check does not recognise should be
checked rather than ignored.

## Do not hand-write a parser for a format that has one

Use the ecosystem parser for any format with a specification: Markdown through
`markdown-it-py`, YAML through `PyYAML` or `ruamel.yaml`, HTML and XML through
`lxml`, CSV through the `csv` module, Tom's Obvious Minimal Language (TOML)
through `tomllib`, shell words through
`shlex`. For `git`, use a porcelain format with `-z` and split on the null
byte; the
human-readable output loses any path containing a newline, and the record silently
becomes a shorter path that never existed.

Regular expressions are fine for line-oriented logs and for text with no grammar.

The reason is measured rather than stylistic. A hand-written Markdown fence
scanner in this repository was corrected seven times by review, and every defect
was a way for a runnable block to go *unseen* rather than merely reported: an
over-indented line read as a closing fence, a block quote prefix required to match
byte for byte, a fence inside a block quote invisible entirely, trailing
whitespace stripped when a backslash followed by a space stops escaping a newline.
Replacing it with a parser closed all of them at once and shortened the code.

The signal to watch for is a review finding count that rises or oscillates across
rounds rather than falling. That means the fixes are producing new surface faster
than they close it, and the answer is to change the approach rather than write
another fix.

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
