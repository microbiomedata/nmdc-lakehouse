# Package versions and releases

`nmdc-lakehouse` is installable but remains in active development. The existing
`v0.1.0` repository tag predates this release policy. Version `0.2.0.dev0` is
the next pre-release foundation, not a claim of a stable public API or a
published PyPI release.

## Version source

`[project].version` in `pyproject.toml` is the single version source. Hatchling
writes that value into distribution metadata, and both
`nmdc_lakehouse.__version__` and `nmdc-lakehouse --version` read the installed
metadata. The wheel, source distribution, import, and CLI must agree.

Versions follow PEP 440 and use semantic-versioning-compatible release numbers:

- increment the major version for incompatible supported-interface changes;
- increment the minor version for compatible features;
- increment the patch version for compatible fixes; and
- use `.devN`, `aN`, `bN`, or `rcN` suffixes for development and pre-release
  builds.

## Validate a release candidate

Use the locked Python 3.13 environment:

<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
```bash
just check
just test-dist
```

`just test-dist` builds one wheel and one source distribution in a temporary
directory. It then creates an isolated Python 3.13 environment containing the
built wheel, not the source checkout, and verifies package metadata, the public
version import, the CLI version, and inclusion of the MIT license in both
archives. Temporary build products are removed automatically. The ordinary
`just build` recipe writes archives to `dist/` when a maintainer wants to
inspect or retain them.

## Prepare a release

1. Choose the PEP 440 version and update `[project].version` in
   `pyproject.toml` in a pull request.
2. Summarize user-visible changes in the pull request and resulting GitHub
   release notes. This repository does not yet maintain a separate changelog.
3. Run `just check` and `just test-dist`, and require the corresponding CI
   checks on the final commit.
4. Merge through the normal human-maintainer process.
5. From the merged `main` commit, create an annotated `v<version>` tag and a
   matching GitHub release.

There is no automated PyPI publication workflow yet. If public publication is
enabled, use PyPI Trusted Publishing from a protected GitHub environment. Do
not create or store a long-lived PyPI API token in repository secrets.

## The flat schema has its own version

The package version and the flat metadata schema version are different things and move for
different reasons. `src/nmdc_lakehouse/schemas/nmdc_metadata.yaml` declares

```
version: <nmdc-schema version>+flat.<flattener version>
```

for example `11.23.0+flat.1.0.0`. The first half says which upstream release it derives from. The
second is `FLATTENER_VERSION` in `src/nmdc_lakehouse/transforms/schema_generator.py`, and it
belongs to this repository's projection rules.

Until 2026-08-25 the artifact declared only the upstream version, so three committed revisions all
said `11.23.0` while differing in content. A consumer holding two tables written from two of those
could not tell them apart, which is the first question a consumer asks.

**Bump `FLATTENER_VERSION` in the same pull request that changes what the flattener emits.**
Raise the minor part when a table, attribute or range changes, and the patch part when only
descriptions or annotations move. `just check-flat-schema` will tell you the artifact is stale; it
cannot tell you the version should have moved, because that judgement is about what changed and why.

The artifact also carries `flat_schema_sha256`, a digest of its own rendered text with that field
blanked. It is computed rather than typed, and `just check-flat-schema` recomputes it, so a
hand-edited artifact is refused even when the edit looks harmless.

### Produce the diff report for a release

<!-- verified: run on 2026-08-25 against the two revisions named in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/293, which reported 79 changed
     table descriptions -->
```bash
just schema-diff <older-revision> <newer-revision>
```

Include the report in the release notes when the flat schema moved. It lists tables and attributes
added or removed, ranges and other attribute properties changed, and table descriptions changed.
When two artifacts differ in a way the report does not model, it says so explicitly rather than
reporting no differences, so a quiet "nothing changed" is not something it can produce.

## Supported Python

The package supports Python 3.13 and intentionally excludes Python 3.14. Keep
`.python-version`, `requires-python`, uv configuration, CI, documentation, and
wheel validation aligned when that policy changes.
