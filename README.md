# nmdc-lakehouse

ETL pipeline that reads [NMDC](https://microbiomedata.org/) metadata from
**MongoDB**, uses [`nmdc-schema`](https://github.com/microbiomedata/nmdc-schema)
to project the nested object model into tabular records, and writes local
Parquet artifacts for downstream lakehouse publication.

> Status: **active development** – the maintained MongoDB-to-Parquet pipeline
> is functional against NMDC production MongoDB via a GCP SSH tunnel. Other
> capabilities are classified below rather than implied by package structure.

## Implementation status

“Implemented” means exercised by package code and tests. “Prototype/manual”
means operational work exists, but not as a supported package job. “Planned”
means an interface or dependency may exist without executable support.

| Layer | Capability | Status | Current contract |
|---|---|---|---|
| Source | NMDC MongoDB | **Implemented** | Read-only `linkml-store` iteration; `functional_annotation_agg` uses a read-only raw `pymongo` path for scale. |
| Source | PostgreSQL | **Planned** | `PostgresSource.iter_records()` is a `NotImplementedError` stub. |
| Transform | Schema-driven metadata flattening | **Implemented** | Uses LinkML definitions for projection and Arrow type construction. This is not full per-record LinkML validation. |
| Transform | Publish the `nmdc_metadata` target schema | **Implemented** | Canonical generated LinkML YAML covers primary and side-table classes and is checked for drift. |
| Transform | Validate flattened target rows | **Implemented** | Offline full or deterministic bounded validation consumes the manifested table/class identity and packaged target schema; reports contain sanitized categories, not row values. |
| Sink | Parquet | **Implemented** | Local filesystem only; one `{table}.parquet` file per primary or side table, with streamed row groups. It is not a partitioned dataset. |
| Sink | Remote/object-store Parquet | **Planned** | `ParquetSink` does not support remote roots; URI strings may be interpreted as malformed local paths rather than rejected. |
| Sink | Apache Iceberg | **Planned** | `IcebergSink.write()` is a `NotImplementedError` stub. Iceberg is one possible destination adapter, not the required or preferred publication path. |
| Workflow results | Fetch/cache/parse NERSC result files | **Prototype/manual** | File-type-specific notebooks and scripts produce Parquet; migration into registered package jobs is tracked in [#130](https://github.com/microbiomedata/nmdc-lakehouse/issues/130). |
| Publication | Generate a reviewable metadata bundle | **Implemented** | An offline command joins exact snapshot footer descriptions with a snapshot-bound, reviewed profile; it does not apply metadata to a destination. |
| Publication | Draft a snapshot-bound metadata profile | **Implemented** | An offline command reads the validated snapshot identity and combines it with explicit operator-supplied namespace content for review. |
| Publication | Stage snapshots and register destination assets | **Manual/external** | Maintained ETL stops at schema-directed local Parquet. The portable publication contract is destination-neutral; BERDL is one documented profile. |
| Upstream mutation | Write flattened data back to MongoDB | **Legacy only** | Maintained jobs never write to production MongoDB. A copied EMA script does write `flattened_*` collections and is tracked for retirement in [#27](https://github.com/microbiomedata/nmdc-lakehouse/issues/27). |
| JGI/GOLD integration | Read or publish JGI/GOLD data | **Not implemented** | There is no JGI adapter or job. A copied, unregistered CSV utility retains “GOLD” in its filename and defaults, but is only a generic MongoDB collection exporter. |

## Layout

```
nmdc-lakehouse/
├── pyproject.toml          # uv / PEP 621 project definition
├── justfile                # task runner (install, test, lint, run)
├── README.md
├── src/
│   └── nmdc_lakehouse/
│       ├── __init__.py
│       ├── cli.py          # Click CLI entry point
│       ├── config.py       # settings & environment loading
│       ├── sources/        # Mongo source + planned Postgres interface
│       ├── transforms/     # object-model flattening to tabular form
│       ├── sinks/          # local Parquet sink + planned Iceberg interface
│       ├── io/             # large data-file handling
│       └── jobs/           # ETL job definitions & registry
└── tests/
```

### Module responsibilities

| Package                       | Purpose                                                                 |
|-------------------------------|-------------------------------------------------------------------------|
| `nmdc_lakehouse.sources`      | Retrieve NMDC records from MongoDB; reserve an interface for PostgreSQL. |
| `nmdc_lakehouse.transforms`   | Flatten the nested LinkML object model into tabular / relational form.  |
| `nmdc_lakehouse.sinks`        | Write local Parquet files; reserve optional managed-table adapters.      |
| `nmdc_lakehouse.io`           | Stage & reference large genomic / bulk data files alongside metadata.   |
| `nmdc_lakehouse.jobs`         | Declarative ETL jobs composed from a source → transform → sink pipeline.|
| `nmdc_lakehouse.cli`          | Click-based CLI that dispatches to registered jobs.                     |

## Requirements

Install Git, [`uv`](https://docs.astral.sh/uv/), and
[`just`](https://just.systems/), then use the canonical bootstrap command below.
The authoritative host-tool, operating-system, and optional-service matrix is
the [development setup guide](docs/development-setup.md). Bootstrap uses the
checked-in Python 3.13 policy and does not require credentials or live services.

## Getting started

```bash
# From a fresh checkout:
just bootstrap

# Then choose the relevant task:
just doctor
just test
just check
just cli --help
```

`just bootstrap` synchronizes the locked development and documentation
dependencies, installs the repository pre-commit hook when Git uses its default
hooks directory, and performs a credential-free CLI smoke test. If
`core.hooksPath` is already set, bootstrap preserves that configured hooks-path policy and
prints the explicit repository-hook command instead. It is safe to run
repeatedly. It never installs host tools, copies credentials, opens tunnels, or
starts services.

`just doctor` is the read-only counterpart: it verifies the installed tools,
locked environment, Git hook, optional configuration names, and local paths
without synchronizing packages or contacting a service. Warnings identify
optional production capabilities; required failures return a nonzero status.
If `just` exits while parsing a malformed `.env`, bypass its automatic `.env`
loading with `uv run --no-sync nmdc-lakehouse doctor`. Doctor will inspect the
file itself and report the problem without printing its contents.
Live checks require an explicit `--service-check` flag on the
`nmdc-lakehouse doctor` command; see the
[development setup guide](docs/development-setup.md#opt-in-to-live-service-checks).

## Configuration

Unit development and the bootstrap command do not need an `.env` file. For
opt-in live MongoDB or BERDL readiness work, copy `.env.example` to `.env` and
fill in only the applicable profile; `just` and the CLI load it automatically:

```bash
cp .env.example .env
```

Key variables (full list in `.env.example`):

| Variable | Default | Notes |
|---|---|---|
| `MONGO_HOST` | `localhost` | |
| `MONGO_PORT` | `27017` | Use `27124` for the GCP SSH tunnel |
| `MONGO_DBNAME` | `nmdc` | |
| `MONGO_USERNAME` | `admin` | Personal MongoDB account; see connection guide |
| `MONGO_PASSWORD` | | |
| `MONGO_AUTH_SOURCE` | `admin` | Authentication database |
| `MONGO_REPLICA_SET` | | Optional replica set name |
| `MONGO_DIRECT_CONNECTION` | `false` | Set `true` when using the SSH tunnel |
| `NMDC_JUMP_KEY` | `~/.ssh/jump-dev.microbiomedata.org.private_key` | Optional override for the GCP jump-host key |
| `LAKEHOUSE_ROOT` | `./lakehouse` | Local directory; doctor rejects remote URIs |
| `BERIL_CHECKOUT` | | Explicit external checkout inspected by `berdl-doctor` |
| `KBASE_AUTH_TOKEN` | | Short-lived secret; presence only is reported |
| `BERDL_DESTINATION_ID` | | Logical destination observed for the planned publication |
| `BERDL_CATALOG` | | Catalog discovered from the current environment |
| `BERDL_TABLE_FORMAT` | | Table format discovered from the current environment |

For production access via the GCP SSH tunnel, see
**[docs/mongodb-connection.md](docs/mongodb-connection.md)** for the full
setup procedure (NERSC prerequisites, key installation, tunnel command).

## Job runner

The scaffold includes a Click CLI (`nmdc-lakehouse`) as the default entry
point for running ETL jobs. The `jobs/` package is structured so that the
CLI can be swapped for or supplemented by a heavier runner
(Dagster / Prefect / Snakemake / etc.) without reshuffling the core source
and sink modules.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for change-scope, documentation, review,
and pull request expectations.

Selected tasks are shown below; run `just --list` for the complete task and
operational-command inventory.

| Recipe              | What it does                                     |
|---------------------|--------------------------------------------------|
| `just bootstrap`    | Create locked env; install hooks unless `core.hooksPath` is set |
| `just doctor`       | Diagnose local readiness without changing it      |
| `just berdl-doctor SNAPSHOT_ROOT` | Diagnose BERDL publication readiness without mutation |
| `just validate-target-rows SNAPSHOT_ROOT REPORT` | Validate manifested rows against the packaged target LinkML schema |
| `just publication-preflight SNAPSHOT_ROOT BUNDLE INVENTORY PLAN` | Cross-check reviewed publication artifacts before staging |
| `just metadata-application-plan BUNDLE INVENTORY STAGING_NAMESPACE` | Plan metadata operations for an explicit staging namespace |
| `just berdl-upload-plan ...` | Bind reviewed artifacts to the NMDC adapter and exact KBase ingest revision |
| `just berdl-upload ...` | Preview or execute and verify one reviewed BERDL staging plan |
| `just install`      | Synchronize the locked development environment    |
| `just install-all`  | Synchronize locked development and docs extras    |
| `just lock`         | Refresh `uv.lock`                                |
| `just lint-just`    | Check canonical justfile syntax and formatting   |
| `just prose-lint`   | Spell-check maintained Markdown with Vale        |
| `just test-prose-lint-exit` | Prove the prose gate blocks on errors and not on warnings |
| `just shellcheck`   | Lint safely rendered Bash recipes                 |
| `just actionlint`   | Check GitHub Actions workflows and run blocks     |
| `just lint`         | ruff check + format check                        |
| `just deps-lint`    | Check missing, unused, and transitive dependencies|
| `just format`       | ruff format + auto-fix                           |
| `just typecheck`    | `mypy src`                                       |
| `just test`         | pytest                                           |
| `just test-cov`     | pytest with the configured floor and coverage XML|
| `just diff-cover`   | Coverage of lines this branch adds or changes     |
| `just build`        | Build sdist + wheel via `uv build`               |
| `just test-dist`    | Build and test archives in isolated Python 3.13  |
| `just docs-build`   | Build the MkDocs site (requires `install-all`)   |
| `just check`        | just, prose, prose gate, shell, workflow, Python, deps, type, schema, tests, coverage |

### Coverage policy

Two separate gates, measuring different things. Neither replaces the other.

**Total floor, 80%, `fail_under` in `pyproject.toml`.** Raised from 75% on
2026-08-27, when the Python 3.13 suite measured 88.053% (5,357 statements, 640
uncovered), leaving eight points of headroom. It was 75% from 2026-08-18, and
71.462% before that: parent commit `ada7f3f` covered 606 of 848 statements on
2026-08-17. The two live-MongoDB integration tests remain explicitly skipped.
Raise `fail_under` as focused tests improve coverage; do not lower it merely to
merge a regression.

**Changed-line floor, 90%, `just diff-cover`.** The total floor cannot protect
new code. A wholly untested function arriving in a well-tested codebase moves
the total by a fraction of a percent and clears the floor easily. That happened:
`_verify_ingest_checkout` merged with no direct test while the suite reported
85.96% total, and the suppressed review finding that reached the default branch
landed on exactly that function. So this second gate measures only the lines the
branch adds or changes, against the base branch, and reports which added lines are
uncovered. CI passes the pull request's base; `just diff-cover` defaults to
`origin/main` and takes a different base as its argument.

The two numbers differ on purpose and are not in tension. 80% is a statement
about a codebase carrying legacy paths that predate the test suite. 90% is a
statement about code being written now, where there is no such excuse. The
changed-line gate never asks for coverage of unchanged lines, so raising it does
not create work on files nobody touched.

A branch that changes no Python passes it: with no measurable lines in the diff,
`diff-cover` reports nothing to score and succeeds.

Running it needs `coverage.xml`, so run `just test-cov` first, and needs the
base branch present, so a shallow clone has to fetch it. CI does both.

CI runs it on pull requests only. On a push to `main` there is no base branch to
compare against, and a gate that cannot fail is worse than an absent one, because
the green tick claims a check that did not happen.

## License

[MIT](LICENSE)
