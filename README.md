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
| Sink | Parquet | **Implemented** | Local filesystem only; one `{table}.parquet` file per primary or side table, with streamed row groups. It is not a partitioned dataset. |
| Sink | Remote/object-store Parquet | **Planned** | `ParquetSink` does not support remote roots; URI strings may be interpreted as malformed local paths rather than rejected. |
| Sink | Apache Iceberg | **Planned** | `IcebergSink.write()` is a `NotImplementedError` stub; Iceberg remains the intended managed-table format, not a capability to remove. |
| Workflow results | Fetch/cache/parse NERSC result files | **Prototype/manual** | File-type-specific notebooks and scripts produce Parquet; migration into registered package jobs is tracked in [#130](https://github.com/microbiomedata/nmdc-lakehouse/issues/130). |
| BERDL publication | Promote Parquet and register managed tables | **Manual/external** | Maintained ETL stops at schema-directed, typed local Parquet. BERDL publication is a separate operation; its Silver layer uses Iceberg/Polaris. |
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
| `nmdc_lakehouse.sinks`        | Write local Parquet files; reserve an interface for Iceberg publication. |
| `nmdc_lakehouse.io`           | Stage & reference large genomic / bulk data files alongside metadata.   |
| `nmdc_lakehouse.jobs`         | Declarative ETL jobs composed from a source → transform → sink pipeline.|
| `nmdc_lakehouse.cli`          | Click-based CLI that dispatches to registered jobs.                     |

## Requirements

- Python ≥ 3.13 and < 3.14
- [`uv`](https://docs.astral.sh/uv/) for environment & dependency management
- [`just`](https://just.systems/) 1.58.0 for task running and canonical justfile formatting
- Access to an NMDC MongoDB instance for production-data runs

## Getting started

```bash
# Install uv and just first, then:
just install        # uv sync --extra dev
just test           # run unit tests
just lint           # ruff check + format --check
just cli --help     # show the CLI
```

## Configuration

Copy `.env.example` to `.env` and fill in your credentials — `just` and the
CLI load it automatically:

```bash
cp .env.example .env
```

Key variables (full list in `.env.example`):

| Variable | Default | Notes |
|---|---|---|
| `MONGO_HOST` | `localhost` | |
| `MONGO_PORT` | `27017` | Use `27124` for the GCP SSH tunnel |
| `MONGO_DBNAME` | `nmdc` | |
| `MONGO_USERNAME` | `admin` | Personal MongoDB account — see connection guide |
| `MONGO_PASSWORD` | | |
| `MONGO_AUTH_SOURCE` | `admin` | Authentication database |
| `MONGO_REPLICA_SET` | | Optional replica set name |
| `MONGO_DIRECT_CONNECTION` | `false` | Set `true` when using the SSH tunnel |
| `LAKEHOUSE_ROOT` | `./lakehouse` | Local directory; remote URIs are unsupported and are not currently rejected |

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

Common tasks are exposed via `just`:

| Recipe              | What it does                                     |
|---------------------|--------------------------------------------------|
| `just install`      | `uv sync --extra dev`                            |
| `just install-all`  | Install development and documentation extras     |
| `just lock`         | Refresh `uv.lock`                                |
| `just lint-just`    | Check canonical justfile syntax and formatting   |
| `just lint`         | ruff check + format check                        |
| `just format`       | ruff format + auto-fix                           |
| `just typecheck`    | `mypy src`                                       |
| `just test`         | pytest                                           |
| `just test-cov`     | pytest with coverage                             |
| `just build`        | Build sdist + wheel via `uv build`               |
| `just docs-build`   | Build the MkDocs site (requires `install-all`)   |
| `just check`        | justfile formatting + lint + typecheck + test    |

## License

MIT
