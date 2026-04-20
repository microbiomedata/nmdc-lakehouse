# nmdc-lakehouse

ETL pipeline that extracts [NMDC](https://microbiomedata.org/) data via
[`linkml-store`](https://github.com/linkml/linkml-store) from the NMDC
**MongoDB** (and optionally **PostgreSQL**) backends, flattens the nested
object model described by [`nmdc-schema`](https://github.com/microbiomedata/nmdc-schema),
and writes the results to **lakehouse-ready** formats
(Parquet / Apache Iceberg), including references to the large genomic
sequence and other bulk data files that accompany metadata records.

> Status: **project scaffold** – directory layout, build system, and
> developer tooling only. No ETL logic has been implemented yet.

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
│       ├── sources/        # linkml-store clients (Mongo, Postgres)
│       ├── transforms/     # object-model flattening to tabular form
│       ├── sinks/          # Parquet / Iceberg writers
│       ├── io/             # large data-file handling
│       └── jobs/           # ETL job definitions & registry
└── tests/
```

### Module responsibilities

| Package                       | Purpose                                                                 |
|-------------------------------|-------------------------------------------------------------------------|
| `nmdc_lakehouse.sources`      | Retrieve NMDC records via `linkml-store` (Mongo / Postgres handles).    |
| `nmdc_lakehouse.transforms`   | Flatten the nested LinkML object model into tabular / relational form.  |
| `nmdc_lakehouse.sinks`        | Serialize flattened records to Parquet and Iceberg tables.              |
| `nmdc_lakehouse.io`           | Stage & reference large genomic / bulk data files alongside metadata.   |
| `nmdc_lakehouse.jobs`         | Declarative ETL jobs composed from a source → transform → sink pipeline.|
| `nmdc_lakehouse.cli`          | Click-based CLI that dispatches to registered jobs.                     |

## Requirements

- Python ≥ 3.10
- [`uv`](https://docs.astral.sh/uv/) for environment & dependency management
- [`just`](https://just.systems/) for task running
- Access to an NMDC MongoDB instance (and optionally PostgreSQL) for
  anything beyond unit tests

## Getting started

```bash
# Install uv and just first, then:
just install        # uv sync --extra dev
just test           # run unit tests
just lint           # ruff check + format --check
just cli --help     # show the CLI
```

## Configuration

Database connection settings are read from the environment. At minimum:

```bash
# Mongo (via linkml-store)
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_DB=nmdc
export MONGO_USERNAME=admin
export MONGO_PASSWORD=...

# Postgres (optional)
export POSTGRES_DSN=postgresql://user:pass@host:5432/nmdc

# Lakehouse output
export LAKEHOUSE_ROOT=/path/to/lakehouse   # local dir or s3://... uri
```

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
| `just lock`         | Refresh `uv.lock`                                |
| `just lint`         | ruff check + format check                        |
| `just format`       | ruff format + auto-fix                           |
| `just typecheck`    | `mypy src`                                       |
| `just test`         | pytest                                           |
| `just test-cov`     | pytest with coverage                             |
| `just build`        | Build sdist + wheel via `uv build`               |
| `just check`        | lint + typecheck + test                          |

## License

MIT
