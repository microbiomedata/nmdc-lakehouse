# nmdc-lakehouse

ETL pipeline that extracts [NMDC](https://microbiomedata.org/) data via
[`linkml-store`](https://github.com/linkml/linkml-store) from the NMDC
**MongoDB** (and optionally **PostgreSQL**) backends, flattens the nested
object model described by [`nmdc-schema`](https://github.com/microbiomedata/nmdc-schema),
and writes the results to **lakehouse-ready** formats
(Parquet / Apache Iceberg), including references to the large genomic
sequence and other bulk data files that accompany metadata records.

> Status: **active development** – core ETL pipeline is functional;
> running against NMDC production MongoDB via GCP SSH tunnel.

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
| `MONGO_DIRECT_CONNECTION` | `false` | Set `true` when using the SSH tunnel |
| `LAKEHOUSE_ROOT` | `./lakehouse` | Local path or `s3://` URI |

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
