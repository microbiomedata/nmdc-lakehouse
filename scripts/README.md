# scripts/

Direct copies of the NMDC flatten/export pipeline from
[microbiomedata/external-metadata-awareness](https://github.com/microbiomedata/external-metadata-awareness).
Kept verbatim (apart from one import path fix) so we have a working baseline
before re-architecting into `src/nmdc_lakehouse/{sources,transforms,sinks,jobs}/`.

## Contents

`scripts/python/`
- `flatten_nmdc_collections.py` — reads NMDC MongoDB collections (`biosample_set`,
  `study_set`), flattens nested structures (underscore-joined field names,
  pipe-joined arrays), enriches environmental triad CURIEs via `oaklib`
  (envo/pato/uberon), and writes `flattened_*` collections back into MongoDB.
- `export_duckdb_to_parquet.py` — exports every table in a DuckDB file to
  an individual Parquet file (ZSTD).
- `export_flattened_gold_to_csv.py` — generic `flattened_*`-prefix collection
  → CSV exporter. Name mentions GOLD for historical reasons; works for any
  database whose flattened collections follow the `flattened_` prefix.
- `mongodb_connection.py` — `get_mongo_client()` helper used by the CSV
  exporter. Supports merging credentials from an env file into a URI.

## Upstream provenance

| file | upstream path |
| --- | --- |
| `flatten_nmdc_collections.py` | `external_metadata_awareness/flatten_nmdc_collections.py` |
| `export_duckdb_to_parquet.py` | `external_metadata_awareness/export_duckdb_to_parquet.py` |
| `export_flattened_gold_to_csv.py` | `external_metadata_awareness/export_flattened_gold_to_csv.py` |
| `mongodb_connection.py` | `external_metadata_awareness/mongodb_connection.py` |

Only modification from upstream: the `from external_metadata_awareness.mongodb_connection import get_mongo_client`
line in `export_flattened_gold_to_csv.py` was changed to `from mongodb_connection import get_mongo_client`
so the scripts work when run from this directory without the upstream package
installed.

## Dependencies

Already covered by the project dependencies in `pyproject.toml`
(`uv sync` is enough): `pymongo`, `duckdb`, `pandas`, `pyarrow`, `click`,
`tqdm`, `linkml-runtime`, `oaklib`, `python-dotenv`.

External CLI tools required by the justfile recipes (not Python deps):
- `mongoexport` / `mongosh` — MongoDB Database Tools
- `duckdb` — DuckDB CLI

## Prerequisites

- A local MongoDB instance with the NMDC collections loaded (e.g. via
  `mongorestore` from an NMDC dump). Override the target with the
  `MONGO_URI` environment variable.
- For `flatten-nmdc-auth`: a `local/.env.ncbi-loadbalancer.27778` file
  providing `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_HOST`, `MONGO_PORT`,
  `DEST_MONGO_DB`.

## Usage (via justfile)

```bash
# Flatten NMDC collections inside MongoDB
just flatten-nmdc

# Flatten -> DuckDB -> Parquet (suitable for lakehouse ingestion)
just export-nmdc-parquet

# Full pipeline: flatten + DuckDB + Parquet + biosample CSV
just flatten-and-export-nmdc
```

Outputs default to `./local/nmdc_export/` (DuckDB, `parquet/`, `csv/`).
Override via `NMDC_EXPORT_DIR`, `NMDC_PARQUET_DIR`, `NMDC_CSV_DIR`,
`NMDC_DUCKDB_FILE`.

## Known rough edges (Phase 1 is a copy, not a refactor)

- `export_flattened_gold_to_csv.py` uses `print()` everywhere instead of
  logging, and uses `skip`/`limit` pagination (slow on large collections).
- `flatten_nmdc_collections.py` writes results back to MongoDB as
  intermediate `flattened_*` collections rather than going straight to
  Parquet — DuckDB/Parquet export is a separate step.
- `mongodb_connection.py` expects `MONGO_USER` (not `MONGO_USERNAME`) in the
  env file when merging credentials into a URI.
- Scripts are invoked as standalone files (`uv run python scripts/python/...`)
  rather than registered entry points.

Phase 2 will split these into `sources/`, `transforms/`, `sinks/`, and
`jobs/` modules under `src/nmdc_lakehouse/`, replacing the mongo->mongo
intermediate with direct Parquet/Iceberg writes.
