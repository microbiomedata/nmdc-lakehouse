# Architecture

`nmdc-lakehouse` follows a classic ETL shape with three replaceable layers:

```
  ┌──────────┐    ┌────────────┐    ┌────────┐
  │ sources  │ -> │ transforms │ -> │ sinks  │
  └──────────┘    └────────────┘    └────────┘
      ^                                  │
      │                                  v
 linkml-store                     Parquet / Iceberg
 (Mongo, Postgres)                (+ BigDataRef for
                                   genomic payloads)
```

## Sources (`nmdc_lakehouse.sources`)

Thin adapters around `linkml-store`. A source yields NMDC records as
dicts, regardless of the underlying backend (MongoDB today, PostgreSQL
optionally).

## Transforms (`nmdc_lakehouse.transforms`)

Schema-driven flattening of the NMDC / LinkML object model. The LinkML
`SchemaView` determines how nested, multivalued, and inlined slots are
unrolled into one or more tabular outputs.

## Sinks (`nmdc_lakehouse.sinks`)

Write the flattened output to lakehouse formats:

- `ParquetSink` — local or object-store partitioned Parquet datasets.
- `IcebergSink` — append rows to Apache Iceberg tables via a catalog.

## I/O for big data files (`nmdc_lakehouse.io`)

Genomic sequences and other bulk payloads are **not** inlined. They are
represented by `BigDataRef` records (URI, size, checksum, media type)
that live alongside the flattened metadata rows.

## Jobs and the runner (`nmdc_lakehouse.jobs`, `nmdc_lakehouse.cli`)

A `Job` composes a source, zero or more transforms, and a sink. Jobs are
registered in `nmdc_lakehouse.jobs.registry` and dispatched either by the
built-in Click CLI (`nmdc-lakehouse run-job <name>`) or by an external
orchestrator. The boundary is intentionally thin so that swapping
runners (Dagster / Prefect / Snakemake) does not affect the core modules.
