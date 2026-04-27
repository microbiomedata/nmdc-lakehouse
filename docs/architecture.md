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

## Data taxonomy — what this pipeline covers

NMDC data falls into four categories with different loading strategies:

**MongoDB metadata** (scope of this pipeline)
The 17 schema-specified collections (`biosample_set`, `study_set`, `data_generation_set`, etc.)
stored in the NMDC MongoDB instance. These are schema-validated, authoritative, and bounded
in size (largest is `functional_annotation_agg` at ~54M rows). `nmdc-lakehouse` owns this
path: MongoDB → Parquet → BERDL Silver.

**Derived aggregates** (gray zone — already loaded, but not ground truth)
`functional_annotation_agg` lives in MongoDB but is a pre-aggregated summary of GFF file
content. It is a query convenience layer. The per-gene detail lives in NERSC files; the
aggregate is one row per (workflow run, function term). Loading it via this pipeline is
correct, but users should know it is not the source of record.

**Workflow output files** (out of scope for this pipeline today)
NERSC files under `/global/cfs/cdirs/m3408/gsharing/`: GFF annotations, GTDB-tk TSVs,
CheckM TSVs, FAA sequences, MAG bin ZIPs, etc. These are referenced by `data_object_set`
URLs but not loaded by this pipeline. A separate loading mechanism is needed (see issue #57).
`data_object_set` records act as the index/manifest for these files.

**Reference data** (out of scope — lives in other tenants)
KEGG, COG, GTDB taxonomy reference tables. Currently in `nmdc_arkin` (Gazi's tenant) with
no refresh path. Not part of the NMDC data model; not owned by this pipeline.

## Normalization decisions — primary tables vs side tables

Every multivalued slot in the NMDC schema falls into one of three categories,
each handled differently:

### Scalar multivalued slots
Simple lists of primitive values (`alternative_identifiers`, `analysis_type`,
`funding_sources`, `tillage`, etc.). In the primary flat table these are
**pipe-joined into a single string column** (`"a|b|c"`). A side table (junction
table with `parent_id` + value) is also generated for any slot that has data.

**Open question:** The pipe-joined column and the junction table are redundant.
The current default emits both. A `LAKEHOUSE_SCALAR_MV_MODE` env var is planned
to let callers choose `concatenate` (primary table only, no scalar side tables —
reduces 148 → 78 side tables), `tables` (both), or `split` (junction table only,
drop the pipe-joined column from the primary table).

### Ref-class multivalued slots
Lists of references to other NMDC objects (`associated_studies`, `has_input`,
`has_output`, `instrument_used`, etc.). These are true M:M relationships. They
are pipe-joined in the primary flat table **and** emitted as junction side tables
(`parent_id` + foreign key string). The side table is the correct relational form
for joins; the pipe-joined column is a convenience for simple string searches.

### Inlined multivalued slots
Lists of embedded objects (`mags_list`, `chem_administration`, `organism_count`,
`agrochem_addition`, etc.). These cannot be represented in the primary flat table
without data loss. Each becomes a **child side table** (flattened object rows with
`parent_id`). There is no redundancy — the side table is the only representation.

### Side table naming
All side tables follow the pattern `{collection}_{slot_name}`, e.g.
`biosample_set_associated_studies`, `workflow_execution_set_mags_list`.
Only slots that have at least one populated record are written; empty side
tables are silently skipped at runtime.

## Jobs and the runner (`nmdc_lakehouse.jobs`, `nmdc_lakehouse.cli`)

A `Job` composes a source, zero or more transforms, and a sink. Jobs are
registered in `nmdc_lakehouse.jobs.registry` and dispatched either by the
built-in Click CLI (`nmdc-lakehouse run-job <name>`) or by an external
orchestrator. The boundary is intentionally thin so that swapping
runners (Dagster / Prefect / Snakemake) does not affect the core modules.
