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

NMDC data falls into four categories. All four are now in scope and, for the
BERDL deployment target, land in one of three BERDL Delta namespaces. The
sinks (`ParquetSink` / `IcebergSink`) can run outside BERDL — the namespace
split below is the BERDL-side convention, not a property of every output
the pipeline can produce.

### Namespace policy

| Namespace | Contents | Source |
|---|---|---|
| `nmdc_metadata` | Schema-driven Silver tables from the 17 NMDC MongoDB collections. | NMDC MongoDB → linkml-store flattening (with `functional_annotation_agg` as a special-case raw-pymongo loader for performance — see #48). |
| `nmdc_results` | Tables derived from workflow output files (per-gene annotations, taxonomy summaries). | NERSC files referenced by `data_object_set` URLs |
| `nmdc_ref_data` | Reference / ontology tables loaded from external sources. | Pfam terms, GO/EC where redistributable, etc. KEGG term names are excluded — see #103 (KEGG redistribution license). |

`nmdc_arkin` (Gazi's tenant) and other non-NMDC tenants are **read-only** for
this pipeline: we may query them via `spark.sql()` to understand what already
exists, but we never write to them, and they are not used in user-facing
query examples produced by this pipeline.

### Categories

**MongoDB metadata** → `nmdc_metadata`
The 17 schema-specified collections (`biosample_set`, `study_set`,
`data_generation_set`, etc.). Schema-validated, authoritative, bounded in size
(largest is `functional_annotation_agg` at ~54M rows). MongoDB → Parquet →
BERDL Silver via the schema-driven flattener.

**Derived aggregates** → `nmdc_metadata` (gray zone — loaded, but not ground truth)
`functional_annotation_agg` lives in MongoDB but is a pre-aggregated summary of
GFF file content. It is a query convenience layer; the per-gene detail lives in
the workflow output files. Loading it here is correct, but users should know it
is not the source of record.

**Workflow output files** → `nmdc_results`
NERSC files referenced by `data_object_set` URLs: per-gene annotation GFFs and
TSVs, taxonomy summaries (GOTTCHA2 / GTDB-tk / CheckM / Kraken2 reports),
annotation statistics. Loaded via a three-stage cache-then-parse pattern
(fetch manifest in Spark → multi-hour standalone download → streaming parse to
Parquet). The `data_object_type` field on `data_object_set` rows is the
permissible value used to dispatch a loader; that value lines up with
`FileTypeEnum` in nmdc-schema.

**Reference data** → `nmdc_ref_data`
External term and hierarchy tables loaded to support joins from `nmdc_results`
back to canonical IDs (e.g. `pfam_terms` joins to `nmdc_results.pfam_annotation_gff`
on `pfam_id`). Not part of the NMDC data model — owned by this pipeline only
in the sense that we maintain the loader.

## Normalization decisions — primary tables vs side tables

Every multivalued slot in the NMDC schema falls into one of three categories,
each handled differently:

### Scalar multivalued slots
Simple lists of primitive values (`alternative_identifiers`, `analysis_type`,
`funding_sources`, `tillage`, etc.). In the primary flat table these are stored as
**native Parquet ARRAY columns** (`pa.list_(element_type)`). No scalar junction
side table is generated.

### Ref-class multivalued slots
Lists of references to other NMDC objects (`associated_studies`, `has_input`,
`has_output`, `instrument_used`, etc.). These are true M:M relationships. They
are stored as **native ARRAY columns** in the primary flat table and **also**
emitted as junction side tables (`parent_id` + foreign-key string). The side
table is the correct relational form for joins; the ARRAY column supports simple
`array_contains()` lookups without a join.

### Inlined multivalued slots
Lists of embedded objects (`mags_list`, `chem_administration`, `organism_count`,
`agrochem_addition`, etc.). These cannot be represented in the primary flat table
without data loss. Each becomes a **child side table** (flattened object rows with
`parent_id`). There is no redundancy — the side table is the only representation.

### Recursive side tables
Six cases in the current NMDC schema have inlined child classes that themselves
contain multivalued slots. Those child slots are stored as ARRAY columns inside
the side table row (same rule as the primary table):

| Parent side table | Child class | Child ARRAY column |
|---|---|---|
| `workflow_execution_set_mags_list` | `MagBin` | `members_id` |
| `study_set_has_credit_associations` | `CreditAssociation` | `applied_roles` |
| `workflow_execution_set_has_metabolite_identifications` | `MetaboliteIdentification` | `alternative_identifiers` |
| `configuration_set_ordered_mobile_phases` | `MobilePhaseSegment` | `substances_used` |
| `material_processing_set_ordered_mobile_phases` | `MobilePhaseSegment` | `substances_used` |
| `study_set_protocol_link` | `Protocol` | `analysis_type` |

### Side table naming
All side tables follow the pattern `{collection}_{slot_name}`, e.g.
`biosample_set_associated_studies`, `workflow_execution_set_mags_list`.
Only slots that have at least one populated record are written; empty side
tables are silently skipped at runtime.

### Query engine compatibility
All target systems support Parquet native ARRAY types:

| System | ARRAY support | Unnest syntax |
|---|---|---|
| DuckDB | ✅ native | `UNNEST()`, `array_contains()` |
| Spark / Delta (BERDL) | ✅ native | `EXPLODE()`, `array_contains()` |
| Dremio (JGI) | ✅ native | `FLATTEN()` |
| Parquet (file format) | ✅ `pa.list_()` | — |

## Jobs and the runner (`nmdc_lakehouse.jobs`, `nmdc_lakehouse.cli`)

A `Job` composes a source, zero or more transforms, and a sink. Jobs are
registered in `nmdc_lakehouse.jobs.registry` and dispatched either by the
built-in Click CLI (`nmdc-lakehouse run-job <name>`) or by an external
orchestrator. The boundary is intentionally thin so that swapping
runners (Dagster / Prefect / Snakemake) does not affect the core modules.
