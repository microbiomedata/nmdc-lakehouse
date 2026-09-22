# Architecture

The [original design and current conformance](original-design-conformance.md)
page distinguishes the initial scaffold created by Sierra Moxon and the squad commitments
from later project decisions and tracks the remaining outcome gaps.

`nmdc-lakehouse` follows a classic ETL shape with three replaceable layers:

```
  ┌──────────┐    ┌────────────┐    ┌────────┐
  │ sources  │ -> │ transforms │ -> │ sinks  │
  └──────────┘    └────────────┘    └────────┘
      ^                                  │
      │                                  v
 linkml-store                     local Parquet
 (MongoDB)                        (+ BigDataRef for
                                   genomic payloads)
```

## Sources (`nmdc_lakehouse.sources`)

The implemented `linkml-store` adapter yields NMDC MongoDB records as
dictionaries. `PostgresSource` is a planned interface whose record iterator
currently raises `NotImplementedError`.

### Relationship to `linkml-store`

`linkml-store` is a replaceable source adapter, not the pipeline or its output
format. Today `MongoSource` uses it to attach the NMDC MongoDB database, select
a collection, estimate its size, and stream records. The high-volume
`functional_annotation_agg` job retains a raw-`pymongo` path that predates the
Mongo cursor fix in `linkml-store` 0.3.2 and should be re-benchmarked.

After records cross the `Source` protocol, this repository owns the behavior:
`nmdc-schema` and `linkml-runtime` drive projection, and PyArrow writes local
Parquet. `linkml-store` does not write these artifacts, publish BERDL tables, or
write transformed records back to NMDC MongoDB. The BERDL dataset name
`nmdc_linkml_store` is a historical dataset label, not a statement about its
publication implementation.

Long term, keep the `Source` protocol backend-neutral and use `linkml-store`
where its backend semantics and performance fit. Keep snapshot production and
destination publication independent so BERDL, another managed lakehouse, an
object store, or a local query environment can consume the same portable
artifacts without depending on how source records were read.

## Transforms (`nmdc_lakehouse_schema.transforms`)

Schema-directed flattening of the NMDC / LinkML object model. The LinkML
`SchemaView` determines how nested, multivalued, and inlined slots are
projected into one or more tabular outputs and supplies output types. This is
not full LinkML validation of every input record.

The canonical logical target is the flattened schema shipped by the
`nmdc-lakehouse-schema` package, read from
`nmdc_lakehouse_schema/schema/nmdc_schema_flattened.yaml`. It is generated from
the locked NMDC `Database` model and contains primary projections plus the
supported collection-level junction and child-table classes. Its class annotations record
source and table identities; the producing loader is not recorded in the schema, it
is per-write provenance in the Parquet footer and snapshot manifest. Generation,
versioning, and drift checking live in that package. This repository consumes the
artifact and, at validation time, asserts the installed `nmdc-schema` matches the
source version the artifact was built from.

The target copies source enum, type, and prefix definitions needed by retained
slot ranges, making it standalone rather than dependent on an undeclared NMDC
import. It preserves upstream permissible values and prefixes exactly. LinkML
may warn about their naming or canonical-prefix conventions; those warnings
belong upstream and are not rewritten in this generated projection. Target
classes reject undeclared ranges and multiple class identifiers in repository
tests. A source `type` remains a required value column where the source model
requires it, but it does not designate the generated flat class because its
values continue to identify source NMDC classes.

Each snapshot manifest maps an emitted table to a `target_class` in this
schema. The schema contains possible topology, while the manifest records the
tables actually emitted for one snapshot. It is a logical LinkML contract, not
an Arrow physical schema, Parquet integrity proof, metadata-description bundle,
or by itself evidence that rows have passed LinkML instance validation.
`validate-target-rows` first verifies the immutable snapshot and then validates
each table against its exact manifested target class. Full mode checks every
row. The default bounded mode checks every row in tables of at most 10,000 rows
and a deterministic 100-row identity/content sample from each larger table.
Its snapshot-bound report states the coverage explicitly and contains only
sanitized rule, path, and count categories rather than source values.

## Sinks (`nmdc_lakehouse.sinks`)

The package currently writes an interchange artifact and reserves its intended
managed-table interface:

- `ParquetSink`, implemented for local files. Each logical primary or side
  table becomes one `{table}.parquet` file containing streamed row groups; it
  is neither a partitioned dataset nor an object-store writer.
- `IcebergSink`, planned. Its `write()` method currently raises
  `NotImplementedError`. Iceberg is one possible destination adapter, not the
  required or preferred publication path.

The portable publication product is a validated Parquet snapshot accompanied
by its target logical schema, footer metadata, reviewable description content,
and snapshot manifest. The
[publication contract](publication-contract.md) defines destination-neutral
staging, validation, metadata granularity, promotion, and rollback principles.
Destination profiles map that contract to platform-specific transport, catalog,
registry, and authorization operations.

## I/O for big data files (`nmdc_lakehouse.io`)

Genomic sequences and other bulk payloads are **not** inlined. They are
represented by `BigDataRef` records (URI, size, checksum, media type)
that live alongside the flattened metadata rows.

## Data taxonomy: what this pipeline covers

The target architecture covers four NMDC data categories. The maintained
package job currently implements MongoDB metadata extraction; result-file and
reference-data loaders remain notebooks or operational scripts. Parquet is the
portable file and interchange layer. A destination may expose the same logical
groups as directories, datasets, namespaces, schemas, databases, or catalogs.
The current provider and its capabilities must be discovered rather than
inferred from an earlier deployment.

### Logical data-group policy

| Logical group | Contents | Source |
|---|---|---|
| `nmdc_metadata` | Schema-driven tables from the 19 NMDC MongoDB collections in the reviewed `Database.slots` snapshot. | NMDC MongoDB → `linkml-store` source adapter → `nmdc_lakehouse_schema.transforms` schema-driven flattening (with `functional_annotation_agg` as a special-case raw-`pymongo` loader for performance; see #48). |
| `nmdc_results` | Tables derived from workflow output files (per-gene annotations, taxonomy summaries). | NERSC files referenced by `data_object_set` URLs |
| `nmdc_ref_data` | Reference / ontology tables loaded from external sources. | Pfam terms, GO/EC where redistributable, etc. KEGG term names are excluded; see #103 (KEGG redistribution license). |

In the BERDL destination profile, these groups are managed Silver namespaces.
`nmdc_arkin` (Gazi's tenant) and other non-NMDC tenants are **read-only** for
this pipeline: we may query them to understand what already exists, but we
never write to them, and they are not used in user-facing query examples
produced by this pipeline.

### Categories

**MongoDB metadata** → `nmdc_metadata`
The 19 schema-specified collections listed in the
[MongoDB connection guide](mongodb-connection.md#maintained-collection-baseline).
They are schema-directed, authoritative, and bounded in size
(largest is `functional_annotation_agg` at ~54M rows). MongoDB → schema-driven
flattening → portable Parquet snapshot → destination publication.

**Derived aggregates** → `nmdc_metadata` (gray zone: loaded, but not ground truth)
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
back to canonical IDs (e.g. `pfam_terms.pfam_id` joins to
`nmdc_results.pfam_annotation_gff.pfam_accession`). Not part of the NMDC data
model, owned by this pipeline only in the sense that we maintain the loader.

## Normalization decisions: primary tables vs side tables

Projection 1.2.0 applies the following rules to slots directly on a collection
record, including inherited and subtype slots. Embedded expansion is bounded;
the rules do not imply recursive normalization or lossless round trips.

### Scalar multivalued slots
Simple lists of primitive or enum values (`alternative_identifiers`,
`analysis_type`, etc.). In the primary flat table these are stored as
**native Parquet ARRAY columns** (`pa.list_(element_type)`). No scalar junction
side table is generated.

### TextValue slots

A slot with the exact range `TextValue` becomes a string column or string array
on the containing row, using `has_raw_value`. For example, `geo_loc_name` is a
string and `host_diet` is a string array in `biosample_set`; neither needs a
helper table. The projection rejects additional populated TextValue content
and invalid raw-value types. Other wrapper classes use the general expansion
rules. See the [rollout guide](source-schema-1124-rollout.md) for compatibility.

### Ref-class multivalued slots
Lists of references to other NMDC objects (`associated_studies`, `has_input`,
`has_output`, `instrument_used`, etc.). These are true M:M relationships. They
are stored as **native ARRAY columns** in the primary flat table and **also**
emitted as junction side tables (`parent_id` + foreign-key string). The side
table is the correct relational form for joins; the ARRAY column supports simple
`array_contains()` lookups without a join.

### Inlined multivalued slots
Lists of embedded non-TextValue objects (`mags_list`, `chem_administration`,
`organism_count`, `agrochem_addition`, etc.) become **child side tables** with
`parent_id` and the supported flattened child fields. The primary table omits
these objects. This does not preserve every input distinction: helper rows have
no occurrence identifier or position column, and empty lists produce no rows.

### Nested multivalued slots and depth limits

Primitive/enum arrays, such as credit associations' `applied_roles`, and visited
TextValue arrays can remain in child rows. Repeated non-TextValue class members
inside embedded objects are omitted; no grandchild helpers are generated.
Nested references are also not uniformly retained. In particular,
`ordered_mobile_phases.substances_used` is omitted in both configuration and
material-processing records, with no JSON fallback. Populated production data
at these paths blocks a complete export in
[schema #21](https://github.com/microbiomedata/nmdc-lakehouse-schema/issues/21).

Generic primary expansion supports at most two single-object edges to a scalar
leaf. Child-row expansion is shallower than its generated schema can describe,
so a declared optional column can still lose a deeper value. The schema
package's [transformation support reference](https://github.com/microbiomedata/nmdc-lakehouse-schema/blob/v0.4.0/docs/transformation-support.md)
details these boundaries. The [rollout preflight](source-schema-1124-rollout.md)
records which inspected paths were populated; target-row validation alone does
not detect content omitted before writing.

### Side table naming
All side tables follow the pattern `{collection}_{slot_name}`, e.g.
`biosample_set_associated_studies`, `workflow_execution_set_mags_list`.
Only slots that have at least one populated record are written; empty side
tables are silently skipped at runtime.

### Query engine compatibility

The Parquet ARRAY representation is known to work with the tested query
engines. This is a format-compatibility
statement, not evidence that this repository connects to every engine:

| System | ARRAY support | Unnest syntax |
|---|---|---|
| DuckDB | ✅ native | `UNNEST()`, `array_contains()` |
| Spark with a compatible managed-table format | ✅ native | `EXPLODE()`, `array_contains()` |
| Parquet (file format) | ✅ `pa.list_()` | n/a |

## Jobs and the runner (`nmdc_lakehouse.jobs`, `nmdc_lakehouse.cli`)

A `Job` composes a source, zero or more transforms, and a sink. Jobs are
registered in `nmdc_lakehouse.jobs.registry` and dispatched either by the
built-in Click CLI (`nmdc-lakehouse run-job <name>`) or by an external
orchestrator. The boundary is intentionally thin so that swapping
runners (Dagster / Prefect / Snakemake) does not affect the core modules.
