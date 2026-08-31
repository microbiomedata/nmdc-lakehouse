# Original design and current conformance

This page records what the project originally set out to build, how the
maintained implementation compares, and which issues own the remaining work.
It is a status and decision record, not a substitute for the detailed
[architecture](architecture.md).

## Sources and attribution

Two primary sources define the historical baseline:

- The [initial repository scaffold](https://github.com/microbiomedata/nmdc-lakehouse/commit/36dd8918c4ab67965ed1e8a4e0df5ac652d57068)
  was created by Sierra Moxon. It introduced replaceable sources, transforms,
  sinks, jobs, and a thin runner.
  It described `linkml-store` access to MongoDB and optional PostgreSQL,
  `SchemaView`-directed flattening, Parquet and Iceberg targets, and external
  references for large payloads.
- The [April 2026 NMDC Lakehouse squad proposal](https://docs.google.com/document/d/1IdvTk2Fe0r3lLDEYzEtNmnn6H3-u-x4DSnJumX-4zfw/edit)
  named Sierra as squad lead and set three outcomes: a
  `nmdc-schema → transformation specification → nmdc-schema-flattened`
  pipeline, an automated TSV or Parquet dump, and stewardship for provenance,
  refreshes, upstream changes, and deprecations.

Sierra then imported the external-metadata-awareness scripts as a
[working baseline](https://github.com/microbiomedata/nmdc-lakehouse/commit/d8d167a6ed7486499964b7d4d12e7105925e3e0c).
Their historical README explicitly called for a second phase that moved their
MongoDB intermediate into the package's source, transform, sink, and job
modules and wrote Parquet or Iceberg directly. Later normalization rules,
namespace policy, performance exceptions, and publication procedures are
project decisions informed by experience. They should not be attributed to
Sierra unless a primary source says otherwise.

## MongoDB metadata path

The maintained path follows the original structure:

```text
locked nmdc-schema ──► schema-defined MongoDB collections
                              │
                    linkml-store source
                              │
                    SchemaView projection
                              │
             primary tables and normalized side tables
                              │
                  typed local Parquet artifacts
```

Collection selection and target shapes both come from the installed,
locked NMDC schema. The ordinary source adapter streams read-only MongoDB
records through `linkml-store`. The maintained path writes no transformed
records back to MongoDB and requires no flattened MongoDB, DuckDB, or CSV
intermediate. This direct path is the intended replacement for the copied EMA
workflow, whose retirement is tracked in
[#27](https://github.com/microbiomedata/nmdc-lakehouse/issues/27).

`functional_annotation_agg` uses a read-only raw PyMongo cursor while retaining
the same schema-derived Arrow and Parquet path. This is an intentional,
measured exception: [#48](https://github.com/microbiomedata/nmdc-lakehouse/issues/48)
recorded about 30,000 rows per second through PyMongo versus about 34 through
the then-current `linkml-store` iterator. It should remain an exception that is
re-benchmarked, not a second transformation design.

## Conformance status

| Original commitment | Current implementation | Status and owner |
|---|---|---|
| Replaceable source → transform → sink layers | Package boundaries and job composition follow the scaffold. | Implemented |
| Schema-defined MongoDB scope | `Database.slots` determines registered collection jobs. | Implemented; make scope changes reviewable in [#186](https://github.com/microbiomedata/nmdc-lakehouse/issues/186) |
| Schema-directed flattening | `SchemaView` drives primary columns, arrays, and side tables. | Implemented as projection; loss detection remains [#129](https://github.com/microbiomedata/nmdc-lakehouse/issues/129) |
| Versioned transformation specification | Mapping rules live in tested Python beside the schema generator. | Missing as a separate artifact; [#14](https://github.com/microbiomedata/nmdc-lakehouse/issues/14) tracks a `linkml-map` specification |
| `nmdc-schema-flattened` target | `flatten_database_schema()` generates a LinkML target schema. | Implemented on demand; publication and snapshot coupling remain [#110](https://github.com/microbiomedata/nmdc-lakehouse/issues/110) and [#135](https://github.com/microbiomedata/nmdc-lakehouse/issues/135) |
| Automated TSV or Parquet dump | One command streams schema-defined collections to local Parquet. | Implemented for manual runs; each collection stages and promotes its owned file set with rollback, while the full snapshot completion contract is [#206](https://github.com/microbiomedata/nmdc-lakehouse/issues/206) |
| Lakehouse-ready managed tables | Parquet is the interchange layer; Iceberg and BERDL publication are external or incomplete. | Partial; publication work remains, tracked by [#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51). The sink format was decided in [#10](https://github.com/microbiomedata/nmdc-lakehouse/issues/10) and the output consolidation done in [#50](https://github.com/microbiomedata/nmdc-lakehouse/issues/50), both closed |
| Provenance and refresh visibility | Human-readable logs and manual run policy exist. | Partial; structured measurements are [#189](https://github.com/microbiomedata/nmdc-lakehouse/issues/189), reproducible manifests are [#206](https://github.com/microbiomedata/nmdc-lakehouse/issues/206), and update semantics are [#147](https://github.com/microbiomedata/nmdc-lakehouse/issues/147) |
| Upstream-change and deprecation stewardship | Risks and desired semantics are documented in issues. | Designed but not implemented end to end; [#129](https://github.com/microbiomedata/nmdc-lakehouse/issues/129) and [#147](https://github.com/microbiomedata/nmdc-lakehouse/issues/147) own the main gaps |

## Later target-model decisions

The original design required a flattened schema but did not prescribe one
relational normalization strategy. This project now uses an array-native,
selectively normalized model:

- primitive lists stay as Parquet arrays;
- multivalued references remain arrays and also receive junction tables;
- multivalued embedded objects receive child tables;
- common single-valued embedded value objects become columns on the parent.

The evaluation in [#124](https://github.com/microbiomedata/nmdc-lakehouse/issues/124)
found that LinkML's `RelationalModelTransformer` instead targets full relational
normalization. It creates tables for primitive lists and single-valued embedded
objects. That is useful for conventional relational databases but does not
match the established Parquet, Spark, and DuckDB query model here. The project
therefore did not replace its schema generator wholesale. This does not remove
the original requirement for an explicit mapping specification.

## Meaning of "dump complete"

A successful local command is a working export, not yet a complete managed
snapshot. The stronger outcome requires all of the following:

1. Explicit collection scope and exclusions.
2. Observable projection loss or strict failure.
3. Atomic primary and side-table publication.
4. A manifest tying files to source schema, target schema, mapping, software,
   row counts, sizes, checksums, and completion state.
5. Managed-table publication and verification.
6. A declared refresh, correction, and schema-evolution policy.

This distinction prevents an operational convenience from being mistaken for
the completed squad outcome.

## Other data responsibilities

The scaffold also reserved handling for large genomic and other bulk payloads.
The current architecture makes the boundary more explicit:

- MongoDB metadata belongs in `nmdc_metadata`.
- Experimental-result files referenced by `data_object_set` belong in
  `nmdc_results` after type-specific conversion.
- External vocabularies and other reference tables belong in `nmdc_ref_data`.

Only the MongoDB metadata conversion is a maintained package job today.
Experimental-result conversion remains prototype or manual work tracked in
[#130](https://github.com/microbiomedata/nmdc-lakehouse/issues/130), with
versioned source, target, and mapping contracts tracked in
[#146](https://github.com/microbiomedata/nmdc-lakehouse/issues/146).

## Maintenance rule

Update this page when a linked issue changes a status in the table. Preserve
the distinction between historical evidence and later project decisions, link
new claims to their source, and keep implementation detail in the owning issue
or architecture section.
