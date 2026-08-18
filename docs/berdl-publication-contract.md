# BERDL publication and replacement contract

This document defines the safety and metadata contract for publishing a complete
`nmdc-lakehouse` snapshot into BERDL. It is independent of a specific managed-table
provider. Discover the live provider and catalog at run time rather than inferring
them from an older runbook.

This is a design and operating contract, not an implemented one-command workflow.
The current manual procedure is in [the BERDL upload guide](berdl-upload.md), and
automation is tracked in
[#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51) and
[#136](https://github.com/microbiomedata/nmdc-lakehouse/issues/136).

## Terms and layers

BERDL and Spark use some words differently from object stores and data registries.
The following meanings apply in this repository.

| Term | Meaning here | NMDC example |
|---|---|---|
| Tenant or organization | Governance and access-control boundary | `nmdc` |
| Catalog | Service that resolves managed namespaces and tables | Discover at run time; the live 2026-08-18 inventory reported `spark_catalog` |
| Namespace, database, or schema | Query-visible group of managed tables; Spark treats database and schema as synonyms | `nmdc_metadata`, `nmdc_results`, `nmdc_ref_data` |
| Bronze dataset | Versioned or staged Parquet objects and supporting artifacts under an object-store prefix | An NMDC metadata snapshot prefix |
| Managed or Silver table | Catalog-registered, queryable table built from published artifacts | `nmdc_metadata.biosample_set` |
| Column | Typed field within a managed table | `id`, `type`, `env_broad_scale_term_id` |
| Registry record | Dataset-level discovery entry for people and tools; not the table-schema source of truth | A BER data registry entry for `nmdc_metadata` |

A namespace and a Bronze dataset are not the same object. A dataset may provide
the artifacts used to build many managed tables, while the namespace is their
query-visible grouping.

## Data and metadata artifacts

Publication must keep these concerns distinct and link them through stable
identifiers.

| Artifact | Purpose | Source of truth |
|---|---|---|
| Parquet files | Typed table data and portable physical schemas | One immutable local snapshot |
| Target LinkML projection | Complete logical table topology, including side tables | Generated and published by [#110](https://github.com/microbiomedata/nmdc-lakehouse/issues/110) |
| Portable Parquet metadata | Table and field descriptions plus stable schema identifiers in Parquet footers | [#202](https://github.com/microbiomedata/nmdc-lakehouse/issues/202) |
| Metadata content bundle | Reviewable namespace, table, and column descriptions and reviewed overrides | [#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120) |
| Snapshot manifest | Completeness, checksums, row counts, schema and software provenance, and source identity | [#133](https://github.com/microbiomedata/nmdc-lakehouse/issues/133) and [#135](https://github.com/microbiomedata/nmdc-lakehouse/issues/135) |
| Catalog metadata | Namespace properties and managed-table or column comments | Applied idempotently through [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) |
| Registry record | Dataset ownership, access, update policy, keywords, and documentation links | Projection defined by [#52](https://github.com/microbiomedata/nmdc-lakehouse/issues/52) |

The structured ETL metrics JSON records performance and resource use. It is not a
snapshot manifest because it does not currently contain file checksums, a complete
logical schema artifact, or publication lineage.

## Description precedence

Apply descriptions in this order so stale deployed prose cannot silently override
a newer schema:

1. Use descriptions from the exact source `nmdc-schema` version as the machine
   baseline.
2. Add deterministic flattening annotations for references, nested values,
   polymorphic-dispatch columns, and side tables.
3. Apply reviewed, version-controlled human overrides from the metadata content
   bundle. Each override records its rationale and source.
4. Import existing catalog comments only as candidates for review. Do not assume
   deployed text is newer or more accurate than version-controlled content.
5. Generate the dataset-level registry record from approved metadata and manifest
   fields. Do not use it as the source for table or column text.

The same approved descriptions should reach the target LinkML artifact, portable
Parquet metadata, and catalog comments. A verification report identifies any
intentional differences.

## Table disposition plan

Compare the candidate and live table sets before upload. Every table in their union
receives exactly one disposition:

- **replace**: candidate Parquet is the complete authoritative replacement;
- **add**: candidate table does not yet exist and is safe to introduce;
- **preserve**: live content has no verified candidate replacement;
- **rebuild**: derived content must be recreated from the promoted source tables;
- **retire**: removal has separate evidence, approval, and recovery instructions.

No publication command may infer retirement merely because a live table is absent
from the candidate. In particular, `functional_annotation_agg` remains preserved
until a verified replacement exists. `graph_edges` and
`biosample_to_workflow_run` are derived products: the plan names their rebuild
procedure and order, or preserves them when a safe rebuild is unavailable.

The 2026-08-18 candidate/live comparison is an example of why classification is
necessary, not a permanent allowlist. The schema and deployed table sets will
change.

## Staged workflow

### 1. Inventory without mutation

Record:

- candidate snapshot path, manifest identity, files, checksums, Parquet footer row
  counts, and physical schemas;
- live catalog, namespace, provider, table locations, tables, row counts, schemas,
  comments, properties, and current owner;
- source `nmdc-schema`, target schema, mapping, package, and Git identities;
- dependent views and derived tables; and
- the available provider-specific recovery mechanism.

Do not print credentials, connection strings, or production records.

### 2. Produce and approve the disposition plan

Join the candidate and live inventories, assign one disposition to every table,
and print all additions, replacements, preserves, rebuilds, and retirements. The
plan also states the staging destination, promotion mechanism, validation queries,
and rollback procedure.

This is the first mandatory no-mutation checkpoint. Upload does not begin until a
human has reviewed the complete plan.

### 3. Establish rollback evidence

Before changing Bronze objects or managed tables:

- capture catalog DDL or equivalent reconstruction information;
- record provider-specific version or snapshot identifiers when supported;
- retain the previous publication manifest and immutable source artifacts;
- prove the proposed recovery operation against a staging table; and
- state what cannot be rolled back automatically.

An object-store copy alone is not proof that catalog state, comments, properties,
or derived tables can be restored.

### 4. Upload and load into staging

Upload the manifest-defined artifact set to a new snapshot-specific Bronze prefix.
Load it into a staging namespace or another isolated destination supported by
BERDL. Do not overwrite the canonical namespace during this phase.

Apply the approved metadata bundle to staging and rebuild staging copies of every
table classified as **rebuild**.

### 5. Validate staging

At minimum, verify:

- exact table set and disposition coverage;
- source Parquet footer rows against staging managed-table rows;
- physical types, nullability, arrays, and schema fingerprints;
- required identifier presence and uniqueness where declared by the target schema;
- side-table `parent_id` relationships and other declared references;
- namespace, table, and expected column-comment coverage;
- source, target, mapping, software, and snapshot provenance;
- representative scientific joins, including the biosample-to-workflow path; and
- preservation or successful reconstruction of live-only and derived products.

Validation failures leave the canonical namespace untouched.

### 6. Approve and perform promotion

Present the validated plan, staging report, exact canonical objects to be changed,
and rollback instructions. This is the second mandatory human checkpoint.

Promotion uses the current BERDL-supported catalog operation. Do not assume that
namespace rename, table replacement, or a multi-table transaction is atomic without
verifying that behavior for the discovered provider. Never issue a drop-all command.

### 7. Verify and record the published state

Repeat the staging checks against the canonical namespace. Record the final
catalog/provider, table versions or snapshots, counts, metadata coverage, derived
table status, registry update, timestamps, and operator-visible outcome in the
publication record. A partial promotion is a failure until recovered or explicitly
accepted and documented.

## Automation boundary

The eventual command from #136 should have a non-mutating default or explicit
`--dry-run` mode. Its plan should be serializable and reviewable before a separate,
explicit promotion action. Tests cover plan construction, disposition completeness,
unsafe destination rejection, verification failures, and rollback command
construction without BERDL credentials.

Environment setup and transport helpers may live in BERIL or KBase repositories.
The snapshot, metadata, disposition, validation, and publication contracts belong
here so they remain testable without a live cluster.

## Related documentation

- [Architecture](architecture.md)
- [MongoDB connection and snapshot output](mongodb-connection.md)
- [BERDL upload guide](berdl-upload.md)
- [BERDL metadata shaping](berdl-metadata-shaping.md)
- [NMDC metadata tables](nmdc_metadata_tables.md)
- [Biosample-to-workflow derived table](biosample_to_workflow_run.md)
