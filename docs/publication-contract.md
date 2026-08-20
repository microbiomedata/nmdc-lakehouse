# Portable publication and replacement contract

This document defines the safety and metadata contract for publishing a complete
`nmdc-lakehouse` snapshot to any destination. The portable snapshot is the product;
BERDL, another managed lakehouse, an object store, or a local analytical environment
is a destination profile. No destination is architecturally preferred.

This is a design and operating contract, not an implemented one-command workflow.
The current BERDL-specific manual procedure is in
[the BERDL upload guide](berdl-upload.md), and its automation is tracked in
[#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51) and
[#136](https://github.com/microbiomedata/nmdc-lakehouse/issues/136). Other
destinations may use different transport and catalog operations while satisfying
the same contract.

## Terms and layers

Lakehouses, object stores, query engines, and data registries use overlapping words
differently. The following meanings apply in this repository. A destination profile
maps these concepts to platform-specific terms and marks unsupported levels
explicitly.

| Term | Meaning here | Example representation |
|---|---|---|
| Destination | System or storage location receiving a publication | BERDL, an S3-compatible store, or a local directory |
| Tenant or organization | Optional governance and access-control boundary | NMDC's `nmdc` BERDL tenant |
| Catalog | Optional service that resolves managed namespaces and tables | Spark, Iceberg, Hive, or another catalog discovered at run time |
| Namespace, database, or schema | Optional query-visible group of tables | `nmdc_metadata`, `nmdc_results`, `nmdc_ref_data` |
| Dataset or artifact prefix | Immutable snapshot files and supporting artifacts | A versioned object-store prefix or directory |
| Managed table or query asset | Optional catalog-registered table built from published artifacts | `nmdc_metadata.biosample_set` |
| Column | Typed field within a managed table | `id`, `type`, `env_broad_scale_term_id` |
| Registry record | Optional dataset-level discovery entry; not the table-schema source of truth | A catalog or data-portal entry for the metadata snapshot |

A namespace and a dataset are not the same object. A dataset may provide the
artifacts used to build many managed tables, while the namespace is their
query-visible grouping. A file-only destination can support the dataset without
supporting namespaces, managed tables, or registry records.

## Data and metadata artifacts

Publication must keep these concerns distinct and link them through stable
identifiers.

| Artifact | Purpose | Source of truth |
|---|---|---|
| Parquet files | Typed table data and portable physical schemas | One immutable local snapshot |
| Target LinkML projection | Complete logical table topology, including side tables | Generated and published by [#110](https://github.com/microbiomedata/nmdc-lakehouse/issues/110) |
| Portable Parquet metadata | Table and field descriptions plus stable schema identifiers in Parquet footers | [#202](https://github.com/microbiomedata/nmdc-lakehouse/issues/202) |
| Metadata content bundle | Reviewable namespace, table, and column descriptions and reviewed overrides | [#215](https://github.com/microbiomedata/nmdc-lakehouse/issues/215), split from [#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120) |
| Metadata application plan | Supported and unsupported metadata operations for one explicit staging namespace | [#223](https://github.com/microbiomedata/nmdc-lakehouse/issues/223) |
| Snapshot manifest | Completeness, checksums, row counts, schema and software provenance, and source identity | [#206](https://github.com/microbiomedata/nmdc-lakehouse/issues/206) |
| Target validation report | Full or explicitly bounded LinkML instance-validation evidence for the exact snapshot and target classes | [#224](https://github.com/microbiomedata/nmdc-lakehouse/issues/224) |
| Destination metadata | Namespace or dataset properties and table or column comments when supported | Applied idempotently through [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) |
| Registry record | Dataset ownership, access, update policy, keywords, and documentation links | Projection defined by [#52](https://github.com/microbiomedata/nmdc-lakehouse/issues/52) |

The structured ETL metrics JSON records performance and resource use. It is not a
snapshot manifest because it does not currently contain file checksums, a complete
logical schema artifact, or publication lineage.

Code releases, schemas, snapshots, metadata content, and destination publications
have separate linked identities. The package version and Git commit identify the
producer; schema and mapping identifiers identify the contracts; `snapshot_id`
identifies immutable portable content. Later metadata bundles and destination
publication records reference that snapshot rather than changing its identity.

## Description precedence

Apply descriptions in this order so stale deployed prose cannot silently override
a newer schema:

1. Use descriptions from the exact source `nmdc-schema` version as the machine
   baseline.
2. Add deterministic flattening annotations for references, nested values,
   polymorphic-dispatch columns, and side tables.
3. Apply reviewed, version-controlled human overrides from the metadata content
   bundle. Each override records its rationale and source.
4. Import existing destination comments only as candidates for review. Do not assume
   deployed text is newer or more accurate than version-controlled content.
5. Generate the dataset-level registry record from approved metadata and manifest
   fields. Do not use it as the source for table or column text.

The same approved descriptions should reach the target LinkML artifact, portable
Parquet metadata, and destination comments. A verification report identifies any
intentional differences.

## Metadata bundle

Generate the review artifact from a validated snapshot and a version-controlled
profile. The profile is bound to the immutable snapshot identity and supplies
namespace content plus only the human overrides that reviewers have approved:

Start the review from a strict draft whose snapshot identity is read from the
validated manifest rather than copied by hand:

```bash
just metadata-profile ./completed-snapshot \
  nmdc-metadata-2026-08-18 nmdc_metadata \
  "NMDC metadata" "Flattened NMDC metadata tables." \
  --documentation-url https://github.com/microbiomedata/nmdc-lakehouse \
  --property collection=nmdc --property role=metadata \
  --output ./metadata/nmdc-metadata-profile.json
```

The command validates the snapshot offline, fills the exact `snapshot_id`, and
emits an empty `overrides` list. The namespace title, description, URL, and
properties are operator-supplied review content, not generated facts. Review
them and add only evidence-backed table or column overrides before producing the
bundle. The command does not invent descriptions, contact a destination, or
orchestrate later publication steps.

```json
{
  "profile_format_version": 1,
  "profile_id": "nmdc-metadata-2026-08-18",
  "snapshot_id": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
  "namespace": {
    "name": "nmdc_metadata",
    "title": "NMDC metadata",
    "description": "Flattened NMDC metadata tables.",
    "documentation_url": "https://github.com/microbiomedata/nmdc-lakehouse",
    "properties": {
      "collection": "nmdc",
      "role": "metadata"
    }
  },
  "overrides": [
    {
      "table": "biosample_set",
      "column": null,
      "description": "Reviewed description for the published biosample table.",
      "rationale": "Clarify the table's publication role.",
      "source": "NMDC metadata review 2026-08-18"
    }
  ]
}
```

The command operates entirely offline:

```bash
uv run nmdc-lakehouse metadata-bundle ./completed-snapshot \
  --profile ./metadata/nmdc-metadata-profile.json \
  --output ./metadata/nmdc-metadata-bundle.json
```

It validates the snapshot before reading manifested Parquet footers. Every table
and column records its physical type, schema lineage, final description, and
whether that description came from the exact footer baseline, an approved
profile override, or no available description. Duplicate or unknown overrides
fail rather than being silently ignored. The output is canonical JSON; stdout
and `--output` contain the same document.

The profile and bundle contracts are available without a snapshot or service:

```bash
uv run nmdc-lakehouse metadata-bundle-schema profile
uv run nmdc-lakehouse metadata-bundle-schema bundle
```

The bundle is content and evidence, not a destination mutation script. A
destination adapter may translate supported fields into catalog metadata only
after checking the bundle's snapshot identity and the destination's declared
capabilities.

## Table disposition plan

Compare the candidate and target table sets before upload. Every table in their union
receives exactly one disposition:

- **replace**: candidate Parquet is the complete authoritative replacement;
- **add**: candidate table does not yet exist and is safe to introduce;
- **preserve**: target content has no verified candidate replacement;
- **rebuild**: derived content must be recreated from the promoted source tables;
- **retire**: removal has separate evidence, approval, and recovery instructions.

No publication command may infer retirement merely because a target table is absent
from the candidate. In particular, `functional_annotation_agg` remains preserved
until a verified replacement exists. `graph_edges` and
`biosample_to_workflow_run` are derived products: the plan names their rebuild
procedure and order, or preserves them when a safe rebuild is unavailable.

The 2026-08-18 candidate/BERDL comparison is an example of why classification is
necessary, not a permanent allowlist. The schema and deployed table sets will
change.

The maintained offline planner implements this checkpoint without contacting a
destination:

```bash
uv run nmdc-lakehouse publication-plan ./completed-snapshot \
  --inventory destination-inventory.json \
  --policy publication-policy.json \
  --output publication-plan.json
```

It validates the candidate snapshot manifest, a credential-free destination
inventory, and reviewed policy JSON. Common tables default to `replace`, and
candidate-only tables default to `add`. Every live-only table requires an explicit
policy rule and rationale; no omission implies retirement. The command prints the
complete versioned plan and optionally writes the same JSON atomically. It performs
no discovery, upload, catalog change, metadata application, or promotion.

The machine-readable input and output contracts are available without credentials:

```bash
uv run nmdc-lakehouse publication-plan-schema inventory
uv run nmdc-lakehouse publication-plan-schema policy
uv run nmdc-lakehouse publication-plan-schema plan
```

Destination profiles produce the inventory document. Its logical destination,
provider, and table-format labels must be sanitized; its table entries contain row
counts and physical-schema fingerprints, never connection strings or credentials.
Policy may override generated defaults, but incompatible dispositions are rejected:
for example, `retire` cannot apply when a candidate replacement exists, and
`replace` requires both candidate and destination evidence.

Before any provider-specific staging, cross-check the independently reviewed
snapshot, metadata bundle, destination inventory, and publication plan as one
operation:

```bash
just publication-preflight ./completed-snapshot \
  ./metadata/nmdc-metadata-bundle.json \
  destination-inventory.json \
  publication-plan.json
```

This offline command revalidates the snapshot and all three versioned JSON
documents. It requires exact snapshot identity, destination observation,
candidate and destination evidence, metadata coverage, and table-union
agreement. Its JSON summary contains identities, counts, and dispositions, not
paths, credentials, connection details, or records. A successful preflight is a
required input to staging; it does not authorize or perform staging.

## Metadata application plan

Map the approved bundle to the capabilities declared by the same fresh
destination inventory before a provider adapter renders any commands:

```bash
just metadata-application-plan ./metadata/nmdc-metadata-bundle.json \
  destination-inventory.json \
  example_catalog.nmdc_metadata_staging \
  --output metadata-application-plan.json
```

The staging namespace is explicit and may differ by destination. The plan
preserves the snapshot, profile, bundle-generation, destination-observation,
provider, table-format, and capability evidence. It covers the exact bundle
table set and classifies every approved namespace property, table description,
and column description as supported or unsupported. Missing descriptions remain
explicit. Description text is JSON data, never SQL or another provider command.

The output is a strict, versioned review artifact. Its contract is available
offline:

```bash
uv run nmdc-lakehouse metadata-application-plan-schema
```

This step does not contact a catalog, create a namespace, apply metadata, or
authorize staging. A later destination adapter must load this artifact through
the maintained model and recheck its bundle and inventory identities immediately
before rendering or applying provider-specific operations. The adapter must
report unsupported metadata levels rather than silently omitting them.

## BERDL staging command plan

The BERDL destination profile can bind the reviewed portable evidence to an
exact external implementation before any live operation. The
`berdl-upload-plan` command re-runs publication preflight, checks the metadata
application plan against the same snapshot and destination observation, requires
a successful target-schema validation report with exact table coverage, and
selects the complete manifest-owned Parquet table set. It also requires a clean
checkout of the official `kbase/data-lakehouse-ingest` package at an explicit
full Git revision and binds that API to the NMDC-owned adapter. BERIL Research
Observatory is not part of this accountable ingestion boundary.

The current adapter accepts only a destination inventory that identifies the
`spark_catalog` provider and `iceberg` table format. Both values are retained in
the immutable staging plan and its exact adapter arguments; an absent or
incompatible destination contract cannot produce an executable-looking plan.

The generated JSON records local paths and checksums for every reviewed input,
including target validation, the selected Parquet identities, the KBase ingest
revision, official checkout provenance, complete package-tree identity, relevant
source hashes, and the exact plan-only argument vector. Its dataset name must use a unique
`<name>_staging_<suffix>` form, and its object prefix must be inside the tenant's
staging area. The output is created once and is not overwritten.

The plan does not read credentials, start a tunnel, invoke the adapter, upload
data, create a table, apply metadata, or authorize a canonical change. The
maintained executor reloads and reconstructs this plan, previews by default,
and requires explicit snapshot- and full-plan-digest authorization before
invoking the reviewed NMDC adapter without a shell. The plan digest binds the
approved destination tuple as well as all other plan fields. It accepts success
only when the adapter's immutable outcome identifies the planned staging
destination and independently reports matching source Parquet and catalog row
counts for the complete manifested table set. Catalog counts come from reading
each fully qualified destination table after ingest, not from the ingest
writer's report.

Planning and execution occur in the same BERDL JupyterHub pod because the plan
binds its interpreter and absolute evidence, adapter, and official-ingest paths.
The adapter uploads and verifies the Parquet objects, then calls the pinned
stock `data_lakehouse_ingest.ingest` API in-process. BERIL Research Observatory
is not a runtime dependency. A disposable-namespace rehearsal remains required
before using the executor for the authorized staging reload.

The executor revalidates the plan and evidence after every started live command,
including a failed command, then
creates a separate immutable NMDC outcome with status `data-verified`. That
status means data staging passed; it does not mean metadata was applied or the
canonical namespace may be changed. See the
[BERDL upload guide](berdl-upload.md#build-the-maintained-staging-command-plan)
for the complete recipe interface.

## Staged workflow

### 1. Inventory without mutation

Record:

- candidate snapshot path, manifest identity, files, checksums, Parquet footer row
  counts, and physical schemas;
- target LinkML validation mode, selected and eligible rows, schema identity,
  sanitized findings, and report checksum;
- target capabilities and, when supported, catalog, namespace, provider, table
  locations, tables, row counts, schemas, comments, properties, and current owner;
- source `nmdc-schema`, target schema, mapping, package, and Git identities;
- dependent views and derived tables; and
- the available provider-specific recovery mechanism.

Do not print credentials, connection strings, or production records. Discover
provider and catalog behavior instead of inferring it from a historical runbook.

### 2. Produce and approve the disposition and metadata plans

Join the candidate and live inventories, assign one disposition to every table,
and print all additions, replacements, preserves, rebuilds, and retirements.
Generate the metadata application plan for the explicit staging namespace from
the approved bundle and same destination inventory. The provider profile also
states the promotion mechanism, validation queries, and rollback procedure.

This is the first mandatory no-mutation checkpoint. Upload does not begin until a
human has reviewed both plans and `publication-preflight` has confirmed that the
disposition plan, inventory, metadata bundle, and snapshot still agree. The later
adapter independently rechecks the metadata plan's copied identities before use.

### 3. Establish rollback evidence

Before changing destination objects or managed tables:

- capture catalog DDL or equivalent reconstruction information;
- record provider-specific version or snapshot identifiers when supported;
- retain the previous publication manifest and immutable source artifacts;
- prove the proposed recovery operation against a staging table; and
- state what cannot be rolled back automatically.

An object-store copy alone is not proof that catalog state, comments, properties,
or derived tables can be restored.

### 4. Publish into staging

Copy the manifest-defined artifact set to a new snapshot-specific prefix or other
isolated staging location. When the destination supports managed tables, load them
into a staging namespace or equivalent. Do not overwrite the canonical target
during this phase.

Apply only the supported operations in the reviewed metadata application plan to
staging, report unsupported operations, and rebuild staging copies of every table
classified as **rebuild**.

For the BERDL profile, `berdl-apply-metadata` applies and reads back the approved
table and column descriptions only after the staging outcome is data-verified.
It binds the metadata plan, staging outcome, and stock ingest revision. Namespace
properties remain a separate provider operation and are not implied by a
successful table/column metadata outcome.

### 5. Validate staging

At minimum, verify:

- exact table set and disposition coverage;
- source Parquet footer rows against staged rows or managed-table rows;
- physical types, nullability, arrays, and schema fingerprints;
- required identifier presence and uniqueness where declared by the target schema;
- side-table `parent_id` relationships and other declared references;
- expected metadata coverage at every destination-supported level;
- source, target, mapping, software, and snapshot provenance;
- representative scientific joins, including the biosample-to-workflow path; and
- preservation or successful reconstruction of live-only and derived products.

Validation failures leave the canonical target untouched.

### 6. Approve and perform promotion

Present the validated plan, staging report, exact canonical objects to be changed,
and rollback instructions. This is the second mandatory human checkpoint.

Promotion uses an operation declared and tested by the destination profile. Do not
assume that prefix replacement, namespace rename, table replacement, or a
multi-table transaction is atomic without verifying that behavior for the target.
Never issue a drop-all command.

### 7. Verify and record the published state

Repeat the staging checks against the canonical target. Record the final
destination, catalog or provider when applicable, table versions or snapshots,
counts, metadata coverage, derived-table status, registry update, timestamps, and
operator-visible outcome in the publication record. A partial promotion is a
failure until recovered or explicitly accepted and documented.

## Destination profiles

A destination profile supplies only platform-specific behavior. It must declare:

- supported metadata levels: organization or tenant, dataset, namespace, table,
  column, snapshot, and file;
- authentication and transport prerequisites without embedding credentials;
- immutable staging layout and canonical naming rules;
- provider or catalog discovery and table-format behavior, when applicable;
- metadata write APIs and how approved descriptions map to each supported level;
- validation queries or file checks;
- promotion atomicity, failure behavior, rollback operations, and tested limits;
- registry integration, when one exists; and
- unsupported capabilities, rather than silently dropping metadata or safety checks.

BERDL is one such profile. Its current operational details belong in the
[BERDL upload guide](berdl-upload.md) and
[BERDL metadata-shaping guide](berdl-metadata-shaping.md). A local or object-store
profile may stop at an immutable, checksummed snapshot while still preserving the
complete target schema, footer metadata, content bundle, and manifest.

## Automation boundary

The eventual command from #136 should have a non-mutating default or explicit
`--dry-run` mode. Its plan should be serializable and reviewable before a separate,
explicit promotion action. Tests cover plan construction, disposition completeness,
unsafe destination rejection, verification failures, and rollback command
construction without destination credentials.

Destination-specific environment setup and transport helpers may live in BERIL,
KBase, or other integration repositories.
The snapshot, metadata, disposition, validation, and publication contracts belong
here so they remain testable without a live cluster.

## Related documentation

- [Architecture](architecture.md)
- [MongoDB connection and snapshot output](mongodb-connection.md)
- [BERDL upload guide](berdl-upload.md)
- [BERDL metadata shaping](berdl-metadata-shaping.md)
- [NMDC metadata tables](nmdc_metadata_tables.md)
- [Biosample-to-workflow derived table](biosample_to_workflow_run.md)
