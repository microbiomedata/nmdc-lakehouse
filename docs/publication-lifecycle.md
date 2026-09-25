# From NMDC schemas to a published lakehouse snapshot

This is the lifecycle map. Use the linked guides for the complete arguments and
failure handling. Examples use source **11.23.0**, the source of the retained
September candidate. The latest reviewed upstream tag is **11.24.0**. Choosing
the latest schema package does not migrate MongoDB; export with the source/flat
pair that production can actually supply.

## Repositories, environments and versions

| Repository or checkout | Owns | When it changes |
| --- | --- | --- |
| `microbiomedata/nmdc-schema` | Nested source model, release packages and migration history | Upstream schema release |
| `microbiomedata/nmdc-lakehouse-schema` | Projection engine, flat LinkML artifacts and their documentation | Projection change or deliberate source upgrade |
| `microbiomedata/nmdc-lakehouse` | Export, provenance derivation, validation, metadata and publication | Data pipeline or deployment change |
| `kbase/data-lakehouse-ingest` | Reviewed stock BERDL ingest implementation | Deliberate adapter compatibility update |

`nmdc-lakehouse-schema-adoption` is Mark's **local checkout of nmdc-lakehouse**,
not another product or repository. `/Users/mam/...` and `/home/mamillerpa/...`
paths in the run record are specific to his computers and account.

Use the existing root `justfile` in each NMDC repository. The schema repository
imports `project.justfile`; that is an internal extension, not another operator
working directory. Do not create a Just file or edit a script for each data run.
The pod can call the installed `nmdc-lakehouse` executable directly and does not
need Just, development tools, or documentation packages.

Four identities answer different questions:

| Identity | Current example | Meaning |
| --- | --- | --- |
| Source schema | `11.23.0` or `11.24.0` | Which NMDC record contract is understood |
| Projection | `1.3.0` | Which flattening rules are applied |
| Flat-schema package | `0.5.0` | Which released engine and bundled artifacts are installed |
| Derived schema | `1.0.0` | Contract for the two computed provenance tables |

The collection target combines the first two, such as
`11.23.0+flat.1.3.0`. Neither a package install nor a documentation deployment
rewrites existing Parquet or lakehouse tables.

## 1. Generate and release the collection schema

Work in `nmdc-lakehouse-schema`. Its generator reads the installed release
package's `nmdc_materialized_patterns.yaml`, follows the `Database` collection
slots, and generates primary and helper table classes. TextValues become parent
strings or string arrays; collection `type` values remain data. Repeated nested
relations can require helper tables. See the schema repository's
[transformation contract](https://microbiomedata.github.io/nmdc-lakehouse-schema/transformation-support/).

The canonical checked-in file is
`src/nmdc_lakehouse_schema/schema/nmdc_schema_flattened.yaml` for 11.24.0.
The compatibility file is
`src/nmdc_lakehouse_schema/schema/compat/11.23.0/nmdc_schema_flattened.yaml`.
The similarly named `nmdc_lakehouse_schema.yaml` is a template example, not this
product. Never copy the example or a rendered documentation download into ETL.

For an upstream upgrade, review the latest non-prerelease tag and edit the
source dependency group, lockfile, supported-version selection and tests.
For projection changes, edit the schema-owned generator/flattener and update
`FLATTENER_VERSION` according to its documented convention. Regenerate each
supported artifact and review the diff.

<!-- unverified: use the schema repository's checkout and release process; no new schema release is required for this candidate, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
cd /path/to/nmdc-lakehouse-schema
just install
just generate-flat-schema
just generate-compat-schema
just check-flat-schemas
just test
just test-dist
just gen-doc
uv run mkdocs build
```

Merge the reviewed PR with the generated YAML included. Main deploys the docs
from those checked-in bytes. A maintainer then publishes a new version tag and
GitHub Release; the separate trusted-publishing workflow checks and uploads the
wheel and source archive to PyPI. Verify the workflow and published artifacts
before changing the consumer. The package version comes from Git tags, while
the projection version is a manually maintained constant. Follow the
[schema release and adoption instructions](https://microbiomedata.github.io/nmdc-lakehouse-schema/schema-workflow/).
No new release is needed to reuse the current published 0.5.0 pair.

## 2. Adopt the package in the exporter

Work in `nmdc-lakehouse`. Update its exact `nmdc-lakehouse-schema` requirement
and `uv.lock` in a PR. If adding a source release, also update the supported
source extras, source selector and compatibility/integration tests. There is
no YAML copy or vendoring step: the exporter imports the engine and obtains
the exact installed source's target with `flat_schema_resource(...)`.

Run the full checks for each supported source and the distribution check before
merge. [Release policy](releases.md) describes the separate exporter package:
a checkout at a reviewed immutable Git commit is sufficient for this workflow;
an exporter PyPI release is not required, and is not automated today.

<!-- unverified: example future dependency adoption; current 0.5.0 is already adopted, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
cd /path/to/nmdc-lakehouse
just lock
NMDC_SCHEMA_VERSION=11.23.0 just install-all
NMDC_SCHEMA_VERSION=11.23.0 just check
NMDC_SCHEMA_VERSION=11.24.0 just check
just test-dist
```

Use source-aware Just recipes for later operations. A plain `uv run` does not
interpret `NMDC_SCHEMA_VERSION`. Restore the required version after testing
another pair. [Source selection](source-schema-1124-rollout.md) explains the
read-only preflight and the difference between API version and completed
migration history.

## 3. Define the two derived schemas

These are the **derived provenance tables**: `graph_edges` stores upstream
material-provenance edges; `biosample_to_workflow_run` stores reachable
biosample/workflow pairs, minimum hops, workflow type and processing flags.
They turn a recursive relationship query into ordinary joins when the saved
mapping is sufficient. They do not contain annotation results.

Their authored LinkML schema lives here at
`src/nmdc_lakehouse/schemas/provenance.yaml`, independently of the generated
collection YAML. It declares `GraphEdge` and `BiosampleToWorkflowRun`.
The builder loads it and uses the shared `class_def_to_arrow_schema` conversion
to produce Arrow fields and descriptions. The validator uses the same schema,
and checks its version and exact digest against the Parquet footers.

There is no separate schema-generation command or package release for this pair.
Change that YAML, its version and derivation/validation tests in a lakehouse PR
when its contract changes. Keep algorithm changes and schema changes explicit.
[Local provenance](local-provenance.md) documents the flags, pooling limits,
cycle/depth checks and measured query comparison.

## 4. Produce and prepare local Parquet

Client prerequisites are Git, Just, uv, Python 3.13, sufficient disk and memory,
and the selected locked dependencies. A new dump additionally requires an
authorized SSH gateway key and a MongoDB account with read access to the
eligible collections and migration view. NERSC access is needed to obtain the
gateway key; a GCP service-account key and NMDC API token are not required by
this export. See [MongoDB setup](mongodb-connection.md).

Keep credentials in a private `.env` or the inherited environment, outside run
configuration and evidence. Configure `MONGO_HOST=localhost`, `MONGO_PORT=27124`,
`MONGO_DBNAME=nmdc`, `MONGO_DIRECT_CONNECTION=true` and the appropriate
authentication database. Run `just tunnel` in its own terminal during export, then run
`NMDC_SCHEMA_VERSION=11.23.0 just source-preflight` in the export terminal.

The shortest new-run path is `prepare-publication`: its JSON sets
`source_version` and reviewed namespace descriptions. Omit `snapshot` to dump
all eligible collections with empty columns retained, create a manifest,
validate all target rows, and generate publication metadata. See the
[configuration example](berdl-staging-runbook.md#prepare-on-the-workstation).
For an existing run, supply `snapshot` and its saved full `target_validation`
report; this avoids repeating the dump and row validation.

<!-- unverified: combined fresh-export path awaits live acceptance; reuse preparation was exercised on September 23, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
cd /path/to/nmdc-lakehouse
just prepare-publication local/metadata-publication.json local/prepared-metadata
```

The output is `snapshot/`, `evidence/` and `preparation.json`. In a separate new
directory, derive the pair from that exact metadata snapshot. Prepare it with
a second JSON whose `snapshot` names the derived directory. Without a saved
full report, preparation performs full validation once.

<!-- unverified: combined example uses the maintained local derivation and preparation commands, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
export NMDC_SCHEMA_VERSION=11.23.0
just derive-provenance local/prepared-metadata/snapshot local/derived-provenance
just compare-provenance-queries local/prepared-metadata/snapshot local/derived-provenance local/provenance-comparison.json
just prepare-publication local/derived-publication.json local/prepared-provenance
```

Do not append derived files to the collection snapshot. The pair's separate
manifest names its parent snapshot. Collection exports include only the selected
schema's `Database` collections, not every internal MongoDB collection. The
September full export includes `functional_annotation_agg`; downloadable GFF,
taxonomy and other workflow-result datasets are separate pipelines.

## 5. Preserve supplementary metadata

Metadata begins during Parquet creation; it is not an optional upload step.

| Level | Retained evidence | Destination behavior |
| --- | --- | --- |
| Run/snapshot | Schema and software identities, Git revision, source label, metrics, full validation and content-derived ID | Bound to reviewed plans and outcomes |
| File/object | Byte size, row count, SHA-256, physical and footer schema fingerprints | Upload hash readback; `nmdc-sha256` object metadata |
| Table | Source/target classes, mapping, descriptions and projection or derivation identity | Table comments and NMDC properties applied and checked |
| Column | Description, LinkML range, identifier/type designation; Spark comment metadata | All planned comments checked |
| Namespace/dataset | Reviewed title, description, documentation link and properties | Retained in bundle; live namespace/registry operations are deferred |
| Row/relationship | IDs, source `type`, parent references and occurrence positions; derived parent snapshot | Remain columns and lineage evidence, not invented cell annotations |

`prepare-publication` produces the snapshot-bound profile, metadata bundle and
receipt, retaining explicit missing descriptions. The schema and bundle carry
constraints richer than catalog comments. See the
[metadata contract](publication-contract.md) and
[column-description path](column-description-path.md).
Target validation proves conformance, not complete preservation of every
populated MongoDB value; the focused source-to-output audit remains separate.

## 6. Transfer and stage in the pod

Use the [staging runbook](berdl-staging-runbook.md#send-to-the-pod) for the
maintained transfer helper, pinned pod setup, destination JSON and exact staging
commands. One standard-library Python script handles pack/send/receive; it needs
no package installation. Send calls the Jupyter Contents API directly, using an
environment-only JupyterHub API token and the configured Hub URL and username.
The same transferred script verifies and extracts in the pod. It sends only
manifest-owned snapshot files, the preparation receipt and its three evidence
files, excluding logs, `.env`, credentials and machine-bound staging plans.

The pod needs Python 3.13, Git, its existing KBase/BERDL session, Spark Connect,
object-store libraries and grants to read the catalog and create the selected
staging namespace and objects. Use an isolated environment with
`--system-site-packages` to retain platform libraries. Install the locked NMDC
runtime once, without dev/docs extras; keep the reviewed official ingest checkout
pristine. Stock v0.1.5 at `a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3`
is the currently supported ingest revision. BERIL is not a runtime dependency.

Client MongoDB secrets do not belong in the pod. Keep the pod's session and
object-store credentials in its established environment. Starting Spark in an
authenticated notebook may be necessary before using its terminal. Inherited
platform packages are not fully pinned by the NMDC lockfile. Core dependency
reduction remains [#184](https://github.com/microbiomedata/nmdc-lakehouse/issues/184).

Collect a fresh read-only destination inventory, then run `plan-publication`,
`stage-publication` without execution flags, and `publication-status`. Review
the preview and run the exact authorized next command. Use separate unused
staging namespaces and Bronze prefixes for the collection and derived snapshots.
Require `data-and-table-metadata-verified` for each, not merely a successful upload.

The existing 46-table parent staging uses older evidence and must be retained.
Do not reconstruct or overwrite its plan merely to use newer commands. Combined
promotion supports that historical staging evidence directly. The new transfer
helper is for prepared publications and is not an importer for arbitrary old
archives. Reuse already verified old transfers; do not transfer them again for style.

## 7. Promote the pair to canonical tables

In the pod, `berdl-promotion-plan` takes both original staging roots, a new plan
path, the reviewed ingest checkout and the recovery statement. It verifies the
parent relationship, full evidence, retained object hashes, staged row contents,
metadata and current canonical state. `berdl-promote PLAN` previews that plan.
The [promotion procedure](berdl-upload.md#plan-separately-authorized-canonical-promotion)
gives the complete commands.

After live acceptance passes, have Mark approve the exact plan digest,
`nmdc.metadata` namespace and destination identity. Execute with those printed
authorization values during a window without competing writers. This is separate
from merging code or authorizing staging. It copies reviewed Iceberg snapshots,
preserves descriptions/properties and compares copied row contents. It does not
move or rename the staging namespace as one transaction.

The expected September result is **48 canonical tables**: 46 collection/helper
tables plus two derived tables. Nine explicitly named obsolete TextValue helpers
are removed only after replacements verify. Unexpected destination-only tables
cause refusal. Require `promotion-verified` and preserve the before state,
per-operation journal and immutable outcomes. There is no multi-table atomicity,
automatic rollback or demonstrated recovery of dropped tables.

## 8. Routine cleanup and optional historical retirement

Cleanup is a distinct decision after verification and a chosen retention window.
No remote cleanup was performed by the steps documented here.

| Material | Earliest reasonable action | Keep |
| --- | --- | --- |
| MongoDB tunnel | Close after export/source audits complete | Credentials remain private |
| Transfer parts and disposable reassembly files | Remove only the exact run's redundant files after received bytes and planning verify | At least one verified snapshot and transfer receipt |
| Development caches and builds | `just clean` removes its listed development state; avoid active plan runtimes | `local/` data, evidence and credentials |
| Prepared snapshots, source objects and staging tables | Retain through promotion verification and the agreed recovery window | Reproducible input, manifests, reports, profiles, plans and journals |
| Probe namespaces | Inventory exact owned objects and dependencies, then separately approve disposal | Acceptance report and runtime identity |

`clean-parquet` previews recognized local metadata files; deleting individual
files invalidates a manifested snapshot. It is not remote cleanup or a retention
manager. `DROP TABLE` is not proof that object-store bytes are gone. Conversely,
deleting object-store prefixes can corrupt still-registered Iceberg or Delta
tables. Do not remove files referenced by retained snapshots or another table.

More aggressive cleanup needs a fresh read-only inventory across visible
catalogs, namespaces, registered locations, Bronze objects, Iceberg/Delta
metadata and historical staging/probe prefixes. Record access gaps, ownership,
consumers, shared paths and retention requirements. Classify each exact object
as keep, archive, investigate or retire; approve a concrete deletion plan before
execution, and verify catalog and object-store outcomes separately. Names that
contain `nmdc` are evidence to investigate, not permission to delete.

External copies such as `nmdc_arkin` require their owner's involvement. Other
NMDC result/reference tables are not obsolete just because the MongoDB metadata
snapshot lacks them. No wildcard tenant purge, snapshot expiration or orphan-file
removal belongs in routine promotion. The read-only cross-catalog inventory is
tracked in [#295](https://github.com/microbiomedata/nmdc-lakehouse/issues/295);
local retention contracts in [#188](https://github.com/microbiomedata/nmdc-lakehouse/issues/188).
The separate historical retirement design and approval requirements are tracked
in [#367](https://github.com/microbiomedata/nmdc-lakehouse/issues/367).

## Operating through Claude in Chrome

1. Mark signs into the intended KBase JupyterHub account in Chrome and opens its
   JupyterLab pod. The agent reports the intended read or staging operation before
   interacting with the pod. Browser access and API access are different routes.
2. Transfer reviewed files through the documented Contents API sender, or the
   authenticated file browser. A browser agent may be unable to operate the native
   file picker; report that limitation rather than assuming it can upload files.
   In Chrome, open a JupyterLab Terminal and run a short command referring to the
   reviewed file. Avoid pasting a long multiline script into a browser terminal.
3. Confirm the exact Git commit, source version, inventory and checksums before
   planning. Start a command once. Poll its log/status with another terminal;
   silence is not evidence of failure or permission to launch it again.
4. Keep raw diagnostics private in the durable run directory. Return sanitized
   status, elapsed time, paths and hashes, and retrieve evidence for independent
   checking. Do not put session tokens in chat, screenshots, pull requests or script files.
5. Coordinate through immutable agent-mail files: acknowledge task requests with
   accepted/blocked, use actual session IDs, and have the Claude operator read
   existing mail before starting its Monitor. A notification or queued message
   is not an acceptance or proof of execution.

The browser is the operator interface, not a separate ETL implementation. Reuse
the same package commands as a human terminal operator. The September run proves
that a working Chrome session can reach the pod even when a command-line API
route returns 403. This Codex session's later missing browser-runtime module is
a local tooling failure, not evidence that the pod itself is unavailable.

## Current candidate: where to resume

As recorded on **2026-09-25**, without refreshing the data:

| Stage | State |
| --- | --- |
| Source/flat release and consumer adoption | Complete: 11.23.0 compatibility, package 0.5.0, projection 1.3.0 |
| Complete collection export and full validation | Complete: 46 artifacts, 53,217,239 artifact rows, zero invalid |
| Parent data/table/column staging | Verified: `nmdc.nmdc_metadata_staging_20260923_58277b41` |
| Derived schema, files, full validation and metadata | Complete locally: 136,776 edges, 57,786 pairs, all 15 column descriptions |
| Derived transfer and pod receipt | Verified on September 25 at `ce87b7a`: eight file hashes matched; both Parquet artifacts validated |
| Promotion content comparison and disposable writer | Passed on September 25 at `35a54c8`: all 46 staged tables; small-table add/replace and metadata readback |
| Derived staging and new combined path acceptance | Pending live pod execution |
| Canonical promotion | Not executed; needs live acceptance, fresh exact plan and approval |
| Remote retirement or routine data cleanup | Not executed |

The [dated run record](runs/2026-09-23-production-staging.md) holds exact
identities and evidence locations. The parent ID begins `58277b41`; the derived
ID begins `b79eb420` and binds that parent. The source preservation audit remains
[#347](https://github.com/microbiomedata/nmdc-lakehouse/issues/347), and
namespace/registry metadata remains
[#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) and
[#52](https://github.com/microbiomedata/nmdc-lakehouse/issues/52).
Do not repeat the dump, full validation or parent staging to finish these tasks.

Feature-to-Parquet PR #364 is merged, but those annotation files are a separate
local prototype. Model conformance and their production load remain
[#84](https://github.com/microbiomedata/nmdc-lakehouse/issues/84); they are not
included in this 48-table promotion. Complete the retained metadata/provenance
publication before treating broader result loading or historical cleanup as done.
