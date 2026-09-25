# Uploading `lakehouse/` Parquet output to BERDL

Start with the [staging runbook](berdl-staging-runbook.md) for the operator
sequence, credentials, evidence layout, and recovery decisions. The
[2026-09-23 run record](runs/2026-09-23-production-staging.md) records the latest
production export, setup failure and fix, and verified data/metadata staging.
Separate derived staging, namespace metadata, source-preservation auditing, and
canonical promotion remain outstanding in that record.

This document contains two different things, and reading one for the other wastes
time. The boundary is the "Historical off-cluster transport" heading:

- **Everything above it is the maintained path.** It runs inside a BERDL
  JupyterHub pod, uses the reviewed plan commands in this repository, and is what
  a current staging load follows. Two sections above the boundary are exceptions
  and say so in their headings. "Getting table data back out" is unverified,
  because no reviewed command performs an export and nobody has run the one it
  shows. "Move bulk data with `mc`, for a one-off transfer" is verified but is
  not part of the maintained upload: `stage-publication` reads its snapshot from the
  pod filesystem, so `mc` is for moving data around rather than for feeding that
  command.
- **Everything below it is the April 2026 record.** It is kept for provenance. Do
  not use it by itself to overwrite or replace live tables. Its fixed dataset
  name, table count, Delta verification examples, and prerequisites belong to that
  run, not to the maintained path.

Any replacement must follow the reviewed plan and approval checkpoints in the
[portable publication contract](publication-contract.md), discover the live
catalog and provider, and classify every candidate and live table.

## What the maintained path requires

`etl-collections` and `etl-annotations` have already produced local Parquet under
`LAKEHOUSE_ROOT` (see the configuration table in `README.md`), and that output has
been assembled into a completed snapshot with a manifest.

From there, `plan-publication`, `stage-publication`, `publication-status`, and the
destination-inventory script all run **inside a BERDL JupyterHub pod**, where MinIO
and Spark are local. That path needs:

- a running pod and a valid KBase session for it,
- this repository checked out in the pod,
- a clean checkout of [`kbase/data-lakehouse-ingest`](https://github.com/kbase/data-lakehouse-ingest)
  in the pod at the reviewed revision,
- the completed snapshot and every reviewed evidence file present in the pod,
- the hub contents API, or the notebook file browser, to get them there.

It does **not** need SSH access to `login1.berkeley.kbase.us`, the SOCKS tunnels,
or a workstation-side `mc`. Those belong to the historical transport and are listed
under it. Verified on 2026-08-20: with the tunnels down and the bastion unreachable
from the workstation, the full offline plan preview and a live pod-resident
capability probe both ran successfully. Separating these prerequisites from the
maintained path was
[#244](https://github.com/microbiomedata/nmdc-lakehouse/issues/244), closed
2026-08-21.

There is no standalone check that the bucket accepts writes, on either path.
`berdl-doctor` does several things, but none of them contact an object store: its
`mc` check confirms the binary is present and reports a version, and stops there.
Write access is exercised by the staging run itself, which writes to the bronze
prefix from inside the pod where the object store is local, so a permissions problem
surfaces as a failed run rather than as a preflight result.

The adapter reads each uploaded object back and compares its SHA-256 to the
source Parquet digest. The outcome check then compares destination row counts
against source Parquet counts and the verified source digest against the snapshot
manifest. Keep the
reviewed bronze prefix inside the tenant staging area, where the pod's session is
expected to have write access, so the first write is not also the first surprise.

---

## Supported readiness check

`berdl-doctor` validates the completed snapshot and reports on the external
tooling, without changing either repository or contacting BERDL. Run it on either
path:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run --no-sync nmdc-lakehouse berdl-doctor /absolute/path/to/completed-snapshot
```

`--beril-checkout` is optional. Without it the snapshot is still validated, and the
two checks that inspect a BERIL checkout report `SKIP` with a note saying the
maintained path does not use one. A skipped check does not affect the exit status,
so an operator on the maintained path is not told the doctor failed for something
they do not need.

To validate only the snapshot, with nothing else reported:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run --no-sync nmdc-lakehouse validate-snapshot /absolute/path/to/completed-snapshot
```

For a historical off-cluster run, supply the checkout and the two skipped checks
become real:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
export BERIL_CHECKOUT=/path/to/BERIL-research-observatory
export BERDL_DESTINATION_ID=nmdc-production
export BERDL_CATALOG=discovered-catalog
export BERDL_TABLE_FORMAT=discovered-table-format
just berdl-doctor /absolute/path/to/completed-snapshot
```

If `just` stops while parsing a malformed repository `.env`, bypass its dotenv
loading so the doctor can report the sanitized configuration failure itself:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run --no-sync nmdc-lakehouse berdl-doctor \
  /absolute/path/to/completed-snapshot
```

For the optional historical checks, `BERIL_CHECKOUT` must be explicit; the
command does not guess a user-specific checkout location. It validates the
snapshot manifest offline, identifies the
checkout revision, checks for the required ingest resource paths, requires
Python 3.13 in its `.venv-berdl`, checks the `data-lakehouse-ingest` and
`berdl-remote` distributions, and checks `mc`. It also checks for
`KBASE_AUTH_TOKEN` by name in the process, this repository's `.env`, or the
configured BERIL checkout's `.env`. No value is printed or tested. Refresh the
short-lived token through the supported KBase workflow immediately before a
publication attempt.

Those optional checks identify the historical transport's selected revision and
locally available interfaces; they do not certify live-ingest compatibility.
The maintained staging plan instead verifies the selected official KBase ingest
checkout and does not require a BERIL revision.

## Build the maintained staging command plan

Transfer the complete prepared directory from
[workstation preparation](berdl-staging-runbook.md#prepare-on-the-workstation)
to the pod. Obtain a fresh [destination inventory](#capture-a-fresh-destination-inventory-without-mutation)
and a clean official [`kbase/data-lakehouse-ingest`](https://github.com/kbase/data-lakehouse-ingest)
checkout. Use the source pair selected during preparation in the pod environment.
The planner does not install packages or contact the catalog.

Write a destination JSON file, for example `destination.json`. Paths are relative
to this file and must refer to pod-local files. Replace the namespace and object
prefix with the intended unique staging destination:

```json
{
  "inventory": "destination-inventory.json",
  "ingest_checkout": "runtime/data-lakehouse-ingest",
  "ingest_revision": "a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3",
  "staging_namespace": "nmdc.nmdc_metadata_staging_20260923_example",
  "bucket": "cdm-lake",
  "bronze_prefix": "tenant-general-warehouse/nmdc/staging/20260923_example"
}
```

From the NMDC checkout in that environment:

<!-- unverified: combined planning awaits pod acceptance in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
just plan-publication /absolute/path/to/prepared-publication /absolute/path/to/destination.json
```

This replaces the separate `publication-plan`, `publication-preflight`,
`metadata-application-plan`, and `berdl-upload-plan` commands. It checks the
preparation receipt, copies the inventory, and creates the disposition policy,
publication plan, preflight report, metadata application plan and final staging
plan under the prepared directory's `evidence/`. The final file is
`berdl-staging-plan.json`; the command prints its exact SHA-256 digest.

All manifested tables are selected. Other canonical tables receive `preserve`
for this staging attempt; that decision does not approve their later promotion
or retirement. The metadata plan explicitly reports unsupported operations and
missing descriptions. Namespace application remains
[#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114).
Review both the staging plan and `metadata-application-plan.json` before execution.

Rerunning the command rebuilds and checks every plan and permits only identical
existing evidence. A changed inventory, destination or prepared input requires a
new prepared directory. A runtime-check failure can be repaired and retried
without a new dump or full validation. An existing final plan binds its runtime;
never replace its checkout or interpreter silently. Inventory acquisition,
transport and initial pod setup are documented in the runbook; acceptance is tracked in
[#353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353).

The planner re-runs the portable preflight; verifies the metadata plan's
snapshot, destination observation, capabilities, namespace, and table coverage;
requires successful target-schema validation with exact snapshot and table
coverage; and checks that the official ingest checkout is clean at the requested
revision. The maintained compatibility gate currently accepts the stock
`v0.1.5` commit `a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3`, whose writer uses
Spark's catalog-driven Iceberg API. An authentic but unapproved older or newer
revision fails closed until its write contract is reviewed. The planner binds
the NMDC-owned adapter and the official checkout's complete
tracked `data_lakehouse_ingest` package tree, verifies every package file against
the selected revision, and requires an official KBase GitHub remote. It then creates an immutable,
credential-free JSON plan containing local evidence paths, checksums, and the
exact plan-only adapter argument vector. It rejects canonical-looking dataset
names and object prefixes outside the tenant staging area. It also requires and
records a reviewed `provider` naming the destination catalog, together with the
`iceberg` table format used by the selected official ingest path; a label that
names a different catalog than the staging namespace fails closed.
BERIL Research
Observatory remains an optional operator resource and is not a runtime or
release dependency of this workflow.

The generated command intentionally omits the live execution flag and outcome
path. Do not add them by hand.

Applying descriptions records progress in the private staging log as it goes, naming the table it
is on, the columns verified so far against the total, how many of those it
actually wrote, elapsed time, and an estimate of the time remaining. A
description the catalog already holds is not written again, so a rerun after a
partial failure finishes the remainder instead of redoing the whole table. Every
planned description is still verified by read-back, whether or not this run wrote
it, which is why the verified and written counts differ on a rerun. The estimate
is rated on written columns only: a skip costs a catalog read and a write costs a
catalog commit, so counting them together would look fast while skipping and be
wrong as soon as writing resumed. Standard output stays reserved for the parseable
outcome JSON. Expect it to write no column descriptions. Those arrive in the
Parquet footer and Spark applies them when it creates each table, so this step
finds them already present and only reads back to confirm: a full 53-table
namespace cost 0 column writes and about 40 seconds on 2026-08-25. It is not
otherwise read-only, and still writes any missing table descriptions and the
schema identity properties. The path it replaced
applied one column at a time, one catalog commit each, and on 2026-08-20 it ran
for 117 minutes and failed. See
[`column-description-path.md`](column-description-path.md).

## Move the snapshot and evidence into the pod

`plan-publication` binds absolute paths and `stage-publication` runs in the pod, so the
completed snapshot and every reviewed evidence file have to be in the pod
filesystem first. Transfer happens over the JupyterHub contents API, either through
the notebook file browser or through a client that speaks to it. The SOCKS tunnels
play no part in this and do not need to be up.

Use the maintained [pack/send/receive procedure](berdl-staging-runbook.md#send-to-the-pod).
It packages a prepared publication, sends bounded parts through the existing
`labctl pod put`, verifies the helper before execution, and verifies every
received file. It excludes unrelated files and machine-bound runtime plans.
The received snapshot and evidence are then checked by `plan-publication`.
Do not use the earlier manual tar/split/reassembly instructions for a new run.
Existing verified historical transfers remain valid and need not be repeated.

The reason for selecting files explicitly is recorded by the August 20 run:
macOS tar added 54 AppleDouble `._*` siblings to a 52-artifact snapshot.
Snapshot validation rejected the unexpected files even though the Parquet bytes
had transferred correctly. The new helper does not archive macOS extended
attributes or incidental files. If an old transfer has unexpected files, retain
it for diagnosis and review those exact files before removing anything; never
apply a broad deletion command to a manifested snapshot.

That run also observed one client upload succeeding at 112 MB and failing at
352 MB. This was an observation about that client, not a universal API limit.
The maintained helper now creates ordered parts of at most 64 MiB and verifies
part, complete archive and extracted file digests. Manual splitting and
reassembly are retired from the maintained path. The April off-cluster record
below is historical, not a second supported publication procedure.

## Getting table data back out, and the trap that eats it (unverified)

The direction above is workstation to pod. Going the other way, off the platform,
has a failure that is worse than the macOS one, because it produces no error at
all.

**A Spark write to a local path leaves no usable data on the pod filesystem.** The
write itself does not fail, and Spark does not drop it: in a cluster each executor
resolves the path against its own filesystem and writes its partition there. The
driver's directory receives only the marker files. So the data may exist,
scattered across executor filesystems you cannot reach, which is not a backup:

<!-- verified: 2026-08-20 run against nmdc.results; every table printed a
     completed line and a correct row count, and no usable data landed. -->
```python
df.write.parquet("/home/<user>/backup/table.parquet")   # succeeds; nothing usable lands here
```

Observed on 2026-08-20 while exporting `nmdc.results`. The script printed a
completed line and a correct row count for every table, and every output directory
held 55 bytes. Seven directories, no data.

That is dangerous for a backup specifically, because what a failed backup leaves
behind is a set of plausible-looking directories with the right names. Anyone who
then deletes the source has lost it.

**The same trap catches reads, and it is more confusing from that side.** Reading
`file://$HOME/biosample_set.parquet` returns the schema correctly, because the
driver resolves it against its own filesystem, and then fails when an executor
tries to read the data:

<!-- verified: 2026-08-24 in the pod, reading file://$HOME/biosample_set.parquet
while verifying https://github.com/microbiomedata/nmdc-lakehouse/issues/278; the
schema returned correctly and the read failed on executor 8 as shown. -->

```
Lost task 0.3 in stage 3.0 (TID 6) (10.1.129.250 executor 8)
Caused by: java.io.FileNotFoundException:
  File file:/home/mamillerpa/biosample_set.parquet does not exist
```

Observed 2026-08-24 while verifying
[#278](https://github.com/microbiomedata/nmdc-lakehouse/issues/278) in the pod.
The delay between a correct schema and a missing file is what makes it cost
time: the first result looks like the read worked. Object storage is the only
ground the driver and the executors share, in either direction.

**Write to object storage instead**, which every executor can reach, using a
prefix that carries a timestamp so a rerun cannot overwrite an earlier one. Keep
it under the tenant staging area: `berdl_staging.py` rejects a bronze prefix
outside `tenant-general-warehouse/<tenant>/staging/`, and a listing of the tenant
on 2026-08-21 shows `datasets`, `projects`, `shared` and `staging` and no
`exports`, so a top-level export prefix is an unverified permission boundary
rather than an established one:

<!-- unverified: the identifier generation was run, producing 500 distinct
     values inside one second, but nobody has run this write against the tenant.
     Nothing tracks writing a tested export procedure. -->
```python
from datetime import UTC, datetime
from uuid import uuid4

# Generated when this runs, so copying the snippet cannot reuse an earlier run's
# path. The random suffix matters: a timestamp alone resolves to one second, so
# two exports started in the same second would share a prefix.
run = f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
prefix = f"staging/exports/{run}-results-backup"
df.write.parquet(f"s3a://cdm-lake/tenant-general-warehouse/nmdc/{prefix}/annotation_enzyme_commission.parquet")
```

**Then verify the destination holds data, not that the command returned.** The row
counts the writing job prints say nothing about where the bytes went, and in the
2026-08-20 run every one of them was correct. List the object store and check the
tables you exported by name, so one that produced nothing at all is noticed
rather than skipped, and make the check fail rather than only print.

**Check the prefix your run wrote, not a layout you assume.** Listing the tenant
on 2026-08-21 shows single objects:

```
30GiB   datasets/results/annotation_enzyme_commission.parquet
46GiB   datasets/results/annotation_kegg_orthology.parquet
```

Both are single-file uploads written by `mc.fput_object` from a locally built
Parquet file. Both come from `notebooks/ingest_ko_ec_annotations.ipynb`, which
names the two tables in cell 6 and uploads them in cell 8. They are not the output of the
`df.write.parquet` above, which is Spark's directory writer and produces a
directory of `part-*` objects instead. Nobody has run that write here, so this
document has no observation of its output to show you.

Single objects are the only layout observed in this tenant. The Spark layout is
expected rather than observed, and this document does not claim otherwise. That
is the reason not to hard-code a check to either shape: one is unverified here,
and the other describes objects a different tool produced. List the exact prefix
the run just wrote, and make the check fail rather than only print.

**Check bytes, not names.** A name appearing in a listing is not data. Spark's
writer creates a `_SUCCESS` marker, and a prefix holding that and nothing else
lists exactly like a prefix holding a table. So sum the size of the data objects
under each expected prefix, ignoring `_SUCCESS` and any other zero-byte marker,
and require that sum to be greater than zero for every table you asked for.

Non-zero bytes are not proof of a usable table either: a truncated or partially
committed write also has a size. Parse every Parquet footer under the prefix and
confirm each reports the schema you asked for. Note that Spark's directory writer
puts one footer in every `part-` object rather than one per table, so this is a
check on all of them, not on a single file.

**Valid parts still do not mean a complete table.** A partly committed write
leaves a subset of perfectly readable parts, and every content check above passes
on that subset. Compare the row count read back from the destination against the
source, and require it rather than offering it as a stronger option.

That is a minimum, not a proof. Equal counts establish matching cardinality and
nothing about which rows arrived: a duplicated or wrong row set of the right size
passes it with the right schema. Treat it as the floor an export has to clear
before anyone looks further, not as evidence the contents are correct.
`src/nmdc_lakehouse/berdl_staging.py` models that comparison in
`UpstreamTableVerification` at line 155 and performs it at lines 872 to 873,
which is the standard a staged table is already held to.

**None of this authorizes deleting a source, even when every check passes.** The
export lands in the tenant's own staging area, on the same platform as the table
it came from, and the next paragraph says no off-platform transfer is documented
here. A second copy beside the first is not an independent backup, so it does not
carry a deletion. Whatever its parts parse as, treat it as a staging artifact
until someone has performed and recorded a transfer off the platform. No issue
tracks doing that.

**Moving the data anywhere else is not documented here, deliberately.** The
transfer mechanics live in the historical transport section below, which needs
the SOCKS tunnels and a workstation `mc`, and the maintained path has neither, as
stated at the top of this document. Several tables are far too large to move to a
workstation in any case. `pfam_annotation_gff` is 2,684,369,000 rows,
`annotation_kegg_orthology` is 1,831,998,811 and `annotation_enzyme_commission`
is 1,231,453,377, and those are the ones that happen to have been measured rather
than a ranking. A driver-side `collect` is not decided by row count at all: what
has to fit is the size the rows take up once loaded into the driver's memory,
which depends on row width, nested and binary values, and per-object overhead. `annotation_statistics` at
4,815 rows is a candidate for one, not a case for one. Measure the bytes and
compare them against the driver's available memory before choosing that route.

**This section is not part of the maintained path**, despite sitting above the
boundary, because no reviewed plan command performs an export. The trap above was
observed. The export guidance is manual and nobody has run it end to end, so
treat it as a starting point that still needs verifying, not as a capability this
repository offers.

A complete, tested export procedure needs someone to perform one. Until then this
section records the trap and the rule, which are what cost a day on 2026-08-20,
rather than a runbook nobody has executed. No issue tracks writing one.

The trap itself, a Spark write to a local path producing no backup and reporting
nothing, was [#250](https://github.com/microbiomedata/nmdc-lakehouse/issues/250),
closed 2026-08-24.

## Preview and execute data staging with metadata

`stage-publication` applies and verifies the reviewed table and column metadata
as part of staging. Its root directory supplies all input and output paths.
See the [operator runbook](berdl-staging-runbook.md) for preparation, transfer,
runtime setup, planning and the metadata coverage at each level.

From the same pod checkout and environment that created the plan:

<!-- unverified: combined pod workflow awaits acceptance in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
.venv/bin/nmdc-lakehouse stage-publication "$PUBLICATION_ROOT"
.venv/bin/nmdc-lakehouse publication-status "$PUBLICATION_ROOT"
```

The preview checks the manifest, full validation, exact table coverage, metadata
plan, runtime and source hashes without service access. The status response
prints the execution command containing the exact snapshot ID and plan SHA-256.
Review the preview and metadata coverage before running that command. Both
`--authorize-snapshot` and `--authorize-plan-sha256`, together with `--execute`,
are required. They bind the destination and all approved descriptions as well as
the data. A changed plan needs a new review.

Execution first refuses an existing staging namespace or occupied object prefix.
It holds a Spark session through upload and metadata verification, so this
command does not require a separate notebook to keep Spark running. The reviewed adapter
uploads the manifested files, reads back and hashes the objects, and ingests them
with the stock KBase package. It independently counts each resulting catalog
table and requires the exact table set, source hashes and row counts before
recording data success. Every planned table/column description and schema
identity property is then applied as needed and verified by read-back.

The fixed evidence files are `kbase-ingest-outcome.json`,
`nmdc-staging-outcome.json`, and `nmdc-staging-metadata-outcome.json`, under
`evidence/`. Existing outcomes are never replaced. The preview includes explicit
missing descriptions, unsupported operations and deferred namespace operations.
The final response summarizes verified tables/columns, description gaps and
deferred namespace operations; detailed operations remain in the metadata plan.
Success is `data-and-table-metadata-verified`. A data outcome alone is not success.

Runtime diagnostics, including child-process output, go to a private
`evidence/staging-*.log`. Standard error prints its path and a heartbeat every
30 seconds; standard output is credential-free JSON. Retain failed attempts and
logs. An execution lock prevents two staging commands on the same run directory.
The command never promotes or changes canonical tables.

### Retry metadata after a partial staging run

Use the same preview, status and authorized execution commands. If the data
outcome and upstream verification match the original plan, but no metadata
outcome exists, the command previews and retries **metadata only**. It verifies
the same descriptions and properties without uploading or recreating tables.

A complete run returns its checked, saved status without another catalog write.
Status is based on recorded evidence and is explicitly **not a fresh catalog
audit**. Missing or altered evidence cannot establish completion.

If upload started but no valid data outcome exists, status is `partial-staging`.
The command refuses automatic replay: retain the namespace, object keys,
outcomes and private log; inspect the effects before selecting a new unused
destination. Do not delete an attempt marker to make a partial run look fresh.
A failed empty-destination check leaves no attempt marker and can be retried
once its cause is understood. Namespace and registry metadata remain deferred
under [issue 114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114).

## Plan separately authorized canonical promotion

`berdl-promotion-plan` reviews the staged metadata snapshot and its two derived
provenance tables together. Run it in the pod after both staging runs have verified
data and table metadata. It reads the live catalog and writes one immutable plan;
it performs no catalog writes. The canonical target is `nmdc.metadata`.

Use the original staging directories, including their immutable snapshots and
`evidence/` files. The parent may have been staged with an older NMDC adapter:
its saved input hashes and data/metadata outcomes are checked without rebuilding
that historical staging plan with the new adapter. No dump or parent staging
rerun is needed. The two sources must use the same official ingest revision and
destination, and the derived manifest must name the metadata snapshot as parent.
The parent manifest must have `full-mongodb-metadata-snapshot` scope. The derived
manifest must have `derived-provenance-snapshot` scope and contain exactly
`graph_edges` and `biosample_to_workflow_run`.

From the maintained NMDC checkout in the pod, with the matching source pair
already installed as described in the [runtime setup](berdl-staging-runbook.md#set-up-the-pod-runtime-once):

<!-- unverified: combined promotion awaits pod acceptance and exact-plan approval,
     tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/234 -->
```bash
.venv/bin/nmdc-lakehouse berdl-promotion-plan \
  /absolute/path/to/metadata-staging-run \
  /absolute/path/to/derived-staging-run \
  /absolute/path/to/evidence/combined-promotion.json \
  --ingest-checkout /absolute/path/to/reviewed-data-lakehouse-ingest \
  --recovery "Stop writers; inspect the saved before state and restore reviewed content manually. Dropped-table recovery is not proven."
```

Replace the example paths with the verified run's values. The installed source
pair must match the snapshots; setting an environment variable alone does not
change an already installed CLI environment. These pod commands need neither
Just nor another dependency installation. The workstation's equivalent Just
recipes remain available.
The planner checks both complete staging outcomes, rereads table counts and all
planned descriptions/schema properties, and captures current Iceberg `main`
snapshot references. It records current canonical schemas, counts, comments and
NMDC identity properties for before/after comparison and manual investigation.
Diagnostics stay in a private log; a heartbeat reports progress every 30 seconds.
The plan prints each add, replacement and removal, plus the exact destination
identity and plan digest needed for approval.

For the September pair the expected result is 48 canonical tables: 46 metadata
tables and two derived provenance tables. `graph_edges` is replaced;
`biosample_to_workflow_run` is added. This implementation permits only these
nine known obsolete TextValue helper removals, each listed as a separate action:

- `biosample_set_agrochem_addition`
- `biosample_set_air_temp_regm`
- `biosample_set_fertilizer_regm`
- `biosample_set_gaseous_environment`
- `biosample_set_host_diet`
- `biosample_set_humidity_regm`
- `biosample_set_perturbation`
- `biosample_set_phaeopigments`
- `biosample_set_watering_regm`

Each removal requires the corresponding `list<string>` field in the new
`biosample_set` Parquet schema and `array<string>` field in the actual staged
table that promotion will copy. Their values now live in string lists on that
parent table. Any other canonical table absent from both inputs causes refusal;
there is no generic retirement policy or wildcard removal. An already absent
obsolete helper needs no removal. This shape check does not independently prove
source losslessness; keep the source preservation audit separate.

### Performing the promotion

`berdl-promote` previews the saved plan locally unless all three authorizations
are present. Old single-snapshot plans and Spark rebuild instructions are refused.
Incorrect authorization values produce a correction message before any catalog
connection or execution journal is created.
Malformed or legacy plans report validation categories and direct the operator
to regenerate the combined plan, without printing submitted values.

<!-- unverified: combined promotion awaits pod acceptance and exact-plan approval,
     tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/234 -->
```bash
.venv/bin/nmdc-lakehouse berdl-promote /absolute/path/to/evidence/combined-promotion.json
```

Have Mark review this exact plan, the before state and the recovery limits before
execution. Coordinate a window without other canonical writers: per-table guards
cannot make a multi-table publication atomic. After approval, use the values
printed by the preview:

<!-- unverified: canonical execution requires approval of the exact combined plan,
     tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/234 -->
```bash
.venv/bin/nmdc-lakehouse berdl-promote /absolute/path/to/evidence/combined-promotion.json \
  --authorize-plan-sha256 DIGEST_FROM_REVIEWED_PREVIEW \
  --authorize-canonical-namespace nmdc.metadata \
  --authorize-destination-id DESTINATION_FROM_REVIEWED_PREVIEW
```

Execution checks the evidence, implementation and complete live before state
again before the first write, then checks each canonical target immediately
before changing it. Manifest, JSON evidence and Parquet artifact digests are
checked again at the end of source validation. Keep the original run directories
unchanged throughout planning and execution.
The retained Bronze Parquet objects must also remain available and unchanged.
For each table, planning checks the object's SHA-256 against the validated
manifest and compares its rows with the selected Iceberg snapshot using
[Spark's `EXCEPT ALL`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html)
in both directions. This preserves duplicate counts and ignores row order.
The object digest and live catalog state are checked again after comparison.
This independently verifies the selected snapshot even when an older staging
outcome did not record its Iceberg snapshot ID. A rewrite with different values
cannot pass just by preserving the count, schema and descriptions.

Preview and execution revalidation both perform full data comparisons, which
require distributed reads and shuffles. They reuse the retained source objects;
no new MongoDB export or transfer is needed. Missing or changed source objects
stop promotion. The private log and heartbeat cover these checks too.

Each copy reads the reviewed staging snapshot by its Iceberg
snapshot ID. Empty tables with no snapshot reference are copied as empty schema.
A single projection carries column descriptions and existing field metadata;
Spark's table writer supplies the table comment and NMDC identity properties at
table creation/replacement. This avoids a separate per-column canonical backfill.
Immediately before each copy, the command rechecks the staged table's current
snapshot, count, schema, descriptions and NMDC properties against the reviewed
state. This also catches metadata changes that leave the data snapshot unchanged.

The new destination snapshot ID is recorded by read-back, not assumed to equal
the source snapshot ID. The checks compare counts, column names/types, table and
column comments, and every copied NMDC identity property. Before a table is marked
verified, its read-back snapshot is also compared with the reviewed staged snapshot
using `EXCEPT ALL` in both directions. This catches altered or null key values and
changed duplicate counts, even when the table summary is unchanged. The canonical
state is rechecked after comparison. A failure stops further copies and helper
removal and leaves an incomplete journal; it does not undo earlier writes.

Planning compares every staged table's column names and types with the validated
Parquet using the writer's existing Arrow-to-Spark mapping. The staging plan and
validated manifest must each identify one Parquet artifact per table. The type
comparison ignores top-level field nullability because catalog loading can relax it; it does not compare
incompatible Arrow and Spark schema digests.

All replacements/additions and the complete canonical table set must verify
before obsolete helpers are dropped.
Removals use `DROP TABLE` without `PURGE`. Final verification checks the exact
canonical table set and rereads every copied table's data/metadata summary.
A successful result is `promotion-verified`. Namespace and registry metadata
remain deferred as documented above; richer LinkML constraints remain in the
schema and metadata bundle. Staging tables and object-store artifacts remain.

### Partial promotion and recovery limits

The command creates `combined-promotion.execution/` beside the reviewed plan.
It retains `before.json`, a private runtime log, and separate attempted/verified
records for every operation, including timestamps and the new catalog snapshot
identities. The before-state includes the ordered physical column names and types,
comments and NMDC properties, rather than only a schema digest.
Each operation's table name identifies its canonical before-state in
`before.json` under `before`; an absent entry means an addition. The pre-write
guard requires the live state to equal that recorded state. A verified drop has
`after: null`, meaning the read-back confirmed absence. This avoids duplicating
large schema/description records in every journal entry.

An operation recorded as attempted may have taken
effect even if verification failed or execution was interrupted. `failure.json`
names the latest attempt and completed verifications; `outcome.json` is written
only after all final checks pass. Directory creation prevents concurrent attempts
against the same plan, and any previous attempt refuses automatic replay.

There is **no automatic rollback or multi-table atomicity**. Saved snapshot IDs
are evidence, not backups or proof that a dropped table can be restored. The
September probe demonstrated `set_current_snapshot` for a same-schema table;
it did not prove changed-schema, dropped-table or multi-table recovery. See the
[probe record](berdl-promotion-probe.md) for those limits. Keep immutable Parquet,
staging tables and the before state. After failure, stop further publication,
inspect the journal and live catalog, and obtain a reviewed repair plan. Never
delete the journal to bypass the replay refusal. The metadata-copy path passed
a disposable 20-row add/replace and data/metadata readback check on 2026-09-25 at
`35a54c82`, including refusal of altered rows with the same count. Exact combined-plan
approval and canonical execution remain pending, tracked in
[issue 234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234).

## Running a script in the pod

Anything that touches the live catalog needs a Spark session, and a Spark
session means a pod. Preparation, `plan-publication` and previewing an existing
promotion plan read local files. Creating a combined promotion plan rereads the
live catalog and therefore needs the pod runtime. The method is not obvious and `labctl status` is misleading
about it: there is no programmatic exec, but the JupyterHub terminal in a
browser is a real shell in the pod.

Stage the file with `labctl pod put` rather than pasting it. For snapshot data,
use the bounded archive parts and checksum verification in the
[staging runbook](berdl-staging-runbook.md#send-to-the-pod). The maintained
`stage-publication` path reads a pod-local snapshot. The direct `mc` upload below
is a separate manual route and does not satisfy that input contract.

Use `python script.py` when the script builds its own session, which is what
`get_spark_session()` does; the inventory capture below is run that way. Use
`ipython script.py` only when the script relies on the interactive shell's
startup helpers to have a session already, because plain `python` starts without
them and the failure is an unbound name rather than anything about Spark.

**For a sequence of queries, keep one session alive instead.** Every
`ipython file.py` starts its own Spark Connect server, measured 2026-08-24 at
roughly 90 seconds before the first query runs, on every invocation. The log
prints `Starting Spark Connect server... ready at sc://localhost:15002` each
time. Four probe scripts that day paid it four times. Per-file execution is
right for a single self-contained job and wrong for iterating; `ipython`
interactively, or a notebook, pays the cost once.

**Scope any catalog survey.** Walking every catalog the token can see, with
`DESCRIBE EXTENDED` per table, ran past four minutes on 2026-08-24 without
finishing. Catalogs the token cannot read appear to cost time rather than
failing fast, so name the catalogs you need and treat an all-catalogs sweep as a
background job rather than something to wait on.

Redirect to a log and read that, rather than watching the terminal:

<!-- verified: 2026-08-24 used in the pod to run the column-comment probe for
https://github.com/microbiomedata/nmdc-lakehouse/issues/278; the grep returned
the four ANSWER lines quoted in docs/column-description-path.md. -->

```bash
ipython probe.py > probe.log 2>&1
grep -E 'ANSWER|Error|Exception' probe.log
```

A Spark stack trace in a browser terminal is long enough that scrolling back to
the cause is slow, and the useful lines are usually one `grep` away. Printing
answers with a distinctive prefix, as the 2026-08-24 probe did with `ANSWER-1`
through `ANSWER-4`, makes the result one line rather than a hunt.

Two things that waste time if nobody says them. Click the prompt line before
typing: an unfocused JupyterLab terminal accepts keystrokes and silently drops
them, so a pasted command can simply not arrive. And avoid pasting multi-line
input directly, because the terminal's handling of it is unreliable; that is
what `labctl pod put` is for.

## Move bulk data with `mc`, for a one-off transfer

`mc` reaches BERDL object storage directly from a workstation. Verified
2026-08-24 by uploading a whole snapshot in one command: 55 objects, 448 MiB,
checked against local byte counts, with no pod involved and no tunnels beyond
what `labctl up berdl` already provides.

**This does not replace the maintained transfer above, and swapping it in would
break the run.** `plan-publication` binds `--data-dir` to a resolved local
snapshot path (`berdl_staging.py:598-604`) and execution reads those Parquet
files from the pod filesystem, so the snapshot still has to be in the pod for
that command. Reading directly from object storage would require changes to the
plan and adapter. [Issue #353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353)
keeps that transport change separate from its first orchestration slice;
[issue #294](https://github.com/microbiomedata/nmdc-lakehouse/issues/294) owns
documentation of the operational findings.

Use `mc` for bulk data you are placing where the ingest reads from, or moving
off the platform, and for anything large that would otherwise be chunked through
the Jupyter contents API.

This needs an `mc` alias named `berdl-minio`, and this repository does not
create one. The 2026-08-24 transfer used an alias that was already configured,
so no bootstrap was run that day and none is verified here.

<!-- external-scripts: kbaseincubator/BERIL-research-observatory -->
The step that configures it is `bash scripts/configure_mc.sh --berdl-proxy`,
run from a **BERIL-research-observatory** checkout and not from this one;
neither `configure_mc.sh` nor `get_minio_creds.py` exists in this repository.
It is preceded there by
`eval "$(python scripts/get_minio_creds.py --bootstrap-remote --shell)"`, which
reads the credentials rather than setting the alias. Both appear under
[Historical off-cluster transport](#historical-off-cluster-transport), whose
preamble says its prerequisites belong to that section alone. That holds for the
tunnels and the Python environment; the alias is the one thing the `mc` path
needs from it, and neither of those two commands has a recorded run.
<!-- /external-scripts -->

<!-- verified: 2026-08-24 moved the complete 2026-08-21 snapshot this way and
confirmed it by comparing all 55 objects and 448 MiB at the destination against
the local byte counts, not by listing alone. The mc alias was already configured
rather than bootstrapped for the run. -->

```bash
mc cp --recursive /absolute/path/to/snapshot/ \
  berdl-minio/cdm-lake/tenant-general-warehouse/nmdc/staging/DATE/
```

**No proxy is set here, and that is what the 2026-08-24 run used**, from a
workstation where `labctl up berdl` had already made the storage reachable. It is
not what the off-cluster path below uses: that one prefixes every `mc` call with
`https_proxy=http://127.0.0.1:8123` against the SOCKS tunnels, because
`configure_mc.sh` is invoked with `bash` and a variable it exports cannot reach
the caller. So in a shell set up that way, a bare `mc` attempts direct access and
fails. Which of the two you need depends on how you reached BERDL, and neither
is a default: check before assuming this line works in your shell.

This is worth stating because the 2026-08-20 run did it the hard way: tarred the
snapshot to 368 MB, split it into four 100 MB chunks, pushed each through the
Jupyter contents API with `labctl pod put`, and reassembled in the pod. The
pieces were never deleted, so 736 MB of them sat in the pod home afterwards, and
the reason for the workaround survived only as five cryptic filenames.

The rule, with its boundary: **`labctl pod put` is for scripts and small files.
For a one-off transfer, or for data you are placing where something already
reads it from object storage, use `mc`.** It is not a substitute for the
maintained `stage-publication` inputs: that path binds `--data-dir` to a local
snapshot and the adapter reads those Parquet files from the pod filesystem
before uploading them itself (`berdl_staging.py:598-604`,
`berdl_adapter.py:241-251`), so removing the local copy would leave the command
without its required inputs.

Two traps, both hit on 2026-08-24.

**A leading path component is read as an alias.** `mc cp --recursive
local/snapshot/ dest/` fails with `dial tcp [::1]:9000: connection refused`,
because `local` is a configured alias pointing at `localhost:9000` rather than a
directory. Use an absolute source path.

**That failure reported success.** The `mc` error went to a log while the
shell's exit code came from a `tail` later in the pipeline, so the command
printed `exit=0` and moved nothing. It was caught only by counting objects at
the destination afterwards. **Verify a transfer by counting the destination against the source, never by the
exit code of a pipeline.** A listing alone is not enough: a partial transfer
leaves a plausible one, with the right prefix and some of the objects. The
2026-08-24 run was checked by comparing all 55 objects and 448 MiB against the
local byte counts, which is the check worth repeating.

## Qualify every table name with its catalog

A bare namespace resolves to `spark_catalog`, which is Hive, not the `nmdc`
Iceberg catalog. The failure names neither the catalog it chose nor the reason:

<!-- verified: 2026-08-24 in the pod, on nmdc_scratch.some_table; the bare name
resolved to spark_catalog and produced this error verbatim. -->

```
IllegalArgumentException: Cannot open table: path is not set
```

Write `nmdc.<namespace>.<table>`. Observed 2026-08-24 in the pod, on
`nmdc_scratch.some_table`.

This is worth knowing before it happens, because the error text contains no clue
that a catalog was involved, and searching it leads to Iceberg path configuration
rather than to name resolution. The repository defends against it where it can:
`derived_tables` and `berdl_promotion` both refuse a namespace that is not
catalog-qualified, rather than letting one through to be resolved by whatever
`spark_catalog` happens to hold.

The catalogs visible on 2026-08-24 were `bervodata`, `culturebotai`,
`globalusers`, `kbase`, `kescience`, `mamillerpa`, `microbialdiscoveryforge`,
`my`, `nmdc`, `refdata`, and `spark_catalog`. `nmdc` is the Iceberg one and the
only one this repository writes to. That listing is a snapshot of one day and is
not a contract; run `SHOW CATALOGS` rather than trusting it.

## Capture a fresh destination inventory without mutation

The offline publication planner requires current evidence from the selected
destination. Run the maintained audit script from a repository checkout in a
BERDL JupyterHub terminal, where `berdl_notebook_utils` is available. Supply
observed provider and table-format labels rather than copying the historical
Delta values from this guide.

**Name the catalog, in both places.** The namespace must be written
`<catalog>.<namespace>`, and `--provider` must be that same catalog. A bare
namespace resolves in whatever catalog the session is currently pointed at, and
nothing in the inventory records which one that was, so the artifact cannot show
where it looked. The script now refuses both a bare namespace and a provider
that names a different catalog than the one being read.

For NMDC the Iceberg catalog is `nmdc` and the live metadata namespace is
`nmdc.metadata`. `spark_catalog` is the legacy Hive catalog holding the Delta
copy, covered in
https://github.com/microbiomedata/nmdc-lakehouse/issues/248.

<!-- verified: run in the BERDL pod on 2026-08-24, producing a 49-table inventory.
     Only the inventory capture below is covered by this marker. -->
```bash
python scripts/python/audit_database_metadata.py nmdc.metadata \
  --publication-inventory /path/to/nmdc-metadata-destination-inventory.json \
  --destination-id nmdc-production \
  --provider nmdc \
  --table-format iceberg \
  --metadata-capability namespace \
  --metadata-capability table \
  --metadata-capability column
```

This mode performs only catalog descriptions, schema reads, and `COUNT(*)`
queries. It returns no production rows and does not upload, create, alter, or
drop anything. It checks the declared table format against every visible table
and fails without writing an inventory if any table, count, schema, or provider
cannot be observed completely. Counts can still require substantial read work
when a provider cannot answer them from table metadata.

The output contains only the logical destination identity, observation time,
reviewed provider and format labels, metadata capabilities, table names, row
counts, and metadata-free physical-schema fingerprints. It omits credentials,
connection details, locations, owners, comments, and data rows. Copy the JSON
back to the local candidate workspace, validate it through
`plan-publication`, and retain it with that plan as time-specific evidence. Do
not treat a previous inventory as the current live state.

---

<!-- external-scripts: kbaseincubator/BERIL-research-observatory -->
## Historical off-cluster transport

Everything from here to the end of the document is the April 2026 record. It was
verified working end-to-end on 2026-04-25 (see
[#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51)) but as a manual
workaround: SSH access to the tunnel host was blocked at the time, so the actual run
happened on-cluster through JupyterHub instead of following these steps start to
finish. Steps 1 to 7 were validated; step 8 was substituted with an on-cluster
notebook run. Steps 8 and 9 would need re-verifying if this ever ran off-cluster
again.

It moved local Parquet into BERDL Silver as `nmdc_nmdc_linkml_store`. The maintained
path above replaces it and needs none of what follows.

### Prerequisites for the historical transport

These five belong to this section only. The maintained path does not use them.

#### 1. Python 3.13 environment

`data-lakehouse-ingest` requires Python >= 3.13, which may not be your system default.

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv python install 3.13
uv venv .venv-berdl --python 3.13 --seed
```

#### 2. Ingest packages

From [`kbaseincubator/BERIL-research-observatory`](https://github.com/kbaseincubator/BERIL-research-observatory):

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
bash scripts/bootstrap_client.sh
bash scripts/bootstrap_ingest.sh
```

Both scripts belong to the external checkout and can change its dedicated
environment. Run them only when provisioning that checkout. Do not ignore a
failed verification; `just berdl-doctor` must subsequently find both required
distributions in `.venv-berdl`.

#### 3. MinIO client (`mc`)

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
mkdir -p ~/bin
curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o ~/bin/mc   # macOS: darwin-amd64 or darwin-arm64
chmod +x ~/bin/mc
```

#### 4. KBase auth token

`berdl-remote` reads `KBASE_AUTH_TOKEN`. Obtain and refresh it through the
supported KBase authentication workflow. Keep it in the process environment or
an untracked `.env`; never copy it into documentation, logs, or tracked files.

#### 5. SSH access to `login1.berkeley.kbase.us`

Required for the tunnels in the next section, and for nothing else. This blocked
the 2026-04-25 run entirely. It does not block the maintained path, which never
contacts the bastion. If you need it for a historical off-cluster run and do not have
an account, ask in `#ber_lakehouse`.

---

### Per-session: open the tunnels and configure `mc`

**Everything from this point through "Run the ingest notebook" runs from a
[`kbaseincubator/BERIL-research-observatory`](https://github.com/kbaseincubator/BERIL-research-observatory)
checkout, not this repo.** `.venv-berdl` and every `scripts/*.py`/`scripts/*.sh`
path below is relative to that checkout's root. `cd` there first.

Preflight and Upload metadata both need a path back into this repo's
`LAKEHOUSE_ROOT`. Capture it as an absolute path **before** changing
directories, since `LAKEHOUSE_ROOT`'s own default (`./lakehouse`) is relative
and would resolve against the wrong checkout once you've `cd`'d into
BERIL-research-observatory:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
# From the nmdc-lakehouse checkout, before cd'ing anywhere else:
export NMDC_LAKEHOUSE_DATA="$(realpath "${LAKEHOUSE_ROOT:-./lakehouse}")"
```

Two SOCKS tunnels reach BERDL's storage and compute from outside the cluster:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
ssh -f -N -o ServerAliveInterval=60 -D 1338 ac.<your-berkeley-username>@login1.berkeley.kbase.us
ssh -f -N -o ServerAliveInterval=60 -D 1337 ac.<your-berkeley-username>@login1.berkeley.kbase.us
```

Then configure the MinIO client through the proxy:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
source .venv-berdl/bin/activate
eval "$(python scripts/get_minio_creds.py --bootstrap-remote --shell)"
bash scripts/configure_mc.sh --berdl-proxy
```

`--bootstrap-remote` starts the JupyterHub server if it isn't already running and
reads MinIO credentials from it. `configure_mc.sh --berdl-proxy` sets `https_proxy`
to `http://127.0.0.1:8123` and configures the `berdl-minio` `mc` alias.

---

### Preflight

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
source .venv-berdl/bin/activate
python scripts/ingest_preflight.py \
    --data-dir "$NMDC_LAKEHOUSE_DATA" \
    --tenant nmdc --dataset nmdc_linkml_store \
    --mode overwrite --chunk-target-gb 20
```

All 13 tables should show as single-batch.

### Upload metadata

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
https_proxy=http://127.0.0.1:8123 ~/bin/mc cp --recursive \
    "$NMDC_LAKEHOUSE_DATA/metadata/" \
    "berdl-minio/cdm-lake/tenant-general-warehouse/nmdc/datasets/nmdc_linkml_store/metadata/"
```

`mc` interprets relative paths as MinIO URLs. Always use absolute local paths.

### Run the ingest notebook

The notebook itself isn't checked into either repo. It's a
[file attachment on issue #51](https://github.com/user-attachments/files/27073485/nmdc_linkml_store_ingest.ipynb),
adapted on-cluster during the 2026-04-25 run. Download it into your BERIL-research-observatory
checkout (or wherever you're running from) before executing:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
source .venv-berdl/bin/activate
jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.timeout=-1 \
    /path/to/nmdc_linkml_store_ingest.ipynb
```

Poll progress:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
https_proxy=http://127.0.0.1:8123 ~/bin/mc cat \
    "berdl-minio/cdm-lake/tenant-general-warehouse/nmdc/datasets/nmdc_linkml_store/_ingest_progress.jsonl"
```

### Verify in BERDL SQL

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
SHOW TABLES IN nmdc_nmdc_linkml_store;
SELECT COUNT(*) FROM nmdc_nmdc_linkml_store.biosample_set;
SELECT COUNT(*) FROM nmdc_nmdc_linkml_store.functional_annotation_agg;
```

---

### Known gotchas

- **`verify_ingest` reports MISMATCH for every table.** Not a real failure. It counts
  line breaks in source files, which is meaningless for binary Parquet. Trust the
  managed-table row counts from the SQL verification above instead.
- **Namespace naming.** Tenant `nmdc`, dataset `nmdc_linkml_store` -> registered
  namespace `nmdc_nmdc_linkml_store` (tenant prefix + dataset name).
- **`MODE=overwrite`** makes repeated runs idempotent, safe to re-run after a fresh
  `etl-collections`.

### Paths

- Bronze: `s3a://cdm-lake/tenant-general-warehouse/nmdc/datasets/nmdc_linkml_store/`
- Silver: `s3a://cdm-lake/tenant-sql-warehouse/nmdc/nmdc_nmdc_linkml_store.db`
- Progress log: `s3a://cdm-lake/tenant-general-warehouse/nmdc/datasets/nmdc_linkml_store/_ingest_progress.jsonl`

## Related

- `docs/publication-contract.md`: destination-neutral safety, metadata, staging, validation,
  promotion, and rollback requirements for a current replacement.
- [#50](https://github.com/microbiomedata/nmdc-lakehouse/issues/50): consolidated ETL output to `LAKEHOUSE_ROOT`, which is why this doc's paths are stable. Closed 2026-08-17.
- [#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51): the publication automation umbrella.
  The maintained `just stage-publication` command now stages and verifies data and table
  metadata in the pod; remaining operator simplification is tracked in
  [#353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353).
- `README.md`: where `LAKEHOUSE_ROOT` and the other ETL configuration variables are documented.
- `docs/mongodb-connection.md`: the upstream half (MongoDB to local Parquet).
- `docs/berdl-metadata-shaping.md`: what you can set beyond the raw data once it's here.
