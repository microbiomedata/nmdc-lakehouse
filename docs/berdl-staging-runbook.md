# Stage a validated NMDC snapshot in BERDL

Use this page to follow the current path and locate its evidence. Start with
[the complete lifecycle](publication-lifecycle.md) for schema releases, derived
tables, prerequisites, promotion and cleanup. The detailed
command arguments and recovery procedures are in [BERDL upload](berdl-upload.md).
The [2026-09-23 run record](runs/2026-09-23-production-staging.md) distinguishes
what has passed in production from what remains unverified.

## The operator sequence

Start with the preparation command below. Its configuration selects the source
package and determines whether to reuse an existing snapshot or export a new one.
The September production source was 11.23.0; the repository default is 11.24.0.
For source migration decisions, see
[source-version selection](source-schema-1124-rollout.md#select-production-now-and-switch-after-migration).
A saved full validation report avoids another row-validation run.

| Phase | Where | Command or operation | Completion evidence |
| --- | --- | --- | --- |
| Prepare | Workstation | `just prepare-publication CONFIGURATION OUTPUT` | Immutable snapshot, full validation, descriptions and receipt |
| Send | Workstation to pod | `just publication-transfer pack ...`, then `send ...`; receive in pod | Inventory, part/archive hashes and exact prepared file set |
| Plan | Pod | `plan-publication ROOT CONFIGURATION` | Dispositions, preflight, metadata plan and staging plan |
| Stage | Same pod/runtime | `stage-publication ROOT`, then its authorized execution | Data and table metadata outcomes |
| Status/resume | Same pod/runtime | `publication-status ROOT`; repeat stage for metadata-only retry | Checked saved evidence and next action |

Pod commands above are subcommands of `.venv/bin/nmdc-lakehouse` from the NMDC
checkout; each also has a `just` recipe. Runtime setup and the read-only inventory
are explicit prerequisites below. There is no run-specific Python or shell script
to edit. The new combined pod commands await live acceptance under
[issue 353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353).

Full target validation checks generated-table conformance. It does not prove
that every populated MongoDB source value was retained. Keep the focused source
preservation audit separate from the staging outcome.

## Prepare on the workstation

Use a checkout with `just` and `uv` installed, as in
[development setup](development-setup.md). Write a JSON configuration such as
`local/publication.json`. Input paths are relative to that configuration file;
replace these example paths with your existing snapshot and report:

```json
{
  "source_version": "11.23.0",
  "snapshot": "existing-metadata-snapshot",
  "target_validation": "existing-full-validation.json",
  "namespace": {
    "name": "nmdc.metadata",
    "title": "NMDC metadata",
    "description": "Flattened NMDC production metadata.",
    "documentation_url": "https://github.com/microbiomedata/nmdc-lakehouse",
    "properties": {"collection": "nmdc", "role": "metadata"}
  }
}
```

The recipe selects and installs the locked source pair named by `source_version`
through the existing source selector. It does not require another export of
`NMDC_SCHEMA_VERSION`. Direct CLI use requires the matching pair to be installed
already. Keep credentials in the existing environment or `.env`; the JSON is
reviewed metadata, not a credential file.

<!-- verified: 2026-09-23 reused the derived production snapshot and its full report;
     fresh MongoDB export and pod transfer remain unverified, tracked in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
just prepare-publication local/publication.json local/prepared-publication
```

This validates and copies the existing snapshot, checks and retains the full
report, and creates the profile and bundle. It never reruns row validation when
a valid full report is supplied. The source snapshot remains unchanged. Table
and column descriptions, schema identities, checksums, and the derived parent
snapshot identity remain in the copied files and generated evidence. Missing
descriptions remain explicit gaps. The command reports progress every 30 seconds
during long phases. On 2026-09-23 this entry point prepared the existing derived
production snapshot: two tables, 194,562 rows, and all 15 column descriptions. It
retained the exact full-validation report and parent snapshot identity; no new
dump, row validation, or catalog operation ran.

For a **new MongoDB dump**, omit `snapshot` and `target_validation`, retain the
namespace content, and establish the MongoDB credentials and tunnel documented
in [MongoDB connection](mongodb-connection.md). The command exports every
eligible collection, keeps empty columns, creates the completion manifest, and
performs full row validation. `source_label` defaults to `nmdc-production` and
can identify another source. Export diagnostics stay in `OUTPUT/export.log`.
No BERDL credentials are needed during preparation.

For an existing reviewed profile, use `"profile": "path/to/profile.json"`
instead of `namespace` and `overrides`. Otherwise, optional `overrides` follow
the [metadata contract](publication-contract.md#metadata-bundle). A profile or
report must describe the supplied snapshot; mismatched evidence is rejected.

The output directory contains `snapshot/`, `evidence/target-validation.json`,
`evidence/metadata-profile.json`, `evidence/metadata-bundle.json`, and a
`preparation.json` receipt with snapshot identity, counts, and evidence hashes.
The output directory must be new or empty and private (mode `0700`). Symlinked
input files and snapshot directories are refused. Send the snapshot and evidence
using the next steps in this runbook.

Preparation never resumes a non-empty output directory, including one containing
only a lock left after interruption. Keep interrupted output: when its snapshot
manifest validates, name that snapshot and any successful full report explicitly
in a new configuration/output directory. This reuses the dump and validation
without repeating them. A dump without a valid completion manifest needs a new
export directory. For corrected descriptions, also use a new directory and the
previous successful full report. Never write preparation output inside the
immutable source snapshot.

## Send to the pod

The dump needs MongoDB read credentials and the production route/tunnel. Once it
is complete, preparation of saved files and BERDL staging do not need that tunnel.
A later source preservation audit needs MongoDB access again.

The send step needs an authenticated JupyterHub session. These commands use the
operator's existing `labctl pod put LOCAL_FILE REMOTE_FILE`, whose remote paths
are relative to that user's pod home. `labctl` is a separately installed operator
tool, not a Python dependency of this repository. Configure it for your account;
the authenticated Jupyter file browser can transfer the same files instead.
BERDL execution additionally needs the pod's KBase session and permission to read
the catalog and write the selected staging namespace and object prefix. Keep all
credentials in their established environment, outside configurations and evidence.

From the lakehouse checkout, package a prepared publication into a new durable
transfer directory, then send it. This single standard-library Python helper
runs on both the client and pod; no Python package installation is needed for
transport. It includes only manifest-owned snapshot files, the preparation
receipt, and the three receipt-bound evidence files. It excludes runtime logs,
credentials, unrelated files and machine-bound plans. Each transfer has unique
filenames and ordered parts of at most 64 MiB.

<!-- unverified: transport unit tests pass; actual labctl transfer and pod receipt await acceptance in https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
just publication-transfer pack local/prepared-publication local/publication-transfer
just publication-transfer send local/publication-transfer --sha256 SHA256_PRINTED_BY_PACK
```

Pack verifies the prepared file hashes and archived contents before publishing
the transfer inventory. It prints the inventory checksum and a complete send
command carrying that checksum. Use that printed command, or replace
`SHA256_PRINTED_BY_PACK` above with the value printed by pack; do not recompute it
from a changed inventory. Send verifies that original checksum before parsing
the inventory or uploading anything. It checks the parts and helper, invokes the existing
`labctl pod put`, and uploads the inventory last. A failed send can be retried
from the same unchanged transfer directory; this sends the same transfer
files again, not lakehouse tables. No catalog or object-store operation runs here.

A failed pack can leave a helper or parts without a completed inventory. Keep
that directory for diagnosis and use a new pack output directory; send requires
the completed inventory and its original checksum. Temporary archive files close
automatically. After diagnosis, remove only the exact failed transfer directory,
keeping the original prepared publication and any verified received copy.

Send prints a command for the pod home directory. Its `sha256sum` gate checks
the helper against the sender's printed digest **before executing Python**;
this uses the pod's existing checksum utility. Copy that complete command,
replace only `NEW_PUBLICATION_DIRECTORY` with a new durable path, and run it.
The names and SHA-256 in this example are placeholders for the printed values:

<!-- unverified: actual pod receipt awaits acceptance in https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
cd "$HOME"
printf '%s  %s\n' PRINTED_HELPER_SHA256 nmdc-transfer-UNIQUE.py | sha256sum -c - && python3 nmdc-transfer-UNIQUE.py receive nmdc-transfer-UNIQUE.json nmdc-publication-reviewed --sha256 PRINTED_INVENTORY_SHA256
export PUBLICATION_ROOT="$HOME/nmdc-publication-reviewed"
```

Local pack/receive was exercised on the existing derived candidate on
2026-09-25: all eight selected files retained their hashes, and the existing
package validator accepted the received two-artifact snapshot. This did not
contact the pod or repeat target-row validation.

Send and receive refuse an inventory reached through a symlinked parent before
reading it. Receive also checks the adjacent and executing helper against the inventory's
script digest before creating output. It checks the sender's inventory digest, each part and the complete
archive, requires the exact archive member set, and checks the extracted file
hashes against both the transport inventory and preparation evidence. It writes
regular files only beneath a new private directory and refuses path traversal,
symlinked parents and existing output. A failed receive can leave partial files:
keep them for diagnosis and use a new output directory. Never stage a partial
receive. Package-level planning still verifies snapshot semantics, Parquet
footers and full validation; a transport check does not replace those checks.

Keep the local original and verified pod copy. Retain transfer parts until
verification and planning succeed; later cleanup follows the
[lifecycle retention boundaries](publication-lifecycle.md#8-routine-cleanup-and-optional-historical-retirement).
This helper accepts prepared publications, not historical run-specific archives.
Reuse the September parent transfer as recorded rather than packaging it again.

## Set up the pod runtime once

Use a new durable runtime checkout for this publication. Existing reviewed plans
bind their interpreter and adapter, so do not update a runtime serving an older
plan. Clone merged NMDC code at the exact reviewed commit and the approved official
ingest revision. Replace `REVIEWED_NMDC_COMMIT` below; do not use an unreviewed
floating main for a previously approved plan. This one-time shell setup remains
manual; a reusable bootstrap is still part of issue #353.

<!-- unverified: combined pod workflow awaits acceptance in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
mkdir -p "$HOME/nmdc-publication-runtime" &&
cd "$HOME/nmdc-publication-runtime" &&
git clone https://github.com/microbiomedata/nmdc-lakehouse.git &&
cd nmdc-lakehouse &&
git checkout --detach REVIEWED_NMDC_COMMIT &&
git rev-parse HEAD &&
git clone https://github.com/kbase/data-lakehouse-ingest.git ../data-lakehouse-ingest &&
git -C ../data-lakehouse-ingest checkout --detach a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3 &&
python3 -c 'import sys; assert sys.version_info[:2] == (3, 13), "Use the pod Python 3.13"' &&
python3 -m venv .tools &&
.tools/bin/python -m pip install --no-user uv==0.12.17 &&
export PATH="$PWD/.tools/bin:$PATH" &&
uv venv --system-site-packages --python "$(command -v python3)" .venv &&
export NMDC_SCHEMA_VERSION=11.23.0 &&
bash scripts/uv_with_source.sh sync --locked &&
export PATH="$PWD/.venv/bin:$PATH" &&
.venv/bin/python -c 'from berdl_notebook_utils.setup_spark_session import get_spark_session; from berdl_notebook_utils.clients import get_s3_client; import nmdc_lakehouse'
```

Select the version recorded in `preparation.json`; 11.23.0 above is the September
production source, not a permanent default. `--system-site-packages` retains the
pod's Spark/object-store libraries. These inherited packages are not completely
pinned by the NMDC lockfile. `--no-user` avoids the pod's inherited pip user-install
setting. The ingest revision above is the supported stock v0.1.5 revision; the
planner refuses unreviewed revisions. BERIL is not a runtime dependency.

Capture a fresh read-only inventory with the maintained audit script, with raw
runtime diagnostics kept in a private log:

<!-- unverified: combined pod workflow awaits acceptance in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
umask 077
.venv/bin/python scripts/python/audit_database_metadata.py nmdc.metadata \
  --publication-inventory "$PUBLICATION_ROOT/evidence/destination-inventory.json" \
  --destination-id nmdc-production --provider nmdc --table-format iceberg \
  --metadata-capability namespace --metadata-capability table --metadata-capability column \
  > "$PUBLICATION_ROOT/evidence/inventory.log" 2>&1
```

This can take minutes because it counts catalog tables. It changes no lakehouse
tables. Stop if it fails; do not use a partial or historical inventory. The
[inventory reference](berdl-upload.md#capture-a-fresh-destination-inventory-without-mutation)
describes its checks. Write the [destination JSON](berdl-upload.md#build-the-maintained-staging-command-plan)
with the inventory and ingest checkout paths above, plus an unused namespace and
object prefix. All paths in that JSON are relative to the JSON file unless absolute.
Then plan, preview and inspect status from this checkout:

<!-- unverified: combined pod workflow awaits acceptance in
     https://github.com/microbiomedata/nmdc-lakehouse/issues/353 -->
```bash
.venv/bin/nmdc-lakehouse plan-publication "$PUBLICATION_ROOT" /absolute/path/to/destination.json
.venv/bin/nmdc-lakehouse stage-publication "$PUBLICATION_ROOT"
.venv/bin/nmdc-lakehouse publication-status "$PUBLICATION_ROOT"
```

After reviewing the preview and metadata application plan, run the exact
`next_command` from status using `.venv/bin/nmdc-lakehouse` in this environment.
The command requires the reviewed snapshot ID and plan checksum plus `--execute`.
It retains Spark for the run, keeps raw diagnostics in a private log, and reports
a heartbeat every 30 seconds. It refuses occupied destinations before upload.

For the two [derived provenance tables](local-provenance.md), use their separate
snapshot and full report throughout. Their manifest records the parent metadata
snapshot identity. Do not combine their files or reuse the parent's validation.

## One durable run directory

Keep the immutable snapshot separate from the evidence and runtime checkouts.
The evidence directory contains the profile, bundle, inventory, policy,
publication plan, metadata plan, target-validation report, final staging plan,
and outcomes. Preserve failed-attempt logs alongside successful evidence.
Neither the snapshot nor the clean ingest checkout is an outcome directory.

Build the final staging plan in the pod where it will execute: it binds
absolute paths, an interpreter, adapter bytes, and the official ingest sources.
Portable evidence can be created on a workstation and transferred first.

The current adapter reads a pod-local snapshot and uploads those files itself.
A direct workstation-to-object-store copy does not satisfy its local-path input
contract. For large contents-API transfers, use bounded parts and verify the
reassembled archive and manifested files. This workaround is recorded explicitly;
it is not a claim that the current transport is simple.

Workstation paths such as `/Users/mam/...` and pod paths such as
`/home/mamillerpa/...` in the dated record belong to that operator's machines.
Use your own durable directories. Temporary storage is for disposable
intermediates, not the only copy of scripts or verification evidence.

## What success means

`data-and-table-metadata-verified` requires both phases of `stage-publication` to
finish. A `data-verified` file alone means the metadata phase may still need work.

| Level | What the maintained command retains or checks | Limit to report |
| --- | --- | --- |
| Snapshot | Source/target schema identities, software provenance, artifact hashes, validation binding | Not a point-in-time MongoDB snapshot or a source-losslessness proof |
| Object and file | Uploaded Parquet bytes are read back and hashed; footer metadata survives; the object gets an `nmdc-sha256` user metadata field | The whole metadata bundle is not expanded into object-store user metadata |
| Namespace and dataset | Approved content remains in the bundle and plan | Namespace operations are deferred; registry and tenant metadata writes are not implemented |
| Table | Planned descriptions, snapshot ID, and target-schema identity are applied and read back | Coverage follows the reviewed plan |
| Column | Every planned description is read back; existing correct comments need no rewrite | Missing source descriptions stay explicit; richer LinkML constraints remain in the bundle/schema |
| Row and relationship | Exported identifiers, record types, helper parent references, and ordering remain data | No new cell-level annotations are inferred |

For the September candidate, that means 46 table descriptions, 1,994 column
descriptions, 23 missing column descriptions, and nine deferred namespace
operations. Those are dated measurements, not defaults for later snapshots.

## Resume at the failed phase

Run `publication-status ROOT` to validate the local evidence and see the next
action. This reports recorded verification; it does not reread the live catalog.
Each status includes expected plan/data/metadata outcome paths and existing
private log paths. A listed expected path does not assert that the file exists;
the status indicates which phase has completed.

| Status or observation | Next action |
| --- | --- |
| `prepared` | Preparation is verified; planning has not completed. In the pod, run/repeat plan with the same configuration after correcting setup; retain the snapshot and full report |
| Plan failed before completion | Correct setup and repeat plan with the same inputs; changed inputs require a new prepared directory |
| `planned` | Preview, review, then execute the exact authorized staging command |
| `partial-staging` | Retain all artifacts and inspect the private log; automatic data replay is refused |
| `data-verified-metadata-pending` | Repeat stage with the same authorization; it retries only metadata |
| `data-and-table-metadata-verified` | Retain the outcomes; repeated stage returns checked evidence without writes |

A lock refuses concurrent stage invocations on the same directory. Saved plans
and outcomes are immutable. Metadata retry verifies every planned description
and property; it skips values already correct. Failed uploads require inspection
and a new unused destination, not deletion of evidence to bypass the guard.
After both staging runs pass, use the [combined promotion procedure](berdl-upload.md#plan-separately-authorized-canonical-promotion)
to review the parent snapshot and derived pair together. It carries table and
column metadata into canonical tables and verifies the result; it requires Mark's
approval of the exact plan and explicitly records the recovery limits.
Live acceptance remains [issue 234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234).
The September parent snapshot is already staged and must not be reloaded solely
to test this new workflow. Its separately prepared derived pair is the next pod
acceptance run, tracked in [issue 341](https://github.com/microbiomedata/nmdc-lakehouse/issues/341).
