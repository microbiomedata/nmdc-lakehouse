# Stage a validated NMDC snapshot in BERDL

Use this page to follow the current path and locate its evidence. The detailed
command arguments and recovery procedures are in [BERDL upload](berdl-upload.md).
The [2026-09-23 run record](runs/2026-09-23-production-staging.md) distinguishes
what has passed in production from what remains unverified.

## The operator sequence

For a new production export, follow the
[source-version selection procedure](source-schema-1124-rollout.md#select-production-now-and-switch-after-migration)
and [complete production dump instructions](mongodb-connection.md#complete-production-dump-in-two-terminals).
The September source was 11.23.0; the repository default is 11.24.0. In each
shell that installs or runs the source-dependent commands, explicitly select
the intended version. For the recorded production run:

<!-- verified: 2026-09-23 the operator selected 11.23.0, installed that source pair, and passed production preflight. -->
```bash
export NMDC_SCHEMA_VERSION=11.23.0
just install-all
just source-preflight
```

Preflight validates the selected pair; it does not choose the source version.
When continuing from an already validated snapshot, reuse it and its report and
start with metadata preparation or transfer instead of repeating export.

| Phase | Where | Existing command or operation | Completion evidence |
| --- | --- | --- | --- |
| Export | Machine with MongoDB access | Explicit version selection and installation above; `just source-preflight`, then `just etl-collections` | Completed snapshot manifest, ETL metrics, Parquet files |
| Validate rows | Same machine | `just validate-target-rows` with the snapshot, report path, and `--mode full` | Successful report bound to the snapshot and target schema |
| Prepare metadata | Machine holding the snapshot | Follow the [profile and bundle commands](publication-contract.md#metadata-bundle): `metadata-profile`, `metadata-bundle` | Approved content, schema-derived descriptions, explicit coverage gaps |
| Transfer | Workstation to BERDL pod | Transfer the snapshot and evidence; verify hashes in the pod | The same manifest and file hashes; no row validation rerun |
| Observe destination | BERDL pod | `scripts/python/audit_database_metadata.py` with `--publication-inventory` | Fresh inventory of the explicitly selected catalog and namespace |
| Plan | BERDL pod | [Publication dispositions](publication-contract.md#table-disposition-plan), [metadata operations](publication-contract.md#metadata-application-plan), and the [staging command plan](berdl-upload.md#build-the-maintained-staging-command-plan) | Consistent evidence, dispositions, metadata operations, pinned execution environment |
| Preview | Same pod and environment | `berdl-upload` without `--execute-staging` | Exact command and metadata coverage, with no destination writes |
| Stage and verify | Same pod and environment | `berdl-upload --execute-staging` with the reviewed snapshot and plan digests | Data outcome, metadata outcome, and combined coverage report |

The unprefixed names above are `nmdc-lakehouse` subcommands. Several also have
`just` wrappers. This is the current multi-command workflow; a single setup or
resume command does not exist yet.

Full target validation checks generated-table conformance. It does not prove
that every populated MongoDB source value was retained. Keep the focused source
preservation audit separate from the staging outcome.

## Access and runtime

The export needs MongoDB read credentials and a route to the source, such as
the [production tunnel](mongodb-connection.md). After the dump, local validation
and BERDL staging use the saved snapshot. They do not need that tunnel. A later
source preservation audit needs MongoDB access again.

Transfer through JupyterHub needs a Hub API token or an authenticated browser
session. Execution needs a running BERDL pod, its authenticated KBase session,
and permissions to read the selected catalog and write the selected tenant
staging namespace and object prefix. Keep credentials in the established
environment; never put them in the run configuration or evidence files.

Use a clean official `kbase/data-lakehouse-ingest` checkout at the revision
accepted by `berdl-upload-plan`. The September run used v0.1.5 at
`a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3`. BERIL source code is not a runtime
dependency of this maintained path.

The pod interpreter must supply Python 3.13 and the BERDL runtime packages.
An ordinary isolated Python environment may import the NMDC CLI but fail later
when it needs Spark or the object-store client. The September run created its
environment with `--system-site-packages`, installed the locked 11.23.0 source
extra, and checked actual runtime imports before planning. Its inherited pod
packages are not completely pinned by the NMDC lockfile. Record that limitation
and the tested environment; do not describe it as a fully reproducible container.

Pip may inherit a user-install default from the pod. When installing into an
isolated tool environment, pass `pip install --no-user` explicitly. This avoids
the observed `User site-packages are not visible in this virtualenv` failure
without changing the pod's global pip configuration.

Keep a Spark session alive during execution. A notebook session or an outer
IPython process can provide that lifetime while the maintained CLI runs. See
[running a script in the pod](berdl-upload.md#running-a-script-in-the-pod).

For the two [derived provenance tables](local-provenance.md#validate-and-prepare-separate-staging-evidence),
use their separate snapshot throughout these steps. Row validation and staging
planning select its independent provenance schema. Its manifest retains the
parent metadata snapshot identity; keep both snapshots and their evidence.
Do not combine their files or reuse the collection snapshot's validation report.

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

`data-and-table-metadata-verified` requires both phases of `berdl-upload` to
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

| Observed state | Next action |
| --- | --- |
| Bootstrap failed; no staging plan | Repair setup and reuse the transferred files and clean checkouts |
| Plan exists; preview failed | Inspect the saved plan and error; do not silently replace the plan |
| Upload or verification failed | Preserve object keys, namespace, logs, and any outcomes; review partial effects before an explicit retry |
| Data verified; metadata failed | Use `berdl-apply-metadata` with the original staging plan and exact data outcome; do not reload the data |
| Both phases verified | Record the outcomes; stop at staging unless canonical promotion is separately authorized |

A fresh run should use an unused staging namespace and object prefix. Replaying
the same authorized plan can overwrite its staging destination. A retry therefore
needs an explicit decision based on the saved state; it is not an automatic
restart of the whole workflow. See the
[metadata-only recovery procedure](berdl-upload.md#retry-metadata-after-a-partial-staging-run).

## Simplification target

[Issue #353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353) owns the
next implementation slice: one credential-free run configuration, one maintained
entry point with separate prepare and execute modes, and a status command that
gives the next action. The target is at most two invocations after pod setup,
without run-specific script editing or repeated paths and digests to copy.

Reuse the current validators, plans, immutable outcomes, and automatic metadata
verification. Resume from completed phases rather than repeating export or
validation. Transport changes, package slimming, remaining namespace metadata,
and canonical promotion stay separately scoped. The proposed interface is not
available yet; do not mistake this target for a command that can be run today.
