# Uploading `lakehouse/` Parquet output to BERDL

This document contains two different things, and reading one for the other wastes
time. The boundary is the "Historical off-cluster transport" heading:

- **Everything above it is the maintained path.** It runs inside a BERDL
  JupyterHub pod, uses the reviewed plan commands in this repository, and is what
  a current staging load follows.
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

From there, `berdl-upload-plan`, `berdl-upload`, `berdl-apply-metadata`, and the
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
capability probe both ran successfully. See
[#244](https://github.com/microbiomedata/nmdc-lakehouse/issues/244).

There is no standalone check that the bucket accepts writes, on either path.
`berdl-doctor` does several things, but none of them contact an object store: its
`mc` check confirms the binary is present and reports a version, and stops there.
Write access is exercised by the staging run itself, which writes to the bronze
prefix from inside the pod where the object store is local, so a permissions problem
surfaces as a failed run rather than as a preflight result.

What the run verifies afterwards is a different thing. Its outcome check compares
destination row counts against the source Parquet and the source digest against the
snapshot manifest, which establishes that the reviewed bytes are what landed. It
does not re-read the written objects and compare them byte for byte. Keep the
reviewed bronze prefix inside the tenant staging area, where the pod's session is
expected to have write access, so the first write is not also the first surprise.

---

## Supported readiness check

`berdl-doctor` validates the completed snapshot and reports on the external
tooling, without changing either repository or contacting BERDL. Run it on either
path:

```bash
uv run --no-sync nmdc-lakehouse berdl-doctor /absolute/path/to/completed-snapshot
```

`--beril-checkout` is optional. Without it the snapshot is still validated, and the
two checks that inspect a BERIL checkout report `SKIP` with a note saying the
maintained path does not use one. A skipped check does not affect the exit status,
so an operator on the maintained path is not told the doctor failed for something
they do not need.

To validate only the snapshot, with nothing else reported:

```bash
uv run --no-sync nmdc-lakehouse validate-snapshot /absolute/path/to/completed-snapshot
```

For a historical off-cluster run, supply the checkout and the two skipped checks
become real:

```bash
export BERIL_CHECKOUT=/path/to/BERIL-research-observatory
export BERDL_DESTINATION_ID=nmdc-production
export BERDL_CATALOG=discovered-catalog
export BERDL_TABLE_FORMAT=discovered-table-format
just berdl-doctor /absolute/path/to/completed-snapshot
```

If `just` stops while parsing a malformed repository `.env`, bypass its dotenv
loading so the doctor can report the sanitized configuration failure itself:

```bash
uv run --no-sync nmdc-lakehouse berdl-doctor \
  /absolute/path/to/completed-snapshot
```

`BERIL_CHECKOUT` must be explicit; the command does not guess a user-specific
checkout location. It validates the snapshot manifest offline, identifies the
checkout revision, checks for the required ingest resource paths, requires
Python 3.13 in its `.venv-berdl`, checks the `data-lakehouse-ingest` and
`berdl-remote` distributions, and checks `mc`. It also checks for
`KBASE_AUTH_TOKEN` by name in the process, this repository's `.env`, or the
configured BERIL checkout's `.env`. No value is printed or tested. Refresh the
short-lived token through the supported KBase workflow immediately before a
publication attempt.

These checks identify the selected external revision and its locally available
interfaces; they do not certify live-ingest compatibility. Pin and test a BERIL
revision containing the required source-verification and credential fixes
before authorizing publication.

After generating and reviewing the snapshot-bound metadata bundle, fresh live
inventory, and disposition plan, generate the provider-neutral metadata
application plan for the explicitly selected staging namespace:

```bash
just metadata-application-plan \
  /absolute/path/to/metadata-bundle.json \
  /absolute/path/to/destination-inventory.json \
  nmdc.nmdc_metadata_staging_20260819 \
  --output /absolute/path/to/metadata-application-plan.json
```

Use the exact `<tenant>.<dataset>` staging namespace that the later
`berdl-upload-plan` invocation supplies; the example is not a permanent BERDL
default. Review supported operations, unsupported operations, and missing
descriptions. This offline command emits JSON data, not Spark SQL, and does not
contact or change BERDL. The later adapter tracked in
[#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) must recheck
the bundle and inventory identities before applying the plan.

Then run the destination-neutral artifact gate from the `nmdc-lakehouse`
checkout:

```bash
just publication-preflight /absolute/path/to/completed-snapshot \
  /absolute/path/to/metadata-bundle.json \
  /absolute/path/to/destination-inventory.json \
  /absolute/path/to/publication-plan.json
```

This command is offline and non-mutating. It proves that the independently
reviewed artifacts still identify the same snapshot and destination observation
and that their table evidence and coverage agree. It neither contacts BERDL nor
authorizes the historical upload steps below.

## Build the maintained staging command plan

After reviewing the successful preflight and metadata-application plan, bind
them to a clean checkout of the official
[`kbase/data-lakehouse-ingest`](https://github.com/kbase/data-lakehouse-ingest)
package at the exact revision selected for staging:

```bash
just berdl-upload-plan \
  /path/to/completed-snapshot \
  /path/to/metadata-bundle.json \
  /path/to/destination-inventory.json \
  /path/to/publication-plan.json \
  /path/to/metadata-application-plan.json \
  /path/to/target-validation-report.json \
  /path/to/data-lakehouse-ingest \
  a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3 \
  nmdc \
  nmdc_metadata_staging_20260819 \
  cdm-lake \
  tenant-general-warehouse/nmdc/staging/20260819 \
  tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl \
  tenant-general-warehouse/nmdc/staging/20260819/config.json \
  /path/to/berdl-staging-plan.json
```

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
records the reviewed `spark_catalog` provider and `iceberg` table format used by
the selected official ingest path; missing or incompatible labels fail closed.
BERIL Research
Observatory remains an optional operator resource and is not a runtime or
release dependency of this workflow.

The generated command intentionally omits the live execution flag and outcome
path. Do not add them by hand.

Applying descriptions reports progress on stderr as it goes, naming the table it
is on, the columns verified so far against the total, how many of those it
actually wrote, elapsed time, and an estimate of the time remaining. A
description the catalog already holds is not written again, so a rerun after a
partial failure finishes the remainder instead of redoing the whole table. Every
planned description is still verified by read-back, whether or not this run wrote
it, which is why the verified and written counts differ on a rerun. The estimate
is rated on written columns only: a skip costs a catalog read and a write costs a
catalog commit, so counting them together would look fast while skipping and be
wrong as soon as writing resumed. Standard output stays reserved for the parseable
outcome JSON. Expect the run to be dominated by the widest table: descriptions
are applied one column at a time and each is a separate catalog commit, so a
table with over a thousand columns takes far longer than the data load it
describes. See
[#258](https://github.com/microbiomedata/nmdc-lakehouse/issues/258).

## Move the snapshot and evidence into the pod

`berdl-upload-plan` binds absolute paths and `berdl-upload` runs in the pod, so the
completed snapshot and every reviewed evidence file have to be in the pod
filesystem first. Transfer happens over the JupyterHub contents API, either through
the notebook file browser or through a client that speaks to it. The SOCKS tunnels
play no part in this and do not need to be up.

**Archive the snapshot on macOS with `COPYFILE_DISABLE=1`, or it will arrive
corrupted:**

```bash
COPYFILE_DISABLE=1 tar -czf snapshot.tar.gz -C /path/to/parent completed-snapshot
```

Plain `tar -czf` on macOS stores extended attributes. Extracting on Linux
materializes them as AppleDouble `._*` siblings, one per file. A plain `ls` hides
them and the visible directory listing looks correct. On 2026-08-20 a 52-artifact
snapshot arrived in the pod with 54 extra `._*` files and nothing looked wrong until
validation ran.

`validate-snapshot` catches it, fails closed, and names what it found. Verbatim,
from a snapshot with two such siblings planted:

```
Error: Snapshot contents do not match the manifest: unexpected 2: '._instrument_set.parquet', '._study_set.parquet'; 2 of the unexpected files start with '._', which is what extracting a macOS tar archive on Linux produces; re-archive with COPYFILE_DISABLE=1 or delete them.
```

The message is a single line however many files are involved. Missing and
unexpected are reported separately, because they have different causes: missing
means an incomplete transfer, unexpected usually means the archiving step added
something. At most ten names appear per category, followed by `and N more`, so
the real 54-sibling case reads the same way with a longer list.

To clear AppleDouble siblings that are already in place:

```bash
find /path/to/completed-snapshot -name '._*' -delete
```

After that deletion the same snapshot validated with an identical digest, which
confirmed the Parquet bytes themselves had transferred correctly.

**Validate in the pod, before planning:**

```bash
uv run --no-sync nmdc-lakehouse validate-snapshot /absolute/path/to/completed-snapshot
```

A clean run names the digest and the artifact count:

```
Validated sha256:5022cb...a316c: 53 Parquet artifact(s).
```

Compare that digest against the one recorded locally. They must match exactly. A
digest that differs means the snapshot in the pod is not the snapshot that was
reviewed, and everything bound to it downstream is bound to the wrong bytes.

One observation about size, from the 2026-08-20 run and specific to the client used
there rather than to the contents API itself: a 112 MB upload succeeded and a 352 MB
upload failed with a broken pipe. If a large archive fails partway, split it, upload
the parts, reassemble in the pod, and verify the digest of the reassembled archive
before extracting.

## Getting table data back out, and the trap that eats it

The direction above is workstation to pod. Going the other way, off the platform,
has a failure that is worse than the macOS one, because it produces no error at
all.

**A Spark write to a local path leaves no usable data on the pod filesystem.** The
write itself does not fail, and Spark does not drop it: in a cluster each executor
resolves the path against its own filesystem and writes its partition there. What
the driver's directory receives is the success markers. So the data may exist,
scattered across executor filesystems you cannot reach, which is not a backup:

```python
df.write.parquet("/home/<user>/backup/table.parquet")   # succeeds; nothing usable lands here
```

Observed on 2026-08-20 while exporting `nmdc.results`. The script printed a
completed line and a correct row count for every table, and every output directory
held 55 bytes. Seven directories, no data.

That is dangerous for a backup specifically, because what a failed backup leaves
behind is a set of plausible-looking directories with the right names. Anyone who
then deletes the source has lost it.

**Write to object storage instead**, which every executor can reach, and copy down
from there. Use a dated export prefix under the tenant area so a run is
identifiable and two runs cannot collide:

```python
prefix = "exports/20260821-results-backup"     # dated, and specific to this run
df.write.parquet(f"s3a://cdm-lake/tenant-general-warehouse/nmdc/{prefix}/annotation_enzyme_commission.parquet")
```

A small table can be brought to the driver first, but only when it genuinely fits
in driver memory. `nmdc.results` does not. All seven tables were counted during
the 2026-08-20 export, and the two largest are `annotation_kegg_orthology` at
1,831,998,811 rows and `annotation_enzyme_commission` at 1,231,453,377. The
smallest, `annotation_statistics`, is 4,815 rows, so the right answer differs by
table and the size has to be checked rather than assumed.

**End every export by checking the destination holds data, not that the command
returned.** Row counts printed by the writing job say nothing about where the bytes
went, and in the 2026-08-20 run they were all correct:

```bash
du -sh /path/to/export/*                              # bytes present, not just directories
find /path/to/export -type f -name '*.parquet' | head  # actual data files, not the directory
```

`-type f` matters. Spark writes each table as a **directory** named `table.parquet`
containing `part-*.snappy.parquet` files, so a `find` without it matches the
directory entry itself, whose size is filesystem-dependent and on the pod's ext4
is large enough to pass a size filter even when the directory holds nothing but
`_SUCCESS`. That is the exact failure this section is about, so a verification
that can report it as success is worse than none.

The signature of a failed export is a `table.parquet` directory containing only
`_SUCCESS`, with no `part-*.parquet` files inside it. Judge it on whether those
files exist rather than on a byte figure: a directory plus an empty marker still
consumes blocks, and how many depends on the filesystem, which is the trap this
paragraph exists to avoid.

For an object-store destination, list it and compare against what was written:

```bash
mc ls --recursive berdl-minio/cdm-lake/tenant-general-warehouse/nmdc/exports/20260821-results-backup/
```

`mc` is the object-store client this repository supports and the one
`berdl-doctor` checks for. From a workstation it needs the `berdl-minio` alias,
which the tunnel step in the historical transport section below configures, and
which the maintained path otherwise never needs.

See [#250](https://github.com/microbiomedata/nmdc-lakehouse/issues/250).

## Preview and execute verified data staging

Generate the plan and run its preview and execution in the same BERDL
JupyterHub pod. The plan binds absolute paths, the Python interpreter, this
repository's adapter, and the official KBase ingest checkout. The completed
snapshot and all reviewed evidence must therefore be available in that pod.
Start the Spark Connect sidecar with `get_spark_session()` in a notebook before
using the pod terminal for a long-running execution. Preview is the default:

```bash
just berdl-upload \
  /path/to/berdl-staging-plan.json \
  /path/to/kbase-ingest-outcome.json \
  /path/to/nmdc-staging-outcome.json
```

Preview re-hashes and reloads every reviewed input, validates the snapshot,
rechecks the clean official ingest revision and source hashes, and reconstructs
the argument vector. It does not start the adapter, read credentials, contact
a service, upload data, or change a catalog. The upstream and NMDC outcome paths
must be distinct, must not already exist, and must remain outside the immutable
snapshot directory and the reviewed KBase ingest checkout. An outcome created inside
the checkout would make it dirty and invalidate the required post-run revision
check after staging had already changed the destination.

After reviewing that preview, compute the plan file's SHA-256 digest. For
example, use `sha256sum` on Linux or `shasum -a 256` on macOS. Execute the same
plan with both that digest and the snapshot ID printed in the plan as explicit,
plan-bound authorization:

```bash
just berdl-upload \
  /path/to/berdl-staging-plan.json \
  /path/to/kbase-ingest-outcome.json \
  /path/to/nmdc-staging-outcome.json \
  --execute-staging \
  --authorize-snapshot 'sha256:FULL_SNAPSHOT_DIGEST' \
  --authorize-plan-sha256 'FULL_PLAN_FILE_SHA256'
```

These values authorize the exact immutable plan, not one invocation. Reusing
them intentionally replays the overwrite-mode load into the same isolated
staging namespace and object prefix. Use new outcome paths for every attempt,
retain failed-attempt evidence, and do not reuse an authorization after the
reviewed plan or snapshot changes.

The executor passes an argument vector directly to the reviewed NMDC adapter;
it does not invoke a shell. Adapter and KBase ingest progress is routed to
stderr so stdout remains the parseable preview or NMDC outcome JSON. The plan
digest binds authorization to the reviewed destination as well as the snapshot.
After every started adapter process exits, fails, or is interrupted, the
executor revalidates the plan, snapshot, and external source revision before
returning control. After a successful command, it requires the adapter's strict
outcome to report the planned bucket, bronze prefix, staging namespace, exact
table set, object-storage-verified source SHA-256, and matching
source-versus-catalog row counts for every manifested Parquet artifact. The
adapter requires the stock report to name the planned fully qualified table,
then independently counts that table through Spark rather than treating the
ingest report's write count as destination evidence. The
source digest must equal the artifact digest in the reviewed snapshot. Only
then does it create the immutable, credential-free NMDC
outcome with status `data-verified`.

The adapter uploads every manifested Parquet file to the plan's unique bronze
prefix, reads each object back to verify its SHA-256 digest, stores the inline
ingest configuration, and calls stock `data_lakehouse_ingest.ingest` in-process.
It does not depend on BERIL Research Observatory source code. The first live
attempt must use a disposable staging namespace and remains an integration
rehearsal until the pod run and catalog queries confirm the complete contract.

Failure does not remove the unique bronze prefix, progress key, config key, or
staging namespace. Retain them with the upstream outcome for diagnosis and make
any retry an explicit new invocation. A `data-verified` outcome does not claim
that catalog metadata was applied or that canonical replacement is authorized.
Those remain separate work in
[#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) and
[#234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234).

After staging has a `data-verified` outcome, preview the table and column
description operations bound to it:

```bash
just berdl-apply-metadata \
  /path/to/metadata-application-plan.json \
  /path/to/nmdc-staging-outcome.json \
  /path/to/data-lakehouse-ingest \
  /path/to/nmdc-staging-metadata-outcome.json
```

The preview is offline. Execution additionally requires `--execute-metadata`,
`--authorize-plan-sha256`, and `--authorize-staging-outcome-sha256` with the
exact digests printed by the preview. It verifies that the stock KBase helper
package still matches the ingest revision recorded by staging, applies only
approved table and column descriptions, and reads every applied description
back from the catalog. The outcome is created once and records namespace
operations as deferred work for #114. This step does not change canonical
tables, promote staging, or claim that missing descriptions were filled.

The destination, catalog, and table format are explicit observations. Do not
copy the historical Delta examples below unless current discovery confirms
them. A missing or blank value is a readiness failure.
This is a BERDL destination-profile requirement enforced by `berdl-doctor` and
the BERDL inventory producer. The portable publication preflight permits absent
provider or table-format labels for destinations, such as file-only publication,
where those concepts do not apply.

The default check is offline. To make one bounded TCP probe of the separately
managed local BERDL proxy, opt in:

```bash
just berdl-doctor /absolute/path/to/completed-snapshot \
  --service-check berdl-proxy
```

The probe defaults to loopback port 8123. `BERDL_PROXY_HOST` and
`BERDL_PROXY_PORT` can describe a different local proxy. The doctor never
starts a proxy, opens a tunnel, refreshes a token, installs packages, changes
the external checkout, uploads files, or changes a catalog.

## Capture a fresh destination inventory without mutation

The offline publication planner requires current evidence from the selected
destination. Run the maintained audit script from a repository checkout in a
BERDL JupyterHub terminal, where `berdl_notebook_utils` is available. Supply
observed provider and table-format labels rather than copying the historical
Delta values from this guide:

```bash
python scripts/python/audit_database_metadata.py CURRENT_NAMESPACE \
  --publication-inventory /path/to/nmdc-metadata-destination-inventory.json \
  --destination-id nmdc-production \
  --provider CURRENT_PROVIDER \
  --table-format CURRENT_TABLE_FORMAT \
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
`publication-plan`, and retain it with that plan as time-specific evidence. Do
not treat a previous inventory as the current live state.

---

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

```bash
uv python install 3.13
uv venv .venv-berdl --python 3.13 --seed
```

#### 2. Ingest packages

From [`kbaseincubator/BERIL-research-observatory`](https://github.com/kbaseincubator/BERIL-research-observatory):

```bash
bash scripts/bootstrap_client.sh
bash scripts/bootstrap_ingest.sh
```

Both scripts belong to the external checkout and can change its dedicated
environment. Run them only when provisioning that checkout. Do not ignore a
failed verification; `just berdl-doctor` must subsequently find both required
distributions in `.venv-berdl`.

#### 3. MinIO client (`mc`)

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

```bash
# From the nmdc-lakehouse checkout, before cd'ing anywhere else:
export NMDC_LAKEHOUSE_DATA="$(realpath "${LAKEHOUSE_ROOT:-./lakehouse}")"
```

Two SOCKS tunnels reach BERDL's storage and compute from outside the cluster:

```bash
ssh -f -N -o ServerAliveInterval=60 -D 1338 ac.<your-berkeley-username>@login1.berkeley.kbase.us
ssh -f -N -o ServerAliveInterval=60 -D 1337 ac.<your-berkeley-username>@login1.berkeley.kbase.us
```

Then configure the MinIO client through the proxy:

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

```bash
source .venv-berdl/bin/activate
python scripts/ingest_preflight.py \
    --data-dir "$NMDC_LAKEHOUSE_DATA" \
    --tenant nmdc --dataset nmdc_linkml_store \
    --mode overwrite --chunk-target-gb 20
```

All 13 tables should show as single-batch.

### Upload metadata

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

```bash
source .venv-berdl/bin/activate
jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.timeout=-1 \
    /path/to/nmdc_linkml_store_ingest.ipynb
```

Poll progress:

```bash
https_proxy=http://127.0.0.1:8123 ~/bin/mc cat \
    "berdl-minio/cdm-lake/tenant-general-warehouse/nmdc/datasets/nmdc_linkml_store/_ingest_progress.jsonl"
```

### Verify in BERDL SQL

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
- [#50](https://github.com/microbiomedata/nmdc-lakehouse/issues/50): consolidating ETL output to `LAKEHOUSE_ROOT` so this doc's paths are stable.
- [#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51): the original automation issue; this doc is the runbook half of it. A
  `just berdl-upload` recipe wrapping the tunnel/preflight/upload/ingest steps is
  still open there.
- `README.md`: where `LAKEHOUSE_ROOT` and the other ETL configuration variables are documented.
- `docs/mongodb-connection.md`: the upstream half (MongoDB to local Parquet).
- `docs/berdl-metadata-shaping.md`: what you can set beyond the raw data once it's here.
