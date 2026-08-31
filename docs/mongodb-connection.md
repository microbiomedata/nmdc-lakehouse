# Connecting to NMDC Production MongoDB

NMDC's production MongoDB runs inside a GCP Kubernetes cluster and is not publicly
accessible. Access goes through an SSH gateway (`jump-dev.microbiomedata.org`) that
forwards a port into the cluster. Getting there requires NERSC credentials (to fetch
the gateway key) and a personal MongoDB account on the NMDC prod instance.

---

## Prerequisites: obtain before doing anything else

These steps involve waiting on other people or systems; start them early.

### 1. NERSC user account

Required to fetch the SSH gateway key from NERSC project storage.

- Request via NERSC's [account request form](https://iris.nersc.gov). You must be
  sponsored by a PI with an active NERSC project (for NMDC work that is project `m3408`).
- Approval typically takes several business days.

### 2. NERSC multi-factor authentication (MFA)

NERSC requires MFA for all SSH connections. Enroll after your account is approved:

- Follow [NERSC MFA setup instructions](https://docs.nersc.gov/connect/mfa/).
- You will need a TOTP authenticator app (Google Authenticator, Authy, etc.).

### 3. `sshproxy` binary

`sshproxy` exchanges your NERSC password + OTP for a short-lived SSH key/certificate
pair (`~/.ssh/nersc` + `~/.ssh/nersc-cert.pub`, 24-hour lifetime). Without it you
will be prompted for a password + OTP on every SSH command.

- Download from [sshproxy.nersc.gov](https://sshproxy.nersc.gov) and place in
  `~/bin/` (or anywhere on your `$PATH`).
- Make it executable: `chmod +x ~/bin/sshproxy`

### 4. MongoDB credentials for the NMDC production instance

Each developer gets a personal MongoDB username and password. Ask the NMDC
infrastructure team (currently `@eecavanna` or `@pkalita-lbl`) in the NMDC Slack
`#infra-admin` channel. Note which database(s) you need access to. For lakehouse
ETL work that is the `nmdc` database.

---

## Install the SSH gateway key

Do this when setting up a new machine, or any time the gateway key is rotated
by the infrastructure team.

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
# 1. Get a fresh NERSC SSH key (prompts for NERSC password + OTP)
sshproxy -u <your-nersc-username>

# 2. Copy the shared SSH gateway private key from NERSC project storage
scp -i ~/.ssh/nersc \
    <your-nersc-username>@dtn01.nersc.gov:/global/cfs/projectdirs/m3408/nmdc-cloud-deployment/ssh-keys/jump-dev.microbiomedata.org.private_key \
    ~/.ssh/jump-dev.microbiomedata.org.private_key

# 3. Restrict key permissions (SSH will refuse to use a world-readable key)
chmod 400 ~/.ssh/jump-dev.microbiomedata.org.private_key
```

---

## Per-session: open the tunnel

The NERSC SSH key expires every 24 hours and must be refreshed each session.
The tunnel also closes when the terminal exits.

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
# 1. Refresh the NERSC SSH key (prompts for NERSC password + OTP)
sshproxy -u <your-nersc-username>

# 2. Open the SSH tunnel, and leave this terminal open while you work
ssh -i ~/.ssh/jump-dev.microbiomedata.org.private_key \
    -L 27124:runtime-api-mongodb-headless.nmdc-prod.svc.cluster.local:27017 \
    -o ServerAliveInterval=60 \
    ssh-mongo@jump-dev.microbiomedata.org
```

While the tunnel is open, `localhost:27124` forwards to the NMDC production MongoDB.

---

## Configure this repo

Copy `.env.example` to `.env` and fill in your credentials:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
MONGO_HOST=localhost
MONGO_PORT=27124              # tunnel port, not the MongoDB default 27017
MONGO_DBNAME=nmdc
MONGO_USERNAME=<your-mongodb-username>
MONGO_PASSWORD=<your-mongodb-password>
MONGO_DIRECT_CONNECTION=true  # required: skips replica-set discovery
NMDC_JUMP_KEY=~/.ssh/jump-dev.microbiomedata.org.private_key
```

`MONGO_DIRECT_CONNECTION=true` is required because NMDC's MongoDB is a
replica set whose members advertise internal Kubernetes hostnames. Without
it, pymongo tries to reach those hostnames directly and times out.
`MONGO_REPLICA_SET` can be left blank.

The `just` recipes load `.env` via `set dotenv-load := true` in the justfile.
The `nmdc-lakehouse` CLI loads `.env` via pydantic-settings (`env_file=".env"`).
Both mechanisms read the same file; exported shell variables take precedence over
`.env` in both cases.

> **Never commit `.env`.** It is git-ignored. Credentials stay local.

---

## Verify the connection

With the tunnel open and `.env` populated:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
# Validate configuration, key permissions, and the local forwarded port.
uv run --no-sync nmdc-lakehouse doctor --service-check gcp-tunnel

# Then make one bounded, read-only MongoDB ping.
uv run --no-sync nmdc-lakehouse doctor --service-check mongo-ping
```

Doctor never displays the credential-bearing URI and never starts or stops the
tunnel. The checks fail separately for missing configuration, an unavailable
local tunnel, authentication rejection, and network access.

For an independent interactive check:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
mongosh "mongodb://localhost:27124/nmdc" \
    --username <your-mongodb-username> \
    --authenticationDatabase admin \
    --eval 'db.biosample_set.estimatedDocumentCount()'
```

Or a Python-stack dry-run (reads records, writes nothing):

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run nmdc-lakehouse run-job biosample_set --dry-run
```

---

## Running ETL jobs

### Maintained collection baseline

The installed, locked `nmdc-schema` is authoritative for maintained metadata
dump scope. The pipeline reads the slots of its `Database` class rather than
maintaining an independent inclusion list. For `nmdc-schema` 11.23.0, the
reviewed snapshot is these 19 MongoDB collections:

- `biosample_set`
- `calibration_set`
- `collecting_biosamples_from_site_set`
- `configuration_set`
- `data_generation_set`
- `data_object_set`
- `field_research_site_set`
- `functional_annotation_agg`
- `functional_annotation_set`
- `genome_feature_set`
- `instrument_set`
- `manifest_set`
- `material_processing_set`
- `organism_sample_set`
- `organism_set`
- `processed_sample_set`
- `storage_process_set`
- `study_set`
- `workflow_execution_set`

CI compares the schema-derived names with this reviewed snapshot. Updating
`nmdc-schema` therefore requires an explicit review when a collection is added
or removed. Runtime-only collections that are not `Database` slots, including
`flattened_*` and `alldocs`, are outside this maintained dump. Other live
MongoDB collections are not discovered or included automatically.

Repeatable `--skip` options and `LAKEHOUSE_SKIP_COLLECTIONS` are temporary
per-run exclusions; they do not change the permanent schema-derived policy.
`functional_annotation_agg` is part of the baseline, but its size and dedicated
reader make running it separately the normal operational practice.

All other baseline collections go through the linkml-store path.
Throughput is approximately **1,500–2,000 records/sec** for flat collections
(observed: 364,957 rows in ~3.5 minutes on 2026-04-24). Polymorphic collections
(e.g. `workflow_execution_set`) degrade to ~200–300 rows/s after the first 10K records
due to per-record schema dispatch in linkml-store (tracked upstream at
[linkml-store#69](https://github.com/linkml/linkml-store/issues/69)).

`functional_annotation_agg` (54.8M records) bypasses linkml-store entirely via a
raw pymongo cursor, completing in **~17 minutes** at ~30,000 rows/s.

### Expected log output

Each collection going through linkml-store produces three INFO lines that look alarming but are normal:

```
INFO - Initializing databases        # linkml-store opening a fresh client
INFO - Attaching nmdc                # connecting to the nmdc database
INFO - No metadata for <coll>; no derivations  # no pre-loaded schema cache, expected
```

`"No metadata … no derivations"` does **not** mean the collection is empty or missing.
linkml-store uses the installed nmdc-schema at runtime instead of a cached metadata
object, so this message is expected for every collection.

### Step 1: every collection (~22 min), or a chosen subset (~5 min)

For the measured metadata dump, use the recipe:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
just etl-collections
```

That produces a **complete** snapshot: every collection the installed NMDC schema
declares, including `functional_annotation_agg`, which is 53M rows and dominates
the runtime. Complete is the default because a complete snapshot is what the
staging planner expects, and because a snapshot missing a table the destination
already holds forces a disposition decision later.

To leave collections out, name them:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
just etl-collections functional_annotation_agg
```

That takes about five minutes instead of twenty-two. The manifest records the
skipped names, and `snapshot-manifest.json` is rejected unless the included and
skipped sets together cover every collection the installed schema declares, so a
partial snapshot cannot later be mistaken for a complete one. What it cannot do is
tell you why something was skipped, so say so wherever the snapshot is used.

One timestamp associates its default Parquet directory,
`local/mongodb-metadata-<timestamp>`, with
`local/etl-collections-<timestamp>.log`. The snapshot directory contains
`etl-metrics.json` and, only after successful extraction and offline validation,
`snapshot-manifest.json`. The recipe displays the paths before extraction and
refuses to reuse its default output path. `local/` is ignored by Git.

Set `LAKEHOUSE_ROOT` to use an intentional alternative directory. An explicit
value remains authoritative:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
export LAKEHOUSE_ROOT="./local/mongodb-metadata-$(date +%Y%m%d_%H%M%S)"
just etl-collections
```

If an existing output root must be reused, preview recognized schema-derived
metadata Parquet files before deleting them. Unknown files, directories,
manifests, logs, and symlinks are preserved:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
just clean-parquet
just clean-parquet --delete
```

Both commands affect only local files under the repository. They never modify
MongoDB, NERSC, BERDL, or object stores.

Each non-dry-run collection job writes its primary table and any side tables to
a unique `.staging/` directory under the output root. It closes every opened
writer before promoting any file. A conversion or side-table flush error removes
that run's staging directory and leaves the previously completed files unchanged.
On success, promotion replaces the primary table and produced side tables,
removes older schema-owned side tables that the new collection no longer
produces, and preserves unrelated collections and user files. Empty primary
collections still promote a typed, schema-only Parquet file.

The transaction writes a small completion inventory inside staging before
promotion. That internal record is a promotion guard, not another portable
artifact; the snapshot-level `snapshot-manifest.json` remains the durable source
of truth. Multi-file promotion provides rollback for errors raised by the running
process, but the flat-file layout cannot give concurrent readers a single atomic
namespace switch. Do not run two writers against the same output root, and
publish only a separately validated, completed snapshot. A process termination
that bypasses cleanup, or a machine termination, can leave an orphaned
`.staging/` directory; inspect it before removal and rerun the collection into a
new snapshot root.

If promotion rollback itself fails, the command reports that secondary failure
and retains the run-specific staging directory because it may contain the only
remaining copy of an older file. Do not rerun into or delete that output root
until its final paths and the reported
`.staging/<collection>-<run>/.previous/` backup have been inspected and
recovered.

The direct CLI equivalent is:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
mkdir -p local
timestamp="$(date +%Y%m%d_%H%M%S)"
export LAKEHOUSE_ROOT="./local/mongodb-metadata-${timestamp}"
uv run nmdc-lakehouse run-job all-collections \
    --skip functional_annotation_agg \
    --metrics "$LAKEHOUSE_ROOT/etl-metrics.json" \
    2>&1 | tee "local/etl-collections-${timestamp}.log"
uv run nmdc-lakehouse create-snapshot-manifest "$LAKEHOUSE_ROOT" \
    --metrics "$LAKEHOUSE_ROOT/etl-metrics.json" \
    --source-label nmdc-production
uv run nmdc-lakehouse validate-snapshot "$LAKEHOUSE_ROOT"
```

The JSON contains whole-run and per-collection wall time, rows, effective
rates, the resolved output root, and each generated file's row count and byte
size. It also labels the record with the `nmdc-lakehouse` and NMDC schema
versions, Python version, platform, skipped collections, and start and finish
times. It distinguishes a dry run from a writing run. A failed run writes
`status: failed` and the
exception type without the exception message, so a partial output set is not
reported as successful.

`peak_rss_bytes` is the Python process's peak resident-memory high-water mark,
normalized to bytes from the operating system's `resource` interface. It is
not current memory and does not include MongoDB, the SSH process, or other
system services. Compare rates and memory only between records whose platform
and environment are reasonably similar.

### Snapshot manifest

`snapshot-manifest.json` is the immutable completion marker for one portable
full snapshot. Manifest format version 1 records:

- a content-derived `snapshot_id`, full-snapshot scope, included and explicitly
  skipped collections, source label, completion time, and a reserved null
  `parent_snapshot_id`;
- every owned Parquet path, table, footer row count, byte size, SHA-256 checksum,
  physical-schema fingerprint, and footer-schema fingerprint;
- source and target schema identifiers, source schema version, source and target
  classes, mapping identities, and footer metadata contract version;
- `nmdc-lakehouse`, `nmdc-schema`, Python, Git commit, and checkout-dirty
  provenance when available; and
- the relative path and checksum of `etl-metrics.json` without duplicating its
  timing and resource measurements.

The manifest command rejects failed or dry-run metrics, metrics from another
directory or software environment, incomplete collection disposition, stale or
extra files or directories, symlinks, and Parquet files without the current
footer contract. It writes the manifest atomically and refuses to replace an
existing completion marker. Validate a snapshot again before upload:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run nmdc-lakehouse validate-snapshot "$LAKEHOUSE_ROOT"
```

Consumers can obtain the machine-readable JSON Schema for the current manifest
format without connecting to a service:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run nmdc-lakehouse snapshot-manifest-schema
```

Validation requires no MongoDB, tunnel, object store, or destination catalog. It
recomputes the snapshot identity, checksums, footer counts, schema fingerprints,
and the exact owned file set. Parquet files and manifests remain immutable;
`parent_snapshot_id` reserves a future lineage link but does not implement the
patch semantics tracked in [#147](https://github.com/microbiomedata/nmdc-lakehouse/issues/147).

### Logical target-row validation

Snapshot validation proves file integrity and contract identities; it does not
prove that flattened values satisfy the logical LinkML target. Generate separate
snapshot-bound evidence outside the immutable snapshot:

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
just validate-target-rows "$LAKEHOUSE_ROOT" \
  "./local/target-validation-$(date +%Y%m%d_%H%M%S).json"
```

The default bounded mode validates every row in tables with at most 10,000 rows
and deterministically selects 100 rows from each larger table. Selection uses a
SHA-256 score over the target identifier, when declared, and the canonical row;
it is independent of Parquet row order. The report always records eligible and
selected counts and labels sampled tables, so a passing bounded run is not a
claim of full conformance. To validate every row, add `--mode full` after the
report argument.

Before reading rows, the command reruns immutable snapshot validation and
requires exact agreement among the manifest, Parquet footers, packaged target
schema, target class, source class, mapping, and `nmdc-schema` package version.
It returns nonzero for identity mismatch or semantic errors. Finding records
contain only severity, LinkML/JSON-Schema rule, field path, and aggregate count;
they do not contain row values, credentials, or raw validator messages. The
JSON evidence path must not already exist or reside inside the snapshot.

### Portable Parquet schema metadata

Each schema-directed Parquet file carries structural metadata in its Arrow
schema, which Parquet stores in the file footer. This applies to primary
tables, reference junction tables, inlined-child side tables, and schema-only
files with no rows. Removing all-empty columns preserves the table metadata
and the metadata of retained columns.

Table-level keys use the `nmdc_lakehouse.` prefix:

| Key | Meaning |
| --- | --- |
| `footer_metadata_format_version` | Version of the `nmdc_lakehouse.*` footer-key contract. |
| `table_description` | Generated target-table description. Primary tables include the root class description when one exists; inlined-child tables include the child class description; reference junction tables describe the source relationship. |
| `source_schema_id` | LinkML identifier of the source NMDC schema. |
| `source_schema_version` | Version declared by the source NMDC schema. |
| `source_class` | Root NMDC class projected for the collection. For a side table, this remains the collection's root class. |
| `target_schema_id` | Stable identifier of the generated flattened LinkML projection. |
| `target_class` | Generated LinkML class represented by the file. |
| `mapping` | Fully qualified identity of the row mapping used for this table. |

Field-level keys use the same `nmdc_lakehouse.` prefix, all four of them:

| Key | Meaning |
| --- | --- |
| `description` | Source slot description plus any generated reference, nesting, or polymorphic-dispatch explanation. Omitted when no description is available. |
| `linkml_range` | LinkML range used to construct the Arrow type. |
| `identifier` | `true` only when the generated slot is an identifier. |
| `designates_type` | `true` only when the generated slot is a type designator. |

One key is not ours, and it is the only footer entry that is a schema rather
than a label:

| Key | Meaning |
| --- | --- |
| `org.apache.spark.sql.parquet.row.metadata` | The file's schema in the form Spark reads a Parquet schema, with each slot description carried as a field `comment`. Written so that a Spark-based loader can create an already-described table in the commit it was making anyway, rather than needing one `ALTER TABLE ... ALTER COLUMN ... COMMENT` per column afterwards. Confirmed on BERDL on 2026-08-24; see the note below. |

**What is established, and how.** That this key reaches the Parquet footer, and
that its comments match the slot descriptions, is verified by tests in this
repository. That Spark reads it and turns those comments into catalog column
comments was verified on BERDL on 2026-08-24, in the pod, against
`biosample_set.parquet` from the 2026-08-21 snapshot:

```
BASELINE dataframe_columns=1402 with_comment_in_schema=1393
ANSWER-1 columns_described=1393 of 1402
ANSWER-2 metadata_commits=1
ANSWER-3 id_comment='An NMDC assigned unique identifier for a biosample submitted to NMDC.'
```

The comments arrive in the DataFrame schema before any table exists, survive
table creation, cost one commit rather than 1,393, and carry the real text. The
nine columns without a comment are the same nine with no description in the source
Parquet, so nothing is lost in transit. Both
[#278](https://github.com/microbiomedata/nmdc-lakehouse/issues/278) and
[#258](https://github.com/microbiomedata/nmdc-lakehouse/issues/258) are closed.

This paragraph said the opposite until 2026-08-27, and it was right when it was
written: the catalog side was genuinely unobserved, and a loader that ignored
this key would have looked identical from here. It went stale the day after,
when the probe ran.

Because it is a schema, it has a consistency requirement the other keys do not:
it must name exactly the columns the file holds. Any code that changes the field
list has to rebuild it, which is why removing all-empty columns regenerates it
rather than letting the original survive. A stale entry is worse than an absent
one, because Spark would request a column the file no longer contains.

`footer_schema_sha256` in the snapshot manifest is computed over **all** schema
and field metadata, this key included, so emitting it changes every artifact's
footer fingerprint. That is correct rather than incidental: the footer genuinely
differs. Snapshots written before it keep validating against their own
manifests.

These footer values make an individual file interpretable before catalog
registration. They are not a substitute for the complete target LinkML schema
and table topology tracked in [#110](https://github.com/microbiomedata/nmdc-lakehouse/issues/110),
the reviewable description and override content tracked in
[#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120), or BERDL
catalog comments tracked in
[#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114). Run-level
facts such as snapshot identity, source database identity, checksums, package
versions, Git revision, and checkout-dirty state belong in
`snapshot-manifest.json`. The manifest is
the completion marker for a portable snapshot: it inventories every owned file,
links `etl-metrics.json`, and can be checked without MongoDB or a destination.
The
source-to-target contract for experimental-result converters remains separate
under [#146](https://github.com/microbiomedata/nmdc-lakehouse/issues/146).
Footer metadata is structural only. It never includes credentials, connection
strings, source documents, or production values.

### Step 2: functional annotation aggregate (~17 min)

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run nmdc-lakehouse run-job functional_annotation_agg
```

### Run a single collection

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```bash
uv run nmdc-lakehouse run-job biosample_set
uv run nmdc-lakehouse run-job study_set
# etc. Use `list-jobs` to see all registered names
uv run nmdc-lakehouse list-jobs
```
