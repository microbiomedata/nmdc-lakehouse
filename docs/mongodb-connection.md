# Connecting to NMDC Production MongoDB

NMDC's production MongoDB runs inside a GCP Kubernetes cluster and is not publicly
accessible. Access goes through an SSH gateway (`jump-dev.microbiomedata.org`) that
forwards a port into the cluster. Getting there requires NERSC credentials (to fetch
the gateway key) and a personal MongoDB account on the NMDC prod instance.

---

## Prerequisites — obtain before doing anything else

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
infrastructure team (currently Eric Cavanna or Patrick Kalita) in the NMDC Slack
`#infra-admin` channel. Note which database(s) you need access to — for lakehouse
ETL work that is the `nmdc` database.

---

## Install the SSH gateway key

Do this when setting up a new machine, or any time the gateway key is rotated
by the infrastructure team.

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

```bash
# 1. Refresh the NERSC SSH key (prompts for NERSC password + OTP)
sshproxy -u <your-nersc-username>

# 2. Open the SSH tunnel — leave this terminal open while you work
ssh -i ~/.ssh/jump-dev.microbiomedata.org.private_key \
    -L 27124:runtime-api-mongodb-headless.nmdc-prod.svc.cluster.local:27017 \
    -o ServerAliveInterval=60 \
    ssh-mongo@jump-dev.microbiomedata.org
```

While the tunnel is open, `localhost:27124` forwards to the NMDC production MongoDB.

---

## Configure this repo

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
MONGO_HOST=localhost
MONGO_PORT=27124              # tunnel port — not the MongoDB default 27017
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

> **Never commit `.env`** — it is git-ignored. Credentials stay local.

---

## Verify the connection

With the tunnel open and `.env` populated:

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

```bash
mongosh "mongodb://localhost:27124/nmdc" \
    --username <your-mongodb-username> \
    --authenticationDatabase admin \
    --eval 'db.biosample_set.estimatedDocumentCount()'
```

Or a Python-stack dry-run (reads records, writes nothing):

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
INFO - No metadata for <coll>; no derivations  # no pre-loaded schema cache — expected
```

`"No metadata … no derivations"` does **not** mean the collection is empty or missing.
linkml-store uses the installed nmdc-schema at runtime instead of a cached metadata
object, so this message is expected for every collection.

### Step 1 — all collections except the large annotation aggregate (~5 min)

For a new snapshot, prefer a fresh timestamped directory so the preceding output
remains available for comparison. `local/` is ignored by Git:

```bash
export LAKEHOUSE_ROOT="./local/mongodb-metadata-$(date +%Y%m%d_%H%M%S)"
```

If an existing output root must be reused, preview recognized schema-derived
metadata Parquet files before deleting them. Unknown files, directories,
manifests, logs, and symlinks are preserved:

```bash
just clean-parquet
just clean-parquet --delete
```

Both commands affect only local files under the repository. They never modify
MongoDB, NERSC, BERDL, or object stores.

```bash
uv run nmdc-lakehouse run-job all-collections \
    --skip functional_annotation_agg
```

### Step 2 — functional annotation aggregate (~17 min)

```bash
uv run nmdc-lakehouse run-job functional_annotation_agg
```

### Run a single collection

```bash
uv run nmdc-lakehouse run-job biosample_set
uv run nmdc-lakehouse run-job study_set
# etc. — use `list-jobs` to see all registered names
uv run nmdc-lakehouse list-jobs
```
