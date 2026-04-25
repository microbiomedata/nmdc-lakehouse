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

### 3. `autossh`

Required to keep the tunnel alive for multi-hour ETL jobs. Plain `ssh` dies permanently on a connection drop; `autossh` detects the drop and reconnects automatically.

```bash
# Debian/Ubuntu
sudo apt install autossh

# macOS
brew install autossh
```

### 4. `sshproxy` binary

`sshproxy` exchanges your NERSC password + OTP for a short-lived SSH key/certificate
pair (`~/.ssh/nersc` + `~/.ssh/nersc-cert.pub`, 24-hour lifetime). Without it you
will be prompted for a password + OTP on every SSH command.

- Download from [sshproxy.nersc.gov](https://sshproxy.nersc.gov) and place in
  `~/bin/` (or anywhere on your `$PATH`).
- Make it executable: `chmod +x ~/bin/sshproxy`

### 5. MongoDB credentials for the NMDC production instance

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
# autossh reconnects automatically if the tunnel drops (required for overnight jobs)
autossh -M 0 -i ~/.ssh/jump-dev.microbiomedata.org.private_key \
    -L 27124:runtime-api-mongodb-headless.nmdc-prod.svc.cluster.local:27017 \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
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

Throughput through the GCP SSH tunnel is approximately **1,500–2,000 records/sec**
(observed: 364,957 rows in ~3.5 minutes on 2026-04-24).
`functional_annotation_agg` has ~54.8 million records and should be run separately
overnight. All other schema collections total ~365K records.

### Expected log output

Each collection produces three linkml-store INFO lines that look alarming but are normal:

```
INFO - Initializing databases        # linkml-store opening a fresh client
INFO - Attaching nmdc                # connecting to the nmdc database
INFO - No metadata for <coll>; no derivations  # no pre-loaded schema cache — expected
```

`"No metadata … no derivations"` does **not** mean the collection is empty or missing.
linkml-store uses the installed nmdc-schema at runtime instead of a cached metadata
object, so this message is expected for every collection.

### Step 1 — all collections except the large annotation aggregate (~5 min)

```bash
uv run nmdc-lakehouse run-job all-collections \
    --skip functional_annotation_agg
```

### Step 2 — functional annotation aggregate (~13 hours, run overnight)

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
