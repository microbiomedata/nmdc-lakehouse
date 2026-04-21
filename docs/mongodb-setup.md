# MongoDB Setup

The ETL pipeline reads directly from a local MongoDB instance populated with a
production dump. This document covers getting a dump, restoring it, and verifying
the result.

---

## Prerequisites

### System dependencies

| Dependency | Purpose | Install |
|---|---|---|
| [MongoDB Community Server](https://www.mongodb.com/try/download/community) | local database (default port 27017, no auth) | OS installer or package manager |
| [MongoDB Database Tools](https://www.mongodb.com/try/download/database-tools) | `mongorestore` (restore dumps) | OS installer or package manager |
| [`mongosh`](https://www.mongodb.com/try/download/shell) | post-restore verification | OS installer or package manager |
| `rsync` | resumable dump transfer from NERSC | package manager (usually preinstalled on Linux / macOS) |
| `ssh` (OpenSSH client) | NERSC auth and transport | package manager (usually preinstalled on Linux / macOS) |
| [`just`](https://github.com/casey/just#installation) | recipe runner | `cargo install just`, Homebrew, apt, or prebuilt binary |
| [`uv`](https://docs.astral.sh/uv/getting-started/installation/) | Python/env manager | official installer |

### Accounts and credentials

You need a NERSC account with access to the `m3408` (NMDC) project to download dumps.

1. **Request a NERSC account** at [iris.nersc.gov/add-user](https://iris.nersc.gov/add-user).
   Pick "I need a new NERSC account", enter `m3408` as the project name, and wait
   for PI approval. Account vetting can take up to a week.
2. **Set up MFA** in [iris.nersc.gov](https://iris.nersc.gov) after your account is created.
3. **Download `sshproxy`** from [portal.nersc.gov/cfs/mfa/](https://portal.nersc.gov/cfs/mfa/).
   Linux (x86-64 and ARM), macOS, and Windows builds are available.
4. **Refresh your SSH cert** with `sshproxy -u <your-nersc-username>` (24h lifetime).

If you already have a NERSC account but not `m3408` membership, ask the NMDC PI
or a PI proxy to add you via Iris.

### OS compatibility

- **Linux** (Ubuntu 24.04, tested): works. All dependencies available via the
  system package manager.
- **macOS**: expected to work via [Homebrew](https://brew.sh/) (not tested by
  this project's maintainers).
- **Windows**: expected to work inside [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install)
  with Ubuntu (not tested). Native Windows (PowerShell / cmd) is not expected
  to work — `rsync`, `just`, and the MongoDB server all have friction there.

---

## Getting a Production Dump

Production dumps are generated nightly and synced from GCS to NERSC at
`/global/cfs/cdirs/m3408/nmdc-mongodumps/from_google_cloud/nmdc-runtime-prod-mongo-backup/<YYYYMMDD_HHMMSS>/`.
The underlying GCS bucket is private (service-account-gated), so NERSC SSH is
the only path for users.

**First, refresh your NERSC SSH cert** (24-hour lifetime):

```bash
sshproxy -u <your-nersc-username>
```

Set `NERSC_USER` in `local/.env` if your NERSC username differs from `$USER`.

**List available dumps** (newest last):

```bash
just list-dumps        # default: last 20
just list-dumps 50     # last 50
```

**Fetch a dump:**

The preferred dump is read from `$NMDC_DUMP` in `local/.env` (default `latest`).
Override on the command line to pick a different one without editing `.env`:

```bash
just fetch-dump                            # uses $NMDC_DUMP (latest by default)
just fetch-dump 20260420_060655            # specific timestamp
just fetch-dump latest /path/to/dumps      # custom destination
```

Pin `NMDC_DUMP=20260420_060655` in `local/.env` when you want reproducible runs
against a specific snapshot.

`just fetch-dump` transfers only the files for the 17 schema-specified
collections (from `scripts/python/schema_collections.py`, which reads the
installed `nmdc-schema` package). That's ~1.2 GB of a ~3 GB full dump; all
minting, runtime, operational, and GridFS files are skipped at the rsync layer.

---

## Restoring Locally

Since `fetch-dump` already selected the schema collections, `restore-dump`
loads whatever's on disk — no `--nsInclude` filtering, no chance of loading
collections you didn't ask for.

Collections are renamed on restore from the dump's internal `nmdc.*` namespace
to the db named in `MONGO_URI`'s path (default `nmdc_lakehouse_prep`). Any
existing `nmdc` database (e.g. from nmdc-runtime dev work) stays untouched.

`MONGO_URI` is the single source of truth for the connection. `MONGO_DB` is
only used to build the default URI; at restore time the target db is parsed
back out of `MONGO_URI`, so the two can't drift. Override either:

```bash
# Default: restore into nmdc_lakehouse_prep
just restore-dump ./local/dumps/YYYYMMDD_HHMMSS

# Use a different db name
MONGO_DB=nmdc_scratch just restore-dump ./local/dumps/YYYYMMDD_HHMMSS

# Authenticated or remote host — the db in the URI path is the target
MONGO_URI=mongodb://admin:root@localhost:27018/nmdc_scratch \
    just restore-dump ./local/dumps/YYYYMMDD_HHMMSS
```

Pass the **parent** dump directory — `mongorestore` reads the `nmdc/` subdir
inside it to infer the source namespace.

### Why not all collections?

A full dump contains ~132 collections. The skipped categories:

| Category | Examples | Count |
|---|---|---|
| Minting | `minter.*`, `ids_*` | ~19 |
| Operational | `operations`, `jobs`, `triggers` | ~12 |
| Runtime | `_runtime.*`, `_migration_*` | ~53 |
| Temporary | `tmp.*`, `wf_file_staging.*` | ~4 |
| Utility | `notes`, `date_created` | ~3 |

---

## Verifying the Restore

`$MONGO_URI` already points at the target db, so `mongosh` can connect
directly:

```bash
mongosh "$MONGO_URI" --eval '
  const colls = db.getCollectionNames().sort();
  print(db.getName() + " has " + colls.length + " collections:");
  colls.forEach(c => print("  " + c + ": " + db.getCollection(c).estimatedDocumentCount()));
'
```

---

## Python / uv Setup

```bash
uv sync
```

`pymongo` and `linkml-runtime` are declared in `pyproject.toml` and installed by `uv sync`.

Configure the connection in `local/.env` (copy from `local/.env.example`):

```bash
cp local/.env.example local/.env
```

The default connection is `mongodb://localhost:27017/nmdc_lakehouse_prep` (derived from `MONGO_DB`). Works out of the box for a standard local install.
