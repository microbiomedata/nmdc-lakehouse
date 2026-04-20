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

- NERSC account with `sshproxy` configured — required for downloading dumps

### OS compatibility

- **Linux**: fully supported. All dependencies available via standard package managers (`apt`, `dnf`, `pacman`, etc.).
- **macOS**: fully supported. All dependencies available via [Homebrew](https://brew.sh/).
- **Windows**: not directly tested. Recommended path is [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) with an Ubuntu distribution, which gives you the same experience as native Linux. Native Windows (PowerShell / cmd) is not supported — `rsync`, `just`, and the MongoDB server all have friction there.

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

A full dump is ~3 GB; the `nmdc/` subdirectory is ~2.6 GB of that.

---

## Restoring Locally

Use the `just restore-dump` recipe, which restores only the ~32 data collections
and skips minting, runtime, and operational collections.

All recipes read `MONGO_URI` from `local/.env` (default: `mongodb://localhost:27017/nmdc`).
Override on the command line for any connection — including auth, non-standard ports, or remote hosts:

```bash
# Local, no auth (default)
just restore-dump DUMP_DIR=/tmp/YYYYMMDD_HHMMSS

# Authenticated (e.g. nmdc-runtime dev instance)
MONGO_URI=mongodb://admin:root@localhost:27018/nmdc just restore-dump DUMP_DIR=/tmp/YYYYMMDD_HHMMSS

# Remote host
MONGO_URI=mongodb://user:pass@myhost:27017/nmdc just restore-dump DUMP_DIR=/tmp/YYYYMMDD_HHMMSS
```

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

```bash
mongosh nmdc --eval '
  const colls = db.getCollectionNames().sort();
  print("Collections: " + colls.length);
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

The default `MONGO_URI=mongodb://localhost:27017/nmdc` works for a standard local install.
