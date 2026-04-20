# MongoDB Setup

The ETL pipeline reads directly from a local MongoDB instance populated with a
production dump. This document covers getting a dump, restoring it, and verifying
the result.

---

## Prerequisites

- [MongoDB Community Server](https://www.mongodb.com/try/download/community) running locally (default port 27017, no auth required)
- [MongoDB Database Tools](https://www.mongodb.com/try/download/database-tools) (`mongorestore`, `mongosh`) on your PATH
- NERSC account with `sshproxy` configured (for downloading dumps)

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

```bash
just fetch-dump                            # latest, into ./local/dumps/
just fetch-dump 20260420_060655            # specific timestamp
just fetch-dump latest /path/to/dumps      # custom destination
```

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
