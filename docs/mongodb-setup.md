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

Production dumps are generated nightly and synced to NERSC at:

```
/global/cfs/cdirs/m3408/nmdc-mongodumps/from_google_cloud/nmdc-runtime-prod-mongo-backup/YYYYMMDD_HHMMSS/
```

```bash
# Refresh your NERSC SSH key (valid 24 hours)
sshproxy

# List available dumps
ssh -i ~/.ssh/nersc mamillerpa@dtn01.nersc.gov \
  ls /global/cfs/cdirs/m3408/nmdc-mongodumps/from_google_cloud/nmdc-runtime-prod-mongo-backup/

# Download the dump you want
rsync \
  --archive --human-readable --no-perms --progress --verbose \
  --rsh "ssh -q -i ~/.ssh/nersc" \
  mamillerpa@dtn01.nersc.gov:/global/cfs/cdirs/m3408/nmdc-mongodumps/from_google_cloud/nmdc-runtime-prod-mongo-backup/YYYYMMDD_HHMMSS/ \
  /tmp/YYYYMMDD_HHMMSS/
```

---

## Restoring Locally

Use the `just restore-dump` recipe, which restores only the ~32 data collections
and skips minting, runtime, and operational collections:

```bash
just restore-dump DUMP_DIR=/tmp/YYYYMMDD_HHMMSS
```

To restore against an authenticated MongoDB (e.g. an nmdc-runtime dev instance on port 27018):

```bash
just restore-dump DUMP_DIR=/tmp/YYYYMMDD_HHMMSS MONGO_HOST=localhost MONGO_PORT=27018 MONGO_USERNAME=admin MONGO_PASSWORD=root
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
