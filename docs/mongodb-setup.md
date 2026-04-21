# MongoDB Setup

**What this document covers:** how to populate a local MongoDB with NMDC data
so the `nmdc-lakehouse` ETL can read from it. The ETL (in progress) flattens
NMDC's nested metadata into tabular rows and writes them out as Parquet /
Iceberg for the BERDL lakehouses.

The ETL reads from any MongoDB whose contents come from a `mongodump` of
NMDC's schema-conformant data. The recipes in this repo default to fetching
**nightly `mongodump` archives of the NMDC production MongoDB** from NERSC
(where they are mirrored from a private GCS bucket), but the restore step
works with any compatible dump — test-server dumps or hand-made test dumps
included.

---

## Prerequisites

### System dependencies

| Dependency | Purpose |
|---|---|
| [MongoDB server](https://www.mongodb.com/docs/manual/installation/) | local database that the ETL reads and writes |
| [MongoDB Database Tools](https://www.mongodb.com/docs/database-tools/) | `mongorestore` — restore dumps |
| [`mongosh`](https://www.mongodb.com/docs/mongodb-shell/) | post-restore verification |
| [`rsync`](https://rsync.samba.org/) | resumable dump transfer from NERSC |
| [`ssh`](https://www.openssh.com/) | NERSC auth and transport |
| [`just`](https://just.systems/) | recipe runner |
| [`uv`](https://docs.astral.sh/uv/) | Python / environment manager |

Each dependency name links to its upstream install instructions.

### Accounts and credentials

You need a NERSC account with access to the `m3408` (NMDC) project to download dumps.

> **Steps below are not directly verified.** Only the URLs and the
> `sshproxy -u <username>` invocation are verified. The exact click-through
> flow in Iris has not been walked through with a fresh account — follow
> NERSC's current docs if the UI differs, and please open a PR to correct
> the wording here.

1. **Request a NERSC account** and project membership at
   [iris.nersc.gov/add-user](https://iris.nersc.gov/add-user). The project
   name is `m3408`. Account vetting can take up to a week.
2. **Set up MFA** in [iris.nersc.gov](https://iris.nersc.gov) after your
   account is created.
3. **Download `sshproxy`** from
   [portal.nersc.gov/cfs/mfa/](https://portal.nersc.gov/cfs/mfa/). Linux
   (x86-64 and ARM), macOS, and Windows builds are available.
4. **Refresh your SSH cert** with `sshproxy -u <your-nersc-username>` (24h
   lifetime) — verified. When prompted for `Password+OTP`, enter your NERSC
   password immediately followed by your current MFA one-time code, with no
   space or separator between them (e.g. `mypass123456`).

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

## Getting a Dump from NERSC

`mongodump` archives of the NMDC MongoDB are generated nightly and synced
from GCS to NERSC under
`/global/cfs/cdirs/m3408/nmdc-mongodumps/from_google_cloud/`. The underlying
GCS buckets are private (service-account-gated), so NERSC SSH is the only
path for users.

Two mirrors are available, matching NMDC's bucket naming:

| `NMDC_MONGO_SOURCE` | Mirror path |
|---|---|
| `prod` (default) | `nmdc-runtime-prod-mongo-backup/` |
| `test` | `nmdc-runtime-test-mongo-backup/` |

Set `NMDC_MONGO_SOURCE=test` in `local/.env` (or on the command line) to use the
test mirror.

**First, refresh your NERSC SSH cert** (24-hour lifetime):

```bash
sshproxy -u <your-nersc-username>
```

Set `NERSC_USER` in `local/.env` if your NERSC username differs from `$USER`.

**List available dumps** (newest last):

```bash
just list-dumps        # default: last 20
just list-dumps 50     # last 50
NMDC_MONGO_SOURCE=test just list-dumps   # from the test mirror
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

`MONGO_URI` is the single source of truth for the connection. `MONGO_DBNAME`
is only used to build the default URI; at restore time the target db is parsed
back out of `MONGO_URI`, so the two can't drift. Override either:

```bash
# Default: restore into nmdc_lakehouse_prep
just restore-dump ./local/dumps/YYYYMMDD_HHMMSS

# Use a different db name
MONGO_DBNAME=nmdc_scratch just restore-dump ./local/dumps/YYYYMMDD_HHMMSS

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

## Relation to nmdc-runtime

[`nmdc-runtime`](https://github.com/microbiomedata/nmdc-runtime) runs its own
local MongoDB via Docker Compose (typically on port 27018 with `admin`/`root`
auth, database named `nmdc`). That stack is independent from this project:

- This repo defaults to a **different database name** (`nmdc_lakehouse_prep`)
  on the **default port** (27017, no auth) so it won't collide with an
  nmdc-runtime dev stack running on the same machine.
- If you'd rather point this project at an existing nmdc-runtime dev stack,
  override `MONGO_URI`:
  ```bash
  MONGO_URI=mongodb://admin:root@localhost:27018/nmdc_lakehouse_prep
  ```
  (The database name in the URI path is what `restore-dump` will write into;
  it does not have to be `nmdc`.)

Env-var naming follows `nmdc-runtime`'s convention (`MONGO_DBNAME`,
`MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_HOST`). The only difference is that
this project prefers the monolithic `MONGO_URI` for pymongo / mongorestore
compatibility; `MONGO_DBNAME` is exposed as a convenience for building the
default URI.

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

The default connection is `mongodb://localhost:27017/nmdc_lakehouse_prep`
(derived from `MONGO_DBNAME`). Works out of the box for a standard local install.
