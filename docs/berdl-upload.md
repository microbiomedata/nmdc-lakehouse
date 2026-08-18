# Uploading `lakehouse/` Parquet output to BERDL

> **Historical mechanics only:** this guide records the April 2026 transport and
> ingest path. Do not use it by itself to overwrite or replace live tables. Its
> fixed dataset name, table count, and Delta verification examples are not the
> current replacement contract. Any replacement must follow the reviewed plan
> and approval checkpoints in the
> [portable publication contract](publication-contract.md), discover the live
> catalog/provider, and classify every candidate and live table.

This is the off-cluster path: `etl-collections`/`etl-annotations` already produced
local Parquet under `LAKEHOUSE_ROOT` (see the configuration table in `README.md`).
This document covers getting that output into BERDL Silver as `nmdc_nmdc_linkml_store`.

**Status:** verified working end-to-end 2026-04-25 (see [#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51)), but as a manual
workaround: SSH access to the tunnel host was blocked at the time, so the actual
run happened on-cluster via JupyterHub instead of following the steps below start
to finish. The steps themselves (1-7) were validated; step 8 was substituted with
an on-cluster notebook run. Re-verify steps 8-9 the next time this runs off-cluster.

---

## Supported readiness check

Before following any historical transport step, validate the completed snapshot
and the external tooling without changing either repository or contacting BERDL:

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
inventory, and disposition plan, run the destination-neutral artifact gate from
the `nmdc-lakehouse` checkout:

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

The destination, catalog, and table format are explicit observations. Do not
copy the historical Delta examples below unless current discovery confirms
them. A missing or blank value is a readiness failure.

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

---

## Prerequisites, obtain before doing anything else

### 1. Python 3.13 environment

`data-lakehouse-ingest` requires Python >= 3.13, which may not be your system default.

```bash
uv python install 3.13
uv venv .venv-berdl --python 3.13 --seed
```

### 2. Ingest packages

From [`kbaseincubator/BERIL-research-observatory`](https://github.com/kbaseincubator/BERIL-research-observatory):

```bash
bash scripts/bootstrap_client.sh
bash scripts/bootstrap_ingest.sh
```

Both scripts belong to the external checkout and can change its dedicated
environment. Run them only when provisioning that checkout. Do not ignore a
failed verification; `just berdl-doctor` must subsequently find both required
distributions in `.venv-berdl`.

### 3. MinIO client (`mc`)

```bash
mkdir -p ~/bin
curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o ~/bin/mc   # macOS: darwin-amd64 or darwin-arm64
chmod +x ~/bin/mc
```

### 4. KBase auth token

`berdl-remote` reads `KBASE_AUTH_TOKEN`. Obtain and refresh it through the
supported KBase authentication workflow. Keep it in the process environment or
an untracked `.env`; never copy it into documentation, logs, or tracked files.

### 5. SSH access to `login1.berkeley.kbase.us`

Required for the tunnels in the next section. This blocked the 2026-04-25 run
entirely. If you don't have an account there yet, ask in `#ber_lakehouse` before
starting anything else in this doc.

---

## Per-session: open the tunnels and configure `mc`

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

## Preflight

```bash
source .venv-berdl/bin/activate
python scripts/ingest_preflight.py \
    --data-dir "$NMDC_LAKEHOUSE_DATA" \
    --tenant nmdc --dataset nmdc_linkml_store \
    --mode overwrite --chunk-target-gb 20
```

All 13 tables should show as single-batch.

## Upload metadata

```bash
https_proxy=http://127.0.0.1:8123 ~/bin/mc cp --recursive \
    "$NMDC_LAKEHOUSE_DATA/metadata/" \
    "berdl-minio/cdm-lake/tenant-general-warehouse/nmdc/datasets/nmdc_linkml_store/metadata/"
```

`mc` interprets relative paths as MinIO URLs. Always use absolute local paths.

## Run the ingest notebook

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

## Verify in BERDL SQL

```sql
SHOW TABLES IN nmdc_nmdc_linkml_store;
SELECT COUNT(*) FROM nmdc_nmdc_linkml_store.biosample_set;
SELECT COUNT(*) FROM nmdc_nmdc_linkml_store.functional_annotation_agg;
```

---

## Known gotchas

- **`verify_ingest` reports MISMATCH for every table.** Not a real failure. It counts
  line breaks in source files, which is meaningless for binary Parquet. Trust the
  managed-table row counts from the SQL verification above instead.
- **Namespace naming.** Tenant `nmdc`, dataset `nmdc_linkml_store` -> registered
  namespace `nmdc_nmdc_linkml_store` (tenant prefix + dataset name).
- **`MODE=overwrite`** makes repeated runs idempotent, safe to re-run after a fresh
  `etl-collections`.

## Paths

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
