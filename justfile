# nmdc-lakehouse justfile
# Run `just` (no args) for the list of available recipes.

set dotenv-load := true

# ---------- Meta ----------

# Default recipe: list all recipes.
_default:
    @just --list

# ---------- Environment ----------

# Create / update the uv-managed virtualenv with dev extras.
install:
    uv sync --extra dev

# Install dev + docs extras.
install-all:
    uv sync --extra dev --extra docs

# Upgrade the lockfile.
lock:
    uv lock --upgrade

# Remove the virtualenv and build artifacts.
clean:
    rm -rf .venv dist build .pytest_cache .ruff_cache .mypy_cache
    find . -type d -name __pycache__ -exec rm -rf {} +

# ---------- Quality ----------

# Run all linters & formatters in check mode.
lint:
    uv run ruff check src tests
    uv run ruff format --check src tests

# Auto-format the codebase.
format:
    uv run ruff format src tests
    uv run ruff check --fix src tests

# Type-check with mypy.
typecheck:
    uv run mypy src

# ---------- Tests ----------

# Run the full unit test suite.
test:
    uv run pytest

# Run tests with coverage report.
test-cov:
    uv run pytest --cov=nmdc_lakehouse --cov-report=term-missing

# Run only integration tests (require live DBs).
test-integration:
    ENABLE_DB_TESTS=true uv run pytest -m integration

# ---------- Run / Jobs ----------

# Show CLI help.
cli *ARGS:
    uv run nmdc-lakehouse {{ARGS}}

# Placeholder: run a named ETL job (wired up once jobs exist).
run-job JOB *ARGS:
    uv run nmdc-lakehouse run-job {{JOB}} {{ARGS}}

# ---------- Docs ----------

# Serve documentation locally.
docs-serve:
    uv run mkdocs serve

# Build the documentation site.
docs-build:
    uv run mkdocs build

# ---------- Build / Release ----------

# Build sdist and wheel.
build:
    uv build

# Run the full pre-commit gauntlet.
check: lint typecheck test

# ---------- MongoDB ----------

# MONGO_URI is the single source of truth for the connection string (host,
# credentials, and target database name as the path component). MONGO_DB
# exists only to make the default URI easy to construct; if you override
# MONGO_URI, recipes parse the target db back out of the URI so the two can
# never drift. The dump's internal namespace is "nmdc" but we restore into a
# dedicated db so an existing "nmdc" database (from nmdc-runtime or earlier
# work) stays untouched.
mongo_db  := env_var_or_default("MONGO_DB", "nmdc_lakehouse_prep")
mongo_uri := env_var_or_default("MONGO_URI", "mongodb://localhost:27017/" + mongo_db)

# NERSC connection details for fetching production MongoDB dumps.
# Refresh the SSH cert (24h lifetime) with: sshproxy -u <nersc-username>
nersc_user      := env_var_or_default("NERSC_USER", env_var_or_default("USER", ""))
nersc_key       := env_var_or_default("NERSC_SSH_KEY", "~/.ssh/nersc")
nersc_host      := env_var_or_default("NERSC_HOST", "dtn01.nersc.gov")
nersc_dump_root := "/global/cfs/cdirs/m3408/nmdc-mongodumps/from_google_cloud/nmdc-runtime-prod-mongo-backup"

# Preferred dump — "latest" resolves to the newest timestamp on NERSC.
# Override in local/.env (NMDC_DUMP=20260420_060655) or on the command line.
nmdc_dump := env_var_or_default("NMDC_DUMP", "latest")

_ssh_opts := "-o IdentitiesOnly=yes -o ConnectTimeout=10 -i " + nersc_key

# List the last N dump timestamps on NERSC with total size (newest last).
# Expect ~0.5-1 second per dump — du has to scan every file in each directory.
# Complete dumps are ~3 GB; partial/failed ones are a few hundred MB.
list-dumps N="20":
    ssh {{_ssh_opts}} {{nersc_user}}@{{nersc_host}} \
        "cd {{nersc_dump_root}} && ls -1 | sort | tail -{{N}} | while read d; do du -sh \"\$d\"; done"

# Fetch only the schema-specified collections from a NERSC dump to
# DEST/<timestamp>/. Defaults to $NMDC_DUMP (or "latest"); override with the
# first positional arg. Uses rsync --files-from so only the ~34 files for the
# 17 schema collections are transferred (~1.2 GB of a ~3 GB dump).
# Usage: just fetch-dump                            (uses $NMDC_DUMP / latest)
#        just fetch-dump 20260418_060011
#        just fetch-dump latest /path/to/dumps
fetch-dump DUMP=nmdc_dump DEST="./local/dumps":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{DUMP}}" = "latest" ]; then
        DUMP=$(ssh {{_ssh_opts}} {{nersc_user}}@{{nersc_host}} \
            "ls -1 {{nersc_dump_root}} | sort | tail -1")
        echo "Latest dump on NERSC: $DUMP"
    else
        DUMP="{{DUMP}}"
    fi
    mkdir -p "{{DEST}}/$DUMP"
    uv run python scripts/python/fetch_manifest.py | \
        rsync --archive --human-readable --progress --partial \
            --rsh "ssh {{_ssh_opts}}" \
            --files-from=- \
            {{nersc_user}}@{{nersc_host}}:{{nersc_dump_root}}/$DUMP/ \
            "{{DEST}}/$DUMP/"
    echo "Fetched to {{DEST}}/$DUMP/"

# Delete every fetched dump under ./local/dumps/.
clean-dumps:
    rm -rf ./local/dumps/*

# Drop the entire database named in MONGO_URI's path. Useful before
# restoring into a known-clean state. Destructive — drops everything.
drop-db:
    mongosh "{{mongo_uri}}" --quiet --eval 'print("Dropping " + db.getName()); db.dropDatabase()'

# List the nmdc-schema-specified collections (Database slots).
list-schema-collections:
    @uv run python scripts/python/schema_collections.py

# Restore a local dump directory (the parent, containing an nmdc/ subdir).
# When paired with `just fetch-dump`, only schema-specified collections are
# present on disk so mongorestore naturally loads just those. Files from the
# dump's internal "nmdc" namespace are renamed to the target db derived from
# MONGO_URI's path (default: nmdc_lakehouse_prep).
# Usage: just restore-dump ./local/dumps/YYYYMMDD_HHMMSS
# Override the target db with either MONGO_DB=foo or a full MONGO_URI whose
# path points at the intended db — both stay consistent because the target
# db is parsed out of the URI at runtime.
restore-dump DUMP_DIR:
    #!/usr/bin/env bash
    set -euo pipefail
    # mongorestore treats the URI path as an implicit --db that confuses
    # namespace rewriting. Split into (a) server_uri (no path) for --uri and
    # (b) target_db (the path segment) for --nsTo, so MONGO_URI is the single
    # source of truth for the target database.
    read server_uri target_db < <(python3 -c 'import sys, urllib.parse as u; p=u.urlparse(sys.argv[1]); print(u.urlunparse(p._replace(path="")), p.path.lstrip("/").split("?")[0])' "{{mongo_uri}}")
    if [ -z "$target_db" ]; then
        echo "MONGO_URI must include a database name in the path, e.g. mongodb://localhost:27017/nmdc_lakehouse_prep" >&2
        exit 1
    fi
    echo "Restoring into database: $target_db"
    mongorestore \
        --uri "$server_uri" \
        --gzip --drop --verbose --stopOnError \
        --nsFrom "nmdc.*" --nsTo "$target_db.*" \
        --dir "{{DUMP_DIR}}"

# ---------- NMDC flatten/export pipeline (copied from external-metadata-awareness) ----------
# See scripts/README.md for details. These recipes shell out to the scripts in
# scripts/python/ and depend on a local MongoDB containing the NMDC collections.
nmdc_export_dir             := env_var_or_default("NMDC_EXPORT_DIR", "./local/nmdc_export")
nmdc_parquet_dir            := env_var_or_default("NMDC_PARQUET_DIR", nmdc_export_dir + "/parquet")
nmdc_csv_dir                := env_var_or_default("NMDC_CSV_DIR", nmdc_export_dir + "/csv")
nmdc_duckdb_file            := env_var_or_default("NMDC_DUCKDB_FILE", nmdc_export_dir + "/nmdc_flattened.duckdb")
nmdc_biosample_csv          := env_var_or_default("NMDC_BIOSAMPLE_CSV", nmdc_csv_dir + "/flattened_biosample.csv")
nmdc_biosample_fields_file  := env_var_or_default("NMDC_BIOSAMPLE_FIELDS_FILE", nmdc_csv_dir + "/flattened_biosample.fields")

nmdc_flattened_collections := "flattened_biosample flattened_biosample_chem_administration flattened_biosample_field_counts flattened_study flattened_study_associated_dois flattened_study_has_credit_associations"

# Flatten NMDC MongoDB collections (biosample, study + nested extractions) in place.
flatten-nmdc:
    uv run python scripts/python/flatten_nmdc_collections.py --mongo-uri "{{mongo_uri}}"

# Flatten against an auth-required MongoDB; reads creds from local/.env.ncbi-loadbalancer.27778.
flatten-nmdc-auth:
    #!/usr/bin/env bash
    set -euo pipefail
    set -a && . local/.env.ncbi-loadbalancer.27778 && set +a
    uv run python scripts/python/flatten_nmdc_collections.py \
      --mongo-uri "mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${DEST_MONGO_DB}?authSource=admin&authMechanism=SCRAM-SHA-256&directConnection=true"

# Export flattened_biosample to CSV using the distinct field list from flattened_biosample_field_counts.
export-flattened-biosample-csv:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{nmdc_csv_dir}}"
    echo "Building full field list from flattened_biosample_field_counts..."
    mongosh "{{mongo_uri}}" --quiet \
      --eval 'db.flattened_biosample_field_counts.distinct("field").sort().join("\n")' \
      > "{{nmdc_biosample_fields_file}}"
    echo "Exporting flattened_biosample to CSV..."
    mongoexport --uri="{{mongo_uri}}" \
      --collection="flattened_biosample" \
      --type=csv \
      --fieldFile="{{nmdc_biosample_fields_file}}" \
      --out="{{nmdc_biosample_csv}}"
    echo "Exported to {{nmdc_biosample_csv}}"

# Export all flattened_* collections to a single DuckDB file via mongoexport JSON + read_json.
export-nmdc-duckdb:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{nmdc_export_dir}}"
    echo "=== NMDC Flattened Collections to DuckDB ==="
    for collection in {{nmdc_flattened_collections}}; do
      echo "Processing $collection..."
      json_file="{{nmdc_export_dir}}/$collection.json"
      mongoexport --uri="{{mongo_uri}}" \
        --collection="$collection" \
        --type=json \
        --out="$json_file" 2>&1 | grep -v "connected to" || true
      if [ ! -s "$json_file" ]; then
        echo "  FAILED: mongoexport produced no output for $collection"
        continue
      fi
      duckdb "{{nmdc_duckdb_file}}" -c \
        "CREATE OR REPLACE TABLE $collection AS SELECT * EXCLUDE (_id) FROM read_json('$json_file', auto_detect=true, union_by_name=true, dateformat='DISABLED', timestampformat='DISABLED');"
      echo "  $collection loaded"
      rm -f "$json_file"
    done
    echo "=== DuckDB export complete: {{nmdc_duckdb_file}} ==="

# Export DuckDB tables to individual Parquet files for lakehouse ingestion.
export-nmdc-parquet: export-nmdc-duckdb
    uv run python scripts/python/export_duckdb_to_parquet.py "{{nmdc_duckdb_file}}" --output-dir "{{nmdc_parquet_dir}}"

# Full pipeline: flatten in Mongo -> DuckDB -> Parquet -> biosample CSV.
flatten-and-export-nmdc: flatten-nmdc export-nmdc-parquet export-flattened-biosample-csv
    @echo ""
    @echo "=== NMDC flatten and export complete ==="
    @echo "DuckDB:   {{nmdc_duckdb_file}}"
    @echo "Parquet:  {{nmdc_parquet_dir}}"
    @echo "CSV:      {{nmdc_biosample_csv}}"
