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

# ---------- MongoDB tunnel ----------

nmdc_jump_key := env_var_or_default("NMDC_JUMP_KEY", "~/.ssh/jump-dev.microbiomedata.org.private_key")

# Open the GCP SSH tunnel — leave this terminal open while running ETL jobs.
# Override the key path with NMDC_JUMP_KEY if needed.
tunnel:
    ssh -i {{nmdc_jump_key}} \
        -L 27124:runtime-api-mongodb-headless.nmdc-prod.svc.cluster.local:27017 \
        -o ServerAliveInterval=60 \
        ssh-mongo@jump-dev.microbiomedata.org

# ---------- Run / Jobs ----------

# Show CLI help.
cli *ARGS:
    uv run nmdc-lakehouse {{ARGS}}

# Run a named ETL job.
run-job JOB *ARGS:
    uv run nmdc-lakehouse run-job {{JOB}} {{ARGS}}

# Convert all schema collections except functional_annotation_agg to Parquet (~5 min).
# Requires the GCP SSH tunnel to be open — see docs/mongodb-connection.md.
# Logs to local/etl-collections-<timestamp>.log
etl-collections:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p local
    log="local/etl-collections-$(date +%Y%m%d_%H%M%S).log"
    echo "Logging to $log"
    time uv run nmdc-lakehouse run-job all-collections --skip functional_annotation_agg 2>&1 | tee "$log"

# Convert functional_annotation_agg to Parquet via direct pymongo (~17 min, 54.8M records).
# Requires the GCP SSH tunnel to be open — see docs/mongodb-connection.md.
# Run inside screen or tmux so the job survives terminal close.
# Logs to local/etl-annotations-<timestamp>.log
etl-annotations:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p local
    log="local/etl-annotations-$(date +%Y%m%d_%H%M%S).log"
    echo "Logging to $log"
    time uv run nmdc-lakehouse run-job functional_annotation_agg 2>&1 | tee "$log"

# Convert functional_annotation_agg via the linkml-store schema-driven path (bypasses DirectMongoToParquetJob).
# Use this to benchmark the linkml-store find_iter fix against the direct pymongo path.
# Requires the GCP SSH tunnel to be open — see docs/mongodb-connection.md.
# Logs to local/etl-faa-linkml-<timestamp>.log
etl-annotations-linkml:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p local
    log="local/etl-faa-linkml-$(date +%Y%m%d_%H%M%S).log"
    echo "Logging to $log"
    time uv run nmdc-lakehouse run-job functional_annotation_agg__linkml 2>&1 | tee "$log"

lakehouse_root := env_var_or_default("LAKEHOUSE_ROOT", "./lakehouse")

# Delete every generated Parquet file under LAKEHOUSE_ROOT.
clean-parquet:
    #!/usr/bin/env bash
    set -euo pipefail
    target="$(realpath -m "{{lakehouse_root}}")"
    repo_root="$(pwd -P)"
    case "$target" in
        ""|"/") echo "Refusing to delete unsafe LAKEHOUSE_ROOT: '{{lakehouse_root}}'" >&2; exit 1 ;;
    esac
    case "$target" in
        "$repo_root"/*) ;;
        *) echo "Refusing to delete outside repository: '$target'" >&2; exit 1 ;;
    esac
    rm -rf -- "$target"/*

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

# ---------- NMDC flatten/export pipeline (copied from external-metadata-awareness) ----------
# See scripts/README.md for details. These recipes shell out to the scripts in
# scripts/python/ and depend on a local MongoDB containing the NMDC collections.

mongo_uri                   := env_var_or_default("MONGO_URI", "mongodb://localhost:27017/nmdc")
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
# Generate the flattened LinkML schema (one class per Database slot).
# Output: ./local/nmdc_schema_flattened.yaml
generate-flat-schema:
    @uv run python scripts/python/generate_flattened_schema.py

flatten-and-export-nmdc: flatten-nmdc export-nmdc-parquet export-flattened-biosample-csv
    @echo ""
    @echo "=== NMDC flatten and export complete ==="
    @echo "DuckDB:   {{nmdc_duckdb_file}}"
    @echo "Parquet:  {{nmdc_parquet_dir}}"
    @echo "CSV:      {{nmdc_biosample_csv}}"
