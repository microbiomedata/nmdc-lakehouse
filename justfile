# nmdc-lakehouse justfile
# Run `just` (no args) for the list of available recipes.

set dotenv-load

# ---------- Meta ----------

# Default recipe: list all recipes.
_default:
    @just --list

# ---------- Environment ----------

# Create the locked development environment, install hooks, and smoke-test it.
bootstrap: install-all
    @just _install-pre-commit-hook
    uv run nmdc-lakehouse --help > /dev/null
    @echo "Bootstrap complete. Next: just test, just check, or just cli --help"

# Diagnose the installed local environment without syncing or contacting services.
doctor:
    uv run --no-sync nmdc-lakehouse doctor

# Preview or run the BERDL promotion and recovery capability probe on disposable tables.
berdl-promotion-probe TENANT SOURCE_NAMESPACE DESTINATION_NAMESPACE OUTPUT *ARGS:
    uv run --no-sync nmdc-lakehouse berdl-promotion-probe \
        "{{ TENANT }}" "{{ SOURCE_NAMESPACE }}" "{{ DESTINATION_NAMESPACE }}" \
        --output "{{ OUTPUT }}" {{ ARGS }}

# Inspect a completed snapshot and explicitly configured BERDL tooling without mutation.
berdl-doctor SNAPSHOT_ROOT *ARGS:
    uv run --no-sync nmdc-lakehouse berdl-doctor "{{ SNAPSHOT_ROOT }}" {{ ARGS }}

# Validate manifested rows against the packaged target LinkML schema without mutation.
validate-target-rows SNAPSHOT_ROOT REPORT *ARGS:
    uv run --no-sync nmdc-lakehouse validate-target-rows "{{ SNAPSHOT_ROOT }}" --output "{{ REPORT }}" {{ ARGS }}

# Generate a reviewable metadata profile draft bound to a validated snapshot.
metadata-profile SNAPSHOT_ROOT PROFILE_ID NAMESPACE TITLE DESCRIPTION *ARGS:
    uv run --no-sync nmdc-lakehouse metadata-profile "{{ SNAPSHOT_ROOT }}" --profile-id "{{ PROFILE_ID }}" --namespace-name "{{ NAMESPACE }}" --title "{{ TITLE }}" --description "{{ DESCRIPTION }}" {{ ARGS }}

# Cross-check all reviewed publication artifacts before provider-specific staging.
publication-preflight SNAPSHOT_ROOT BUNDLE INVENTORY PLAN:
    uv run --no-sync nmdc-lakehouse publication-preflight "{{ SNAPSHOT_ROOT }}" --bundle "{{ BUNDLE }}" --inventory "{{ INVENTORY }}" --plan "{{ PLAN }}"

# Map approved metadata to one explicit staging namespace without mutation.
metadata-application-plan BUNDLE INVENTORY STAGING_NAMESPACE *ARGS:
    uv run --no-sync nmdc-lakehouse metadata-application-plan "{{ BUNDLE }}" --inventory "{{ INVENTORY }}" --staging-namespace "{{ STAGING_NAMESPACE }}" {{ ARGS }}

# Bind reviewed evidence to an exact plan-only BERIL staging command.
berdl-upload-plan SNAPSHOT_ROOT BUNDLE INVENTORY PLAN METADATA_PLAN TARGET_VALIDATION INGEST_CHECKOUT INGEST_REVISION TENANT DATASET BUCKET BRONZE_PREFIX PROGRESS_KEY CONFIG_KEY OUTPUT *ARGS:
    uv run --no-sync nmdc-lakehouse berdl-upload-plan "{{ SNAPSHOT_ROOT }}" --bundle "{{ BUNDLE }}" --inventory "{{ INVENTORY }}" --plan "{{ PLAN }}" --metadata-plan "{{ METADATA_PLAN }}" --target-validation "{{ TARGET_VALIDATION }}" --ingest-checkout "{{ INGEST_CHECKOUT }}" --ingest-revision "{{ INGEST_REVISION }}" --tenant "{{ TENANT }}" --dataset "{{ DATASET }}" --bucket "{{ BUCKET }}" --bronze-prefix "{{ BRONZE_PREFIX }}" --progress-key "{{ PROGRESS_KEY }}" --config-key "{{ CONFIG_KEY }}" --output "{{ OUTPUT }}" {{ ARGS }}

# Preview or execute one reviewed BERDL staging plan and verify its outcome.
berdl-upload PLAN UPSTREAM_OUTCOME OUTCOME *ARGS:
    uv run --no-sync nmdc-lakehouse berdl-upload "{{ PLAN }}" --upstream-outcome "{{ UPSTREAM_OUTCOME }}" --output "{{ OUTCOME }}" {{ ARGS }}

# Preview or apply approved table/column descriptions to verified BERDL staging tables.
berdl-apply-metadata METADATA_PLAN STAGING_OUTCOME INGEST_CHECKOUT OUTCOME *ARGS:
    uv run --no-sync nmdc-lakehouse berdl-apply-metadata "{{ METADATA_PLAN }}" "{{ STAGING_OUTCOME }}" --ingest-checkout "{{ INGEST_CHECKOUT }}" --output "{{ OUTCOME }}" {{ ARGS }}

# Preserve an existing configured Git hooks-path policy instead of replacing it.
[private]
_install-pre-commit-hook:
    #!/usr/bin/env bash
    set -euo pipefail
    if git config --get core.hooksPath >/dev/null; then
      echo "Git core.hooksPath is configured; leaving it unchanged."
      echo "Run repository hooks with: uv run pre-commit run --all-files"
    else
      uv run pre-commit install
    fi

# Create / update the uv-managed virtualenv with dev extras.
install:
    uv sync --locked --extra dev

# Install dev + docs extras.
install-all:
    uv sync --locked --extra dev --extra docs

# Upgrade the lockfile.
lock:
    uv lock --upgrade

# Remove the virtualenv and build artifacts.
clean:
    rm -rf .venv dist build .pytest_cache .ruff_cache .mypy_cache .vale-home
    find . -type d -name __pycache__ -exec rm -rf {} +

# ---------- Quality ----------

# Check the justfile with the canonical formatter used in CI.
lint-just:
    just --fmt --check

# Lint maintained prose with the repository-owned Vale configuration.
#
# HOME is isolated so a contributor's global Vale configuration cannot affect the result.
# Without it, Vale merges ~/Library/Application Support/vale (or the XDG equivalent) into
# every run, and a local pass would say nothing about CI.
#
# --minAlertLevel=suggestion is DISPLAY ONLY and is safe to keep here even though the
# repository currently carries 93 warnings and 103 suggestions. Vale's exit status keys on
# errors alone; MinAlertLevel governs what is printed, not what fails. Measured 2026-08-20
# on this tree: this recipe exits 0 with those 196 alerts present, and exits 1 as soon as one
# error-level alert appears. So neither this recipe nor the pre-commit hook that calls it can
# block a commit on a warning or a suggestion. `just test-prose-lint-exit` asserts both
# directions, because the invariant is load-bearing and not obvious: a reader who assumes
# Vale exits on whatever it prints concludes the opposite, which is how it was queried in
# review on https://github.com/microbiomedata/nmdc-lakehouse/pull/265.
prose-lint:
    mkdir -p .vale-home
    HOME="$PWD/.vale-home" vale --config=.vale.ini --minAlertLevel=suggestion --glob='**/*.md' README.md CONTRIBUTING.md AGENTS.md .github/pull_request_template.md docs scripts/README.md notebooks

# Assert that prose-lint's alert level cannot block a commit, and that errors still do.
# Guards the invariant documented on prose-lint above. Runs offline, touches no tracked file.
#
# THE WARNING-ONLY FIXTURE DEPENDS ON TWO RULES, AND ON A CAPITALISATION. Both are deliberate.
#
# It uses Mark.BareRefWord and Mark.Undefined, chosen because both sit at warning in .vale.ini
# with no promotion planned beside either. It deliberately does NOT use Mark.EmDash, which is at
# warning only until its backlog is cleared: promoting it, tracked in
# https://github.com/microbiomedata/nmdc-lakehouse/issues/264, would make this test fail while
# the contract it asserts stayed true, and whoever hit it would have to work out that the fixture
# rather than the behaviour had changed. Do not put an em dash back in.
#
# The token must stay ALL-CAPS. Vale's speller skips an all-caps word as an acronym, so `ZZZZZ`
# yields a warning from Mark.Undefined and nothing else. Measured 2026-08-20: `ZZZZZ` exits 0
# with one warning, while `zzzzz` and `Zzzzz` each exit 1 with a `Vale.Spelling` error. The exit
# codes are measured; which speller rule does the skipping is not, so do not rely on the
# mechanism beyond "all-caps is skipped". The first assertion's failure message says this too,
# because it appears exactly when someone needs it.
#
# Two independent warning-severity signals rather than one, so a change to either rule leaves the
# test still asserting something. Raised in review on
# https://github.com/microbiomedata/nmdc-lakehouse/pull/265.
test-prose-lint-exit:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
    mkdir -p "$tmp/home"
    printf '# t\n\nSee issue 3366 for the ZZZZZ term.\n' > "$tmp/warn.md"
    printf '# t\n\nWe leverage a robust design.\n' > "$tmp/err.md"
    rc=0; HOME="$tmp/home" vale --config=.vale.ini --minAlertLevel=suggestion "$tmp/warn.md" >/dev/null 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || { echo "prose-lint would block on a warning; exit was $rc."; \
        echo "If the warn fixture's token was edited, check it is still ALL-CAPS: the speller"; \
        echo "skips all-caps as an acronym, and any other casing is a Vale.Spelling error."; exit 1; }
    rc=0; HOME="$tmp/home" vale --config=.vale.ini --minAlertLevel=suggestion "$tmp/err.md" >/dev/null 2>&1 || rc=$?
    [ "$rc" -ne 0 ] || { echo "prose-lint did NOT block on an error; the gate is inert"; exit 1; }
    echo "prose-lint exit contract holds: warnings pass, errors fail"

# Require every runnable fenced block in docs/ to say whether anyone has run it.
#
# Five review rounds on https://github.com/microbiomedata/nmdc-lakehouse/pull/285 traced to one
# cause: the document described a procedure nobody had executed, and the claims around it were
# inferred from unrelated observations. Reviewers caught that five times and the repository caught
# it zero times, which is the gap this closes.
#
# A block passes by carrying either marker above it. `verified` records a run; `unverified` states
# that it has not been run and where that is tracked. Both pass, because forcing execution is not
# possible from a workstation for most of these, and an honestly labelled unrun procedure is not
# the failure. The failure is an unrun procedure that reads like a tested one.
#
# CI does not run `just check`; .github/workflows/ci.yml invokes each recipe as its own step, so
# both of these are listed there individually. A gate that only `just check` runs is a gate CI does
# not have.
#
# Blocks that predate the rule are grandfathered in docs/procedure-baseline.json by file and
# content hash, with a count of how many undeclared copies each file may keep. Content-only
# baselines were the first design and were wrong: docs/berdl-upload.md already repeats its
# validate-snapshot block, so any new block could have passed by copying it. Editing one changes
# its hash and brings it under the rule, which is the point: the blocks being changed are the
# blocks being claimed about. Do not add baseline entries by hand.
doc-procedures:
    uv run python scripts/python/doc_procedures.py docs --baseline docs/procedure-baseline.json

# Drop doc-procedure baseline entries the tree no longer needs.
#
# Run this after marking a grandfathered block. Marking changes its hash, which leaves its old
# entry with nothing to spend, and `doc-procedures` reports that. Pruning takes the minimum of
# each recorded allowance and what is actually undeclared now, so it can only shrink the baseline.
# It cannot exempt anything you have just written, which is exactly what regenerating would do.
prune-doc-baseline:
    uv run python scripts/python/doc_procedures.py docs --baseline docs/procedure-baseline.json --prune-baseline

# Assert that doc-procedures can actually fail. Runs offline, touches no tracked file.
#
# A guard tested only on input it must accept asserts nothing, and this repository has shipped
# that twice: a prose-lint call that could not match Vale's singular "1 error", and an export
# check that iterated its destination so a wholly absent table passed. So this asserts both
# directions against a fixture, never against docs/.
test-doc-procedures-exit:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
    printf 'intro\n\n```bash\necho hi\n```\n' > "$tmp/undeclared.md"
    printf 'intro\n\n<!-- unverified: fixture, never run -->\n```bash\necho hi\n```\n' > "$tmp/declared.md"
    rc=0; uv run --no-sync python scripts/python/doc_procedures.py "$tmp/undeclared.md" --baseline "$tmp/absent.json" >/dev/null 2>&1 || rc=$?
    [ "$rc" -ne 0 ] || { echo "doc-procedures did NOT fail on an undeclared block; the gate is inert"; exit 1; }
    rc=0; uv run --no-sync python scripts/python/doc_procedures.py "$tmp/declared.md" --baseline "$tmp/absent.json" >/dev/null 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || { echo "doc-procedures blocked a declared block; exit was $rc"; exit 1; }
    echo "doc-procedures exit contract holds: undeclared fails, declared passes"

# Dry-render one recipe with explicitly safe values, then lint without executing it.
[private]
_shellcheck-recipe RECIPE:
    @LAKEHOUSE_ROOT=./lakehouse MONGO_URI=mongodb://localhost:27017/nmdc NMDC_EXPORT_DIR=./local/nmdc_export NMDC_PARQUET_DIR=./local/nmdc_export/parquet NMDC_CSV_DIR=./local/nmdc_export/csv NMDC_DUCKDB_FILE=./local/nmdc_export/nmdc_flattened.duckdb NMDC_BIOSAMPLE_CSV=./local/nmdc_export/csv/flattened_biosample.csv NMDC_BIOSAMPLE_FIELDS_FILE=./local/nmdc_export/csv/flattened_biosample.fields bash -c 'rendered=$(just --dry-run "$1" 2>&1) || { printf "%s\n" "$rendered" >&2; exit 1; }; printf "%s\n" "$rendered" | uv run shellcheck --shell=bash -' _ "{{ RECIPE }}"

# Check the pinned ShellCheck and every maintained shell source.
shellcheck:
    @version="$(uv run shellcheck --version | sed -n 's/^version: //p')"; test "$version" = "0.11.0" || { echo "Expected ShellCheck 0.11.0, found $version" >&2; exit 1; }
    @if output="$(printf '#!/usr/bin/env bash\necho $unquoted\n' | uv run shellcheck --shell=bash - 2>&1)"; then echo "ShellCheck negative control unexpectedly passed" >&2; exit 1; fi; printf '%s\n' "$output" | grep -q 'SC2086'
    @just _shellcheck-recipe etl-collections
    @just _shellcheck-recipe etl-annotations
    @just _shellcheck-recipe etl-annotations-linkml
    @just _shellcheck-recipe clean-parquet
    @just _shellcheck-recipe flatten-nmdc-auth
    @just _shellcheck-recipe export-flattened-biosample-csv
    @just _shellcheck-recipe export-nmdc-duckdb
    @just _shellcheck-recipe _install-pre-commit-hook
    @find . \( -path './.git' -o -path './.venv' -o -path './build' -o -path './dist' \) -prune -o -type f -name '*.sh' -exec uv run shellcheck --shell=bash {} +

# Lint every GitHub Actions workflow with the pre-commit-pinned actionlint.
actionlint:
    uv run pre-commit run actionlint --all-files

# Run all linters & formatters in check mode.
# scripts/python is in scope (see .pre-commit-config.yaml); scripts/*.py
# at the top level is EMA legacy and deliberately excluded.
lint:
    uv run ruff check src tests scripts/python
    uv run ruff format --check src tests scripts/python

# Check for missing, unused, and transitive Python dependencies.
deps-lint:
    uv run deptry .

# Auto-format the codebase.
format:
    uv run ruff format src tests scripts/python
    uv run ruff check --fix src tests scripts/python

# Type-check with mypy.
typecheck:
    uv run mypy src scripts/python

# ---------- Tests ----------

# Run the full unit test suite.
test:
    uv run pytest

# Run tests with coverage report.
test-cov:
    uv run pytest --cov=nmdc_lakehouse --cov-report=term-missing --cov-report=xml
    uv run coverage report

# Gate lines added or changed against a base branch on direct test coverage.
# Separate from the total floor in pyproject: the total floor protects the codebase
# as it stands, and cannot notice an untested function arriving in a large tested
# codebase. This measures only what the change touches. Requires coverage.xml, so
# run `just test-cov` first, and requires the base branch to be fetched.
diff-cover BASE="origin/main":
    @test -s coverage.xml || { echo "coverage.xml is missing or empty; run 'just test-cov' first" >&2; exit 1; }
    uv run diff-cover coverage.xml --compare-branch="{{ BASE }}" --fail-under=90 --show-uncovered

# Run only integration tests (require live DBs).
test-integration:
    ENABLE_DB_TESTS=true uv run pytest -m integration

# ---------- MongoDB tunnel ----------

nmdc_jump_key := env_var_or_default("NMDC_JUMP_KEY", "~/.ssh/jump-dev.microbiomedata.org.private_key")

# Open the GCP SSH tunnel — leave this terminal open while running ETL jobs.
# Override the key path with NMDC_JUMP_KEY if needed.
tunnel:
    ssh -i {{ nmdc_jump_key }} \
        -o IdentitiesOnly=yes \
        -L 27124:runtime-api-mongodb-headless.nmdc-prod.svc.cluster.local:27017 \
        -o ServerAliveInterval=60 \
        ssh-mongo@jump-dev.microbiomedata.org

# ---------- Run / Jobs ----------

# Show CLI help.
cli *ARGS:
    uv run nmdc-lakehouse {{ ARGS }}

# Run a named ETL job.
run-job JOB *ARGS:
    uv run nmdc-lakehouse run-job {{ JOB }} {{ ARGS }}

# Dump MongoDB collections to Parquet and manifest the result.
#
# Complete by default (~22 min): every collection the installed schema declares, including
# functional_annotation_agg, which is 53M rows and dominates the runtime. Name collections to
# leave out for a faster partial run, for example
# `just etl-collections functional_annotation_agg` (~5 min). The manifest records what was
# skipped, so a partial snapshot cannot be mistaken for a complete one later.
#
# Requires the GCP SSH tunnel to be open, see docs/mongodb-connection.md.
# Defaults Parquet to local/mongodb-metadata-<timestamp> unless LAKEHOUSE_ROOT is set.
# Writes a matching local log plus metrics and a manifest inside the snapshot.
etl-collections *SKIP:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p local
    timestamp="$(date +%Y%m%d_%H%M%S)"
    log="local/etl-collections-${timestamp}.log"
    if [[ -z "${LAKEHOUSE_ROOT:-}" ]]; then
      LAKEHOUSE_ROOT="local/mongodb-metadata-${timestamp}"
      if [[ -e "$LAKEHOUSE_ROOT" ]]; then
        echo "Refusing to reuse default output path: $LAKEHOUSE_ROOT" >&2
        exit 1
      fi
    fi
    export LAKEHOUSE_ROOT
    metrics="$LAKEHOUSE_ROOT/etl-metrics.json"
    source_label="${LAKEHOUSE_SOURCE_LABEL:-nmdc-production}"
    echo "Writing Parquet to $LAKEHOUSE_ROOT"
    echo "Logging to $log"
    echo "Writing metrics to $metrics"
    echo "Recording validated source identity in the snapshot manifest"
    skip_args=()
    for collection in {{ SKIP }}; do
      skip_args+=(--skip "$collection")
    done
    if [[ ${#skip_args[@]} -eq 0 ]]; then
      echo "Dumping every collection. This includes functional_annotation_agg, which is"
      echo "53M+ rows and dominates the runtime; name collections to skip if that is not wanted."
    else
      echo "Skipping: {{ SKIP }}"
    fi
    time uv run nmdc-lakehouse run-job all-collections \
      "${skip_args[@]+"${skip_args[@]}"}" --metrics "$metrics" 2>&1 | tee "$log"
    uv run nmdc-lakehouse create-snapshot-manifest "$LAKEHOUSE_ROOT" \
      --metrics "$metrics" --source-label "$source_label"
    uv run nmdc-lakehouse validate-snapshot "$LAKEHOUSE_ROOT"

# Convert functional_annotation_agg alone to Parquet via direct pymongo (~17 min, 54.8M records).
# `just etl-collections` already includes this collection; use this recipe only to redo that one
# collection without redumping the rest, which produces no manifest on its own.
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

# Delete zero-row Parquet files under LAKEHOUSE_ROOT (e.g. empty collections).
drop-empty-parquet:
    uv run python scripts/python/drop_empty_parquet.py "{{ lakehouse_root }}"

# Preview recognized metadata Parquet files under LAKEHOUSE_ROOT.
# Pass --delete explicitly to remove the previewed files.
clean-parquet *ARGS:
    uv run nmdc-lakehouse clean-parquet --root "{{ lakehouse_root }}" {{ ARGS }}

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

# Build and smoke-test the installable wheel and source distribution.
test-dist:
    bash scripts/check_distribution.sh

# Run the deterministic local quality checks.
check: lint-just prose-lint test-prose-lint-exit doc-procedures test-doc-procedures-exit shellcheck actionlint lint deps-lint typecheck check-flat-schema test-cov diff-cover

# ---------- NMDC flatten/export pipeline (copied from external-metadata-awareness) ----------
# See scripts/README.md for details. These recipes shell out to utilities under
# scripts/ and depend on a local MongoDB containing the NMDC collections.

mongo_uri := env_var_or_default("MONGO_URI", "mongodb://localhost:27017/nmdc")
nmdc_export_dir := env_var_or_default("NMDC_EXPORT_DIR", "./local/nmdc_export")
nmdc_parquet_dir := env_var_or_default("NMDC_PARQUET_DIR", nmdc_export_dir + "/parquet")
nmdc_csv_dir := env_var_or_default("NMDC_CSV_DIR", nmdc_export_dir + "/csv")
nmdc_duckdb_file := env_var_or_default("NMDC_DUCKDB_FILE", nmdc_export_dir + "/nmdc_flattened.duckdb")
nmdc_biosample_csv := env_var_or_default("NMDC_BIOSAMPLE_CSV", nmdc_csv_dir + "/flattened_biosample.csv")
nmdc_biosample_fields_file := env_var_or_default("NMDC_BIOSAMPLE_FIELDS_FILE", nmdc_csv_dir + "/flattened_biosample.fields")

nmdc_flattened_collections := "flattened_biosample flattened_biosample_chem_administration flattened_biosample_field_counts flattened_study flattened_study_associated_dois flattened_study_has_credit_associations"

# Flatten NMDC MongoDB collections (biosample, study + nested extractions) in place.
flatten-nmdc:
    uv run python scripts/flatten_nmdc_collections.py --mongo-uri "{{ mongo_uri }}"

# Flatten against an auth-required MongoDB; reads creds from local/.env.ncbi-loadbalancer.27778.
flatten-nmdc-auth:
    #!/usr/bin/env bash
    set -euo pipefail
    # The runtime-only credentials file is intentionally absent from CI.
    # shellcheck disable=SC1091
    set -a && . local/.env.ncbi-loadbalancer.27778 && set +a
    uv run python scripts/flatten_nmdc_collections.py \
      --mongo-uri "mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/${DEST_MONGO_DB}?authSource=admin&authMechanism=SCRAM-SHA-256&directConnection=true"

# Export flattened_biosample to CSV using the distinct field list from flattened_biosample_field_counts.
export-flattened-biosample-csv:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ nmdc_csv_dir }}"
    echo "Building full field list from flattened_biosample_field_counts..."
    mongosh "{{ mongo_uri }}" --quiet \
      --eval 'db.flattened_biosample_field_counts.distinct("field").sort().join("\n")' \
      > "{{ nmdc_biosample_fields_file }}"
    echo "Exporting flattened_biosample to CSV..."
    mongoexport --uri="{{ mongo_uri }}" \
      --collection="flattened_biosample" \
      --type=csv \
      --fieldFile="{{ nmdc_biosample_fields_file }}" \
      --out="{{ nmdc_biosample_csv }}"
    echo "Exported to {{ nmdc_biosample_csv }}"

# Export all flattened_* collections to a single DuckDB file via mongoexport JSON + read_json.
export-nmdc-duckdb:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{ nmdc_export_dir }}"
    echo "=== NMDC Flattened Collections to DuckDB ==="
    for collection in {{ nmdc_flattened_collections }}; do
      echo "Processing $collection..."
      json_file="{{ nmdc_export_dir }}/$collection.json"
      mongoexport --uri="{{ mongo_uri }}" \
        --collection="$collection" \
        --type=json \
        --out="$json_file" 2>&1 | grep -v "connected to" || true
      if [ ! -s "$json_file" ]; then
        echo "  FAILED: mongoexport produced no output for $collection"
        continue
      fi
      duckdb "{{ nmdc_duckdb_file }}" -c \
        "CREATE OR REPLACE TABLE $collection AS SELECT * EXCLUDE (_id) FROM read_json('$json_file', auto_detect=true, union_by_name=true, dateformat='DISABLED', timestampformat='DISABLED');"
      echo "  $collection loaded"
      rm -f "$json_file"
    done
    echo "=== DuckDB export complete: {{ nmdc_duckdb_file }} ==="

# Export DuckDB tables to individual Parquet files for lakehouse ingestion.
export-nmdc-parquet: export-nmdc-duckdb
    uv run python scripts/export_duckdb_to_parquet.py "{{ nmdc_duckdb_file }}" --output-dir "{{ nmdc_parquet_dir }}"

# Full pipeline: flatten in Mongo -> DuckDB -> Parquet -> biosample CSV.
# Generate the canonical primary and side-table LinkML target schema.
generate-flat-schema *ARGS:
    @uv run python scripts/python/generate_flattened_schema.py {{ ARGS }}

# Fail when the committed LinkML target schema differs from current generation.
check-flat-schema:
    @uv run python scripts/python/generate_flattened_schema.py --check

flatten-and-export-nmdc: flatten-nmdc export-nmdc-parquet export-flattened-biosample-csv
    @echo ""
    @echo "=== NMDC flatten and export complete ==="
    @echo "DuckDB:   {{ nmdc_duckdb_file }}"
    @echo "Parquet:  {{ nmdc_parquet_dir }}"
    @echo "CSV:      {{ nmdc_biosample_csv }}"
