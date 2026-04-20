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
