"""Apply and verify approved table and column descriptions in BERDL staging."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import sys
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from nmdc_lakehouse.berdl_staging import (
    BerdlStagingOutcome,
    BerdlStagingPlanError,
    _require_pristine_checkout,
    _require_revision_package,
    _run_command,
)
from nmdc_lakehouse.metadata_application import (
    MetadataApplicationPlan,
    MetadataOperation,
    MetadataOperationKind,
)


class BerdlMetadataError(ValueError):
    """Raised when staging metadata cannot be applied and verified safely."""


class MetadataTarget(BaseModel):
    """One staging table and its approved description coverage."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    table_description: bool
    column_descriptions: int = Field(ge=0)
    missing_descriptions: int = Field(ge=0)


class BerdlMetadataPreview(BaseModel):
    """Credential-free summary of the exact metadata application slice."""

    model_config = ConfigDict(extra="forbid", strict=True)

    status: Literal["preview-only"]
    snapshot_id: str
    destination_id: str
    staging_namespace: str
    staging_outcome_sha256: str
    metadata_plan_sha256: str
    deferred_namespace_operations: int = Field(ge=0)
    targets: list[MetadataTarget]


class AppliedMetadataTarget(BaseModel):
    """Verified metadata coverage for one staged table."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    table_description_status: Literal["verified", "not-planned"]
    columns_verified: list[str]
    # Verified by read-back but not written on this run, because the catalog already held the
    # planned description. Defaulted so outcomes written before this field remain readable.
    columns_already_correct: list[str] = []
    # The same distinction for the table description, which "verified" alone cannot express.
    # False when no table description was planned, which table_description_status already says.
    table_description_already_correct: bool = False


class BerdlMetadataOutcome(BaseModel):
    """Credential-free evidence of staging table and column metadata read-back."""

    model_config = ConfigDict(extra="forbid", strict=True)

    outcome_format_version: Literal[1]
    status: Literal["metadata-verified"]
    snapshot_id: str
    destination_id: str
    staging_namespace: str
    staging_outcome_sha256: str
    metadata_plan_sha256: str
    deferred_namespace_operations: int = Field(ge=0)
    targets: list[AppliedMetadataTarget]


def _read_model(path: Path, model: type[BaseModel], label: str) -> tuple[BaseModel, str]:
    document = path.expanduser()
    if not document.is_file() or document.is_symlink():
        raise BerdlMetadataError(f"The {label} must be an ordinary file.")
    try:
        contents = document.read_bytes()
        parsed = model.model_validate_json(contents, strict=True)
    except (OSError, UnicodeDecodeError, ValidationError) as error:
        raise BerdlMetadataError(f"The {label} is not valid.") from error
    return parsed, hashlib.sha256(contents).hexdigest()


def _description_operations(
    plan: MetadataApplicationPlan,
) -> tuple[dict[str, MetadataOperation], dict[str, list[MetadataOperation]], int]:
    table_operations: dict[str, MetadataOperation] = {}
    column_operations: dict[str, list[MetadataOperation]] = defaultdict(list)
    deferred = 0
    for operation in plan.supported_operations:
        if operation.kind == MetadataOperationKind.TABLE_DESCRIPTION:
            if operation.table is None:
                raise BerdlMetadataError("A table-description operation has no table target.")
            table_operations[operation.table] = operation
        elif operation.kind == MetadataOperationKind.COLUMN_DESCRIPTION:
            if operation.table is None or operation.column is None:
                raise BerdlMetadataError("A column-description operation has an incomplete target.")
            column_operations[operation.table].append(operation)
        else:
            deferred += 1
    return table_operations, column_operations, deferred


def build_berdl_metadata_preview(
    plan: MetadataApplicationPlan,
    staging: BerdlStagingOutcome,
    *,
    metadata_plan_sha256: str,
    staging_outcome_sha256: str,
) -> BerdlMetadataPreview:
    """Cross-check the approved metadata plan against verified staged data."""
    if (
        plan.snapshot_id != staging.snapshot_id
        or plan.destination_id != staging.destination_id
        or plan.staging_namespace != staging.staging_namespace
    ):
        raise BerdlMetadataError("The metadata plan does not match the verified staging outcome.")
    if plan.destination_provider != "spark_catalog" or plan.destination_table_format != "iceberg":
        raise BerdlMetadataError("BERDL metadata application requires the staged Spark Iceberg destination.")
    staged_tables = sorted(table.table for table in staging.tables)
    if staged_tables != plan.tables or len(staged_tables) != len(set(staged_tables)):
        raise BerdlMetadataError("The metadata plan and staging outcome table sets do not match.")
    table_operations, column_operations, deferred = _description_operations(plan)
    missing: dict[str, int] = defaultdict(int)
    for item in plan.missing_descriptions:
        missing[item.table] += 1
    return BerdlMetadataPreview(
        status="preview-only",
        snapshot_id=plan.snapshot_id,
        destination_id=plan.destination_id,
        staging_namespace=plan.staging_namespace,
        staging_outcome_sha256=staging_outcome_sha256,
        metadata_plan_sha256=metadata_plan_sha256,
        deferred_namespace_operations=deferred,
        targets=[
            MetadataTarget(
                table=table,
                table_description=table in table_operations,
                column_descriptions=len(column_operations[table]),
                missing_descriptions=missing[table],
            )
            for table in plan.tables
        ],
    )


def load_berdl_metadata_preview(
    metadata_plan_path: Path,
    staging_outcome_path: Path,
) -> tuple[MetadataApplicationPlan, BerdlStagingOutcome, BerdlMetadataPreview]:
    """Load, hash, and cross-check the exact reviewed input bytes."""
    plan_model, plan_sha256 = _read_model(metadata_plan_path, MetadataApplicationPlan, "metadata plan")
    staging_model, staging_sha256 = _read_model(staging_outcome_path, BerdlStagingOutcome, "verified staging outcome")
    plan = plan_model
    staging = staging_model
    assert isinstance(plan, MetadataApplicationPlan)
    assert isinstance(staging, BerdlStagingOutcome)
    return (
        plan,
        staging,
        build_berdl_metadata_preview(
            plan,
            staging,
            metadata_plan_sha256=plan_sha256,
            staging_outcome_sha256=staging_sha256,
        ),
    )


def _plural(count: int, noun: str) -> str:
    """Return a count with a correctly pluralised noun, since this text is read by operators."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _default_progress(message: str) -> None:
    """Report progress on stderr, keeping stdout reserved for the parseable outcome JSON."""
    print(message, file=sys.stderr, flush=True)


def _runtime(checkout: Path) -> tuple[Any, Callable[..., dict[str, Any]], Callable[..., dict[str, Any]]]:
    source_root = (checkout.expanduser() / "src").resolve()
    package_root = source_root / "data_lakehouse_ingest"
    sys.path.insert(0, str(source_root))
    try:
        from berdl_notebook_utils.setup_spark_session import get_spark_session

        comments = importlib.import_module("data_lakehouse_ingest.utils.delta_comments")
    except ImportError as error:
        raise BerdlMetadataError("The selected BERDL metadata runtime is not importable.") from error
    finally:
        sys.path.remove(str(source_root))
    module_file = getattr(comments, "__file__", None)
    if module_file is None or not Path(module_file).resolve().is_relative_to(package_root):
        raise BerdlMetadataError("The metadata helpers were not imported from the selected checkout.")
    return get_spark_session(), comments.apply_table_comment, comments.apply_comments_from_table_schema


def _verify_ingest_checkout(checkout: Path, revision: str) -> None:
    resolved = checkout.expanduser().resolve()
    try:
        _require_pristine_checkout(resolved, revision, _run_command)
        _require_revision_package(resolved, revision, _run_command)
    except BerdlStagingPlanError as error:
        raise BerdlMetadataError("The KBase ingest checkout does not match the verified ingest revision.") from error


def _catalog_description(value: Any) -> str | None:
    description = getattr(value, "description", None)
    return description if isinstance(description, str) else None


def _read_table_description(spark: Any, table: str) -> str | None:
    try:
        return _catalog_description(spark.catalog.getTable(table))
    except Exception as error:
        raise BerdlMetadataError(f"Cannot read back the table description for '{table}'.") from error


def _read_column_descriptions(spark: Any, table: str) -> dict[str, str | None]:
    try:
        return {column.name: _catalog_description(column) for column in spark.catalog.listColumns(table)}
    except Exception as error:
        raise BerdlMetadataError(f"Cannot read back column descriptions for '{table}'.") from error


def apply_berdl_staging_metadata(
    plan: MetadataApplicationPlan,
    staging: BerdlStagingOutcome,
    preview: BerdlMetadataPreview,
    *,
    ingest_checkout: Path,
    runtime: Callable[[Path], tuple[Any, Callable[..., dict[str, Any]], Callable[..., dict[str, Any]]]] = _runtime,
    checkout_verifier: Callable[[Path, str], None] = _verify_ingest_checkout,
    progress: Callable[[str], None] = _default_progress,
) -> BerdlMetadataOutcome:
    """Apply approved descriptions and require exact catalog read-back."""
    expected_preview = build_berdl_metadata_preview(
        plan,
        staging,
        metadata_plan_sha256=preview.metadata_plan_sha256,
        staging_outcome_sha256=preview.staging_outcome_sha256,
    )
    if preview != expected_preview:
        raise BerdlMetadataError("The metadata preview does not match the plan and staging outcome.")
    checkout_verifier(ingest_checkout, staging.ingest_revision)
    try:
        spark, apply_table_comment, apply_column_comments = runtime(ingest_checkout)
    except BerdlMetadataError:
        raise
    except Exception as error:
        raise BerdlMetadataError("Cannot initialize the BERDL metadata runtime.") from error
    table_operations, column_operations, _deferred = _description_operations(plan)
    planned_columns = sum(len(column_operations[name]) for name in plan.tables)
    started = time.monotonic()
    verified_columns_total = 0
    written_columns_total = 0
    progress(
        f"applying descriptions to {_plural(len(plan.tables), 'table')} and "
        f"{_plural(planned_columns, 'column')} in {plan.staging_namespace}"
    )
    targets: list[AppliedMetadataTarget] = []
    for index, table in enumerate(plan.tables, start=1):
        full_table = f"{plan.staging_namespace}.{table}"
        table_operation = table_operations.get(table)
        table_status: Literal["verified", "not-planned"] = "not-planned"
        table_already_correct = False
        if table_operation is not None:
            # The probe decides whether to write. It is not the verification: the read-back below
            # runs either way, so a skipped write is never a skipped check. Same rule as the
            # columns, which is the point of doing it in both places rather than one.
            table_already_correct = _read_table_description(spark, full_table) == table_operation.value
            if not table_already_correct:
                report = apply_table_comment(spark, full_table, table_operation.value, require_existing_table=True)
                if report.get("status") != "success":
                    raise BerdlMetadataError(f"The table description failed for '{table}'.")
            if _read_table_description(spark, full_table) != table_operation.value:
                raise BerdlMetadataError(f"The table description read-back failed for '{table}'.")
            table_status = "verified"
        operations = column_operations[table]
        verified_columns: list[str] = []
        already_correct: list[str] = []
        if operations:
            # Each ALTER is one catalog commit that rewrites the whole schema document, so a column
            # that already carries its planned description is worth a read to avoid a write. On a
            # rerun after a partial failure this is the difference between re-describing the whole
            # table and finishing the part that is left. See #258.
            named = []
            for item in operations:
                if item.column is None:
                    # A column operation without a column name cannot be applied or verified.
                    raise BerdlMetadataError(f"A column description for '{table}' names no column.")
                named.append((item.column, item.value))
            current = _read_column_descriptions(spark, full_table)
            pending = [(column, value) for column, value in named if current.get(column) != value]
            pending_columns = {column for column, _ in pending}
            already_correct = sorted(column for column, _ in named if column not in pending_columns)
            if pending:
                progress(
                    f"[{index}/{len(plan.tables)}] {table}: applying "
                    f"{_plural(len(pending), 'column description')}"
                    + (f", {len(already_correct)} already correct" if already_correct else "")
                )
                report = apply_column_comments(
                    spark,
                    full_table,
                    [{"column": column, "comment": value} for column, value in pending],
                    require_existing_table=True,
                )
                if report.get("status") != "success":
                    raise BerdlMetadataError(f"Column descriptions failed for '{table}'.")
            else:
                progress(
                    f"[{index}/{len(plan.tables)}] {table}: "
                    f"{_plural(len(already_correct), 'column description')} already correct, nothing to write"
                )
            # Read back and verify every planned column, not only the ones written. Skipping a write
            # must not skip the check that the description is actually there.
            observed_columns = _read_column_descriptions(spark, full_table)
            for column, value in named:
                if observed_columns.get(column) != value:
                    raise BerdlMetadataError(f"The column description read-back failed for '{table}.{column}'.")
                verified_columns.append(column)
        verified_columns_total += len(verified_columns)
        written_columns_total += len(verified_columns) - len(already_correct)
        elapsed = time.monotonic() - started
        # Rated on columns written, not columns verified. A skipped column costs a catalog read and
        # a written one costs a catalog commit, so counting them together produces an estimate that
        # is fast while the run is skipping and wrong as soon as it starts writing again.
        remaining = planned_columns - verified_columns_total
        rate = written_columns_total / elapsed if elapsed > 0 and written_columns_total else 0.0
        estimate = f", about {remaining / rate / 60:.0f} min left" if rate > 0 and remaining else ""
        progress(
            f"[{index}/{len(plan.tables)}] {table}: verified {_plural(len(verified_columns), 'column')} "
            f"({verified_columns_total}/{planned_columns} verified, {written_columns_total} written, "
            f"{elapsed / 60:.1f} min elapsed{estimate})"
        )
        targets.append(
            AppliedMetadataTarget(
                table=table,
                table_description_status=table_status,
                columns_verified=sorted(verified_columns),
                columns_already_correct=already_correct,
                table_description_already_correct=table_already_correct,
            )
        )
    return BerdlMetadataOutcome(
        outcome_format_version=1,
        status="metadata-verified",
        snapshot_id=staging.snapshot_id,
        destination_id=staging.destination_id,
        staging_namespace=staging.staging_namespace,
        staging_outcome_sha256=preview.staging_outcome_sha256,
        metadata_plan_sha256=preview.metadata_plan_sha256,
        deferred_namespace_operations=preview.deferred_namespace_operations,
        targets=targets,
    )


def render_berdl_metadata(value: BaseModel) -> str:
    """Render stable credential-free preview or outcome JSON."""
    return json.dumps(value.model_dump(mode="json"), indent=2, sort_keys=True)


def write_berdl_metadata_outcome(path: Path, outcome: BerdlMetadataOutcome) -> Path:
    """Atomically create metadata evidence without replacing an earlier outcome."""
    destination = path.expanduser()
    if destination.exists() or destination.is_symlink():
        raise BerdlMetadataError("Refusing to replace an existing BERDL metadata outcome.")
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise BerdlMetadataError("The BERDL metadata outcome parent must be an ordinary directory.")
    destination = parent.resolve() / destination.name
    descriptor: int | None = None
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
        temporary = Path(temporary_name)
        stream = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = None
        with stream:
            stream.write(render_berdl_metadata(outcome))
            stream.write("\n")
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise BerdlMetadataError("Refusing to replace an existing BERDL metadata outcome.") from error
        except OSError as error:
            raise BerdlMetadataError("Cannot publish the BERDL metadata outcome atomically.") from error
    except OSError as error:
        raise BerdlMetadataError("Cannot write the BERDL metadata outcome.") from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
    return destination
