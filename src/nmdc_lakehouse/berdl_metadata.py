"""Apply and verify approved table and column descriptions in BERDL staging."""

from __future__ import annotations

import hashlib
import importlib
import json
import sys
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from nmdc_lakehouse.berdl_staging import (
    BerdlStagingOutcome,
    BerdlStagingPlanError,
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
    try:
        _require_revision_package(checkout.expanduser().resolve(), revision, _run_command)
    except BerdlStagingPlanError as error:
        raise BerdlMetadataError("The KBase metadata helpers do not match the verified ingest revision.") from error


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
) -> BerdlMetadataOutcome:
    """Apply approved descriptions and require exact catalog read-back."""
    checkout_verifier(ingest_checkout, staging.ingest_revision)
    try:
        spark, apply_table_comment, apply_column_comments = runtime(ingest_checkout)
    except BerdlMetadataError:
        raise
    except Exception as error:
        raise BerdlMetadataError("Cannot initialize the BERDL metadata runtime.") from error
    table_operations, column_operations, _deferred = _description_operations(plan)
    targets: list[AppliedMetadataTarget] = []
    for table in plan.tables:
        full_table = f"{plan.staging_namespace}.{table}"
        table_operation = table_operations.get(table)
        table_status: Literal["verified", "not-planned"] = "not-planned"
        if table_operation is not None:
            report = apply_table_comment(spark, full_table, table_operation.value, require_existing_table=True)
            if report.get("status") != "success":
                raise BerdlMetadataError(f"The table description failed for '{table}'.")
            if _read_table_description(spark, full_table) != table_operation.value:
                raise BerdlMetadataError(f"The table description read-back failed for '{table}'.")
            table_status = "verified"
        operations = column_operations[table]
        if operations:
            report = apply_column_comments(
                spark,
                full_table,
                [{"column": item.column, "comment": item.value} for item in operations],
                require_existing_table=True,
            )
            if report.get("status") != "success":
                raise BerdlMetadataError(f"Column descriptions failed for '{table}'.")
        observed_columns = _read_column_descriptions(spark, full_table)
        verified_columns: list[str] = []
        for operation in operations:
            assert operation.column is not None
            if observed_columns.get(operation.column) != operation.value:
                raise BerdlMetadataError(f"The column description read-back failed for '{table}.{operation.column}'.")
            verified_columns.append(operation.column)
        targets.append(
            AppliedMetadataTarget(
                table=table,
                table_description_status=table_status,
                columns_verified=sorted(verified_columns),
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
