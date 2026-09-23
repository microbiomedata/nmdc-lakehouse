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

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from nmdc_lakehouse.berdl_staging import (
    BerdlStagingOutcome,
    BerdlStagingPlanError,
    _evidence_paths,
    _read_berdl_staging_plan,
    _require_pristine_checkout,
    _require_revision_package,
    _run_command,
    execute_berdl_staging,
    is_staging_dataset,
    render_berdl_staging_outcome,
)
from nmdc_lakehouse.metadata_application import (
    MetadataApplicationPlan,
    MetadataOperation,
    MetadataOperationKind,
    catalog_of_namespace,
    dataset_of_namespace,
)

# Bumped from 1 when AppliedMetadataTarget gained columns_already_correct and
# table_description_already_correct. Every model here forbids extra fields, so any added key
# is a format change whether or not it has a default.
# Bumped to 3 when AppliedMetadataTarget gained the schema_properties fields. A version that does
# not move when the serialized shape does is not a version: a consumer keying off it would read a
# 3-shaped document believing it was 2.
METADATA_OUTCOME_FORMAT_VERSION: Literal[3] = 3


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
    # Whether this table now carries the schema-identity properties, and whether they had to be
    # written. "not-planned" for a version 1 plan, which cannot name a flat schema version.
    schema_properties_status: Literal["verified", "not-planned"] = "not-planned"
    schema_properties_already_correct: bool = False

    # Both of the fields above are absent from a version 1 outcome, hence the defaults. They are
    # what the outcome format version was raised for.


class BerdlMetadataOutcome(BaseModel):
    """Credential-free evidence of staging table and column metadata read-back."""

    model_config = ConfigDict(extra="forbid", strict=True)

    # Both accepted on read. Version 2 added the two already-correct fields on each target, which
    # are optional, so a version 1 document is still fully valid under this model. Reading is
    # widened rather than the version being left alone, because BerdlMetadataOutcome forbids extra
    # fields: a reader pinned to version 1 rejects a version 2 document outright, and an optional
    # field with a default does nothing to prevent that. Writing emits the constant below.
    outcome_format_version: Literal[1, 2, 3]

    @model_validator(mode="after")
    def validate_version_matches_targets(self) -> "BerdlMetadataOutcome":
        """Keep the version an honest guide to what the targets may claim.

        The schema-property fields exist on every target whatever the version says, so a version 1
        or 2 outcome could report `schema_properties_status="verified"` and validate. That is
        evidence of an application that the version says did not happen, and this file is
        evidence. The bundle and the plan already refuse the same pairing; this was the third
        instance and the one I did not notice while fixing the other two.
        """
        if self.outcome_format_version < 3:
            for target in self.targets:
                if target.schema_properties_status != "not-planned" or target.schema_properties_already_correct:
                    raise ValueError(
                        f"A version {self.outcome_format_version} outcome cannot report schema "
                        f"properties for '{target.table}'."
                    )
        return self

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


def _require_staging_target(namespace: str) -> None:
    """Refuse to describe a canonical namespace one column at a time.

    Mark decided on 2026-08-27 that a live per-column update is not supported and the answer is
    to reload the table. The reason is measured, not preference. On 2026-08-20 this path applied
    560 columns to `biosample_set` and then raised `RESTException` on the remaining 833, so it
    does not slow down at width, it stops, and it stops with the table partly described. Batching
    the statements is the obvious repair and it is not available: a grouped
    `ALTER TABLE ... ALTER COLUMN a COMMENT ..., ALTER COLUMN b COMMENT ...` is a
    PARSE_SYNTAX_ERROR here, measured 2026-08-20, so batching cannot be expressed at the SQL
    layer at all.

    Staging is unaffected, and is how descriptions are meant to arrive. They ride in the Parquet
    footer and cost one metadata commit per table at creation, which is why a whole namespace now
    writes no column descriptions. This function still writes missing table descriptions and the
    schema identity properties. See `docs/column-description-path.md` and
    https://github.com/microbiomedata/nmdc-lakehouse/issues/297.
    """
    dataset = dataset_of_namespace(namespace, "staging namespace")
    if not is_staging_dataset(dataset):
        raise BerdlMetadataError(
            f"'{namespace}' is not a staging namespace, and describing a canonical table one "
            "column at a time is not supported: that path applied 560 columns to biosample_set "
            "and failed on the remaining 833. Reload the table into a fresh staging namespace "
            "instead, where the descriptions arrive in the Parquet footer at no extra cost. See "
            "docs/column-description-path.md."
        )


def _description_operations(
    plan: MetadataApplicationPlan,
) -> tuple[dict[str, MetadataOperation], dict[str, list[tuple[str, str]]], int]:
    """Split the plan into table and column descriptions, with incomplete targets rejected here.

    Column descriptions come back as (column, description) pairs rather than operations, so the
    completeness check lives in exactly one place and callers do not have to re-narrow a
    nullable column name that this function has already refused to pass on.
    """
    table_operations: dict[str, MetadataOperation] = {}
    column_operations: dict[str, list[tuple[str, str]]] = defaultdict(list)
    deferred = 0
    for operation in plan.supported_operations:
        if operation.kind == MetadataOperationKind.TABLE_DESCRIPTION:
            if operation.table is None:
                raise BerdlMetadataError("A table-description operation has no table target.")
            table_operations[operation.table] = operation
        elif operation.kind == MetadataOperationKind.COLUMN_DESCRIPTION:
            # MetadataOperation already validates this, so this is a second line for a model built
            # without validation rather than the primary guard.
            if operation.table is None or operation.column is None:
                raise BerdlMetadataError("A column-description operation has an incomplete target.")
            column_operations[operation.table].append((operation.column, operation.value))
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
    staged_catalog = catalog_of_namespace(plan.staging_namespace, "staging namespace")
    if plan.destination_provider != staged_catalog or plan.destination_table_format != "iceberg":
        raise BerdlMetadataError("BERDL metadata application requires an Iceberg destination in the staged catalog.")
    _require_staging_target(plan.staging_namespace)
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
    *,
    staging_plan_path: Path,
    output_path: Path,
    ingest_checkout: Path,
) -> tuple[MetadataApplicationPlan, BerdlStagingOutcome, BerdlMetadataPreview]:
    """Load, hash, and cross-check the exact reviewed input bytes."""
    plan_model, plan_sha256 = _read_model(metadata_plan_path, MetadataApplicationPlan, "metadata plan")
    staging_model, staging_sha256 = _read_model(staging_outcome_path, BerdlStagingOutcome, "verified staging outcome")
    plan = plan_model
    staging = staging_model
    assert isinstance(plan, MetadataApplicationPlan)
    assert isinstance(staging, BerdlStagingOutcome)
    try:
        reviewed, reviewed_sha256 = _read_berdl_staging_plan(staging_plan_path)
        _evidence_paths(reviewed)
    except BerdlStagingPlanError as error:
        raise BerdlMetadataError(str(error)) from error
    if reviewed_sha256 != staging.staging_plan_sha256:
        raise BerdlMetadataError("The staging plan does not match the plan recorded by data verification.")
    reviewed_metadata_sha256 = next(
        item.sha256 for item in reviewed.evidence if item.name == "metadata-application-plan.json"
    )
    if plan_sha256 != reviewed_metadata_sha256:
        raise BerdlMetadataError("The metadata plan digest differs from the original reviewed staging plan.")
    _require_metadata_output(
        output_path,
        (
            _evidence_paths(reviewed)["snapshot-manifest.json"].parent,
            Path(reviewed.ingest.checkout),
            ingest_checkout,
        ),
    )
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


# Deliberately the same names the Parquet footer uses. A reader who has seen one should not have
# to learn a second vocabulary for the other, and the footer and the table are now the two places
# the same fact is recorded.
SCHEMA_PROPERTY_PREFIX = "nmdc_lakehouse."


def _schema_properties(plan: Any) -> dict[str, str]:
    """Return the identity properties a staged table should carry, or nothing to write.

    Empty when the plan cannot name a flat schema version, which is true of every plan written
    before the schema had one. Labelling those tables with a guess would be worse than leaving
    them unlabelled, because a consumer cannot tell a guess from a fact.
    """
    version = getattr(plan, "target_schema_version", "")
    if not version:
        return {}
    return {
        f"{SCHEMA_PROPERTY_PREFIX}target_schema_version": version,
        f"{SCHEMA_PROPERTY_PREFIX}snapshot_id": plan.snapshot_id,
    }


def _read_table_properties(spark: Any, table: str) -> dict[str, str]:
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    except Exception as error:
        raise BerdlMetadataError(f"Cannot read back table properties for '{table}'.") from error
    return {row[0]: row[1] for row in rows}


def _quote_property(value: str) -> str:
    """Quote one property value for SQL, refusing what cannot be quoted safely.

    These values come from a reviewed plan rather than from a caller, but they are interpolated
    into a statement, and a plan is a file somebody can edit.
    """
    if "'" in value or "\\" in value or "\n" in value or "\r" in value:
        raise BerdlMetadataError(f"A table property value cannot be quoted safely: {value!r}")
    return f"'{value}'"


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
            current = _read_column_descriptions(spark, full_table)
            pending = [(column, value) for column, value in operations if current.get(column) != value]
            pending_columns = {column for column, _ in pending}
            already_correct = sorted(column for column, _ in operations if column not in pending_columns)
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
            for column, value in operations:
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
        # The identity properties, applied last so a table that fails earlier is not labelled as
        # carrying a schema it does not describe. One ALTER sets them all, so this is one commit
        # per table rather than one per property, and it is skipped when they already match.
        properties_status: Literal["verified", "not-planned"] = "not-planned"
        properties_already_correct = False
        wanted = _schema_properties(plan)
        if wanted:
            observed = _read_table_properties(spark, full_table)
            properties_already_correct = all(observed.get(key) == value for key, value in wanted.items())
            if properties_already_correct:
                # `observed` is the catalog's answer and nothing was written after it, so it is
                # the confirmation. Re-reading would cost one round trip per table on every rerun
                # and could only differ if something else changed the table mid-run, which a
                # second read does not protect against either.
                confirmed = observed
            else:
                assignments = ", ".join(
                    f"{_quote_property(key)} = {_quote_property(value)}" for key, value in wanted.items()
                )
                try:
                    spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES ({assignments})")
                except Exception as error:
                    raise BerdlMetadataError(f"Setting schema properties failed for '{full_table}'.") from error
                # Read back what was just written. The check below runs either way, so a skipped
                # write is still never a skipped check; only the extra fetch is skipped.
                confirmed = _read_table_properties(spark, full_table)
            for key, value in wanted.items():
                if confirmed.get(key) != value:
                    raise BerdlMetadataError(
                        f"The schema property read-back failed for '{full_table}', property {key!r}."
                    )
            properties_status = "verified"

        targets.append(
            AppliedMetadataTarget(
                table=table,
                table_description_status=table_status,
                columns_verified=sorted(verified_columns),
                columns_already_correct=already_correct,
                table_description_already_correct=table_already_correct,
                schema_properties_status=properties_status,
                schema_properties_already_correct=properties_already_correct,
            )
        )
    return BerdlMetadataOutcome(
        outcome_format_version=METADATA_OUTCOME_FORMAT_VERSION,
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


def _require_metadata_output(output: Path, protected_roots: tuple[Path, ...]) -> Path:
    """Preflight an immutable outcome before either staging path changes the catalog."""
    destination = output.expanduser()
    if destination.exists() or destination.is_symlink():
        raise BerdlMetadataError("Refusing to replace an existing BERDL metadata outcome.")
    if not destination.parent.is_dir() or destination.parent.is_symlink():
        raise BerdlMetadataError("The BERDL metadata outcome parent must be an ordinary directory.")
    resolved = destination.resolve()
    if any(resolved.is_relative_to(root.expanduser().resolve()) for root in protected_roots):
        raise BerdlMetadataError("Metadata outcomes must remain outside the snapshot and ingest checkout.")
    return resolved


def execute_berdl_staging_with_metadata(
    plan_path: Path,
    *,
    upstream_outcome_path: Path,
    output_path: Path,
    metadata_output_path: Path,
    authorize_snapshot: str | None,
    authorize_plan_sha256: str | None,
    execute_staging: bool,
) -> dict[str, Any]:
    """Make catalog metadata verification part of the normal staging command.

    The staging plan already binds the metadata plan by digest. The same authorization
    therefore covers applying its table metadata after data verification. Separate immutable
    outcomes retain the existing promotion contract and allow a metadata-only retry.
    """
    staging_plan, staging_plan_sha256 = _read_berdl_staging_plan(plan_path)
    paths = _evidence_paths(staging_plan)
    metadata_path = paths["metadata-application-plan.json"]
    model, metadata_sha256 = _read_model(metadata_path, MetadataApplicationPlan, "metadata plan")
    assert isinstance(model, MetadataApplicationPlan)
    metadata_plan = model
    expected_sha256 = next(
        item.sha256 for item in staging_plan.evidence if item.name == "metadata-application-plan.json"
    )
    if metadata_sha256 != expected_sha256:
        raise BerdlMetadataError("The metadata plan no longer matches the reviewed staging plan.")

    # Check before starting the upload, not after data has already been staged. The final
    # atomic writer repeats the existence check to refuse concurrent replacement.
    resolved_output = _require_metadata_output(
        metadata_output_path,
        (paths["snapshot-manifest.json"].parent, Path(staging_plan.ingest.checkout)),
    )
    if resolved_output in {upstream_outcome_path.expanduser().resolve(), output_path.expanduser().resolve()}:
        raise BerdlMetadataError("The metadata and data outcomes must use distinct paths.")

    table_ops, column_ops, _ = _description_operations(metadata_plan)
    coverage = {
        "table_descriptions_planned": len(table_ops),
        "column_descriptions_planned": sum(len(items) for items in column_ops.values()),
        "schema_properties_planned": bool(_schema_properties(metadata_plan)),
        "missing_descriptions": [item.model_dump(mode="json") for item in metadata_plan.missing_descriptions],
        "unsupported_operations": [item.model_dump(mode="json") for item in metadata_plan.unsupported_operations],
        "deferred_namespace_operations": [
            item.model_dump(mode="json")
            for item in metadata_plan.supported_operations
            if item.kind not in {MetadataOperationKind.TABLE_DESCRIPTION, MetadataOperationKind.COLUMN_DESCRIPTION}
        ],
    }
    command, staging = execute_berdl_staging(
        plan_path,
        upstream_outcome_path=upstream_outcome_path,
        output_path=output_path,
        authorize_snapshot=authorize_snapshot,
        authorize_plan_sha256=authorize_plan_sha256,
        execute_staging=execute_staging,
    )
    if staging is None:
        return {
            "status": "preview-only",
            "command": command,
            "metadata_plan_sha256": metadata_sha256,
            "metadata_output": str(resolved_output),
            "metadata_coverage": coverage,
        }
    try:
        # Use the exact bytes produced by the atomic data-outcome writer, not a hash of
        # whatever happens to be on disk later. This also catches whitespace-only rewrites.
        written_staging_sha256 = hashlib.sha256(
            (render_berdl_staging_outcome(staging) + "\n").encode("utf-8")
        ).hexdigest()
        plan, recorded_staging, preview = load_berdl_metadata_preview(
            metadata_path,
            output_path,
            staging_plan_path=plan_path,
            output_path=metadata_output_path,
            ingest_checkout=Path(staging_plan.ingest.checkout),
        )
        if (
            staging.staging_plan_sha256 != staging_plan_sha256
            or preview.metadata_plan_sha256 != metadata_sha256
            or preview.staging_outcome_sha256 != written_staging_sha256
            or recorded_staging != staging
        ):
            raise BerdlMetadataError("The data or metadata evidence changed between staging phases.")
        _default_progress("data verified; applying and verifying approved table metadata")
        metadata = apply_berdl_staging_metadata(
            plan, staging, preview, ingest_checkout=Path(staging_plan.ingest.checkout)
        )
        write_berdl_metadata_outcome(metadata_output_path, metadata)
    except (BerdlMetadataError, OSError) as error:
        raise BerdlMetadataError(
            "Data staging passed, but metadata completion failed. Retain the data outcome and "
            "use berdl-apply-metadata to preview and retry metadata without uploading again. " + str(error)
        ) from error
    return {
        "status": "data-and-table-metadata-verified",
        "data": staging.model_dump(mode="json"),
        "metadata": metadata.model_dump(mode="json"),
        "metadata_coverage": coverage,
    }


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
