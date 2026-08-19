"""Validate manifested Parquet rows against the published target LinkML schema."""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import re
import tempfile
import time
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import resources
from importlib.metadata import version
from pathlib import Path
from typing import Any, Literal

import pyarrow.parquet as pq
from linkml.validator import Validator
from linkml.validator.plugins import JsonschemaValidationPlugin
from linkml.validator.report import ValidationResult
from linkml_runtime import SchemaView
from pydantic import BaseModel, ConfigDict, Field

from nmdc_lakehouse.snapshot_manifest import ArtifactRecord, SnapshotManifest, validate_snapshot

REPORT_FORMAT_VERSION = 1
DEFAULT_FULL_TABLE_MAX_ROWS = 10_000
DEFAULT_SAMPLE_ROWS = 100
SAMPLING_ALGORITHM = "sha256-target-identity-and-canonical-row-minhash-v1"


class TargetValidationError(ValueError):
    """Raised when target validation cannot produce trustworthy evidence."""


class IssueCategory(BaseModel):
    """A sanitized aggregate of equivalent LinkML validation findings."""

    model_config = ConfigDict(extra="forbid")

    severity: str
    rule: str
    path: str
    count: int = Field(ge=1)


class TableValidationRecord(BaseModel):
    """Validation coverage and findings for one manifested Parquet table."""

    model_config = ConfigDict(extra="forbid")

    table: str
    artifact_path: str
    target_class: str
    mode: Literal["full", "sampled"]
    selection_basis: str
    eligible_rows: int = Field(ge=0)
    selected_rows: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    invalid_rows: int = Field(ge=0)
    elapsed_seconds: float = Field(ge=0)
    issue_categories: list[IssueCategory]


class TargetValidationReport(BaseModel):
    """Snapshot-bound, credential-free logical target validation evidence."""

    model_config = ConfigDict(extra="forbid")

    report_format_version: int
    status: Literal["success", "failure"]
    generated_at: str
    snapshot_id: str
    target_schema_id: str
    target_schema_sha256: str
    target_schema_source_version: str
    target_schema_source_package_version: str
    linkml_version: str
    requested_mode: Literal["bounded", "full"]
    full_table_max_rows: int = Field(ge=0)
    sample_rows: int = Field(ge=1)
    sampling_algorithm: str
    elapsed_seconds: float = Field(ge=0)
    eligible_rows: int = Field(ge=0)
    selected_rows: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    invalid_rows: int = Field(ge=0)
    tables: list[TableValidationRecord]


def _annotation(value: Any, name: str) -> str:
    annotation = value.annotations.get(name)
    result = getattr(annotation, "value", None)
    if not isinstance(result, str) or not result:
        raise TargetValidationError(f"Published target schema is missing required {name!r} metadata.")
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_default(value: Any) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"Unsupported canonical value type: {type(value).__name__}")


def _instance(row: dict[str, Any]) -> dict[str, Any]:
    """Treat Parquet nulls as absent optional LinkML slots."""
    return {name: value for name, value in row.items() if value is not None}


def _canonical_bytes(row: dict[str, Any]) -> bytes:
    try:
        return json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
            default=_canonical_default,
        ).encode()
    except (TypeError, ValueError) as error:
        raise TargetValidationError(
            "A Parquet row cannot be represented as a canonical validation instance."
        ) from error


def _selection_digest(row: dict[str, Any], target_class: str, identifier: str | None) -> tuple[int, bytes]:
    canonical = _canonical_bytes(row)
    identity = row.get(identifier) if identifier is not None else None
    seed = _canonical_bytes({"target_class": target_class, "identifier": identity})
    return int.from_bytes(hashlib.sha256(seed + b"\0" + canonical).digest()), canonical


def _sample_rows(
    parquet: pq.ParquetFile,
    *,
    target_class: str,
    identifier: str | None,
    sample_rows: int,
) -> list[dict[str, Any]]:
    heap: list[tuple[int, int, bytes, dict[str, Any]]] = []
    sequence = 0
    for batch in parquet.iter_batches(batch_size=2048):
        for raw_row in batch.to_pylist():
            row = _instance(raw_row)
            digest, canonical = _selection_digest(row, target_class, identifier)
            entry = (-digest, sequence, canonical, row)
            sequence += 1
            if len(heap) < sample_rows:
                heapq.heappush(heap, entry)
            elif digest < -heap[0][0]:
                heapq.heapreplace(heap, entry)
    return [entry[3] for entry in sorted(heap, key=lambda item: (-item[0], item[2]))]


def _sanitized_category(result: ValidationResult) -> tuple[str, str, str]:
    source = result.source
    raw_rule = str(getattr(source, "validator", "validation"))
    rule = raw_rule if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", raw_rule) else "validation"
    components = []
    for component in getattr(source, "absolute_path", ()):
        if isinstance(component, int):
            components.append("*")
        else:
            name = str(component)
            components.append(name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name) else "field")
    path = "/" + "/".join(components) if components else "/"
    return result.severity.value, rule, path


def _validate_rows(
    rows: Any,
    *,
    validator: Validator,
    target_class: str,
) -> tuple[int, int, list[IssueCategory]]:
    valid = 0
    invalid = 0
    categories: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        results = list(validator.iter_results(row, target_class))
        errors = [result for result in results if result.severity.value in {"ERROR", "FATAL"}]
        if errors:
            invalid += 1
        else:
            valid += 1
        categories.update(_sanitized_category(result) for result in results)
    issues = [
        IssueCategory(severity=severity, rule=rule, path=path, count=count)
        for (severity, rule, path), count in sorted(categories.items())
    ]
    return valid, invalid, issues


def _iter_rows(parquet: pq.ParquetFile) -> Any:
    for batch in parquet.iter_batches(batch_size=2048):
        for row in batch.to_pylist():
            yield _instance(row)


def _validate_table(
    root: Path,
    artifact: ArtifactRecord,
    *,
    schema_view: SchemaView,
    validator: Validator,
    requested_mode: Literal["bounded", "full"],
    full_table_max_rows: int,
    sample_rows: int,
) -> TableValidationRecord:
    started = time.monotonic()
    target_class = schema_view.get_class(artifact.target_class)
    if target_class is None:
        raise TargetValidationError(f"Manifest table {artifact.table!r} names an unknown target class.")
    expected = {
        "table_name": artifact.table,
        "source_class": artifact.source_class,
        "mapping": artifact.mapping,
    }
    for name, value in expected.items():
        if _annotation(target_class, name) != value:
            raise TargetValidationError(
                f"Manifest table {artifact.table!r} disagrees with target-class {name} metadata."
            )

    parquet = pq.ParquetFile(root / artifact.path)
    identifier_slot = schema_view.get_identifier_slot(artifact.target_class)
    identifier = identifier_slot.name if identifier_slot is not None else None
    selection_basis = f"target-identifier:{identifier}" if identifier is not None else "canonical-row"
    full = requested_mode == "full" or artifact.rows <= full_table_max_rows
    if full:
        rows = _iter_rows(parquet)
        selected = artifact.rows
        mode: Literal["full", "sampled"] = "full"
    else:
        rows = _sample_rows(
            parquet,
            target_class=artifact.target_class,
            identifier=identifier,
            sample_rows=sample_rows,
        )
        selected = len(rows)
        mode = "sampled"
    valid, invalid, issues = _validate_rows(rows, validator=validator, target_class=artifact.target_class)
    return TableValidationRecord(
        table=artifact.table,
        artifact_path=artifact.path,
        target_class=artifact.target_class,
        mode=mode,
        selection_basis=selection_basis,
        eligible_rows=artifact.rows,
        selected_rows=selected,
        valid_rows=valid,
        invalid_rows=invalid,
        elapsed_seconds=time.monotonic() - started,
        issue_categories=issues,
    )


def build_target_validation_report(
    root: Path,
    manifest: SnapshotManifest,
    schema_path: Path,
    *,
    requested_mode: Literal["bounded", "full"] = "bounded",
    full_table_max_rows: int = DEFAULT_FULL_TABLE_MAX_ROWS,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    generated_at: str | None = None,
) -> TargetValidationReport:
    """Validate one already integrity-checked snapshot and return sanitized evidence."""
    if full_table_max_rows < 0 or sample_rows < 1:
        raise TargetValidationError("Validation thresholds must be nonnegative and sample rows must be positive.")
    started = time.monotonic()
    schema_view = SchemaView(str(schema_path))
    schema_id = schema_view.schema.id
    if not isinstance(schema_id, str) or not schema_id:
        raise TargetValidationError("Published target schema has no stable identifier.")
    artifact_target_schema_ids = {artifact.target_schema_id for artifact in manifest.artifacts}
    if set(manifest.target_schema_ids) != artifact_target_schema_ids:
        raise TargetValidationError(
            "Snapshot target schema identities do not match the manifested artifact identities."
        )
    artifact_mapping_ids = {artifact.mapping for artifact in manifest.artifacts}
    if set(manifest.mapping_ids) != artifact_mapping_ids:
        raise TargetValidationError("Snapshot mapping identities do not match the manifested artifact identities.")
    if set(manifest.target_schema_ids) != {schema_id}:
        raise TargetValidationError("Snapshot and published target schema identities do not match exactly.")
    source_version = _annotation(schema_view.schema, "source_schema_version")
    source_package_version = _annotation(schema_view.schema, "source_package_version")
    if manifest.software.nmdc_schema_version != source_package_version:
        raise TargetValidationError("Snapshot and published target schema use different nmdc-schema package versions.")

    validator = Validator(
        schema_view.schema,
        validation_plugins=[JsonschemaValidationPlugin(closed=True)],
    )
    tables = [
        _validate_table(
            root,
            artifact,
            schema_view=schema_view,
            validator=validator,
            requested_mode=requested_mode,
            full_table_max_rows=full_table_max_rows,
            sample_rows=sample_rows,
        )
        for artifact in manifest.artifacts
    ]
    invalid = sum(table.invalid_rows for table in tables)
    return TargetValidationReport(
        report_format_version=REPORT_FORMAT_VERSION,
        status="failure" if invalid else "success",
        generated_at=generated_at or datetime.now(UTC).isoformat(),
        snapshot_id=manifest.snapshot_id,
        target_schema_id=schema_id,
        target_schema_sha256=_sha256(schema_path),
        target_schema_source_version=source_version,
        target_schema_source_package_version=source_package_version,
        linkml_version=version("linkml"),
        requested_mode=requested_mode,
        full_table_max_rows=full_table_max_rows,
        sample_rows=sample_rows,
        sampling_algorithm=SAMPLING_ALGORITHM,
        elapsed_seconds=time.monotonic() - started,
        eligible_rows=sum(table.eligible_rows for table in tables),
        selected_rows=sum(table.selected_rows for table in tables),
        valid_rows=sum(table.valid_rows for table in tables),
        invalid_rows=invalid,
        tables=tables,
    )


def validate_target_snapshot(
    root: Path,
    *,
    requested_mode: Literal["bounded", "full"] = "bounded",
    full_table_max_rows: int = DEFAULT_FULL_TABLE_MAX_ROWS,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
) -> TargetValidationReport:
    """Integrity-check a snapshot, then validate rows with the packaged schema."""
    root = root.expanduser()
    manifest = validate_snapshot(root)
    root = root.resolve()
    schema_resource = resources.files("nmdc_lakehouse").joinpath("schemas/nmdc_metadata.yaml")
    with resources.as_file(schema_resource) as schema_path:
        return build_target_validation_report(
            root,
            manifest,
            schema_path,
            requested_mode=requested_mode,
            full_table_max_rows=full_table_max_rows,
            sample_rows=sample_rows,
        )


def write_target_validation_report(output: Path, report: TargetValidationReport, *, snapshot_root: Path) -> Path:
    """Write a report atomically without modifying its immutable snapshot."""
    output = output.expanduser()
    parent = output.parent.resolve()
    snapshot_root = snapshot_root.expanduser().resolve()
    destination = parent / output.name
    if destination.is_relative_to(snapshot_root):
        raise TargetValidationError("Target validation evidence must be written outside the immutable snapshot.")
    if destination.exists() or destination.is_symlink():
        raise TargetValidationError("Refusing to replace an existing target validation report.")
    if not parent.is_dir():
        raise TargetValidationError("Target validation report parent must be an existing directory.")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(report.model_dump_json(indent=2))
            stream.write("\n")
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise TargetValidationError("Refusing to replace an existing target validation report.") from error
        except OSError as error:
            raise TargetValidationError("Cannot publish the target validation report atomically.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination
