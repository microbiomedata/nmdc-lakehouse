"""Review and promote the metadata snapshot together with its staged provenance pair.

Promotion is deliberately not resumable or atomic across tables. It records the
before-state and each attempted operation, carries metadata in the table write,
and refuses automatic replay after a partial attempt. Mark approves the exact
combined plan before any canonical mutation.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import tempfile
import traceback
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, model_validator

from nmdc_lakehouse import berdl_metadata, berdl_staging
from nmdc_lakehouse.berdl_promotion_probe import _scalar, _schema_fingerprint
from nmdc_lakehouse.metadata_application import MetadataApplicationPlan
from nmdc_lakehouse.publication_prepare import file_digest, progress, save_json
from nmdc_lakehouse.publication_staging import verified_staging_metadata
from nmdc_lakehouse.snapshot_manifest import SnapshotManifest, validate_snapshot

# The one-time September cleanup from issue 234, never a wildcard or user-defined drop list.
OBSOLETE_TEXTVALUE_TABLES = frozenset(
    "biosample_set_" + slot
    for slot in (
        "agrochem_addition",
        "air_temp_regm",
        "fertilizer_regm",
        "gaseous_environment",
        "host_diet",
        "humidity_regm",
        "perturbation",
        "phaeopigments",
        "watering_regm",
    )
)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class PromotionPlanError(ValueError):
    """The evidence, live state or authorization cannot support this promotion."""


class CatalogTable(BaseModel):
    """Read-only state relevant to data, metadata and manual recovery."""

    model_config = ConfigDict(extra="forbid", strict=True)
    rows: int = Field(ge=0)
    snapshot_id: str | None
    schema_sha256: str
    table_description: str | None
    columns: dict[str, str | None]
    properties: dict[str, str]


class PromotionSource(BaseModel):
    """The exact completed staging evidence retained for one input snapshot."""

    model_config = ConfigDict(extra="forbid", strict=True)
    root: str
    snapshot_id: str
    parent_snapshot_id: str | None
    staging_namespace: str
    destination_id: str
    source_version: str
    ingest_revision: str
    evidence: dict[str, str]
    tables: dict[str, int]


class PromotionOperation(BaseModel):
    """One explicit canonical replacement, addition or obsolete helper removal."""

    model_config = ConfigDict(extra="forbid", strict=True)
    table: str
    action: Literal["replace", "add", "drop"]
    source_namespace: str | None
    expected: CatalogTable | None


class BerdlPromotionPlan(BaseModel):
    """One immutable review covering both inputs and every current canonical table."""

    model_config = ConfigDict(extra="forbid", strict=True)
    plan_format_version: Literal[3] = 3
    status: Literal["plan-only"] = "plan-only"
    canonical_namespace: Literal["nmdc.metadata"] = "nmdc.metadata"
    sources: list[PromotionSource] = Field(min_length=2, max_length=2)
    ingest_checkout: str
    ingest_revision: str
    implementation_sha256: str
    before: dict[str, CatalogTable]
    operations: list[PromotionOperation]
    recovery: str = Field(min_length=1)
    recovery_limits: Literal[
        "No automatic rollback or multi-table atomicity; saved snapshot IDs do not prove dropped-table recovery."
    ] = "No automatic rollback or multi-table atomicity; saved snapshot IDs do not prove dropped-table recovery."

    @model_validator(mode="after")
    def consistent_operations(self) -> BerdlPromotionPlan:
        """Require the parent pair and an exact, ordered canonical replacement plan."""
        metadata, derived = self.sources
        if metadata.parent_snapshot_id is not None or derived.parent_snapshot_id != metadata.snapshot_id:
            raise ValueError("The derived snapshot must name the selected metadata snapshot as its parent.")
        if metadata.source_version != derived.source_version or metadata.destination_id != derived.destination_id:
            raise ValueError("Both sources must describe the same source version and destination.")
        if set(derived.tables) != {"graph_edges", "biosample_to_workflow_run"}:
            raise ValueError("The derived input must contain exactly the two provenance tables.")
        if set(metadata.tables) & set(derived.tables):
            raise ValueError("The input table sets overlap.")
        for source in self.sources:
            berdl_metadata._require_staging_target(source.staging_namespace)
            if not source.staging_namespace.startswith("nmdc."):
                raise ValueError("Both inputs must be staged in the nmdc catalog.")
        if metadata.ingest_revision != derived.ingest_revision or self.ingest_revision != metadata.ingest_revision:
            raise ValueError("Both staged sources and promotion must use the same official ingest revision.")
        candidates = metadata.tables | derived.tables
        actions = [op.action == "drop" for op in self.operations]
        if actions != sorted(actions):
            raise ValueError("All copies must precede obsolete helper removals.")
        if set(self.before) - set(candidates) - OBSOLETE_TEXTVALUE_TABLES:
            raise ValueError("Unexpected canonical-only tables require a separate explicit decision.")
        if len(self.operations) != len({op.table for op in self.operations}):
            raise ValueError("Duplicate promotion operations.")
        if {op.table for op in self.operations} != set(self.before) | set(candidates):
            raise ValueError("Promotion operations must cover the exact candidate and canonical union.")
        for op in self.operations:
            if not _IDENTIFIER.fullmatch(op.table):
                raise ValueError("Promotion table names must be plain identifiers.")
            if op.table not in candidates:
                if op.action != "drop" or op.source_namespace is not None or op.expected is not None:
                    raise ValueError("An obsolete helper must have an explicit drop operation.")
                continue
            source = next(s for s in self.sources if op.table in s.tables)
            if (
                op.action != ("replace" if op.table in self.before else "add")
                or op.source_namespace != source.staging_namespace
                or op.expected is None
                or op.expected.rows != candidates[op.table]
            ):
                raise ValueError("Copy operation does not match the staged source and canonical inventory.")
        return self


def _load_source(root: Path) -> tuple[PromotionSource, MetadataApplicationPlan, SnapshotManifest]:
    root = root.expanduser().absolute()
    if any(p.is_symlink() or not p.is_dir() for p in (root, root / "snapshot", root / "evidence")):
        raise PromotionPlanError("Use the original ordinary staging run directories.")
    root = root.resolve()
    evidence = root / "evidence"
    plan_path = evidence / "berdl-staging-plan.json"
    plan = berdl_staging.load_berdl_staging_plan(plan_path)
    coverage = plan.target_validation
    if coverage.requested_mode != "full" or coverage.selected_rows != coverage.eligible_rows:
        raise PromotionPlanError("Combined promotion requires full target-row validation for each input.")
    bindings = {}
    for item in plan.evidence:
        if file_digest(Path(item.path)) != item.sha256:
            raise PromotionPlanError(f"Changed staging evidence: {item.name}.")
        bindings[item.path] = item.sha256
    metadata_path = evidence / "metadata-application-plan.json"
    if bindings.get(str(metadata_path)) != file_digest(metadata_path):
        raise PromotionPlanError("The staging plan must bind this metadata plan.")
    manifest_path = root / "snapshot/snapshot-manifest.json"
    if bindings.get(str(manifest_path)) != file_digest(manifest_path):
        raise PromotionPlanError("The staging plan must bind this snapshot manifest.")
    manifest = validate_snapshot(manifest_path.parent)
    if manifest.snapshot_id != plan.snapshot_id:
        raise PromotionPlanError("The staged snapshot identity differs from the manifest.")
    metadata, _ = verified_staging_metadata(evidence, plan)
    for name in (
        "berdl-staging-plan.json",
        "nmdc-staging-outcome.json",
        "kbase-ingest-outcome.json",
        "nmdc-staging-metadata-outcome.json",
    ):
        bindings[str(evidence / name)] = file_digest(evidence / name)
    if {a.table: (a.rows, a.sha256) for a in plan.artifacts} != {
        a.table: (a.rows, a.sha256) for a in manifest.artifacts
    }:
        raise PromotionPlanError("Staging and manifest table coverage differs.")
    return (
        PromotionSource(
            root=str(root),
            snapshot_id=manifest.snapshot_id,
            parent_snapshot_id=manifest.parent_snapshot_id,
            staging_namespace=plan.staging_namespace,
            destination_id=plan.destination_id,
            source_version=manifest.software.nmdc_schema_version,
            ingest_revision=plan.ingest.revision,
            evidence=bindings,
            tables={a.table: a.rows for a in manifest.artifacts},
        ),
        metadata,
        manifest,
    )


def _table_names(spark: Any, namespace: str) -> set[str]:
    rows = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    if any(row["isTemporary"] for row in rows):
        raise PromotionPlanError("Temporary tables cannot be part of publication.")
    names = [row["tableName"] for row in rows]
    if len(names) != len(set(names)) or any(not _IDENTIFIER.fullmatch(n) for n in names):
        raise PromotionPlanError("The catalog returned duplicate or unsafe table names.")
    return set(names)


def _catalog_table(spark: Any, namespace: str, table: str) -> CatalogTable:
    name = f"{namespace}.{table}"
    # refs.main names the current snapshot even after a rollback; the newest
    # entry in snapshots may belong to a different branch or abandoned history.
    query = f"SELECT snapshot_id FROM {name}.refs WHERE name = 'main'"
    current = _scalar(spark, query)
    rows = _scalar(spark, f"SELECT COUNT(*) FROM {name}")
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise PromotionPlanError(f"Unusable row count for {table}.")
    if rows and current is None:
        raise PromotionPlanError(f"A populated staged table must have an Iceberg snapshot ID: {table}.")
    properties = berdl_metadata._read_table_properties(spark, name)
    state = CatalogTable(
        rows=rows,
        snapshot_id=str(current) if current is not None else None,
        schema_sha256=_schema_fingerprint(spark, name),
        table_description=berdl_metadata._read_table_description(spark, name) or None,
        columns={k: v or None for k, v in berdl_metadata._read_column_descriptions(spark, name).items()},
        properties={k: v for k, v in properties.items() if k.startswith(berdl_metadata.SCHEMA_PROPERTY_PREFIX)},
    )
    if current != _scalar(spark, query):
        raise PromotionPlanError(f"Table changed while reading its catalog state: {table}.")
    return state


def _require_planned_metadata(table: str, state: CatalogTable, metadata: MetadataApplicationPlan) -> None:
    descriptions, columns, _ = berdl_metadata._description_operations(metadata)
    if table in descriptions and state.table_description != descriptions[table].value:
        raise PromotionPlanError(f"Staged table description changed: {table}.")
    if any(state.columns.get(name) != value for name, value in columns[table]):
        raise PromotionPlanError(f"Staged column descriptions changed: {table}.")
    if any(state.properties.get(k) != v for k, v in berdl_metadata._schema_properties(metadata).items()):
        raise PromotionPlanError(f"Staged schema identity properties changed: {table}.")


def _check_textvalue_replacements(root: Path, drops: set[str]) -> None:
    if not drops:
        return
    schema = pq.read_schema(root / "snapshot/biosample_set.parquet")
    for table in drops:
        slot = table.removeprefix("biosample_set_")
        if slot not in schema.names:
            raise PromotionPlanError(f"Missing replacement column for {table}.")
        kind = schema.field(slot).type
        if not (pa.types.is_list(kind) and pa.types.is_string(kind.value_type)):
            raise PromotionPlanError(f"Expected a TextValue string-list projection for {table}.")


def _implementation_digest() -> str:
    digest = hashlib.sha256()
    root = Path(__file__).parent
    for path in sorted(root.rglob("*.py")):
        digest.update(str(path.relative_to(root)).encode() + b"\0" + file_digest(path).encode())
    return digest.hexdigest()


def build_promotion_plan(
    metadata_root: Path, derived_root: Path, *, ingest_checkout: Path, recovery: str, spark: Any
) -> BerdlPromotionPlan:
    """Read both verified inputs and live state; perform no catalog mutation."""
    inputs = [_load_source(metadata_root), _load_source(derived_root)]
    sources = [item[0] for item in inputs]
    source_tables = sources[0].tables | sources[1].tables
    before_names = _table_names(spark, "nmdc.metadata")
    unknown = before_names - set(source_tables) - OBSOLETE_TEXTVALUE_TABLES
    if unknown:
        raise PromotionPlanError("Unmatched canonical tables: " + ", ".join(sorted(unknown)))
    drops = before_names - set(source_tables)
    _check_textvalue_replacements(metadata_root, drops)
    before = {name: _catalog_table(spark, "nmdc.metadata", name) for name in sorted(before_names)}
    operations = []
    for source, metadata, _ in inputs:
        if _table_names(spark, source.staging_namespace) != set(source.tables):
            raise PromotionPlanError("Staging namespace no longer contains the exact verified table set.")
        for table, count in sorted(source.tables.items()):
            state = _catalog_table(spark, source.staging_namespace, table)
            if state.rows != count:
                raise PromotionPlanError(f"Staged row count changed: {table}.")
            _require_planned_metadata(table, state, metadata)
            operations.append(
                PromotionOperation(
                    table=table,
                    action="replace" if table in before else "add",
                    source_namespace=source.staging_namespace,
                    expected=state,
                )
            )
    operations.extend(
        PromotionOperation(table=n, action="drop", source_namespace=None, expected=None) for n in sorted(drops)
    )
    return BerdlPromotionPlan(
        sources=sources,
        ingest_checkout=str(ingest_checkout.expanduser().resolve()),
        ingest_revision=sources[0].ingest_revision,
        implementation_sha256=_implementation_digest(),
        before=before,
        operations=operations,
        recovery=recovery,
    )


def _runtime(checkout: Path, revision: str) -> Any:
    berdl_metadata._verify_ingest_checkout(checkout, revision)
    spark, _, _ = berdl_metadata._runtime(checkout)
    return spark


def _require_output_location(output: Path, roots: list[Path], checkout: Path) -> None:
    if output.is_symlink() or not output.parent.is_dir() or output.parent.is_symlink():
        raise PromotionPlanError("Use an ordinary existing directory for promotion evidence.")
    protected = [checkout.resolve(), *((root / "snapshot").resolve() for root in roots)]
    if any(output.resolve().is_relative_to(path) for path in protected):
        raise PromotionPlanError("Promotion evidence must remain outside snapshots and the ingest checkout.")


def plan_promotion(
    metadata_root: Path, derived_root: Path, output: Path, *, ingest_checkout: Path, recovery: str
) -> BerdlPromotionPlan:
    """Create the review artifact once, with private diagnostics and visible progress."""
    _require_output_location(output, [metadata_root, derived_root], ingest_checkout)
    if output.exists():
        raise PromotionPlanError("Use a new plan path; reviewed plans are never overwritten.")
    fd, log_name = tempfile.mkstemp(prefix="promotion-preview-", suffix=".log", dir=output.parent)
    print(f"Private planning log: {log_name}", file=sys.stderr, flush=True)
    with progress("combined promotion preview"), os.fdopen(fd, "w") as log:
        with redirect_stdout(log), redirect_stderr(log):
            try:
                revision = berdl_staging.load_berdl_staging_plan(
                    metadata_root / "evidence/berdl-staging-plan.json"
                ).ingest.revision
                plan = build_promotion_plan(
                    metadata_root,
                    derived_root,
                    ingest_checkout=ingest_checkout,
                    recovery=recovery,
                    spark=_runtime(ingest_checkout, revision),
                )
                save_json(output, plan.model_dump(mode="json"))
            except Exception as error:
                traceback.print_exc(file=log)
                raise PromotionPlanError(
                    f"Preview failed; inspect {log_name}. No canonical writes were attempted."
                ) from error
    return plan


def load_promotion_plan(path: Path) -> tuple[BerdlPromotionPlan, str]:
    """Read one ordinary plan and hash exactly the bytes that were parsed."""
    file_digest(path)
    raw = path.read_bytes()
    return BerdlPromotionPlan.model_validate_json(raw), hashlib.sha256(raw).hexdigest()


def render_promotion_plan(plan: BerdlPromotionPlan) -> str:
    """Show every input and operation, with explicit recovery limitations."""
    lines = [f"Promotion into {plan.canonical_namespace}; no changes have been made."]
    lines.extend(
        f"Input {s.snapshot_id} from {s.staging_namespace}; parent={s.parent_snapshot_id}" for s in plan.sources
    )
    lines.extend(
        f"{op.action}: {op.table}" + (f" from {op.source_namespace}; {op.expected.rows} rows" if op.expected else "")
        for op in plan.operations
    )
    lines.extend(
        [
            f"Expected result: {sum(op.action != 'drop' for op in plan.operations)} tables; read-back required.",
            f"Recovery (manual): {plan.recovery}",
            plan.recovery_limits,
        ]
    )
    return "\n".join(lines)


def _copy_table(spark: Any, plan: BerdlPromotionPlan, op: PromotionOperation) -> None:
    assert op.expected is not None and op.source_namespace is not None
    source = f"{op.source_namespace}.{op.table}"
    state = op.expected
    if state.snapshot_id is None:
        if state.rows:
            raise PromotionPlanError("A populated staged table must have an Iceberg snapshot ID.")
        frame = spark.table(source).limit(0)
    else:
        frame = spark.read.format("iceberg").option("snapshot-id", state.snapshot_id).load(source)
    # Attach all descriptions to one projection before the table write, avoiding
    # per-column catalog commits and the historical canonical backfill timeout.
    columns = []
    for field in frame.schema.fields:
        metadata = dict(field.metadata)
        comment = state.columns.get(field.name)
        if comment is not None:
            metadata["comment"] = comment
        else:
            metadata.pop("comment", None)
        columns.append(frame[field.name].alias(field.name, metadata=metadata))
    writer = frame.select(*columns).writeTo(f"{plan.canonical_namespace}.{op.table}").using("iceberg")
    for key, value in {"comment": state.table_description or "", **state.properties}.items():
        writer = writer.tableProperty(key, value)
    if op.action == "add":
        writer.create()
    else:
        writer.replace()


def _same_content_and_metadata(observed: CatalogTable, expected: CatalogTable) -> bool:
    return observed.model_dump(exclude={"snapshot_id"}) == expected.model_dump(exclude={"snapshot_id"})


def _verify_copies(spark: Any, plan: BerdlPromotionPlan, copies: dict[str, CatalogTable]) -> None:
    for table, expected in copies.items():
        if _catalog_table(spark, plan.canonical_namespace, table) != expected:
            raise PromotionPlanError(f"Promoted data or metadata changed after verification: {table}.")


def execute_promotion(
    plan_path: Path,
    *,
    authorize_plan_sha256: str,
    authorize_canonical_namespace: str,
    authorize_destination_id: str,
) -> dict[str, Any]:
    """Perform a reviewed combined plan, retaining an immutable per-operation journal."""
    plan, digest = load_promotion_plan(plan_path)
    if (
        authorize_plan_sha256 != digest
        or authorize_canonical_namespace != plan.canonical_namespace
        or authorize_destination_id != plan.sources[0].destination_id
    ):
        raise PromotionPlanError("Supply the exact reviewed plan digest, canonical namespace and destination identity.")
    _require_output_location(plan_path, [Path(s.root) for s in plan.sources], Path(plan.ingest_checkout))
    journal = plan_path.with_suffix(".execution")
    if journal.exists() or journal.is_symlink():
        raise PromotionPlanError(
            "A promotion attempt already exists; inspect its journal. Automatic replay is refused."
        )
    journal.mkdir(mode=0o700)
    fd, log_name = tempfile.mkstemp(prefix="runtime-", suffix=".log", dir=journal)
    print(f"Private promotion log: {log_name}", file=sys.stderr, flush=True)
    started_at = datetime.now(UTC).isoformat()
    attempted = None
    verified = []
    with progress("combined promotion"), os.fdopen(fd, "w") as log:
        with redirect_stdout(log), redirect_stderr(log):
            try:
                spark = _runtime(Path(plan.ingest_checkout), plan.ingest_revision)
                refreshed = build_promotion_plan(
                    Path(plan.sources[0].root),
                    Path(plan.sources[1].root),
                    ingest_checkout=Path(plan.ingest_checkout),
                    recovery=plan.recovery,
                    spark=spark,
                )
                if refreshed != plan:
                    raise PromotionPlanError("Evidence, implementation or live state changed after review.")
                save_json(journal / "before.json", plan.model_dump(mode="json"))
                copies: dict[str, CatalogTable] = {}
                checked_before_drops = False
                for index, op in enumerate(plan.operations):
                    if op.action == "drop" and not checked_before_drops:
                        _verify_copies(spark, plan, copies)
                        checked_before_drops = True
                    attempted = op.table
                    save_json(
                        journal / f"{index:03d}-attempt.json",
                        {"operation": op.model_dump(mode="json"), "attempted_at": datetime.now(UTC).isoformat()},
                    )
                    # Recheck each canonical target immediately before its mutation.
                    names = _table_names(spark, plan.canonical_namespace)
                    before = _catalog_table(spark, plan.canonical_namespace, op.table) if op.table in names else None
                    if before != plan.before.get(op.table):
                        raise PromotionPlanError(f"Canonical state changed before {op.table}.")
                    after = None
                    if op.action == "drop":
                        spark.sql(f"DROP TABLE {plan.canonical_namespace}.{op.table}")
                        if op.table in _table_names(spark, plan.canonical_namespace):
                            raise PromotionPlanError(f"Removal did not verify: {op.table}.")
                    else:
                        _copy_table(spark, plan, op)
                        after = _catalog_table(spark, plan.canonical_namespace, op.table)
                        assert op.expected is not None
                        if not _same_content_and_metadata(after, op.expected):
                            raise PromotionPlanError(f"Promoted data or metadata did not verify: {op.table}.")
                        copies[op.table] = after
                    verified.append(op.table)
                    save_json(
                        journal / f"{index:03d}-verified.json",
                        {
                            "table": op.table,
                            "status": "verified",
                            "verified_at": datetime.now(UTC).isoformat(),
                            "after": after.model_dump(mode="json") if after else None,
                        },
                    )
                expected_names = {op.table for op in plan.operations if op.action != "drop"}
                if _table_names(spark, plan.canonical_namespace) != expected_names:
                    raise PromotionPlanError("The final canonical table set differs from the plan.")
                _verify_copies(spark, plan, copies)
                result = {
                    "status": "promotion-verified",
                    "started_at": started_at,
                    "finished_at": datetime.now(UTC).isoformat(),
                    "snapshot_ids": [source.snapshot_id for source in plan.sources],
                    "plan_sha256": digest,
                    "canonical_namespace": plan.canonical_namespace,
                    "tables": sorted(expected_names),
                    "dropped": [op.table for op in plan.operations if op.action == "drop"],
                }
                save_json(journal / "outcome.json", result)
                return result
            except (Exception, KeyboardInterrupt) as error:
                traceback.print_exc(file=log)
                save_json(
                    journal / "failure.json",
                    {
                        "status": "incomplete",
                        "started_at": started_at,
                        "finished_at": datetime.now(UTC).isoformat(),
                        "attempted": attempted,
                        "verified": verified,
                        "recovery_attempted": False,
                    },
                )
                raise PromotionPlanError(f"Promotion stopped; inspect {journal}. No recovery was attempted.") from error
