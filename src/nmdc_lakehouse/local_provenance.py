"""Build small, described provenance Parquet tables from an immutable local snapshot."""

from __future__ import annotations

import hashlib
import json
import platform
import time
from collections import defaultdict, deque
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from graphlib import CycleError, TopologicalSorter
from importlib.resources import files
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from linkml_runtime.linkml_model import SchemaDefinition
from linkml_runtime.loaders import yaml_loader

from nmdc_lakehouse.derived_tables import DEFAULT_MAX_DEPTH, EDGE_SOURCES, PROCESSING_TYPES, DerivedTableError
from nmdc_lakehouse.sinks.parquet_sink import StreamingWriter, class_def_to_arrow_schema, with_spark_schema
from nmdc_lakehouse.snapshot_manifest import (
    MANIFEST_FORMAT_VERSION,
    PerformanceRecord,
    SnapshotManifest,
    SoftwareRecord,
    _artifact,
    _git_state,
    _package_version,
    _sha256,
    _snapshot_identity,
    validate_snapshot,
    write_manifest,
)

ALGORITHM = "nmdc_lakehouse.local_provenance.breadth_first_v1"
TABLE_CLASSES = {"graph_edges": "GraphEdge", "biosample_to_workflow_run": "BiosampleToWorkflowRun"}
PRIMARY_TABLES = (
    "biosample_set",
    "workflow_execution_set",
    "material_processing_set",
    "data_generation_set",
    "processed_sample_set",
)
REQUIRED_TABLES = (*PRIMARY_TABLES, *(source[0] for source in EDGE_SOURCES))


def provenance_schema_resource():
    """Locate the independent derived-table schema shipped with this package."""
    return files("nmdc_lakehouse").joinpath("schemas/provenance.yaml")


def provenance_schema() -> tuple[SchemaDefinition, str]:
    """Read the packaged, versioned LinkML contract and its exact content digest."""
    text = provenance_schema_resource().read_text(encoding="utf-8")
    return yaml_loader.loads(text, target_class=SchemaDefinition), hashlib.sha256(text.encode()).hexdigest()


def _columns(root: Path, table: str, names: list[str]) -> list[tuple[str, ...]]:
    try:
        data = pq.ParquetFile(root / f"{table}.parquet").read(columns=names)
        if data.column_names != names:
            raise DerivedTableError(f"{table} does not contain the required columns.")
        columns = [data[name].to_pylist() for name in names]
        if any(not isinstance(value, str) or not value.strip() for column in columns for value in column):
            raise DerivedTableError(f"{table} requires nonempty string identifiers and types.")
        return list(zip(*columns, strict=True))
    except DerivedTableError:
        raise
    except Exception as error:
        raise DerivedTableError(f"Cannot read required columns from {table}.") from error


def _inputs(root: Path) -> tuple[list[tuple[str, str, str]], dict[str, str], dict[str, str], set[str]]:
    ids: dict[str, set[str]] = {}
    types: dict[str, dict[str, str]] = {}
    all_ids: set[str] = set()
    for name in PRIMARY_TABLES:
        columns = ["id", "type"] if name in {"workflow_execution_set", "material_processing_set"} else ["id"]
        rows = _columns(root, name, columns)
        identifiers = {row[0] for row in rows}
        if len(identifiers) != len(rows) or identifiers.intersection(all_ids):
            raise DerivedTableError("Primary identifiers must be unique within and across provenance input tables.")
        all_ids.update(identifiers)
        ids[name] = identifiers
        if len(columns) == 2:
            types[name] = {row[0]: row[1] for row in rows}
    workflows = types["workflow_execution_set"]
    processing = types["material_processing_set"]
    if not workflows or not ids["biosample_set"]:
        raise DerivedTableError("Provenance requires nonempty workflow and biosample tables.")
    if set(processing.values()).difference(PROCESSING_TYPES):
        raise DerivedTableError("MaterialProcessing types are not all covered by PROCESSING_TYPES.")

    samples = ids["biosample_set"] | ids["processed_sample_set"]
    domains = (
        (ids["workflow_execution_set"], ids["data_generation_set"]),
        (ids["data_generation_set"], samples),
        (ids["processed_sample_set"], ids["material_processing_set"]),
        (ids["material_processing_set"], samples),
    )
    edges: list[tuple[str, str, str]] = []
    for (table, src, dst, slot), (source_ids, target_ids) in zip(EDGE_SOURCES, domains, strict=True):
        rows = _columns(root, table, [src, dst])
        if any(source not in source_ids or target not in target_ids for source, target in rows):
            raise DerivedTableError(f"{table} contains references absent from the expected primary tables.")
        edges.extend((source, target, slot) for source, target in rows)
    if not edges:
        raise DerivedTableError("The provenance edge tables are empty.")
    return sorted(edges), workflows, processing, ids["biosample_set"]


def provenance_pairs(
    edges: list[tuple[str, str, str]],
    workflows: dict[str, str],
    processing: dict[str, str],
    biosamples: set[str],
    *,
    max_depth: int = DEFAULT_MAX_DEPTH,
    progress: Callable[[str], None] = lambda _message: None,
) -> Iterator[dict[str, Any]]:
    """Yield unique pairs, shortest hop counts, and workflow-wide processing flags.

    Each node is expanded once per workflow. Flags retain the existing Spark contract:
    a processing class on any upstream branch is true on every pair for that workflow.
    """
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
        raise DerivedTableError("max_depth must be an integer of at least 1.")
    if set(processing.values()).difference(PROCESSING_TYPES):
        raise DerivedTableError("MaterialProcessing types are not all covered by PROCESSING_TYPES.")
    graph: dict[str, set[str]] = defaultdict(set)
    for source, target, _slot in edges:
        graph[source].add(target)
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as error:
        raise DerivedTableError("The provenance graph contains a cycle; refusing incomplete lineage.") from error

    for index, (origin, workflow_type) in enumerate(sorted(workflows.items()), 1):
        seen = {origin}
        frontier = deque([(origin, 0)])
        reached: dict[str, int] = {}
        flags: set[str] = set()
        while frontier:
            node, depth = frontier.popleft()
            if node in processing:
                flags.add(PROCESSING_TYPES[processing[node]])
            for neighbor in sorted(graph.get(node, ())):
                if neighbor in seen:
                    continue
                if depth + 1 > max_depth:
                    raise DerivedTableError(f"The provenance walk exceeds max_depth={max_depth}; refusing truncation.")
                seen.add(neighbor)
                if neighbor in biosamples:
                    reached[neighbor] = depth + 1
                else:
                    frontier.append((neighbor, depth + 1))
        if not reached:
            raise DerivedTableError("A workflow has no reachable biosample; refusing an incomplete mapping.")
        for biosample, hops in sorted(reached.items()):
            yield {
                "biosample_id": biosample,
                "workflow_run_id": origin,
                "workflow_type": workflow_type,
                "n_hops": hops,
                **{column: column in flags for column in PROCESSING_TYPES.values()},
            }
        if index % 1000 == 0 or index == len(workflows):
            progress(f"walked {index}/{len(workflows)} workflows")


def _output_schema(
    name: str,
    parent: SnapshotManifest,
    source_identity: tuple[str, str],
    max_depth: int,
    target: SchemaDefinition,
    target_digest: str,
) -> pa.Schema:
    schema = class_def_to_arrow_schema(
        target.classes[TABLE_CLASSES[name]],
        source_schema=SchemaDefinition(id=source_identity[0], name="nmdc", version=source_identity[1]),
        source_class="Database",
        target_schema_id=str(target.id),
        target_schema_version=str(target.version),
        mapping=ALGORITHM,
    )
    metadata = dict(schema.metadata or {})
    metadata.update(
        {
            b"nmdc_lakehouse.input_snapshot_id": parent.snapshot_id.encode(),
            b"nmdc_lakehouse.target_schema_sha256": target_digest.encode(),
            b"nmdc_lakehouse.derivation_max_depth": str(max_depth).encode(),
        }
    )
    return with_spark_schema(pa.schema([field.with_nullable(False) for field in schema], metadata=metadata))


def derive_provenance(
    snapshot_root: Path,
    output_root: Path,
    *,
    max_depth: int = DEFAULT_MAX_DEPTH,
    progress: Callable[[str], None] = lambda _message: None,
) -> SnapshotManifest:
    """Write a separate derived snapshot, completing its manifest only after verification.

    An existing destination is refused. Failed attempts can leave unmanifested partial files
    in the new output directory; the source snapshot is never modified.
    """
    started = time.monotonic()
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
        raise DerivedTableError("max_depth must be an integer of at least 1.")
    if output_root.expanduser().exists() or output_root.expanduser().is_symlink():
        raise DerivedTableError("Derived output must be a new directory.")
    # validate_snapshot rejects a symlinked root before resolving it.
    progress("validating input snapshot integrity")
    parent = validate_snapshot(snapshot_root)
    source = snapshot_root.expanduser().resolve()
    output = output_root.expanduser().resolve()
    if output.is_relative_to(source):
        raise DerivedTableError("Derived output must be outside the input snapshot.")
    if parent.scope != "full-mongodb-metadata-snapshot":
        raise DerivedTableError("Input must be a MongoDB metadata snapshot.")
    artifacts = {artifact.table: artifact for artifact in parent.artifacts}
    if set(REQUIRED_TABLES).difference(artifacts):
        raise DerivedTableError("Input snapshot is missing required provenance tables.")
    identities = {(artifacts[name].source_schema_id, artifacts[name].source_schema_version) for name in REQUIRED_TABLES}
    targets = {(artifacts[name].target_schema_id, artifacts[name].target_schema_version) for name in REQUIRED_TABLES}
    if len(identities) != 1 or len(targets) != 1:
        raise DerivedTableError("Provenance inputs must use one source and one flattened schema identity.")
    edges, workflows, processing, biosamples = _inputs(source)
    target, target_digest = provenance_schema()
    schemas = {
        name: _output_schema(name, parent, next(iter(identities)), max_depth, target, target_digest)
        for name in TABLE_CLASSES
    }
    # Reserve exclusively; no overwrite even if another invocation created it during input checks.
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.mkdir()
    except FileExistsError as error:
        raise DerivedTableError("Derived output must be a new directory.") from error
    counts: dict[str, int] = {}
    max_hops = 0
    for name, schema in schemas.items():
        progress(f"writing {name}")
        writer = StreamingWriter(output / f"{name}.parquet", schema)
        try:
            if name == "graph_edges":
                for src, dst, slot in edges:
                    writer.append({"src": src, "next_id": dst, "slot": slot})
            else:
                for row in provenance_pairs(
                    edges, workflows, processing, biosamples, max_depth=max_depth, progress=progress
                ):
                    max_hops = max(max_hops, row["n_hops"])
                    writer.append(row)
        finally:
            counts[name] = writer.close()
        stored = pq.ParquetFile(output / f"{name}.parquet")
        if stored.metadata.num_rows != counts[name] or not stored.schema_arrow.equals(schema, check_metadata=True):
            raise DerivedTableError(f"Written {name} does not match its row count or declared schema.")
    progress("rechecking input snapshot and completing derived manifest")
    if validate_snapshot(source) != parent:
        raise DerivedTableError("Input snapshot changed during provenance generation.")
    generated_at = datetime.now(UTC).isoformat()
    metrics_path = output / "derivation-metrics.json"
    metrics_path.write_text(
        json.dumps(
            {
                "format_version": 1,
                "status": "success",
                "algorithm": ALGORITHM,
                "parent_snapshot_id": parent.snapshot_id,
                "generated_at": generated_at,
                "max_depth": max_depth,
                "max_biosample_hops": max_hops,
                "workflows": len(workflows),
                "rows_written": counts,
                "elapsed_seconds": time.monotonic() - started,
                "target_schema_sha256": target_digest,
                "inputs": [artifacts[name].model_dump(mode="json") for name in REQUIRED_TABLES],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    produced = [_artifact(output / f"{name}.parquet") for name in sorted(TABLE_CLASSES)]
    commit, dirty = _git_state(Path(__file__).resolve().parents[2])
    manifest = SnapshotManifest(
        manifest_format_version=MANIFEST_FORMAT_VERSION,
        snapshot_id="pending",
        generated_at=generated_at,
        scope="derived-provenance-snapshot",
        parent_snapshot_id=parent.snapshot_id,
        source_label=parent.source_label,
        included_collections=[],
        skipped_collections=[],
        footer_metadata_format_version="2",
        target_schema_ids=[str(target.id)],
        target_schema_versions=[str(target.version)],
        mapping_ids=[ALGORITHM],
        software=SoftwareRecord(
            nmdc_lakehouse_version=_package_version("nmdc-lakehouse"),
            git_commit=commit,
            git_dirty=dirty,
            nmdc_schema_version=_package_version("nmdc-schema"),
            python_version=platform.python_version(),
        ),
        performance_record=PerformanceRecord(path=metrics_path.name, sha256=_sha256(metrics_path)),
        artifacts=produced,
    )
    manifest.snapshot_id = _snapshot_identity(manifest)
    write_manifest(output, manifest)
    return validate_snapshot(output)
