"""Validate publication artifacts as one coherent, non-mutating checkpoint."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from nmdc_lakehouse.metadata_bundle import MetadataBundle, load_metadata_bundle
from nmdc_lakehouse.publication_plan import (
    DestinationInventory,
    DestinationTable,
    Disposition,
    PlanEntry,
    PublicationPlan,
    load_destination_inventory,
    load_publication_plan,
)
from nmdc_lakehouse.snapshot_manifest import ArtifactRecord, SnapshotManifest, validate_snapshot


class PublicationPreflightError(ValueError):
    """Raised when reviewed publication artifacts do not describe one operation."""


class PublicationPreflightReport(BaseModel):
    """Credential-free summary of a coherent publication artifact set."""

    model_config = ConfigDict(extra="forbid", strict=True)

    status: str
    snapshot_id: str
    destination_id: str
    destination_observed_at: str
    candidate_tables: int
    destination_tables: int
    metadata_tables: int
    dispositions: dict[str, int]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PublicationPreflightError(message)


def _candidate_evidence_matches(entry: PlanEntry, artifact: ArtifactRecord | None) -> bool:
    expected = (
        artifact.path if artifact else None,
        artifact.rows if artifact else None,
        artifact.physical_schema_sha256 if artifact else None,
        artifact.target_schema_id if artifact else None,
        artifact.mapping if artifact else None,
    )
    actual = (
        entry.candidate_path,
        entry.candidate_rows,
        entry.candidate_physical_schema_sha256,
        entry.candidate_target_schema_id,
        entry.candidate_mapping_id,
    )
    return actual == expected


def _destination_evidence_matches(entry: PlanEntry, table: DestinationTable | None) -> bool:
    expected: tuple[int | None, str | None]
    if table is None:
        expected = (None, None)
    else:
        expected = (table.rows, table.physical_schema_sha256)
    return (entry.destination_rows, entry.destination_physical_schema_sha256) == expected


def _disposition_is_compatible(entry: PlanEntry, *, candidate: bool, destination: bool) -> bool:
    return {
        Disposition.ADD: candidate and not destination,
        Disposition.REPLACE: candidate and destination,
        Disposition.PRESERVE: destination,
        Disposition.REBUILD: destination,
        Disposition.RETIRE: destination and not candidate,
    }[entry.disposition]


def build_publication_preflight(
    manifest: SnapshotManifest,
    bundle: MetadataBundle,
    inventory: DestinationInventory,
    plan: PublicationPlan,
) -> PublicationPreflightReport:
    """Check already loaded publication evidence for exact cross-document agreement."""
    _require(bundle.snapshot_id == manifest.snapshot_id, "Metadata bundle snapshot identity does not match.")
    _require(plan.candidate_snapshot_id == manifest.snapshot_id, "Publication plan snapshot identity does not match.")
    _require(plan.destination_id == inventory.destination_id, "Publication plan destination identity does not match.")
    _require(
        plan.destination_observed_at == inventory.observed_at,
        "Publication plan destination observation does not match.",
    )
    _require(plan.destination_provider == inventory.provider, "Publication plan destination provider does not match.")
    _require(
        plan.destination_table_format == inventory.table_format,
        "Publication plan destination table format does not match.",
    )
    plan_capabilities = plan.destination_metadata_capabilities
    inventory_capabilities = inventory.metadata_capabilities
    _require(
        len(plan_capabilities) == len(set(plan_capabilities)),
        "Publication plan contains duplicate destination metadata capabilities.",
    )
    _require(
        len(inventory_capabilities) == len(set(inventory_capabilities)),
        "Destination inventory contains duplicate metadata capabilities.",
    )
    _require(
        set(plan_capabilities) == set(inventory_capabilities),
        "Publication plan destination metadata capabilities do not match.",
    )

    candidate = {artifact.table: artifact for artifact in manifest.artifacts}
    destination = {table.name: table for table in inventory.tables}
    metadata = {table.name: table for table in bundle.tables}
    entries = {entry.table: entry for entry in plan.tables}
    _require(len(candidate) == len(manifest.artifacts), "Snapshot manifest contains duplicate table names.")
    _require(len(destination) == len(inventory.tables), "Destination inventory contains duplicate table names.")
    _require(len(metadata) == len(bundle.tables), "Metadata bundle contains duplicate table names.")
    _require(len(entries) == len(plan.tables), "Publication plan contains duplicate table names.")
    _require(set(metadata) == set(candidate), "Metadata bundle table coverage does not match the snapshot.")
    _require(
        set(entries) == set(candidate).union(destination),
        "Publication plan table coverage does not match the exact candidate/destination union.",
    )

    expected_source_schemas = sorted(
        {(artifact.source_schema_id, artifact.source_schema_version) for artifact in manifest.artifacts}
    )
    actual_source_schemas = sorted((item.schema_id, item.version) for item in bundle.source_schemas)
    _require(actual_source_schemas == expected_source_schemas, "Metadata bundle source schema identities do not match.")
    _require(
        sorted(bundle.target_schema_ids) == sorted(manifest.target_schema_ids),
        "Metadata bundle target schemas do not match.",
    )
    _require(
        sorted(bundle.mapping_ids) == sorted(manifest.mapping_ids),
        "Metadata bundle mappings do not match.",
    )

    for name, artifact in candidate.items():
        table = metadata[name]
        _require(
            (
                table.physical_schema_sha256,
                table.footer_schema_sha256,
                table.target_schema_id,
                table.mapping_id,
            )
            == (
                artifact.physical_schema_sha256,
                artifact.footer_schema_sha256,
                artifact.target_schema_id,
                artifact.mapping,
            ),
            f"Metadata bundle evidence does not match table '{name}'.",
        )

    counts = {disposition.value: 0 for disposition in Disposition}
    for name, entry in entries.items():
        candidate_artifact = candidate.get(name)
        destination_table = destination.get(name)
        _require(
            _candidate_evidence_matches(entry, candidate_artifact),
            f"Publication plan candidate evidence differs for '{name}'.",
        )
        _require(
            _destination_evidence_matches(entry, destination_table),
            f"Publication plan destination evidence differs for '{name}'.",
        )
        _require(
            _disposition_is_compatible(
                entry,
                candidate=candidate_artifact is not None,
                destination=destination_table is not None,
            ),
            f"Publication plan disposition is unsafe for '{name}'.",
        )
        counts[entry.disposition.value] += 1

    return PublicationPreflightReport(
        status="ready",
        snapshot_id=manifest.snapshot_id,
        destination_id=inventory.destination_id,
        destination_observed_at=inventory.observed_at,
        candidate_tables=len(candidate),
        destination_tables=len(destination),
        metadata_tables=len(metadata),
        dispositions=counts,
    )


def validate_publication_artifacts(
    snapshot_root: Path,
    bundle_path: Path,
    inventory_path: Path,
    plan_path: Path,
) -> PublicationPreflightReport:
    """Load, independently validate, and cross-check all pre-staging artifacts."""
    return build_publication_preflight(
        validate_snapshot(snapshot_root),
        load_metadata_bundle(bundle_path),
        load_destination_inventory(inventory_path),
        load_publication_plan(plan_path),
    )


def render_publication_preflight(report: PublicationPreflightReport) -> str:
    """Render a stable credential-free readiness summary."""
    return json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True)
