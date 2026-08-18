"""Tests for the destination-neutral pre-staging publication checkpoint."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.metadata_bundle import (
    ColumnMetadata,
    DescriptionRecord,
    MetadataBundle,
    NamespaceProfile,
    SchemaIdentity,
    TableMetadata,
)
from nmdc_lakehouse.publication_plan import (
    DestinationInventory,
    DestinationTable,
    Disposition,
    MetadataCapability,
    PublicationPlan,
    PublicationPolicy,
    build_publication_plan,
)
from nmdc_lakehouse.publication_preflight import (
    PublicationPreflightError,
    PublicationPreflightReport,
    build_publication_preflight,
)
from nmdc_lakehouse.snapshot_manifest import (
    ArtifactRecord,
    PerformanceRecord,
    SnapshotManifest,
    SoftwareRecord,
)

SNAPSHOT_ID = "sha256:" + "1" * 64


def _artifact(name: str, fingerprint: str) -> ArtifactRecord:
    return ArtifactRecord(
        path=f"{name}.parquet",
        table=name,
        rows=1,
        bytes=100,
        sha256="f" * 64,
        physical_schema_sha256=fingerprint * 64,
        footer_schema_sha256=("c" if fingerprint == "a" else "d") * 64,
        source_schema_id="https://w3id.org/nmdc/nmdc",
        source_schema_version="11.10.0",
        source_class=name,
        target_schema_id="https://w3id.org/nmdc/nmdc-schema-flattened",
        target_class=name,
        mapping="nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener",
    )


def _manifest() -> SnapshotManifest:
    return SnapshotManifest(
        manifest_format_version=1,
        snapshot_id=SNAPSHOT_ID,
        generated_at="2026-08-18T16:00:00+00:00",
        scope="full-mongodb-metadata-snapshot",
        source_label="nmdc-production",
        included_collections=["biosample_set"],
        skipped_collections=["functional_annotation_agg"],
        footer_metadata_format_version="1",
        target_schema_ids=["https://w3id.org/nmdc/nmdc-schema-flattened"],
        mapping_ids=["nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener"],
        software=SoftwareRecord(
            nmdc_lakehouse_version="0.2.0.dev0",
            nmdc_schema_version="11.10.0",
            python_version="3.13.13",
            git_commit=None,
            git_dirty=None,
        ),
        performance_record=PerformanceRecord(path="etl-metrics.json", sha256="e" * 64),
        artifacts=[_artifact("biosample_set", "a"), _artifact("study_set", "b")],
    )


def _bundle(manifest: SnapshotManifest) -> MetadataBundle:
    tables = [
        TableMetadata(
            name=artifact.table,
            source_class=artifact.source_class,
            target_schema_id=artifact.target_schema_id,
            target_class=artifact.target_class,
            mapping_id=artifact.mapping,
            physical_schema_sha256=artifact.physical_schema_sha256,
            footer_schema_sha256=artifact.footer_schema_sha256,
            description=DescriptionRecord(value="Table description.", origin="footer"),
            columns=[
                ColumnMetadata(
                    name="id",
                    arrow_type="string",
                    nullable=True,
                    linkml_range="string",
                    identifier=True,
                    designates_type=False,
                    description=DescriptionRecord(value="Identifier.", origin="footer"),
                )
            ],
        )
        for artifact in manifest.artifacts
    ]
    return MetadataBundle(
        bundle_format_version=1,
        generated_at="2026-08-18T18:00:00+00:00",
        snapshot_id=manifest.snapshot_id,
        profile_id="nmdc-metadata-reviewed",
        source_schemas=[SchemaIdentity(schema_id="https://w3id.org/nmdc/nmdc", version="11.10.0")],
        target_schema_ids=manifest.target_schema_ids,
        mapping_ids=manifest.mapping_ids,
        namespace=NamespaceProfile(
            name="nmdc_metadata",
            title="NMDC metadata",
            description="Flattened NMDC metadata tables.",
        ),
        tables=tables,
    )


def _inventory() -> DestinationInventory:
    return DestinationInventory(
        inventory_format_version=1,
        destination_id="nmdc-production",
        observed_at="2026-08-18T17:00:00+00:00",
        provider="spark_catalog",
        table_format="delta",
        metadata_capabilities=[MetadataCapability.NAMESPACE, MetadataCapability.TABLE, MetadataCapability.COLUMN],
        tables=[DestinationTable(name="biosample_set", rows=1, physical_schema_sha256="a" * 64)],
    )


def _artifacts() -> tuple[SnapshotManifest, MetadataBundle, DestinationInventory, PublicationPlan]:
    manifest = _manifest()
    inventory = _inventory()
    policy = PublicationPolicy(policy_format_version=1, rules=[])
    return manifest, _bundle(manifest), inventory, build_publication_plan(manifest, inventory, policy)


def test_coherent_artifacts_produce_credential_free_summary() -> None:
    manifest, bundle, inventory, plan = _artifacts()

    report = build_publication_preflight(manifest, bundle, inventory, plan)

    assert report.status == "ready"
    assert report.candidate_tables == 2
    assert report.destination_tables == 1
    assert report.metadata_tables == 2
    assert report.dispositions == {"replace": 1, "add": 1, "preserve": 0, "rebuild": 0, "retire": 0}
    assert "path" not in report.model_dump_json()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("bundle-snapshot", "Metadata bundle snapshot identity"),
        ("plan-snapshot", "Publication plan snapshot identity"),
        ("destination", "destination identity"),
        ("observation", "destination observation"),
        ("plan-coverage", "table coverage"),
        ("bundle-evidence", "Metadata bundle evidence"),
    ],
)
def test_preflight_rejects_cross_document_mismatches(mutation: str, message: str) -> None:
    manifest, bundle, inventory, plan = _artifacts()
    if mutation == "bundle-snapshot":
        bundle.snapshot_id = "sha256:" + "2" * 64
    elif mutation == "plan-snapshot":
        plan.candidate_snapshot_id = "sha256:" + "2" * 64
    elif mutation == "destination":
        inventory.destination_id = "different-destination"
    elif mutation == "observation":
        inventory.observed_at = "2026-08-18T19:00:00+00:00"
    elif mutation == "plan-coverage":
        plan.tables.pop()
    else:
        bundle.tables[0].physical_schema_sha256 = "9" * 64

    with pytest.raises(PublicationPreflightError, match=message):
        build_publication_preflight(manifest, bundle, inventory, plan)


def test_preflight_rejects_unsafe_edited_disposition() -> None:
    manifest, bundle, inventory, plan = _artifacts()
    study = next(entry for entry in plan.tables if entry.table == "study_set")
    study.disposition = Disposition.RETIRE

    with pytest.raises(PublicationPreflightError, match="disposition is unsafe"):
        build_publication_preflight(manifest, bundle, inventory, plan)


def test_cli_prints_preflight_report_without_service_access(monkeypatch: pytest.MonkeyPatch) -> None:
    report = PublicationPreflightReport(
        status="ready",
        snapshot_id=SNAPSHOT_ID,
        destination_id="nmdc-production",
        destination_observed_at="2026-08-18T17:00:00+00:00",
        candidate_tables=52,
        destination_tables=49,
        metadata_tables=52,
        dispositions={"replace": 46, "add": 6, "preserve": 1, "rebuild": 2, "retire": 0},
    )
    monkeypatch.setattr(
        "nmdc_lakehouse.publication_preflight.validate_publication_artifacts",
        lambda *_args: report,
    )

    result = CliRunner().invoke(
        cli,
        [
            "publication-preflight",
            "snapshot",
            "--bundle",
            "bundle.json",
            "--inventory",
            "inventory.json",
            "--plan",
            "plan.json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == report.model_dump(mode="json")
