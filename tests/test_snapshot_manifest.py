"""Tests for portable snapshot manifest creation and offline validation."""

from __future__ import annotations

import json
import platform
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.jobs.collection_to_parquet import REVIEWED_SCHEMA_COLLECTIONS
from nmdc_lakehouse.snapshot_manifest import (
    MANIFEST_NAME,
    SnapshotManifestError,
    build_manifest,
    validate_snapshot,
    write_manifest,
)


def _snapshot_fixture(root: Path) -> Path:
    metadata = {
        b"nmdc_lakehouse.footer_metadata_format_version": b"1",
        b"nmdc_lakehouse.source_schema_id": b"https://w3id.org/nmdc/nmdc",
        b"nmdc_lakehouse.source_schema_version": b"11.10.0",
        b"nmdc_lakehouse.source_class": b"Biosample",
        b"nmdc_lakehouse.target_schema_id": b"https://w3id.org/nmdc/nmdc-schema-flattened",
        b"nmdc_lakehouse.target_class": b"Biosample",
        b"nmdc_lakehouse.mapping": b"nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener",
    }
    field = pa.field("id", pa.string(), metadata={b"nmdc_lakehouse.identifier": b"true"})
    table = pa.Table.from_arrays([pa.array(["nmdc:bsm-1"])], schema=pa.schema([field], metadata=metadata))
    parquet_path = root / "biosample_set.parquet"
    pq.write_table(table, parquet_path)

    metrics_path = root / "etl-metrics.json"
    metrics_path.write_text(
        json.dumps(
            {
                "format_version": 1,
                "job_name": "all-collections",
                "status": "success",
                "dry_run": False,
                "finished_at": "2026-08-18T16:00:00+00:00",
                "output_root": str(root.resolve()),
                "environment": {
                    "nmdc_lakehouse_version": version("nmdc-lakehouse"),
                    "nmdc_schema_version": version("nmdc-schema"),
                    "python_version": platform.python_version(),
                },
                "skipped_collections": sorted(REVIEWED_SCHEMA_COLLECTIONS - {"biosample_set"}),
                "outputs": [
                    {
                        "table": "biosample_set",
                        "path": parquet_path.name,
                        "rows": 1,
                        "bytes": parquet_path.stat().st_size,
                    }
                ],
                "children": [{"job_name": "biosample_set"}],
            }
        ),
        encoding="utf-8",
    )
    return metrics_path


def test_build_write_and_validate_snapshot(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)

    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    destination = write_manifest(tmp_path, manifest)
    validated = validate_snapshot(tmp_path)

    assert destination == tmp_path / MANIFEST_NAME
    assert validated == manifest
    assert manifest.snapshot_id.startswith("sha256:")
    assert manifest.included_collections == ["biosample_set"]
    assert "functional_annotation_agg" in manifest.skipped_collections
    assert manifest.artifacts[0].rows == 1
    assert manifest.artifacts[0].source_schema_version == "11.10.0"
    assert manifest.artifacts[0].physical_schema_sha256 != manifest.artifacts[0].footer_schema_sha256
    assert not list(tmp_path.glob(f".{MANIFEST_NAME}.*.tmp"))


def test_manifest_cli_creates_and_validates_snapshot(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    runner = CliRunner()

    created = runner.invoke(
        cli,
        [
            "create-snapshot-manifest",
            str(tmp_path),
            "--metrics",
            str(metrics_path),
            "--source-label",
            "nmdc-production",
        ],
    )
    validated = runner.invoke(cli, ["validate-snapshot", str(tmp_path)])

    assert created.exit_code == 0, created.output
    assert "snapshot_id=sha256:" in created.output
    assert "artifacts=1" in created.output
    assert validated.exit_code == 0, validated.output
    assert "1 Parquet artifact(s)" in validated.output


def test_manifest_schema_cli_emits_versioned_json_schema() -> None:
    result = CliRunner().invoke(cli, ["snapshot-manifest-schema"])
    schema = json.loads(result.output)

    assert result.exit_code == 0
    assert schema["title"] == "SnapshotManifest"
    assert schema["x-manifest-format-version"] == 1
    assert "artifacts" in schema["properties"]


def test_snapshot_identity_is_stable_for_the_same_content(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)

    first = build_manifest(tmp_path, metrics_path, "nmdc-production")
    second = build_manifest(tmp_path, metrics_path, "nmdc-production")

    assert first.snapshot_id == second.snapshot_id


def test_manifest_is_an_immutable_completion_marker(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    write_manifest(tmp_path, manifest)

    with pytest.raises(SnapshotManifestError, match="Refusing to replace"):
        write_manifest(tmp_path, manifest)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda record: record.update(status="failed"), "successful all-collections"),
        (lambda record: record.update(dry_run=True), "dry run"),
        (lambda record: record.update(job_name="biosample_set"), "successful all-collections"),
        (lambda record: record.update(output_root="/tmp/somewhere-else"), "output_root"),
    ],
)
def test_manifest_rejects_incomplete_metrics(tmp_path: Path, change, message: str) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    record = json.loads(metrics_path.read_text(encoding="utf-8"))
    change(record)
    metrics_path.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match=message):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


def test_manifest_rejects_unsafe_source_label_and_external_metrics(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)

    with pytest.raises(SnapshotManifestError, match="Source label"):
        build_manifest(tmp_path, metrics_path, "mongodb://user:secret@example.org/nmdc")
    external = tmp_path.parent / "external-metrics.json"
    external.write_bytes(metrics_path.read_bytes())
    with pytest.raises(SnapshotManifestError, match="directly inside"):
        build_manifest(tmp_path, external, "nmdc-production")


def test_manifest_rejects_extra_parquet_before_creation(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    (tmp_path / "stale.parquet").write_bytes(b"stale")

    with pytest.raises(SnapshotManifestError, match="file sets"):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


def test_manifest_rejects_any_extra_file_before_creation(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    (tmp_path / "run.log").write_text("extra", encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match="extra files"):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


@pytest.mark.parametrize("changed_name", ["biosample_set.parquet", "etl-metrics.json"])
def test_validation_detects_changed_owned_file(tmp_path: Path, changed_name: str) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    write_manifest(tmp_path, build_manifest(tmp_path, metrics_path, "nmdc-production"))
    with (tmp_path / changed_name).open("ab") as stream:
        stream.write(b"changed")

    with pytest.raises(SnapshotManifestError):
        validate_snapshot(tmp_path)


def test_validation_detects_extra_file(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    write_manifest(tmp_path, build_manifest(tmp_path, metrics_path, "nmdc-production"))
    (tmp_path / "unmanifested.txt").write_text("extra", encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match="unmanifested"):
        validate_snapshot(tmp_path)
