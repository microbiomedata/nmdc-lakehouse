"""Tests for portable snapshot manifest creation and offline validation."""

from __future__ import annotations

import json
import platform
import subprocess
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from nmdc_lakehouse import snapshot_manifest
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.jobs.collection_to_parquet import REVIEWED_SCHEMA_COLLECTIONS
from nmdc_lakehouse.sinks.parquet_sink import FOOTER_METADATA_FORMAT_VERSION
from nmdc_lakehouse.snapshot_manifest import (
    MANIFEST_FORMAT_VERSION,
    MANIFEST_NAME,
    SnapshotManifestError,
    build_manifest,
    validate_snapshot,
    write_manifest,
)


def _snapshot_fixture(root: Path, footer_version: bytes = b"2") -> Path:
    """Write a one-artifact snapshot. Pass b"1" for a footer predating target_schema_version."""
    metadata = {
        b"nmdc_lakehouse.footer_metadata_format_version": footer_version,
        b"nmdc_lakehouse.source_schema_id": b"https://w3id.org/nmdc/nmdc",
        b"nmdc_lakehouse.source_schema_version": b"11.10.0",
        b"nmdc_lakehouse.source_class": b"Biosample",
        b"nmdc_lakehouse.target_schema_id": b"https://w3id.org/nmdc/nmdc-schema-flattened",
        b"nmdc_lakehouse.target_class": b"Biosample",
        **({b"nmdc_lakehouse.target_schema_version": b"11.10.0+flat.1.0.0"} if footer_version != b"1" else {}),
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


def test_manifest_operations_reject_symlinked_snapshot_root(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    metrics_path = _snapshot_fixture(snapshot)
    manifest = build_manifest(snapshot, metrics_path, "nmdc-production")
    linked_snapshot = tmp_path / "linked-snapshot"
    linked_snapshot.symlink_to(snapshot, target_is_directory=True)

    with pytest.raises(SnapshotManifestError, match="ordinary directory"):
        build_manifest(linked_snapshot, linked_snapshot / metrics_path.name, "nmdc-production")
    with pytest.raises(SnapshotManifestError, match="ordinary directory"):
        write_manifest(linked_snapshot, manifest)
    with pytest.raises(SnapshotManifestError, match="ordinary directory"):
        validate_snapshot(linked_snapshot)


def test_build_manifest_rejects_symlinked_metrics_record(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    linked_metrics = tmp_path / "linked-metrics.json"
    linked_metrics.symlink_to(metrics_path)

    with pytest.raises(SnapshotManifestError, match="ordinary file"):
        build_manifest(tmp_path, linked_metrics, "nmdc-production")


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
    assert schema["x-manifest-format-version"] == MANIFEST_FORMAT_VERSION
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


def test_manifest_write_does_not_clobber_competing_completion_marker(tmp_path: Path, monkeypatch) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    destination = tmp_path / MANIFEST_NAME

    def competing_writer(_temporary: Path, link_destination: Path) -> None:
        link_destination.write_text("competing manifest\n", encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr(snapshot_manifest.os, "link", competing_writer)

    with pytest.raises(SnapshotManifestError, match="Refusing to replace"):
        write_manifest(tmp_path, manifest)

    assert destination.read_text(encoding="utf-8") == "competing manifest\n"
    assert not list(tmp_path.glob(f".{MANIFEST_NAME}.*.tmp"))


def test_manifest_write_translates_atomic_publication_error(tmp_path: Path, monkeypatch) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")

    def reject_hard_link(_temporary: Path, _destination: Path) -> None:
        raise OSError("hard links unavailable")

    monkeypatch.setattr(snapshot_manifest.os, "link", reject_hard_link)

    with pytest.raises(SnapshotManifestError, match="Cannot publish.*atomically"):
        write_manifest(tmp_path, manifest)

    assert not (tmp_path / MANIFEST_NAME).exists()
    assert not list(tmp_path.glob(f".{MANIFEST_NAME}.*.tmp"))


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


@pytest.mark.parametrize("version_name", ["nmdc_schema_version", "nmdc_lakehouse_version", "python_version"])
def test_manifest_rejects_missing_producer_version(tmp_path: Path, version_name: str) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    record = json.loads(metrics_path.read_text(encoding="utf-8"))
    record["environment"][version_name] = None
    metrics_path.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match="complete producer version metadata"):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


def test_manifest_rejects_unavailable_schema_collection_inventory(tmp_path: Path, monkeypatch) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    monkeypatch.setattr("nmdc_lakehouse.jobs.collection_to_parquet._db_collection_map", lambda: {})

    with pytest.raises(SnapshotManifestError, match="does not expose any MongoDB collections"):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


def test_manifest_rejects_unsafe_source_label_and_external_metrics(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)

    with pytest.raises(SnapshotManifestError, match="start with a letter or digit"):
        build_manifest(tmp_path, metrics_path, "mongodb://user:secret@example.org/nmdc")
    with pytest.raises(SnapshotManifestError, match="1–64 characters"):
        build_manifest(tmp_path, metrics_path, "a" * 65)
    external = tmp_path.parent / "external-metrics.json"
    external.write_bytes(metrics_path.read_bytes())
    with pytest.raises(SnapshotManifestError, match="directly inside"):
        build_manifest(tmp_path, external, "nmdc-production")


@pytest.mark.parametrize("bad_path", [None, 1, "../biosample_set.parquet", "biosample-set.parquet"])
def test_manifest_rejects_malformed_metrics_output_path(tmp_path: Path, bad_path: object) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    record = json.loads(metrics_path.read_text(encoding="utf-8"))
    record["outputs"][0]["path"] = bad_path
    metrics_path.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match="malformed output paths"):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


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


def test_manifest_rejects_extra_directory_before_creation(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    (tmp_path / "unmanifested").mkdir()

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

    # Assert the offending name, not the category. Naming the file is the point; a category word
    # sent an operator looking at the manifest when the problem was the copy. See #270.
    with pytest.raises(SnapshotManifestError, match=r"unexpected 1: 'unmanifested\.txt'"):
        validate_snapshot(tmp_path)


def test_validation_detects_extra_directory(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    write_manifest(tmp_path, build_manifest(tmp_path, metrics_path, "nmdc-production"))
    (tmp_path / "unmanifested").mkdir()

    with pytest.raises(SnapshotManifestError, match=r"unexpected 1: 'unmanifested'"):
        validate_snapshot(tmp_path)


def test_validation_names_a_missing_file_and_separates_it_from_an_unexpected_one(tmp_path: Path) -> None:
    """Missing and unexpected have different causes and different fixes; #270."""
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    write_manifest(tmp_path, manifest)
    gone = tmp_path / manifest.artifacts[0].path
    gone.unlink()
    (tmp_path / "leftover.tmp").write_text("x", encoding="utf-8")

    with pytest.raises(SnapshotManifestError) as caught:
        validate_snapshot(tmp_path)

    message = str(caught.value)
    assert f"missing 1: '{gone.name}'" in message
    assert "unexpected 1: 'leftover.tmp'" in message


def test_validation_explains_appledouble_siblings(tmp_path: Path) -> None:
    """The failure that actually happened, on 2026-08-20, with the remedy in the message."""
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    write_manifest(tmp_path, manifest)
    for artifact in manifest.artifacts:
        (tmp_path / f"._{artifact.path}").write_text("resource fork", encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match="COPYFILE_DISABLE=1") as caught:
        validate_snapshot(tmp_path)

    message = str(caught.value)
    assert "start with '._'" in message
    assert "macOS tar archive on Linux" in message


def test_validation_caps_the_names_it_lists(tmp_path: Path) -> None:
    """A snapshot with hundreds of stray files must not produce hundreds of lines of error."""
    metrics_path = _snapshot_fixture(tmp_path)
    write_manifest(tmp_path, build_manifest(tmp_path, metrics_path, "nmdc-production"))
    for index in range(25):
        (tmp_path / f"stray{index:02d}.tmp").write_text("x", encoding="utf-8")

    with pytest.raises(SnapshotManifestError) as caught:
        validate_snapshot(tmp_path)

    message = str(caught.value)
    assert "unexpected 25:" in message, "the count is reported in full"
    assert "and 15 more" in message, "only the first ten are named"
    assert "stray24.tmp" not in message


def test_validation_rejects_duplicate_artifact_paths(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    manifest.artifacts.append(manifest.artifacts[0].model_copy(deep=True))
    manifest.snapshot_id = snapshot_manifest._snapshot_identity(manifest)
    write_manifest(tmp_path, manifest)

    with pytest.raises(SnapshotManifestError, match="one-to-one inventory"):
        validate_snapshot(tmp_path)


def test_validation_rejects_manifest_path_as_owned_content(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    manifest.performance_record.path = MANIFEST_NAME
    write_manifest(tmp_path, manifest)

    with pytest.raises(SnapshotManifestError, match="one-to-one inventory"):
        validate_snapshot(tmp_path)


def test_validation_rejects_unsupported_footer_metadata_format(tmp_path: Path) -> None:
    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    manifest.footer_metadata_format_version = "unsupported"
    manifest.snapshot_id = snapshot_manifest._snapshot_identity(manifest)
    write_manifest(tmp_path, manifest)

    with pytest.raises(SnapshotManifestError, match="Unsupported Parquet footer metadata format version"):
        validate_snapshot(tmp_path)


@pytest.mark.parametrize("error", [FileNotFoundError(), subprocess.TimeoutExpired("git", 5)])
def test_missing_or_unresponsive_git_does_not_block_manifest(monkeypatch, tmp_path: Path, error: Exception) -> None:
    (tmp_path / "pyproject.toml").touch()
    package_file = tmp_path / "src" / "nmdc_lakehouse" / "snapshot_manifest.py"
    package_file.parent.mkdir(parents=True)
    package_file.touch()
    monkeypatch.setattr(snapshot_manifest.subprocess, "run", lambda *_args, **_kwargs: (_ for _ in ()).throw(error))

    assert snapshot_manifest._git_state(tmp_path) == (None, None)


def test_a_version_1_manifest_keeps_the_identity_it_was_written_with(tmp_path: Path) -> None:
    """Adding a field must not invalidate snapshots already on disk.

    This is a regression, not a hypothetical. Adding `target_schema_version` hashed it into the
    identity of every manifest, and `validate-snapshot` on the 2026-08-21 snapshot, the one the
    verified staging run was built from, began reporting "Snapshot identity does not match the
    manifest content".
    """
    from nmdc_lakehouse.snapshot_manifest import _snapshot_identity

    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    manifest.manifest_format_version = 1
    manifest.target_schema_versions = []
    for artifact in manifest.artifacts:
        artifact.target_schema_version = ""
    before = _snapshot_identity(manifest)

    # A version 1 manifest read by a version 2 reader picks the new fields up as defaults. Those
    # must not reach the hash.
    manifest.target_schema_versions = ["11.23.0+flat.1.0.0"]
    for artifact in manifest.artifacts:
        artifact.target_schema_version = "11.23.0+flat.1.0.0"

    assert _snapshot_identity(manifest) == before


def test_a_version_2_manifest_does_hash_the_schema_version(tmp_path: Path) -> None:
    """The exclusion is scoped to version 1, or the new field would record nothing at all."""
    from nmdc_lakehouse.snapshot_manifest import _snapshot_identity

    metrics_path = _snapshot_fixture(tmp_path)
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    manifest.manifest_format_version = 2
    manifest.target_schema_versions = ["11.23.0+flat.1.0.0"]
    before = _snapshot_identity(manifest)

    manifest.target_schema_versions = ["11.23.0+flat.2.0.0"]

    assert _snapshot_identity(manifest) != before


def test_a_written_artifact_declares_the_flat_schema_that_produced_it(tmp_path: Path) -> None:
    """The point of issue 293: a Parquet file that can name its own schema.

    Asserting the version numbers alone would pass with the field absent from every footer, which
    is what the first version of this test did.
    """
    metrics_path = _snapshot_fixture(tmp_path)

    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")

    declared = {artifact.target_schema_version for artifact in manifest.artifacts}
    assert declared == {"11.10.0+flat.1.0.0"}, declared
    assert manifest.target_schema_versions == ["11.10.0+flat.1.0.0"]
    assert manifest.footer_metadata_format_version == "2"
    assert all(artifact.footer_metadata_format_version == "2" for artifact in manifest.artifacts)


def test_a_version_2_footer_must_carry_the_field_version_2_added(tmp_path: Path) -> None:
    """Optional-unconditionally would let a footer claim a contract it does not implement."""
    _snapshot_fixture(tmp_path)
    parquet_path = tmp_path / "biosample_set.parquet"
    table = pq.read_table(parquet_path)
    metadata = dict(table.schema.metadata)
    del metadata[b"nmdc_lakehouse.target_schema_version"]
    pq.write_table(table.replace_schema_metadata(metadata), parquet_path)

    with pytest.raises(SnapshotManifestError, match="declares format 2 but omits"):
        build_manifest(tmp_path, tmp_path / "etl-metrics.json", "nmdc-production")


def test_a_version_1_footer_may_omit_it(tmp_path: Path) -> None:
    """The requirement is scoped to the version that introduced the field, not to all footers."""
    metrics_path = _snapshot_fixture(tmp_path, footer_version=b"1")

    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")

    assert manifest.footer_metadata_format_version == "1"
    assert manifest.artifacts[0].target_schema_version == ""


def test_the_manifest_reports_the_footer_version_its_artifacts_carry(tmp_path: Path) -> None:
    """Not the writer's current constant.

    Recording the constant meant a snapshot of version 1 files declared version 2, and nothing
    could detect it, because the per-artifact record did not carry a version to compare against.
    """
    metrics_path = _snapshot_fixture(tmp_path, footer_version=b"1")

    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")

    assert FOOTER_METADATA_FORMAT_VERSION == "2", "the constant and the artifacts must differ here"
    assert manifest.footer_metadata_format_version == "1"


def test_a_snapshot_mixing_footer_versions_is_refused(tmp_path: Path) -> None:
    """One manifest cannot honestly describe two footer contracts."""
    metrics_path = _snapshot_fixture(tmp_path, footer_version=b"1")
    older = pq.read_table(tmp_path / "biosample_set.parquet")
    newer_metadata = dict(older.schema.metadata)
    newer_metadata[b"nmdc_lakehouse.footer_metadata_format_version"] = b"2"
    newer_metadata[b"nmdc_lakehouse.target_schema_version"] = b"11.10.0+flat.1.0.0"
    pq.write_table(older.replace_schema_metadata(newer_metadata), tmp_path / "study_set.parquet")

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    metrics["outputs"].append(
        {
            "table": "study_set",
            "path": "study_set.parquet",
            "rows": 1,
            "bytes": (tmp_path / "study_set.parquet").stat().st_size,
        }
    )
    metrics_path.write_text(json.dumps(metrics), encoding="utf-8")

    with pytest.raises(SnapshotManifestError, match="mixes Parquet footer metadata formats"):
        build_manifest(tmp_path, metrics_path, "nmdc-production")


def test_a_version_1_snapshot_still_validates_end_to_end(tmp_path: Path) -> None:
    """The whole point of the version-aware handling, exercised through validate_snapshot.

    `local/mongodb-metadata-20260821_104214`, the snapshot the verified staging run was built
    from, is a version 1 manifest. Everything here is what that file does on disk.
    """
    from nmdc_lakehouse.snapshot_manifest import _snapshot_identity

    metrics_path = _snapshot_fixture(tmp_path, footer_version=b"1")
    manifest = build_manifest(tmp_path, metrics_path, "nmdc-production")
    manifest.manifest_format_version = 1
    manifest.target_schema_versions = []
    for artifact in manifest.artifacts:
        artifact.target_schema_version = ""
        artifact.footer_metadata_format_version = ""
    manifest.snapshot_id = _snapshot_identity(manifest)
    write_manifest(tmp_path, manifest)

    assert validate_snapshot(tmp_path) == manifest


def test_a_snapshot_with_no_artifacts_falls_back_to_the_current_footer_version() -> None:
    from nmdc_lakehouse.snapshot_manifest import _single_footer_version

    assert _single_footer_version([]) == FOOTER_METADATA_FORMAT_VERSION


def test_a_footer_missing_a_key_required_at_every_version_is_refused(tmp_path: Path) -> None:
    """Distinct from the version 2 rule: these keys have been required since version 1."""
    _snapshot_fixture(tmp_path)
    parquet_path = tmp_path / "biosample_set.parquet"
    table = pq.read_table(parquet_path)
    metadata = dict(table.schema.metadata)
    del metadata[b"nmdc_lakehouse.source_class"]
    pq.write_table(table.replace_schema_metadata(metadata), parquet_path)

    with pytest.raises(SnapshotManifestError, match="missing required metadata: source_class"):
        build_manifest(tmp_path, tmp_path / "etl-metrics.json", "nmdc-production")
