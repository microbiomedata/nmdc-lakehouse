"""Tests for destination-neutral snapshot metadata bundles."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.metadata_bundle import (
    BUNDLE_FORMAT_VERSION,
    DescriptionOverride,
    MetadataBundle,
    MetadataBundleError,
    MetadataProfile,
    NamespaceProfile,
    build_metadata_bundle,
    build_metadata_profile,
    load_metadata_bundle,
    load_metadata_profile,
    metadata_json_schema,
    render_metadata_bundle,
    render_metadata_profile,
    write_metadata_bundle,
    write_metadata_profile,
)
from nmdc_lakehouse.snapshot_manifest import (
    ArtifactRecord,
    PerformanceRecord,
    SnapshotManifest,
    SoftwareRecord,
)

SNAPSHOT_ID = "sha256:" + "1" * 64
PREFIX = b"nmdc_lakehouse."


def _write_table(
    root: Path,
    name: str,
    *,
    table_description: str | None,
    column_description: str | None,
) -> None:
    schema_metadata = {
        PREFIX + b"table_description": table_description.encode() if table_description else None,
    }
    field_metadata = {
        PREFIX + b"description": column_description.encode() if column_description else None,
        PREFIX + b"linkml_range": b"string",
        PREFIX + b"identifier": b"true",
    }
    schema = pa.schema(
        [pa.field("id", pa.string(), metadata={key: value for key, value in field_metadata.items() if value})],
        metadata={key: value for key, value in schema_metadata.items() if value},
    )
    table = pa.Table.from_arrays([pa.array([f"nmdc:{name}-1"])], schema=schema)
    pq.write_table(table, root / f"{name}.parquet")


def _artifact(name: str) -> ArtifactRecord:
    return ArtifactRecord(
        path=f"{name}.parquet",
        table=name,
        rows=1,
        bytes=100,
        sha256="f" * 64,
        physical_schema_sha256=("a" if name == "biosample_set" else "b") * 64,
        footer_schema_sha256=("c" if name == "biosample_set" else "d") * 64,
        source_schema_id="https://w3id.org/nmdc/nmdc",
        source_schema_version="11.10.0",
        source_class="Biosample",
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
        parent_snapshot_id=None,
        source_label="nmdc-production",
        included_collections=["biosample_set"],
        skipped_collections=["functional_annotation_agg"],
        footer_metadata_format_version="1",
        target_schema_ids=["https://w3id.org/nmdc/nmdc-schema-flattened"],
        mapping_ids=["nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener"],
        software=SoftwareRecord(
            nmdc_lakehouse_version="0.2.0.dev0",
            git_commit=None,
            git_dirty=None,
            nmdc_schema_version="11.10.0",
            python_version="3.13.13",
        ),
        performance_record=PerformanceRecord(path="etl-metrics.json", sha256="e" * 64),
        artifacts=[_artifact("biosample_set"), _artifact("biosample_set_associated_studies")],
    )


def _namespace() -> NamespaceProfile:
    return NamespaceProfile(
        name="nmdc_metadata",
        title="NMDC metadata",
        description="Flattened NMDC metadata tables.",
        documentation_url="https://github.com/microbiomedata/nmdc-lakehouse",
        properties={"role": "metadata", "collection": "nmdc"},
    )


def _profile(*overrides: DescriptionOverride, snapshot_id: str = SNAPSHOT_ID) -> MetadataProfile:
    return MetadataProfile(
        profile_format_version=1,
        profile_id="nmdc-metadata-2026-08-18",
        snapshot_id=snapshot_id,
        namespace=_namespace(),
        overrides=list(overrides),
    )


def _snapshot_files(root: Path) -> None:
    _write_table(
        root,
        "biosample_set",
        table_description="Generated biosample table.",
        column_description="Stable biosample identifier.",
    )
    _write_table(
        root,
        "biosample_set_associated_studies",
        table_description="Generated relationship table.",
        column_description=None,
    )


def test_profile_draft_uses_validated_snapshot_identity() -> None:
    profile = build_metadata_profile(
        _manifest(),
        profile_id="nmdc-metadata-2026-08-18",
        namespace_name="nmdc_metadata",
        title="NMDC metadata",
        description="Flattened NMDC metadata tables.",
        documentation_url="https://github.com/microbiomedata/nmdc-lakehouse",
        properties={"collection": "nmdc", "role": "metadata"},
    )

    assert profile.snapshot_id == SNAPSHOT_ID
    assert profile.namespace.name == "nmdc_metadata"
    assert profile.overrides == []


def test_profile_draft_rejects_invalid_review_content() -> None:
    with pytest.raises(MetadataBundleError, match="valid metadata profile"):
        build_metadata_profile(
            _manifest(),
            profile_id="nmdc metadata",
            namespace_name="nmdc_metadata",
            title="NMDC metadata",
            description="Flattened NMDC metadata tables.",
        )


def test_bundle_uses_footer_baseline_and_reviewed_overrides(tmp_path: Path) -> None:
    _snapshot_files(tmp_path)
    profile = _profile(
        DescriptionOverride(
            table="biosample_set",
            column=None,
            description="Reviewed biosample table.",
            rationale="Clarify the table's publication role.",
            source="NMDC metadata review 2026-08-18",
        ),
        DescriptionOverride(
            table="biosample_set_associated_studies",
            column="id",
            description="Identifier in the associated-study relationship.",
            rationale="The generated schema has no field description.",
            source="NMDC schema review 2026-08-18",
        ),
    )

    bundle = build_metadata_bundle(
        tmp_path,
        _manifest(),
        profile,
        generated_at="2026-08-18T18:00:00+00:00",
    )

    assert bundle.snapshot_id == SNAPSHOT_ID
    assert bundle.namespace.name == "nmdc_metadata"
    assert [table.name for table in bundle.tables] == ["biosample_set", "biosample_set_associated_studies"]
    primary = bundle.tables[0]
    side = bundle.tables[1]
    assert primary.description.value == "Reviewed biosample table."
    assert primary.description.origin == "profile"
    assert primary.columns[0].description.value == "Stable biosample identifier."
    assert primary.columns[0].description.origin == "footer"
    assert primary.columns[0].identifier is True
    assert side.columns[0].description.origin == "profile"
    assert bundle.source_schemas[0].version == "11.10.0"


def test_missing_description_is_explicit(tmp_path: Path) -> None:
    _snapshot_files(tmp_path)

    bundle = build_metadata_bundle(
        tmp_path,
        _manifest(),
        _profile(),
        generated_at="2026-08-18T18:00:00+00:00",
    )

    description = bundle.tables[1].columns[0].description
    assert description.value is None
    assert description.origin == "none"


def test_blank_footer_metadata_is_rejected(tmp_path: Path) -> None:
    _write_table(
        tmp_path,
        "biosample_set",
        table_description="   ",
        column_description="Stable biosample identifier.",
    )
    _write_table(
        tmp_path,
        "biosample_set_associated_studies",
        table_description="Generated relationship table.",
        column_description=None,
    )

    with pytest.raises(MetadataBundleError, match="blank portable metadata"):
        build_metadata_bundle(
            tmp_path,
            _manifest(),
            _profile(),
            generated_at="2026-08-18T18:00:00+00:00",
        )


def test_profile_rejects_duplicate_and_unknown_overrides(tmp_path: Path) -> None:
    _snapshot_files(tmp_path)
    duplicate = DescriptionOverride(
        table="biosample_set",
        description="Reviewed table.",
        rationale="Review.",
        source="Review source.",
    )
    with pytest.raises(MetadataBundleError, match="duplicate"):
        build_metadata_bundle(
            tmp_path,
            _manifest(),
            _profile(duplicate, duplicate),
            generated_at="2026-08-18T18:00:00+00:00",
        )

    unknown = DescriptionOverride(
        table="missing_table",
        description="Unknown table.",
        rationale="Review.",
        source="Review source.",
    )
    with pytest.raises(MetadataBundleError, match="missing_table"):
        build_metadata_bundle(
            tmp_path,
            _manifest(),
            _profile(unknown),
            generated_at="2026-08-18T18:00:00+00:00",
        )


def test_profile_must_match_snapshot_identity(tmp_path: Path) -> None:
    _snapshot_files(tmp_path)

    with pytest.raises(MetadataBundleError, match="snapshot identity"):
        build_metadata_bundle(
            tmp_path,
            _manifest(),
            _profile(snapshot_id="sha256:" + "2" * 64),
            generated_at="2026-08-18T18:00:00+00:00",
        )


def test_profile_loader_rejects_symlink_and_sensitive_property(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(_profile().model_dump_json(), encoding="utf-8")
    linked = tmp_path / "linked.json"
    linked.symlink_to(profile_path)

    with pytest.raises(MetadataBundleError, match="ordinary JSON"):
        load_metadata_profile(linked)

    document = json.loads(profile_path.read_text(encoding="utf-8"))
    document["namespace"]["properties"]["access_token"] = "must-not-be-accepted"
    profile_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(MetadataBundleError, match="valid metadata profile"):
        load_metadata_profile(profile_path)


def test_bundle_loader_rejects_non_utf8_input(tmp_path: Path) -> None:
    path = tmp_path / "bundle.json"
    path.write_bytes(b"\xff\xfe")

    with pytest.raises(MetadataBundleError, match="valid metadata bundle"):
        load_metadata_bundle(path)


@pytest.mark.parametrize("duplicate", ["table", "column"])
def test_bundle_loader_rejects_duplicate_names(tmp_path: Path, duplicate: str) -> None:
    _snapshot_files(tmp_path)
    bundle = build_metadata_bundle(
        tmp_path,
        _manifest(),
        _profile(),
        generated_at="2026-08-18T18:00:00+00:00",
    )
    document = bundle.model_dump(mode="json")
    if duplicate == "table":
        document["tables"].append(document["tables"][0])
        expected = "duplicate table names"
    else:
        document["tables"][0]["columns"].append(document["tables"][0]["columns"][0])
        expected = "duplicate columns"
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MetadataBundleError, match=expected):
        load_metadata_bundle(path)


def test_render_and_atomic_write_use_the_same_canonical_json(tmp_path: Path) -> None:
    _snapshot_files(tmp_path)
    bundle = build_metadata_bundle(
        tmp_path,
        _manifest(),
        _profile(),
        generated_at="2026-08-18T18:00:00+00:00",
    )
    destination = tmp_path / "output" / "metadata-bundle.json"

    assert write_metadata_bundle(destination, bundle) == destination.resolve()
    assert destination.read_text(encoding="utf-8") == render_metadata_bundle(bundle) + "\n"
    assert not list(destination.parent.glob(".metadata-bundle.json.*.tmp"))

    linked = tmp_path / "linked-output.json"
    linked.symlink_to(destination)
    with pytest.raises(MetadataBundleError, match="ordinary file path"):
        write_metadata_bundle(linked, bundle)


def test_render_and_atomic_write_profile_use_the_same_json(tmp_path: Path) -> None:
    profile = _profile()
    destination = tmp_path / "output" / "metadata-profile.json"

    assert write_metadata_profile(destination, profile) == destination.resolve()
    assert destination.read_text(encoding="utf-8") == render_metadata_profile(profile) + "\n"

    blocked_parent = tmp_path / "blocked"
    blocked_parent.write_text("not a directory", encoding="utf-8")
    with pytest.raises(MetadataBundleError, match="Cannot write the metadata profile"):
        write_metadata_profile(blocked_parent / "profile.json", profile)


def test_metadata_schema_cli_emits_versioned_contracts() -> None:
    runner = CliRunner()
    profile = runner.invoke(cli, ["metadata-bundle-schema", "profile"])
    bundle = runner.invoke(cli, ["metadata-bundle-schema", "bundle"])

    assert profile.exit_code == 0, profile.output
    assert bundle.exit_code == 0, bundle.output
    assert json.loads(profile.output)["x-format-version"] == 1
    assert json.loads(bundle.output)["x-format-version"] == BUNDLE_FORMAT_VERSION
    assert metadata_json_schema("bundle")["title"] == "MetadataBundle"


def test_metadata_bundle_cli_prints_and_writes_the_same_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _snapshot_files(tmp_path)
    generated = build_metadata_bundle(
        tmp_path,
        _manifest(),
        _profile(),
        generated_at="2026-08-18T18:00:00+00:00",
    )
    monkeypatch.setattr("nmdc_lakehouse.metadata_bundle.generate_metadata_bundle", lambda *_args: generated)
    destination = tmp_path / "metadata-bundle.json"

    result = CliRunner().invoke(
        cli,
        [
            "metadata-bundle",
            str(tmp_path),
            "--profile",
            str(tmp_path / "profile.json"),
            "--output",
            str(destination),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == generated.model_dump(mode="json")
    assert destination.read_text(encoding="utf-8") == result.output


def test_metadata_profile_cli_prints_and_writes_the_same_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = _profile()
    monkeypatch.setattr("nmdc_lakehouse.metadata_bundle.generate_metadata_profile", lambda *_args, **_kwargs: profile)
    destination = tmp_path / "metadata-profile.json"

    result = CliRunner().invoke(
        cli,
        [
            "metadata-profile",
            str(tmp_path),
            "--profile-id",
            profile.profile_id,
            "--namespace-name",
            "nmdc_metadata",
            "--title",
            "NMDC metadata",
            "--description",
            "Flattened NMDC metadata tables.",
            "--property",
            "role=metadata",
            "--output",
            str(destination),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == profile.model_dump(mode="json")
    assert destination.read_text(encoding="utf-8") == result.output


def test_metadata_profile_cli_rejects_duplicate_properties(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "metadata-profile",
            str(tmp_path),
            "--profile-id",
            "profile-1",
            "--namespace-name",
            "nmdc_metadata",
            "--title",
            "NMDC metadata",
            "--description",
            "Flattened NMDC metadata tables.",
            "--property",
            "role=metadata",
            "--property",
            "role=duplicate",
        ],
    )

    assert result.exit_code != 0
    assert "unique KEY=VALUE" in result.output


def _bundle_fixture(tmp_path: Path) -> MetadataBundle:
    """A built bundle, so the version pairing is exercised on a real document."""
    _snapshot_files(tmp_path)
    return build_metadata_bundle(
        tmp_path,
        _manifest(),
        _profile(),
        generated_at="2026-08-18T18:00:00+00:00",
    )


def test_a_version_1_bundle_cannot_carry_flat_schema_versions(tmp_path: Path) -> None:
    """A format version that does not constrain its own fields is a label, not a contract."""
    from pydantic import ValidationError

    document = _bundle_fixture(tmp_path).model_dump()
    document["bundle_format_version"] = 1
    document["target_schema_versions"] = ["11.23.0+flat.1.0.0"]

    with pytest.raises(ValidationError, match="version 1 bundle cannot carry"):
        MetadataBundle.model_validate(document)


def test_a_version_1_bundle_with_no_versions_is_still_readable(tmp_path: Path) -> None:
    document = _bundle_fixture(tmp_path).model_dump()
    document["bundle_format_version"] = 1
    document["target_schema_versions"] = []

    assert MetadataBundle.model_validate(document).bundle_format_version == 1
