"""Tests for logical target validation of manifested Parquet rows."""

import json
from importlib import resources
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from nmdc_lakehouse_schema.transforms.schema_generator import DEFAULT_FLATTENED_SCHEMA_ID

from nmdc_lakehouse.producer_identity import flattener_mapping_id
from nmdc_lakehouse.snapshot_manifest import (
    ArtifactRecord,
    PerformanceRecord,
    SnapshotManifest,
    SnapshotManifestError,
    SoftwareRecord,
)
from nmdc_lakehouse.target_validation import (
    TargetValidationError,
    _sample_rows,
    assert_source_schema_aligned,
    build_target_validation_report,
    load_target_validation_report,
    validate_target_snapshot,
    write_target_validation_report,
)

# The flattened target schema is consumed from the nmdc-lakehouse-schema package (#4).
PUBLISHED_SCHEMA = Path(str(resources.files("nmdc_lakehouse_schema").joinpath("schema/nmdc_schema_flattened.yaml")))

# Producer identity is recorded (not validated) provenance: package==version (#333). Primary and
# side tables share one flattener identity; these are just footer values the test artifacts carry.
PRIMARY_MAPPING_ID = flattener_mapping_id()
SIDE_TABLE_MAPPING_ID = flattener_mapping_id()


def _artifact(path: Path, *, target_class: str, source_class: str, mapping: str) -> ArtifactRecord:
    parquet = pq.ParquetFile(path)
    return ArtifactRecord(
        path=path.name,
        table=path.stem,
        rows=parquet.metadata.num_rows,
        bytes=path.stat().st_size,
        sha256="0" * 64,
        physical_schema_sha256="1" * 64,
        footer_schema_sha256="2" * 64,
        source_schema_id="https://w3id.org/nmdc/nmdc",
        source_schema_version="11.24.0",
        source_class=source_class,
        target_schema_id=DEFAULT_FLATTENED_SCHEMA_ID,
        target_class=target_class,
        mapping=mapping,
    )


def _manifest(artifacts: list[ArtifactRecord]) -> SnapshotManifest:
    return SnapshotManifest(
        manifest_format_version=1,
        snapshot_id="sha256:" + "a" * 64,
        generated_at="2026-08-18T00:00:00+00:00",
        scope="full-mongodb-metadata-snapshot",
        source_label="test",
        included_collections=["study_set"],
        skipped_collections=[],
        footer_metadata_format_version="1",
        target_schema_ids=[DEFAULT_FLATTENED_SCHEMA_ID],
        mapping_ids=sorted({artifact.mapping for artifact in artifacts}),
        software=SoftwareRecord(
            nmdc_lakehouse_version=version("nmdc-lakehouse"),
            git_commit=None,
            git_dirty=None,
            nmdc_schema_version=version("nmdc-schema"),
            python_version="3.13.0",
        ),
        performance_record=PerformanceRecord(path="metrics.json", sha256="3" * 64),
        artifacts=artifacts,
    )


def test_primary_and_side_table_semantic_errors_are_sanitized(tmp_path: Path) -> None:
    primary = tmp_path / "study_set.parquet"
    side = tmp_path / "study_set_associated_dois.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"},
                {"id": "nmdc:sty-2", "study_category": "private-invalid-category", "type": "nmdc:Study"},
            ]
        ),
        primary,
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "doi_category": "private-invalid-doi-category",
                    "doi_value": "doi:10.1/example",
                    "type": "nmdc:Doi",
                    "parent_id": "nmdc:sty-1",
                }
            ]
        ),
        side,
    )
    artifacts = [
        _artifact(primary, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID),
        _artifact(
            side,
            target_class="study_set_associated_dois",
            source_class="Study",
            mapping=SIDE_TABLE_MAPPING_ID,
        ),
    ]

    report = build_target_validation_report(
        tmp_path,
        _manifest(artifacts),
        PUBLISHED_SCHEMA,
        requested_mode="full",
        generated_at="2026-08-18T00:00:00+00:00",
    )

    assert report.status == "failure"
    assert report.eligible_rows == report.selected_rows == 3
    assert report.invalid_rows == 2
    categories = {(issue.rule, issue.path) for table in report.tables for issue in table.issue_categories}
    assert ("enum", "/study_category") in categories
    assert ("enum", "/doi_category") in categories
    serialized = report.model_dump_json()
    assert "private-invalid-category" not in serialized
    assert "private-invalid-doi-category" not in serialized


def test_bounded_selection_is_independent_of_parquet_row_order(tmp_path: Path) -> None:
    rows = [
        {"id": f"nmdc:sty-{index}", "study_category": "research_study", "type": "nmdc:Study"} for index in range(10)
    ]
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    pq.write_table(pa.Table.from_pylist(rows), first)
    pq.write_table(pa.Table.from_pylist(list(reversed(rows))), second)

    selected_first = _sample_rows(pq.ParquetFile(first), target_class="StudyFlat", identifier="id", sample_rows=3)
    selected_second = _sample_rows(pq.ParquetFile(second), target_class="StudyFlat", identifier="id", sample_rows=3)

    assert [row["id"] for row in selected_first] == [row["id"] for row in selected_second]


def test_bounded_report_distinguishes_sampling_from_full_validation(tmp_path: Path) -> None:
    path = tmp_path / "study_set.parquet"
    rows = [
        {"id": f"nmdc:sty-{index}", "study_category": "research_study", "type": "nmdc:Study"} for index in range(10)
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)
    artifact = _artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)

    report = build_target_validation_report(
        tmp_path,
        _manifest([artifact]),
        PUBLISHED_SCHEMA,
        full_table_max_rows=2,
        sample_rows=3,
    )

    assert report.status == "success"
    assert report.requested_mode == "bounded"
    assert report.tables[0].mode == "sampled"
    assert report.tables[0].eligible_rows == 10
    assert report.tables[0].selected_rows == 3

    report_path = tmp_path / "target-validation.json"
    report_path.write_text(report.model_dump_json(), encoding="utf-8")
    assert load_target_validation_report(report_path) == report


def test_target_validation_loader_rejects_symlinks_and_invalid_json(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.json"
    invalid.write_text("not JSON\n", encoding="utf-8")
    linked = tmp_path / "linked.json"
    linked.symlink_to(invalid)

    with pytest.raises(TargetValidationError, match="valid target validation"):
        load_target_validation_report(invalid)
    with pytest.raises(TargetValidationError, match="ordinary JSON"):
        load_target_validation_report(linked)


def test_target_validation_loader_requires_current_strict_format(tmp_path: Path) -> None:
    path = tmp_path / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        path,
    )
    report = build_target_validation_report(
        tmp_path,
        _manifest([_artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)]),
        PUBLISHED_SCHEMA,
    )
    document = report.model_dump(mode="json")
    report_path = tmp_path / "report.json"

    document["report_format_version"] = 99
    report_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(TargetValidationError, match="Unsupported.*format version"):
        load_target_validation_report(report_path)

    document["report_format_version"] = 1
    document["full_table_max_rows"] = "10000"
    report_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(TargetValidationError, match="valid target validation"):
        load_target_validation_report(report_path)

    document["full_table_max_rows"] = 10_000
    document["elapsed_seconds"] = float("inf")
    report_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(TargetValidationError, match="valid target validation"):
        load_target_validation_report(report_path)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_numbers_are_sanitized_failures_in_full_and_sampled_modes(
    tmp_path: Path,
    value: float,
) -> None:
    path = tmp_path / "biosample_set.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "id": "nmdc:bsm-1",
                    "type": "nmdc:Biosample",
                    "abs_air_humidity_has_numeric_value": value,
                }
            ]
        ),
        path,
    )
    artifact = _artifact(path, target_class="BiosampleFlat", source_class="Biosample", mapping=PRIMARY_MAPPING_ID)
    manifest = _manifest([artifact])

    full = build_target_validation_report(tmp_path, manifest, PUBLISHED_SCHEMA, requested_mode="full")
    sampled = build_target_validation_report(
        tmp_path,
        manifest,
        PUBLISHED_SCHEMA,
        full_table_max_rows=0,
        sample_rows=1,
    )

    assert full.tables[0].issue_categories == sampled.tables[0].issue_categories
    for report in (full, sampled):
        assert report.status == "failure"
        assert report.invalid_rows == 1
        categories = {
            (issue.severity, issue.rule, issue.path, issue.count) for issue in report.tables[0].issue_categories
        }
        assert ("ERROR", "finite-number", "/abs_air_humidity_has_numeric_value", 1) in categories
        assert "nan" not in report.model_dump_json().lower()
        assert "infinity" not in report.model_dump_json().lower()


def test_schema_and_class_contract_mismatches_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        path,
    )
    artifact = _artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)
    manifest = _manifest([artifact])
    manifest.target_schema_ids = ["https://example.org/wrong"]
    with pytest.raises(TargetValidationError, match="identities do not match"):
        build_target_validation_report(tmp_path, manifest, PUBLISHED_SCHEMA)


@pytest.mark.parametrize(
    ("footer_version", "target_version"),
    [("1", ""), ("2", "11.24.0+flat.1.3.0")],
)
def test_matching_projection_and_legacy_v1_remain_valid(
    tmp_path: Path, footer_version: str, target_version: str
) -> None:
    path = tmp_path / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        path,
    )
    artifact = _artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)
    artifact.footer_metadata_format_version = footer_version
    artifact.target_schema_version = target_version
    manifest = _manifest([artifact])
    manifest.manifest_format_version = 2
    manifest.footer_metadata_format_version = footer_version
    manifest.target_schema_versions = [target_version] if target_version else []

    assert build_target_validation_report(tmp_path, manifest, PUBLISHED_SCHEMA).status == "success"


@pytest.mark.parametrize(
    ("versions", "aggregate", "message"),
    [
        (["11.24.0+flat.1.1.0"], ["11.24.0+flat.1.1.0"], "versions do not match the published"),
        (
            ["11.24.0+flat.1.3.0", "11.24.0+flat.1.1.0"],
            ["11.24.0+flat.1.1.0", "11.24.0+flat.1.3.0"],
            "versions do not match the published",
        ),
        (["11.24.0+flat.1.3.0"], ["11.24.0+flat.1.1.0"], "versions do not match the manifested"),
        ([""], [], "Version 2 artifacts must declare"),
    ],
)
def test_wrong_projection_versions_fail_before_row_validation(
    tmp_path: Path, monkeypatch, versions: list[str], aggregate: list[str], message: str
) -> None:
    path = tmp_path / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        path,
    )
    primary = _artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)
    artifacts = [primary]
    if len(versions) == 2:
        side = tmp_path / "study_set_associated_dois.parquet"
        pq.write_table(
            pa.Table.from_pylist([{"parent_id": "nmdc:sty-1", "doi_value": "doi:10.1/example", "type": "nmdc:Doi"}]),
            side,
        )
        artifacts.append(
            _artifact(
                side, target_class="study_set_associated_dois", source_class="Study", mapping=SIDE_TABLE_MAPPING_ID
            )
        )
    for artifact, target_version in zip(artifacts, versions, strict=True):
        artifact.footer_metadata_format_version = "2"
        artifact.target_schema_version = target_version
    manifest = _manifest(artifacts)
    manifest.manifest_format_version = 2
    manifest.footer_metadata_format_version = "2"
    manifest.target_schema_versions = aggregate

    def reject_row_validation(*args, **kwargs):
        pytest.fail("Projection mismatch must be rejected before reading rows")

    monkeypatch.setattr("nmdc_lakehouse.target_validation._validate_table", reject_row_validation)
    with pytest.raises(TargetValidationError, match=message):
        build_target_validation_report(tmp_path, manifest, PUBLISHED_SCHEMA)


@pytest.mark.parametrize(
    ("aggregate", "value", "message"),
    [
        ("target_schema_ids", ["https://example.org/wrong"], "target schema identities"),
        ("mapping_ids", ["https://example.org/wrong"], "mapping identities"),
    ],
)
def test_aggregate_identities_must_match_artifact_identities(
    tmp_path: Path,
    aggregate: str,
    value: list[str],
    message: str,
) -> None:
    path = tmp_path / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        path,
    )
    manifest = _manifest([_artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)])
    setattr(manifest, aggregate, value)

    with pytest.raises(TargetValidationError, match=message):
        build_target_validation_report(tmp_path, manifest, PUBLISHED_SCHEMA)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source_schema_id", "https://example.org/wrong", "source schema identities"),
        ("source_schema_version", "0.0.0", "source schema versions"),
    ],
)
def test_artifact_source_schema_must_match_published_target_annotations(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    path = tmp_path / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        path,
    )
    artifact = _artifact(path, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)
    setattr(artifact, field, value)

    with pytest.raises(TargetValidationError, match=message):
        build_target_validation_report(tmp_path, _manifest([artifact]), PUBLISHED_SCHEMA)


def test_snapshot_root_symlink_is_rejected_before_resolution(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    symlink = tmp_path / "snapshot-link"
    symlink.symlink_to(snapshot, target_is_directory=True)

    with pytest.raises(SnapshotManifestError, match="ordinary directory"):
        validate_target_snapshot(symlink)


def test_report_writer_preserves_the_snapshot_and_refuses_replacement(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    evidence = tmp_path / "evidence"
    snapshot.mkdir()
    evidence.mkdir()
    parquet = snapshot / "study_set.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"id": "nmdc:sty-1", "study_category": "research_study", "type": "nmdc:Study"}]),
        parquet,
    )
    report = build_target_validation_report(
        snapshot,
        _manifest([_artifact(parquet, target_class="StudyFlat", source_class="Study", mapping=PRIMARY_MAPPING_ID)]),
        PUBLISHED_SCHEMA,
        requested_mode="full",
    )

    with pytest.raises(TargetValidationError, match="outside the immutable snapshot"):
        write_target_validation_report(snapshot / "validation.json", report, snapshot_root=snapshot)

    destination = write_target_validation_report(evidence / "validation.json", report, snapshot_root=snapshot)
    assert destination.is_file()
    with pytest.raises(TargetValidationError, match="Refusing to replace"):
        write_target_validation_report(destination, report, snapshot_root=snapshot)


def test_source_schema_alignment_guard(monkeypatch) -> None:
    """The guard passes when installed nmdc-schema matches the flat artifact, and fails on drift."""
    from nmdc_lakehouse import target_validation as tv

    assert_source_schema_aligned()  # installed nmdc-schema matches the packaged flat schema

    monkeypatch.setattr(tv, "version", lambda package: "0.0.0")
    with pytest.raises(TargetValidationError, match="Unsupported NMDC source package version"):
        assert_source_schema_aligned()

    # Also reject an artifact that declares a different source than its selection key.
    monkeypatch.setattr(tv, "_published_target_schema_resource", lambda: PUBLISHED_SCHEMA)
    with pytest.raises(TargetValidationError, match="does not match the flattened target schema"):
        assert_source_schema_aligned()
