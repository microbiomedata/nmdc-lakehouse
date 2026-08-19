"""Tests for the offline NMDC-to-BERDL staging command plan."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.berdl_staging import (
    BerdlStagingPlanError,
    EvidenceDigest,
    build_berdl_staging_plan,
    plan_berdl_staging,
    write_berdl_staging_plan,
)
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.metadata_application import build_metadata_application_plan
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
    MetadataCapability,
    PublicationPolicy,
    build_publication_plan,
)
from nmdc_lakehouse.snapshot_manifest import (
    ArtifactRecord,
    PerformanceRecord,
    SnapshotManifest,
    SoftwareRecord,
)
from nmdc_lakehouse.target_validation import (
    TableValidationRecord,
    TargetValidationReport,
    packaged_target_schema_sha256,
)

REVISION = "a" * 40
SNAPSHOT_ID = "sha256:" + "1" * 64


def _manifest() -> SnapshotManifest:
    artifact = ArtifactRecord(
        path="biosample_set.parquet",
        table="biosample_set",
        rows=1,
        bytes=100,
        sha256="b" * 64,
        physical_schema_sha256="c" * 64,
        footer_schema_sha256="d" * 64,
        source_schema_id="https://w3id.org/nmdc/nmdc",
        source_schema_version="11.10.0",
        source_class="Biosample",
        target_schema_id="https://w3id.org/nmdc/nmdc-schema-flattened",
        target_class="Biosample",
        mapping="nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener",
    )
    return SnapshotManifest(
        manifest_format_version=1,
        snapshot_id=SNAPSHOT_ID,
        generated_at="2026-08-19T12:00:00+00:00",
        scope="full-mongodb-metadata-snapshot",
        source_label="nmdc-production",
        included_collections=["biosample_set"],
        skipped_collections=["functional_annotation_agg"],
        footer_metadata_format_version="1",
        target_schema_ids=[artifact.target_schema_id],
        mapping_ids=[artifact.mapping],
        software=SoftwareRecord(
            nmdc_lakehouse_version="0.2.0.dev0",
            nmdc_schema_version="11.10.0",
            python_version="3.13.13",
            git_commit=None,
            git_dirty=None,
        ),
        performance_record=PerformanceRecord(path="etl-metrics.json", sha256="e" * 64),
        artifacts=[artifact],
    )


def _bundle(manifest: SnapshotManifest) -> MetadataBundle:
    artifact = manifest.artifacts[0]
    table = TableMetadata(
        name=artifact.table,
        source_class=artifact.source_class,
        target_schema_id=artifact.target_schema_id,
        target_class=artifact.target_class,
        mapping_id=artifact.mapping,
        physical_schema_sha256=artifact.physical_schema_sha256,
        footer_schema_sha256=artifact.footer_schema_sha256,
        description=DescriptionRecord(value="Environmental sample metadata.", origin="footer"),
        columns=[
            ColumnMetadata(
                name="id",
                arrow_type="string",
                nullable=False,
                linkml_range="string",
                identifier=True,
                designates_type=False,
                description=DescriptionRecord(value="Stable identifier.", origin="footer"),
            )
        ],
    )
    return MetadataBundle(
        bundle_format_version=1,
        generated_at="2026-08-19T12:30:00+00:00",
        snapshot_id=manifest.snapshot_id,
        profile_id="nmdc-metadata-reviewed",
        source_schemas=[SchemaIdentity(schema_id=artifact.source_schema_id, version=artifact.source_schema_version)],
        target_schema_ids=manifest.target_schema_ids,
        mapping_ids=manifest.mapping_ids,
        namespace=NamespaceProfile(
            name="nmdc_metadata",
            title="NMDC metadata",
            description="Flattened NMDC metadata tables.",
        ),
        tables=[table],
    )


def _inventory() -> DestinationInventory:
    return DestinationInventory(
        inventory_format_version=1,
        destination_id="nmdc-production",
        observed_at="2026-08-19T12:15:00+00:00",
        provider="spark_catalog",
        table_format="delta",
        metadata_capabilities=[MetadataCapability.NAMESPACE, MetadataCapability.TABLE, MetadataCapability.COLUMN],
        tables=[DestinationTable(name="biosample_set", rows=1, physical_schema_sha256="c" * 64)],
    )


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "beril"
    (checkout / "scripts").mkdir(parents=True, exist_ok=True)
    (checkout / "scripts" / "ingest_dataset.py").write_text("# staging command\n", encoding="utf-8")
    (checkout / "scripts" / "ingest_lib.py").write_text("# ingest library\n", encoding="utf-8")
    python = checkout / ".venv-berdl" / "bin" / "python"
    windows_python = checkout / ".venv-berdl" / "Scripts" / "python.exe"
    if not windows_python.is_file():
        python.parent.mkdir(parents=True, exist_ok=True)
        python.write_text("test interpreter\n", encoding="utf-8")
        python.chmod(0o755)
    return checkout


def _target_validation(manifest: SnapshotManifest) -> TargetValidationReport:
    artifact = manifest.artifacts[0]
    table = TableValidationRecord(
        table=artifact.table,
        artifact_path=artifact.path,
        target_class=artifact.target_class,
        mode="full",
        selection_basis="target-identifier:id",
        eligible_rows=artifact.rows,
        selected_rows=artifact.rows,
        valid_rows=artifact.rows,
        invalid_rows=0,
        elapsed_seconds=0.1,
        issue_categories=[],
    )
    return TargetValidationReport(
        report_format_version=1,
        status="success",
        generated_at="2026-08-19T12:20:00+00:00",
        snapshot_id=manifest.snapshot_id,
        target_schema_id=artifact.target_schema_id,
        target_schema_sha256=packaged_target_schema_sha256(),
        target_schema_source_id=artifact.source_schema_id,
        target_schema_source_version=artifact.source_schema_version,
        target_schema_source_package_version=artifact.source_schema_version,
        linkml_version="1.11.1",
        requested_mode="bounded",
        full_table_max_rows=10_000,
        sample_rows=100,
        sampling_algorithm="sha256-target-identity-and-canonical-row-minhash-v1",
        elapsed_seconds=0.1,
        eligible_rows=artifact.rows,
        selected_rows=artifact.rows,
        valid_rows=artifact.rows,
        invalid_rows=0,
        tables=[table],
    )


class GitRunner:
    """Provide only the local Git observations used by the planner."""

    def __init__(self, *, revision: str = REVISION, dirty: str = "", tracked: bool = True) -> None:
        self.revision = revision
        self.dirty = dirty
        self.tracked = tracked
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, args):
        command = tuple(args)
        self.commands.append(command)
        if "rev-parse" in command:
            return subprocess.CompletedProcess(args, 0, self.revision + "\n", "")
        if "status" in command:
            return subprocess.CompletedProcess(args, 0, self.dirty, "")
        if "ls-files" in command:
            output = "scripts/ingest_dataset.py\nscripts/ingest_lib.py\n" if self.tracked else ""
            return subprocess.CompletedProcess(args, 0 if self.tracked else 1, output, "")
        raise AssertionError(f"Unexpected external command: {command}")


def _inputs(tmp_path: Path):
    manifest = _manifest()
    bundle = _bundle(manifest)
    inventory = _inventory()
    publication_plan = build_publication_plan(
        manifest,
        inventory,
        PublicationPolicy(policy_format_version=1, rules=[]),
    )
    metadata_plan = build_metadata_application_plan(
        bundle,
        inventory,
        "nmdc.nmdc_metadata_staging_20260819",
    )
    return (
        manifest,
        bundle,
        inventory,
        publication_plan,
        metadata_plan,
        _target_validation(manifest),
        _checkout(tmp_path),
    )


def _build(tmp_path: Path, **changes):
    manifest, bundle, inventory, publication_plan, metadata_plan, target_validation, checkout = _inputs(tmp_path)
    values = {
        "snapshot_root": tmp_path / "snapshot",
        "manifest": manifest,
        "bundle": bundle,
        "inventory": inventory,
        "publication_plan": publication_plan,
        "metadata_plan": metadata_plan,
        "target_validation": target_validation,
        "evidence": [
            EvidenceDigest(
                name="snapshot-manifest.json",
                path=str(tmp_path / "snapshot" / "snapshot-manifest.json"),
                sha256="f" * 64,
            )
        ],
        "beril_checkout": checkout,
        "beril_revision": REVISION,
        "tenant": "nmdc",
        "dataset": "nmdc_metadata_staging_20260819",
        "bucket": "cdm-lake",
        "bronze_prefix": "tenant-general-warehouse/nmdc/staging/20260819",
        "progress_key": "tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
        "config_key": "tenant-general-warehouse/nmdc/staging/20260819/config.json",
        "runner": GitRunner(),
    }
    values.update(changes)
    return build_berdl_staging_plan(**values)


def test_plan_binds_candidate_and_exact_plan_only_command(tmp_path: Path) -> None:
    runner = GitRunner()
    plan = _build(tmp_path, runner=runner)

    assert plan.status == "plan-only"
    assert plan.snapshot_id == SNAPSHOT_ID
    assert [artifact.table for artifact in plan.artifacts] == ["biosample_set"]
    assert plan.artifacts[0].sha256 == "b" * 64
    assert plan.target_validation.requested_mode == "bounded"
    assert plan.target_validation.selected_rows == 1
    assert plan.beril.revision == REVISION
    assert plan.command[-2:] == ["--config-key", plan.config_key]
    assert "--execute-staging" not in plan.command
    assert "--outcome" not in plan.command
    assert len(runner.commands) == 3
    assert all(command[0] == "git" for command in runner.commands)


def test_metadata_plan_must_match_snapshot_and_staging_namespace(tmp_path: Path) -> None:
    manifest, bundle, inventory, publication_plan, metadata_plan, target_validation, checkout = _inputs(tmp_path)
    metadata_plan.destination_observed_at = "2026-08-19T13:00:00+00:00"

    with pytest.raises(BerdlStagingPlanError, match="metadata application plan"):
        _build(
            tmp_path,
            manifest=manifest,
            bundle=bundle,
            inventory=inventory,
            publication_plan=publication_plan,
            metadata_plan=metadata_plan,
            target_validation=target_validation,
            beril_checkout=checkout,
        )


def test_metadata_plan_must_match_all_reviewed_operations(tmp_path: Path) -> None:
    manifest, bundle, inventory, publication_plan, metadata_plan, target_validation, checkout = _inputs(tmp_path)
    assert metadata_plan.supported_operations
    metadata_plan.supported_operations = []

    with pytest.raises(BerdlStagingPlanError, match="metadata application plan"):
        _build(
            tmp_path,
            manifest=manifest,
            bundle=bundle,
            inventory=inventory,
            publication_plan=publication_plan,
            metadata_plan=metadata_plan,
            target_validation=target_validation,
            beril_checkout=checkout,
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"dataset": "nmdc_metadata"}, "unique"),
        ({"dataset": "nmdc_metadata_staging"}, "unique"),
        ({"bronze_prefix": "tenant-general-warehouse/nmdc/canonical/20260819"}, "staging area"),
        ({"progress_key": "elsewhere/progress.jsonl"}, "children"),
        ({"config_key": "tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl"}, "distinct"),
    ],
)
def test_unsafe_or_canonical_destinations_are_rejected(tmp_path: Path, changes, message: str) -> None:
    with pytest.raises(BerdlStagingPlanError, match=message):
        _build(tmp_path, **changes)


def test_checkout_revision_and_cleanliness_are_required(tmp_path: Path) -> None:
    with pytest.raises(BerdlStagingPlanError, match="requested revision"):
        _build(tmp_path, runner=GitRunner(revision="b" * 40))
    with pytest.raises(BerdlStagingPlanError, match="tracked changes"):
        _build(tmp_path, runner=GitRunner(dirty=" M scripts/ingest_lib.py\n"))
    with pytest.raises(BerdlStagingPlanError, match="sources must be tracked"):
        _build(tmp_path, runner=GitRunner(tracked=False))


def test_windows_beril_interpreter_layout_is_supported(tmp_path: Path) -> None:
    _manifest_value, _bundle_value, _inventory_value, _publication, _metadata, _target, checkout = _inputs(tmp_path)
    (checkout / ".venv-berdl" / "bin" / "python").unlink()
    python = checkout / ".venv-berdl" / "Scripts" / "python.exe"
    python.parent.mkdir(parents=True)
    python.write_text("test interpreter\n", encoding="utf-8")
    python.chmod(0o755)

    plan = _build(tmp_path, beril_checkout=checkout)

    assert plan.command[0] == str(python)


def test_broken_symlinked_beril_interpreter_is_rejected(tmp_path: Path) -> None:
    _manifest_value, _bundle_value, _inventory_value, _publication, _metadata, _target, checkout = _inputs(tmp_path)
    python = checkout / ".venv-berdl" / "bin" / "python"
    python.unlink()
    python.symlink_to(tmp_path / "unreviewed-python")
    windows_python = checkout / ".venv-berdl" / "Scripts" / "python.exe"
    windows_python.parent.mkdir(parents=True)
    windows_python.write_text("not executable\n", encoding="utf-8")

    with pytest.raises(BerdlStagingPlanError, match="no .venv-berdl Python interpreter"):
        _build(tmp_path, beril_checkout=checkout)


def test_symlinked_beril_interpreter_with_executable_target_is_supported(tmp_path: Path) -> None:
    _manifest_value, _bundle_value, _inventory_value, _publication, _metadata, _target, checkout = _inputs(tmp_path)
    python = checkout / ".venv-berdl" / "bin" / "python"
    target = tmp_path / "python3.13"
    target.write_text("test interpreter\n", encoding="utf-8")
    target.chmod(0o755)
    python.unlink()
    python.symlink_to(target)

    plan = _build(tmp_path, beril_checkout=checkout)

    assert plan.command[0] == str(python)


def test_failed_or_incomplete_target_validation_is_rejected(tmp_path: Path) -> None:
    target_validation = _target_validation(_manifest())
    target_validation.status = "failure"
    target_validation.invalid_rows = 1
    with pytest.raises(BerdlStagingPlanError, match="not successful"):
        _build(tmp_path, target_validation=target_validation)

    target_validation = _target_validation(_manifest())
    target_validation.tables = []
    with pytest.raises(BerdlStagingPlanError, match="table coverage"):
        _build(tmp_path, target_validation=target_validation)

    target_validation = _target_validation(_manifest())
    target_validation.tables[0].selected_rows = 0
    target_validation.tables[0].valid_rows = 0
    target_validation.selected_rows = 0
    target_validation.valid_rows = 0
    with pytest.raises(BerdlStagingPlanError, match="does not match table"):
        _build(tmp_path, target_validation=target_validation)

    target_validation = _target_validation(_manifest())
    target_validation.target_schema_sha256 = "0" * 64
    with pytest.raises(BerdlStagingPlanError, match="packaged target schema"):
        _build(tmp_path, target_validation=target_validation)


def test_plan_output_is_immutable(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    output = tmp_path / "staging-plan.json"

    assert write_berdl_staging_plan(output, plan) == output.resolve()
    original = output.read_text(encoding="utf-8")
    with pytest.raises(BerdlStagingPlanError, match="Refusing to replace"):
        write_berdl_staging_plan(output, plan)
    assert output.read_text(encoding="utf-8") == original


def test_loaded_plan_hashes_every_reviewed_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, bundle, inventory, publication_plan, metadata_plan, target_validation, checkout = _inputs(tmp_path)
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    paths = {
        "manifest": snapshot / "snapshot-manifest.json",
        "bundle": tmp_path / "bundle.json",
        "inventory": tmp_path / "inventory.json",
        "publication": tmp_path / "publication.json",
        "metadata": tmp_path / "metadata.json",
        "target": tmp_path / "target-validation.json",
    }
    for name, path in paths.items():
        path.write_text(name + "\n", encoding="utf-8")
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.validate_snapshot", lambda _path: manifest)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_metadata_bundle", lambda _path: bundle)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_destination_inventory", lambda _path: inventory)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_publication_plan", lambda _path: publication_plan)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_metadata_application_plan", lambda _path: metadata_plan)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_target_validation_report", lambda _path: target_validation)

    plan = plan_berdl_staging(
        snapshot,
        bundle_path=paths["bundle"],
        inventory_path=paths["inventory"],
        publication_plan_path=paths["publication"],
        metadata_plan_path=paths["metadata"],
        target_validation_path=paths["target"],
        beril_checkout=checkout,
        beril_revision=REVISION,
        tenant="nmdc",
        dataset="nmdc_metadata_staging_20260819",
        bucket="cdm-lake",
        bronze_prefix="tenant-general-warehouse/nmdc/staging/20260819",
        progress_key="tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
        config_key="tenant-general-warehouse/nmdc/staging/20260819/config.json",
        runner=GitRunner(),
    )

    assert [item.name for item in plan.evidence] == [
        "snapshot-manifest.json",
        "metadata-bundle.json",
        "destination-inventory.json",
        "publication-plan.json",
        "metadata-application-plan.json",
        "target-validation-report.json",
    ]
    assert [item.path for item in plan.evidence] == [str(path.resolve()) for path in paths.values()]
    assert len({item.sha256 for item in plan.evidence}) == 6

    def mutate_after_loading(path: Path):
        path.write_text("changed while loading\n", encoding="utf-8")
        return bundle

    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_metadata_bundle", mutate_after_loading)
    with pytest.raises(BerdlStagingPlanError, match="changed while"):
        plan_berdl_staging(
            snapshot,
            bundle_path=paths["bundle"],
            inventory_path=paths["inventory"],
            publication_plan_path=paths["publication"],
            metadata_plan_path=paths["metadata"],
            target_validation_path=paths["target"],
            beril_checkout=checkout,
            beril_revision=REVISION,
            tenant="nmdc",
            dataset="nmdc_metadata_staging_20260819",
            bucket="cdm-lake",
            bronze_prefix="tenant-general-warehouse/nmdc/staging/20260819",
            progress_key="tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
            config_key="tenant-general-warehouse/nmdc/staging/20260819/config.json",
            runner=GitRunner(),
        )


def test_cli_writes_the_same_plan_it_prints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _build(tmp_path)
    output = tmp_path / "cli-plan.json"
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.plan_berdl_staging", lambda *_args, **_kwargs: plan)

    result = CliRunner().invoke(
        cli,
        [
            "berdl-upload-plan",
            "snapshot",
            "--bundle",
            "bundle.json",
            "--inventory",
            "inventory.json",
            "--plan",
            "publication.json",
            "--metadata-plan",
            "metadata.json",
            "--target-validation",
            "target-validation.json",
            "--beril-checkout",
            "beril",
            "--beril-revision",
            REVISION,
            "--tenant",
            "nmdc",
            "--dataset",
            "nmdc_metadata_staging_20260819",
            "--bucket",
            "cdm-lake",
            "--bronze-prefix",
            "tenant-general-warehouse/nmdc/staging/20260819",
            "--progress-key",
            "tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
            "--config-key",
            "tenant-general-warehouse/nmdc/staging/20260819/config.json",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == plan.model_dump(mode="json")
    assert json.loads(output.read_text(encoding="utf-8")) == plan.model_dump(mode="json")
    assert f"plan={output.resolve()}" in result.stderr
