"""Tests for the offline NMDC-to-BERDL staging command plan."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from nmdc_lakehouse import berdl_staging
from nmdc_lakehouse.berdl_staging import (
    BerdlStagingPlanError,
    EvidenceDigest,
    UpstreamStagingOutcome,
    build_berdl_staging_outcome,
    build_berdl_staging_plan,
    execute_berdl_staging,
    plan_berdl_staging,
    revalidate_berdl_staging_plan,
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
    IssueCategory,
    TableValidationRecord,
    TargetValidationReport,
    packaged_target_schema_sha256,
)

REVISION = "a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3"
SNAPSHOT_ID = "sha256:" + "1" * 64
_INGEST_SOURCE_PATHS = (
    "src/data_lakehouse_ingest/__init__.py",
    "src/data_lakehouse_ingest/config_loader.py",
    "src/data_lakehouse_ingest/core.py",
)


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
        target_class="BiosampleFlat",
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
        table_format="iceberg",
        metadata_capabilities=[MetadataCapability.NAMESPACE, MetadataCapability.TABLE, MetadataCapability.COLUMN],
        tables=[DestinationTable(name="biosample_set", rows=1, physical_schema_sha256="c" * 64)],
    )


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "data-lakehouse-ingest"
    package = checkout / "src" / "data_lakehouse_ingest"
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("from .core import ingest\n", encoding="utf-8")
    (package / "config_loader.py").write_text("class ConfigLoader: ...\n", encoding="utf-8")
    (package / "core.py").write_text("def ingest(config): ...\n", encoding="utf-8")
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

    def __init__(
        self,
        *,
        revision: str = REVISION,
        remote_url: str = "https://github.com/kbase/data-lakehouse-ingest.git",
        dirty: str = "",
        tracked: bool = True,
        index_flags: str = "".join(f"H {path}\n" for path in _INGEST_SOURCE_PATHS),
        source_matches: bool = True,
        final_source_matches: bool | None = None,
        final_revision: str | None = None,
        final_dirty: str | None = None,
    ) -> None:
        self.revision = revision
        self.remote_url = remote_url
        self.dirty = dirty
        self.tracked = tracked
        self.index_flags = index_flags
        self.source_matches = source_matches
        self.final_source_matches = final_source_matches
        self.final_revision = final_revision
        self.final_dirty = final_dirty
        self.revision_calls = 0
        self.status_calls = 0
        self.source_round = 0
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, args):
        command = tuple(args)
        self.commands.append(command)
        if "rev-parse" in command and command[-1] == "HEAD":
            self.revision_calls += 1
            revision = self.final_revision if self.revision_calls > 1 and self.final_revision else self.revision
            return subprocess.CompletedProcess(args, 0, revision + "\n", "")
        if "status" in command:
            self.status_calls += 1
            dirty = self.final_dirty if self.status_calls > 1 and self.final_dirty is not None else self.dirty
            return subprocess.CompletedProcess(args, 0, dirty, "")
        if "remote" in command and "get-url" in command:
            return subprocess.CompletedProcess(args, 0, self.remote_url + "\n", "")
        if "ls-files" in command:
            if "-v" in command:
                self.source_round += 1
                return subprocess.CompletedProcess(args, 0, self.index_flags, "")
            output = "\n".join(_INGEST_SOURCE_PATHS) + "\n" if self.tracked else ""
            return subprocess.CompletedProcess(args, 0 if self.tracked else 1, output, "")
        if "rev-parse" in command:
            return subprocess.CompletedProcess(args, 0, "1" * 40 + "\n", "")
        if "hash-object" in command:
            matches = self.source_matches
            if self.source_round > 1 and self.final_source_matches is not None:
                matches = self.final_source_matches
            return subprocess.CompletedProcess(args, 0, ("1" if matches else "2") * 40 + "\n", "")
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
        "ingest_checkout": checkout,
        "ingest_revision": REVISION,
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


def _persisted_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
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
    plan_path = tmp_path / "staging-plan.json"
    write_berdl_staging_plan(plan_path, plan)
    return plan, plan_path, paths, checkout


def _upstream_outcome(plan, **changes) -> UpstreamStagingOutcome:
    document = {
        "schema_version": "1.0.0",
        "status": "verified",
        "started_at": "2026-08-19T17:00:00+00:00",
        "finished_at": "2026-08-19T17:02:00+00:00",
        "destination": {
            "bucket": plan.bucket,
            "bronze_prefix": plan.bronze_prefix,
            "namespace": plan.staging_namespace,
            "mode": "overwrite",
        },
        "verification": {
            "verified": True,
            "namespace": plan.staging_namespace,
            "tables": [
                {
                    "table": "biosample_set",
                    "status": "verified",
                    "source_rows": 1,
                    "destination_rows": 1,
                    "source_basis": "source parquet",
                }
            ],
        },
    }
    document.update(changes)
    return UpstreamStagingOutcome.model_validate(document)


def test_plan_binds_candidate_and_exact_plan_only_command(tmp_path: Path) -> None:
    runner = GitRunner()
    plan = _build(tmp_path, runner=runner)

    assert plan.status == "plan-only"
    assert plan.snapshot_id == SNAPSHOT_ID
    assert [artifact.table for artifact in plan.artifacts] == ["biosample_set"]
    assert plan.artifacts[0].sha256 == "b" * 64
    assert plan.target_validation.requested_mode == "bounded"
    assert plan.target_validation.selected_rows == 1
    assert plan.ingest.revision == REVISION
    assert plan.ingest.repository == "https://github.com/kbase/data-lakehouse-ingest"
    assert plan.ingest.checkout == str((tmp_path / "data-lakehouse-ingest").resolve())
    assert plan.ingest.checkout_remote == "https://github.com/kbase/data-lakehouse-ingest.git"
    assert plan.ingest.package_tree_git_oid == "1" * 40
    assert plan.destination_provider == "spark_catalog"
    assert plan.destination_table_format == "iceberg"
    assert "--destination-provider" in plan.command
    assert "--destination-table-format" in plan.command
    assert plan.command[-2:] == ["--config-key", plan.config_key]
    assert "--execute-staging" not in plan.command
    assert "--outcome" not in plan.command
    assert all(command[0] == "git" for command in runner.commands)
    assert any("hash-object" in command and command[-1].endswith("config_loader.py") for command in runner.commands)


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
            ingest_checkout=checkout,
        )


@pytest.mark.parametrize(("provider", "table_format"), [(None, "iceberg"), ("spark_catalog", None), ("trino", "delta")])
def test_staging_requires_reviewed_supported_destination_contract(
    tmp_path: Path, provider: str | None, table_format: str | None
) -> None:
    inventory = _inventory().model_copy(update={"provider": provider, "table_format": table_format})
    publication_plan = build_publication_plan(_manifest(), inventory, PublicationPolicy(policy_format_version=1))

    with pytest.raises(BerdlStagingPlanError, match="spark_catalog destination using the Iceberg"):
        _build(tmp_path, inventory=inventory, publication_plan=publication_plan)


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
            ingest_checkout=checkout,
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"dataset": "nmdc_metadata"}, "unique"),
        ({"dataset": "nmdc_metadata_staging"}, "unique"),
        ({"bucket": "a..b"}, "bucket"),
        ({"bucket": "a.-b"}, "bucket"),
        ({"bucket": "192.168.1.1"}, "bucket"),
        ({"bucket": "xn--reserved"}, "bucket"),
        ({"bucket": "reserved--x-s3"}, "bucket"),
        ({"bronze_prefix": "tenant-general-warehouse/nmdc/canonical/20260819"}, "staging area"),
        ({"progress_key": "elsewhere/progress.jsonl"}, "children"),
        ({"config_key": "tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl"}, "distinct"),
        (
            {"progress_key": "tenant-general-warehouse/nmdc/staging/20260819/biosample_set.parquet"},
            "artifact keys",
        ),
    ],
)
def test_unsafe_or_canonical_destinations_are_rejected(tmp_path: Path, changes, message: str) -> None:
    with pytest.raises(BerdlStagingPlanError, match=message):
        _build(tmp_path, **changes)


def test_checkout_revision_and_cleanliness_are_required(tmp_path: Path) -> None:
    with pytest.raises(BerdlStagingPlanError, match="requested revision"):
        _build(tmp_path, runner=GitRunner(revision="b" * 40))
    with pytest.raises(BerdlStagingPlanError, match="tracked or untracked changes"):
        _build(tmp_path, runner=GitRunner(dirty=" M src/data_lakehouse_ingest/core.py\n"))
    with pytest.raises(BerdlStagingPlanError, match="tracked or untracked changes"):
        _build(tmp_path, runner=GitRunner(dirty="?? scripts/csv.py\n"))
    with pytest.raises(BerdlStagingPlanError, match="package must be tracked"):
        _build(tmp_path, runner=GitRunner(tracked=False))
    with pytest.raises(BerdlStagingPlanError, match="special Git index flags"):
        _build(
            tmp_path,
            runner=GitRunner(
                index_flags="S src/data_lakehouse_ingest/__init__.py\nh src/data_lakehouse_ingest/core.py\n"
            ),
        )
    with pytest.raises(BerdlStagingPlanError, match="package bytes do not match"):
        _build(tmp_path, runner=GitRunner(source_matches=False))
    with pytest.raises(BerdlStagingPlanError, match="official GitHub repository"):
        _build(tmp_path, runner=GitRunner(remote_url="https://github.com/example/data-lakehouse-ingest.git"))
    with pytest.raises(BerdlStagingPlanError, match="changed while"):
        _build(tmp_path, runner=GitRunner(final_revision="b" * 40))
    with pytest.raises(BerdlStagingPlanError, match="changed while"):
        _build(tmp_path, runner=GitRunner(final_dirty=" M src/data_lakehouse_ingest/core.py\n"))
    with pytest.raises(BerdlStagingPlanError, match="package bytes do not match"):
        _build(tmp_path, runner=GitRunner(final_source_matches=False))


def test_ingest_source_digest_is_rechecked_after_git_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_sha256 = berdl_staging._sha256
    adapter = Path(berdl_staging.__file__).with_name("berdl_adapter.py")
    adapter_hashes = iter(("1" * 64, "2" * 64))

    def unstable_sha256(path: Path, label: str) -> str:
        if path == adapter:
            return next(adapter_hashes)
        return original_sha256(path, label)

    monkeypatch.setattr(berdl_staging, "_sha256", unstable_sha256)

    with pytest.raises(BerdlStagingPlanError, match="changed while being hashed"):
        _build(tmp_path)


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
    target_validation.tables[0].selection_basis = "canonical-row"
    with pytest.raises(BerdlStagingPlanError, match="does not match table"):
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

    target_validation = _target_validation(_manifest())
    target_validation.tables[0].issue_categories = [
        IssueCategory(severity="ERROR", rule="required", path="/id", count=1)
    ]
    with pytest.raises(BerdlStagingPlanError, match="not successful"):
        _build(tmp_path, target_validation=target_validation)


def test_plan_output_is_immutable(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    output = tmp_path / "staging-plan.json"

    assert write_berdl_staging_plan(output, plan) == output.resolve()
    original = output.read_text(encoding="utf-8")
    with pytest.raises(BerdlStagingPlanError, match="Refusing to replace"):
        write_berdl_staging_plan(output, plan)
    assert output.read_text(encoding="utf-8") == original


def test_plan_output_cannot_be_written_inside_snapshot(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()

    with pytest.raises(BerdlStagingPlanError, match="outside the immutable snapshot"):
        write_berdl_staging_plan(snapshot / "staging-plan.json", plan)


def test_plan_output_cannot_be_written_inside_ingest_checkout(tmp_path: Path) -> None:
    plan = _build(tmp_path)

    with pytest.raises(BerdlStagingPlanError, match="outside the KBase ingest checkout"):
        write_berdl_staging_plan(Path(plan.ingest.checkout) / "staging-plan.json", plan)


def test_plan_rejects_unapproved_ingest_revision(tmp_path: Path) -> None:
    with pytest.raises(BerdlStagingPlanError, match="approved Iceberg-compatible stock release"):
        _build(tmp_path, ingest_revision="a" * 40)


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
        ingest_checkout=checkout,
        ingest_revision=REVISION,
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
            ingest_checkout=checkout,
            ingest_revision=REVISION,
            tenant="nmdc",
            dataset="nmdc_metadata_staging_20260819",
            bucket="cdm-lake",
            bronze_prefix="tenant-general-warehouse/nmdc/staging/20260819",
            progress_key="tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
            config_key="tenant-general-warehouse/nmdc/staging/20260819/config.json",
            runner=GitRunner(),
        )


def test_loaded_plan_revalidates_snapshot_after_assembly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, bundle, inventory, publication_plan, metadata_plan, target_validation, checkout = _inputs(tmp_path)
    changed_manifest = manifest.model_copy(deep=True)
    changed_manifest.artifacts[0].sha256 = "0" * 64
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
    monkeypatch.setattr(
        "nmdc_lakehouse.berdl_staging.validate_snapshot",
        lambda _path: validate_results.pop(0),
    )
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_metadata_bundle", lambda _path: bundle)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_destination_inventory", lambda _path: inventory)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_publication_plan", lambda _path: publication_plan)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_metadata_application_plan", lambda _path: metadata_plan)
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_target_validation_report", lambda _path: target_validation)
    validate_results = [manifest, changed_manifest]

    with pytest.raises(BerdlStagingPlanError, match="manifested snapshot changed"):
        plan_berdl_staging(
            snapshot,
            bundle_path=paths["bundle"],
            inventory_path=paths["inventory"],
            publication_plan_path=paths["publication"],
            metadata_plan_path=paths["metadata"],
            target_validation_path=paths["target"],
            ingest_checkout=checkout,
            ingest_revision=REVISION,
            tenant="nmdc",
            dataset="nmdc_metadata_staging_20260819",
            bucket="cdm-lake",
            bronze_prefix="tenant-general-warehouse/nmdc/staging/20260819",
            progress_key="tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
            config_key="tenant-general-warehouse/nmdc/staging/20260819/config.json",
            runner=GitRunner(),
        )


def test_loaded_plan_reloads_every_parsed_model_after_assembly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, bundle, inventory, publication_plan, metadata_plan, target_validation, checkout = _inputs(tmp_path)
    changed_validation = target_validation.model_copy(update={"requested_mode": "full"})
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
    validations = [changed_validation, target_validation]
    monkeypatch.setattr("nmdc_lakehouse.berdl_staging.load_target_validation_report", lambda _path: validations.pop(0))

    with pytest.raises(BerdlStagingPlanError, match="Reviewed evidence changed"):
        plan_berdl_staging(
            snapshot,
            bundle_path=paths["bundle"],
            inventory_path=paths["inventory"],
            publication_plan_path=paths["publication"],
            metadata_plan_path=paths["metadata"],
            target_validation_path=paths["target"],
            ingest_checkout=checkout,
            ingest_revision=REVISION,
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
            "--ingest-checkout",
            "data-lakehouse-ingest",
            "--ingest-revision",
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


def test_revalidation_rejects_stale_evidence_and_changed_beril_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _plan_path, paths, checkout = _persisted_plan(tmp_path, monkeypatch)
    assert revalidate_berdl_staging_plan(plan, runner=GitRunner()) == plan

    paths["bundle"].write_text("changed\n", encoding="utf-8")
    with pytest.raises(BerdlStagingPlanError, match="no longer matches"):
        revalidate_berdl_staging_plan(plan, runner=GitRunner())

    paths["bundle"].write_text("bundle\n", encoding="utf-8")
    (checkout / "scripts" / "ingest_lib.py").write_text("changed\n", encoding="utf-8")
    with pytest.raises(BerdlStagingPlanError, match="no longer matches"):
        revalidate_berdl_staging_plan(plan, runner=GitRunner())


def test_revalidation_rejects_edited_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan, _plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)
    plan.command.append("--unreviewed-option")

    with pytest.raises(BerdlStagingPlanError, match="no longer matches"):
        revalidate_berdl_staging_plan(plan, runner=GitRunner())


def test_preview_revalidates_without_starting_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)

    def fail_if_called(_command):
        pytest.fail("preview must not start the staging subprocess")

    command, outcome = execute_berdl_staging(
        plan_path,
        upstream_outcome_path=tmp_path / "upstream.json",
        output_path=tmp_path / "outcome.json",
        authorize_snapshot=None,
        execute_staging=False,
        checkout_runner=GitRunner(),
        staging_runner=fail_if_called,
    )

    assert outcome is None
    assert command[:-3] == plan.command
    assert command[-3:] == ["--outcome", str((tmp_path / "upstream.json").resolve()), "--execute-staging"]


def test_preview_rejects_existing_upstream_outcome(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)
    upstream_path = tmp_path / "upstream.json"
    upstream_path.write_text("existing\n", encoding="utf-8")

    with pytest.raises(BerdlStagingPlanError, match="Refusing to replace"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=upstream_path,
            output_path=tmp_path / "outcome.json",
            authorize_snapshot=None,
            execute_staging=False,
            checkout_runner=GitRunner(),
            staging_runner=lambda _command: pytest.fail("preview must not stage"),
        )


@pytest.mark.parametrize("outcome", ["upstream", "nmdc"])
def test_staging_outcomes_cannot_be_written_inside_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    _plan, plan_path, paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)
    snapshot_root = paths["manifest"].parent
    upstream_path = snapshot_root / "upstream.json" if outcome == "upstream" else tmp_path / "upstream.json"
    output_path = snapshot_root / "outcome.json" if outcome == "nmdc" else tmp_path / "outcome.json"

    with pytest.raises(BerdlStagingPlanError, match="outside the immutable snapshot"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=upstream_path,
            output_path=output_path,
            authorize_snapshot=None,
            execute_staging=False,
            checkout_runner=GitRunner(),
            staging_runner=lambda _command: pytest.fail("invalid output paths must not stage"),
        )


def test_staging_outcomes_require_distinct_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)
    outcome_path = tmp_path / "outcome.json"

    with pytest.raises(BerdlStagingPlanError, match="distinct paths"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=outcome_path,
            output_path=outcome_path,
            authorize_snapshot=None,
            execute_staging=False,
            checkout_runner=GitRunner(),
            staging_runner=lambda _command: pytest.fail("invalid output paths must not stage"),
        )


def test_execution_requires_exact_snapshot_authorization(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)

    with pytest.raises(BerdlStagingPlanError, match="exact snapshot ID"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=tmp_path / "upstream.json",
            output_path=tmp_path / "outcome.json",
            authorize_snapshot="sha256:stale",
            execute_staging=True,
            checkout_runner=GitRunner(),
            staging_runner=lambda _command: pytest.fail("authorization must precede staging"),
        )


def test_execution_rejects_subprocess_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)

    with pytest.raises(BerdlStagingPlanError, match="did not complete"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=tmp_path / "upstream.json",
            output_path=tmp_path / "outcome.json",
            authorize_snapshot=SNAPSHOT_ID,
            execute_staging=True,
            checkout_runner=GitRunner(),
            staging_runner=lambda command: subprocess.CompletedProcess(command, 1),
        )


def test_execution_reports_failure_to_start_subprocess(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)

    def cannot_start(_command):
        raise FileNotFoundError("missing reviewed interpreter")

    with pytest.raises(BerdlStagingPlanError, match="Cannot start"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=tmp_path / "upstream.json",
            output_path=tmp_path / "outcome.json",
            authorize_snapshot=SNAPSHOT_ID,
            execute_staging=True,
            checkout_runner=GitRunner(),
            staging_runner=cannot_start,
        )


@pytest.mark.parametrize("contents", [None, "not JSON"])
def test_execution_rejects_missing_or_malformed_upstream_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, contents: str | None
) -> None:
    _plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)
    upstream_path = tmp_path / "upstream.json"

    def run_staging(command):
        if contents is not None:
            upstream_path.write_text(contents, encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(BerdlStagingPlanError, match="upstream outcome"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=upstream_path,
            output_path=tmp_path / "outcome.json",
            authorize_snapshot=SNAPSHOT_ID,
            execute_staging=True,
            checkout_runner=GitRunner(),
            staging_runner=run_staging,
        )


def test_execution_rejects_evidence_changed_during_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _plan, plan_path, paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)

    def run_staging(command):
        paths["bundle"].write_text("changed during staging\n", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    with pytest.raises(BerdlStagingPlanError, match="no longer matches"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=tmp_path / "upstream.json",
            output_path=tmp_path / "outcome.json",
            authorize_snapshot=SNAPSHOT_ID,
            execute_staging=True,
            checkout_runner=GitRunner(),
            staging_runner=run_staging,
        )


def test_execution_writes_independently_verified_immutable_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, plan_path, _paths, _checkout_value = _persisted_plan(tmp_path, monkeypatch)
    upstream_path = tmp_path / "upstream.json"
    output_path = tmp_path / "outcome.json"
    calls = []

    def run_staging(command):
        calls.append(command)
        upstream_path.write_text(_upstream_outcome(plan).model_dump_json(), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)

    command, outcome = execute_berdl_staging(
        plan_path,
        upstream_outcome_path=upstream_path,
        output_path=output_path,
        authorize_snapshot=SNAPSHOT_ID,
        execute_staging=True,
        checkout_runner=GitRunner(),
        staging_runner=run_staging,
    )

    assert calls == [command]
    assert outcome is not None
    assert outcome.status == "data-verified"
    assert outcome.tables[0].rows == outcome.tables[0].destination_rows == 1
    assert json.loads(output_path.read_text(encoding="utf-8")) == outcome.model_dump(mode="json")
    with pytest.raises(BerdlStagingPlanError, match="Refusing to replace"):
        execute_berdl_staging(
            plan_path,
            upstream_outcome_path=tmp_path / "second-upstream.json",
            output_path=output_path,
            authorize_snapshot=SNAPSHOT_ID,
            execute_staging=True,
            checkout_runner=GitRunner(),
            staging_runner=lambda command: subprocess.CompletedProcess(command, 0),
        )


@pytest.mark.parametrize("mismatch", ["destination", "tables", "rows", "basis"])
def test_upstream_outcome_must_match_destination_tables_and_counts(tmp_path: Path, mismatch: str) -> None:
    plan = _build(tmp_path)
    upstream = _upstream_outcome(plan)
    if mismatch == "destination":
        upstream.destination.namespace = "nmdc.other_staging_20260819"
    elif mismatch == "tables":
        upstream.verification.tables = []
    elif mismatch == "rows":
        upstream.verification.tables[0].destination_rows = 0
    else:
        upstream.verification.tables[0].source_basis = "progress log"

    with pytest.raises(BerdlStagingPlanError):
        build_berdl_staging_outcome(
            plan,
            upstream,
            staging_plan_sha256="a" * 64,
            upstream_outcome_sha256="b" * 64,
        )


def test_cli_previews_staging_without_execution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected_command = ["python", "ingest_dataset.py", "--execute-staging"]
    monkeypatch.setattr(
        "nmdc_lakehouse.berdl_staging.execute_berdl_staging",
        lambda *_args, **_kwargs: (expected_command, None),
    )

    result = CliRunner().invoke(
        cli,
        [
            "berdl-upload",
            "plan.json",
            "--upstream-outcome",
            str(tmp_path / "upstream.json"),
            "--output",
            str(tmp_path / "outcome.json"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"status": "preview-only", "command": expected_command}
