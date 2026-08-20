"""Tests for staged BERDL table and column metadata application."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.berdl_metadata import (
    AppliedMetadataTarget,
    BerdlMetadataError,
    BerdlMetadataOutcome,
    apply_berdl_staging_metadata,
    build_berdl_metadata_preview,
    write_berdl_metadata_outcome,
)
from nmdc_lakehouse.berdl_staging import BerdlStagingOutcome, StagedTable
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.metadata_application import (
    MetadataApplicationPlan,
    MetadataOperation,
    MetadataOperationKind,
)
from nmdc_lakehouse.publication_plan import MetadataCapability

SNAPSHOT_ID = "sha256:" + "1" * 64


def _operation(kind, value, *, table=None, column=None, property_name=None):
    return MetadataOperation(
        kind=kind,
        namespace="nmdc.metadata_staging_20260820",
        table=table,
        column=column,
        property=property_name,
        value=value,
        origin="footer",
    )


def _plan() -> MetadataApplicationPlan:
    return MetadataApplicationPlan(
        plan_format_version=1,
        snapshot_id=SNAPSHOT_ID,
        profile_id="nmdc_metadata",
        bundle_generated_at="2026-08-20T12:00:00+00:00",
        source_namespace="nmdc_metadata",
        destination_id="berdl-production",
        destination_observed_at="2026-08-20T12:05:00+00:00",
        destination_provider="spark_catalog",
        destination_table_format="iceberg",
        destination_metadata_capabilities=[
            MetadataCapability.NAMESPACE,
            MetadataCapability.TABLE,
            MetadataCapability.COLUMN,
        ],
        staging_namespace="nmdc.metadata_staging_20260820",
        tables=["biosample_set"],
        supported_operations=[
            _operation(MetadataOperationKind.NAMESPACE_DESCRIPTION, "NMDC metadata staging"),
            _operation(MetadataOperationKind.TABLE_DESCRIPTION, "NMDC biosamples", table="biosample_set"),
            _operation(
                MetadataOperationKind.COLUMN_DESCRIPTION,
                "Stable biosample identifier",
                table="biosample_set",
                column="id",
            ),
        ],
        unsupported_operations=[],
        missing_descriptions=[],
    )


def _staging() -> BerdlStagingOutcome:
    return BerdlStagingOutcome(
        outcome_format_version=1,
        status="data-verified",
        snapshot_id=SNAPSHOT_ID,
        staging_namespace="nmdc.metadata_staging_20260820",
        destination_id="berdl-production",
        bucket="cdm-lake",
        bronze_prefix="tenant-general-warehouse/nmdc/staging/20260820",
        progress_key="tenant-general-warehouse/nmdc/staging/20260820/progress.jsonl",
        config_key="tenant-general-warehouse/nmdc/staging/20260820/config.json",
        ingest_revision="a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3",
        staging_plan_sha256="2" * 64,
        upstream_outcome_sha256="3" * 64,
        upstream_started_at="2026-08-20T12:10:00+00:00",
        upstream_finished_at="2026-08-20T12:12:00+00:00",
        tables=[
            StagedTable(
                table="biosample_set",
                artifact_sha256="4" * 64,
                rows=1,
                destination_rows=1,
                source_basis="source parquet",
            )
        ],
    )


def test_preview_binds_verified_staging_and_defers_namespace_operations() -> None:
    preview = build_berdl_metadata_preview(
        _plan(),
        _staging(),
        metadata_plan_sha256="5" * 64,
        staging_outcome_sha256="6" * 64,
    )

    assert preview.deferred_namespace_operations == 1
    assert preview.targets[0].table_description is True
    assert preview.targets[0].column_descriptions == 1


def test_preview_rejects_a_different_staging_identity() -> None:
    staging = _staging()
    staging.destination_id = "different"

    with pytest.raises(BerdlMetadataError, match="does not match"):
        build_berdl_metadata_preview(_plan(), staging, metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64)


def test_preview_rejects_a_non_iceberg_destination() -> None:
    plan = _plan()
    plan.destination_table_format = "delta"

    with pytest.raises(BerdlMetadataError, match="Spark Iceberg"):
        build_berdl_metadata_preview(plan, _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64)


def test_apply_requires_exact_catalog_readback(tmp_path: Path) -> None:
    descriptions: dict[str, str] = {}
    columns: dict[str, str] = {}

    class Catalog:
        def getTable(self, name):
            return SimpleNamespace(description=descriptions.get(name))

        def listColumns(self, _name):
            return [SimpleNamespace(name="id", description=columns.get("id"))]

    spark = SimpleNamespace(catalog=Catalog())

    def apply_table(_spark, table, value, **_kwargs):
        descriptions[table] = value
        return {"status": "success"}

    def apply_columns(_spark, _table, schema, **_kwargs):
        columns.update({item["column"]: item["comment"] for item in schema})
        return {"status": "success"}

    preview = build_berdl_metadata_preview(
        _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    verified_checkout = {}
    outcome = apply_berdl_staging_metadata(
        _plan(),
        _staging(),
        preview,
        ingest_checkout=tmp_path,
        runtime=lambda _checkout: (spark, apply_table, apply_columns),
        checkout_verifier=lambda checkout, revision: verified_checkout.update(
            {"checkout": checkout, "revision": revision}
        ),
    )

    assert outcome.status == "metadata-verified"
    assert outcome.targets[0].table_description_status == "verified"
    assert outcome.targets[0].columns_verified == ["id"]
    assert verified_checkout == {
        "checkout": tmp_path,
        "revision": "a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3",
    }


def test_apply_rejects_a_preview_from_different_inputs(tmp_path: Path) -> None:
    preview = build_berdl_metadata_preview(
        _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    preview.targets[0].column_descriptions = 0

    with pytest.raises(BerdlMetadataError, match="preview does not match"):
        apply_berdl_staging_metadata(
            _plan(),
            _staging(),
            preview,
            ingest_checkout=tmp_path,
            runtime=lambda _checkout: pytest.fail("runtime must not be initialized"),
            checkout_verifier=lambda *_args: pytest.fail("checkout must not be inspected"),
        )


def test_cli_preview_is_offline_and_reports_input_hashes(tmp_path: Path) -> None:
    plan_path = tmp_path / "metadata-plan.json"
    staging_path = tmp_path / "staging-outcome.json"
    plan_path.write_text(_plan().model_dump_json(), encoding="utf-8")
    staging_path.write_text(_staging().model_dump_json(), encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "berdl-apply-metadata",
            str(plan_path),
            str(staging_path),
            "--ingest-checkout",
            str(tmp_path / "not-contacted"),
            "--output",
            str(tmp_path / "outcome.json"),
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["status"] == "preview-only"
    assert document["metadata_plan_sha256"] == hashlib.sha256(plan_path.read_bytes()).hexdigest()
    assert document["staging_outcome_sha256"] == hashlib.sha256(staging_path.read_bytes()).hexdigest()
    assert not (tmp_path / "outcome.json").exists()


def test_cli_execution_requires_hashes_and_writes_verified_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path = tmp_path / "metadata-plan.json"
    staging_path = tmp_path / "staging-outcome.json"
    output_path = tmp_path / "metadata-outcome.json"
    plan_path.write_text(_plan().model_dump_json(), encoding="utf-8")
    staging_path.write_text(_staging().model_dump_json(), encoding="utf-8")
    plan_sha256 = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    staging_sha256 = hashlib.sha256(staging_path.read_bytes()).hexdigest()
    expected = BerdlMetadataOutcome(
        outcome_format_version=1,
        status="metadata-verified",
        snapshot_id=SNAPSHOT_ID,
        destination_id="berdl-production",
        staging_namespace="nmdc.metadata_staging_20260820",
        staging_outcome_sha256=staging_sha256,
        metadata_plan_sha256=plan_sha256,
        deferred_namespace_operations=1,
        targets=[
            AppliedMetadataTarget(
                table="biosample_set",
                table_description_status="verified",
                columns_verified=["id"],
            )
        ],
    )
    monkeypatch.setattr(
        "nmdc_lakehouse.berdl_metadata.apply_berdl_staging_metadata",
        lambda *_args, **_kwargs: expected,
    )

    result = CliRunner().invoke(
        cli,
        [
            "berdl-apply-metadata",
            str(plan_path),
            str(staging_path),
            "--ingest-checkout",
            str(tmp_path / "checkout"),
            "--output",
            str(output_path),
            "--execute-metadata",
            "--authorize-plan-sha256",
            plan_sha256,
            "--authorize-staging-outcome-sha256",
            staging_sha256,
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "metadata-verified"


def test_metadata_outcome_publication_is_atomic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "metadata-outcome.json"
    expected = BerdlMetadataOutcome(
        outcome_format_version=1,
        status="metadata-verified",
        snapshot_id=SNAPSHOT_ID,
        destination_id="berdl-production",
        staging_namespace="nmdc.metadata_staging_20260820",
        staging_outcome_sha256="6" * 64,
        metadata_plan_sha256="5" * 64,
        deferred_namespace_operations=1,
        targets=[],
    )

    def fail_link(_source, _destination):
        raise OSError("injected publication failure")

    monkeypatch.setattr(os, "link", fail_link)

    with pytest.raises(BerdlMetadataError, match="publish.*atomically"):
        write_berdl_metadata_outcome(output, expected)

    assert not output.exists()
    assert list(tmp_path.iterdir()) == []
