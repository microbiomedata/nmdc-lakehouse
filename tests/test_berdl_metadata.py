"""Tests for staged BERDL table and column metadata application."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from nmdc_lakehouse import berdl_metadata
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


def test_apply_does_not_rewrite_descriptions_the_catalog_already_holds(tmp_path: Path) -> None:
    """Each write is a catalog commit, so a correct description must not be written again; #258."""
    descriptions = {"nmdc.metadata_staging_20260820.biosample_set": "NMDC biosamples"}
    columns = {"id": "Stable biosample identifier"}
    written_tables: list[str] = []
    written_columns: list[str] = []

    class Catalog:
        def getTable(self, name):
            return SimpleNamespace(description=descriptions.get(name))

        def listColumns(self, _name):
            return [SimpleNamespace(name="id", description=columns.get("id"))]

    spark = SimpleNamespace(catalog=Catalog())

    def apply_table(_spark, table, value, **_kwargs):
        written_tables.append(table)
        descriptions[table] = value
        return {"status": "success"}

    def apply_columns(_spark, _table, schema, **_kwargs):
        written_columns.extend(item["column"] for item in schema)
        columns.update({item["column"]: item["comment"] for item in schema})
        return {"status": "success"}

    preview = build_berdl_metadata_preview(
        _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    outcome = apply_berdl_staging_metadata(
        _plan(),
        _staging(),
        preview,
        ingest_checkout=tmp_path,
        runtime=lambda _checkout: (spark, apply_table, apply_columns),
        checkout_verifier=lambda _checkout, _revision: None,
    )

    assert written_tables == [], "the table description was already correct and was rewritten anyway"
    assert written_columns == [], "the column description was already correct and was rewritten anyway"
    assert outcome.status == "metadata-verified"
    assert outcome.targets[0].table_description_status == "verified"
    assert outcome.targets[0].columns_verified == ["id"]
    assert outcome.targets[0].columns_already_correct == ["id"]


def test_apply_writes_only_the_columns_that_differ(tmp_path: Path) -> None:
    """A rerun after a partial failure must finish the remainder, not redo the whole table."""
    plan = _plan()
    plan.supported_operations.append(
        _operation(
            MetadataOperationKind.COLUMN_DESCRIPTION,
            "Collection date",
            table="biosample_set",
            column="collection_date",
        )
    )
    columns = {"id": "Stable biosample identifier", "collection_date": None}
    written_columns: list[str] = []

    class Catalog:
        def getTable(self, name):
            return SimpleNamespace(description="NMDC biosamples")

        def listColumns(self, _name):
            return [SimpleNamespace(name=name, description=value) for name, value in columns.items()]

    spark = SimpleNamespace(catalog=Catalog())

    def apply_columns(_spark, _table, schema, **_kwargs):
        written_columns.extend(item["column"] for item in schema)
        columns.update({item["column"]: item["comment"] for item in schema})
        return {"status": "success"}

    preview = build_berdl_metadata_preview(
        plan, _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    outcome = apply_berdl_staging_metadata(
        plan,
        _staging(),
        preview,
        ingest_checkout=tmp_path,
        runtime=lambda _checkout: (spark, lambda *_a, **_k: {"status": "success"}, apply_columns),
        checkout_verifier=lambda _checkout, _revision: None,
    )

    assert written_columns == ["collection_date"], "only the column whose description was wrong should be written"
    # Both are still verified: skipping a write must never skip the read-back that proves the
    # description is there.
    assert outcome.targets[0].columns_verified == ["collection_date", "id"]
    assert outcome.targets[0].columns_already_correct == ["id"]


def test_a_version_one_outcome_still_parses_and_new_outcomes_declare_version_two() -> None:
    """Every model here forbids extra fields, so an added key is a format change, default or not."""
    from nmdc_lakehouse.berdl_metadata import METADATA_OUTCOME_FORMAT_VERSION

    v1 = {
        "outcome_format_version": 1,
        "status": "metadata-verified",
        "snapshot_id": SNAPSHOT_ID,
        "destination_id": "berdl-production",
        "staging_namespace": "nmdc.metadata_staging_20260820",
        "staging_outcome_sha256": "6" * 64,
        "metadata_plan_sha256": "5" * 64,
        "deferred_namespace_operations": 1,
        "targets": [
            {
                "table": "biosample_set",
                "table_description_status": "verified",
                "columns_verified": ["id"],
            }
        ],
    }

    parsed = BerdlMetadataOutcome.model_validate(v1, strict=True)

    assert parsed.outcome_format_version == 1
    assert parsed.targets[0].columns_already_correct == []
    assert parsed.targets[0].table_description_already_correct is False
    # And the fields that motivated the bump are rejected under the old version's shape only by
    # the version number, not by the model, which is exactly why the number had to change.
    assert METADATA_OUTCOME_FORMAT_VERSION == 2


def test_a_column_description_without_a_column_cannot_be_built() -> None:
    """Where the completeness check actually lives, tested on the input it must reject.

    `MetadataOperation` validates this, so an incomplete operation cannot reach the application
    step through any ordinary path. The matching check in `_description_operations` is a second
    line for a model built without validation, not the primary guard, and this test names which
    one is which so a later reader does not mistake the fallback for the enforcement.
    """
    with pytest.raises(ValidationError, match="require table and column targets"):
        _operation(MetadataOperationKind.COLUMN_DESCRIPTION, "Nameless", table="biosample_set")


def test_a_skipped_table_description_is_still_read_back(tmp_path: Path) -> None:
    """The probe decides whether to write; it is not the verification. Same rule as the columns."""
    reads: list[str] = []
    # Correct on the probe, changed by the time the read-back runs, which is what a concurrent
    # writer looks like from here.
    values = iter(["NMDC biosamples", "something else"])

    class Catalog:
        def getTable(self, name):
            reads.append(name)
            return SimpleNamespace(description=next(values, "something else"))

        def listColumns(self, _name):
            return [SimpleNamespace(name="id", description="Stable biosample identifier")]

    spark = SimpleNamespace(catalog=Catalog())

    def apply_table(*_args, **_kwargs):
        raise AssertionError("the table description was already correct and must not be rewritten")

    preview = build_berdl_metadata_preview(
        _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    with pytest.raises(BerdlMetadataError, match="table description read-back failed"):
        apply_berdl_staging_metadata(
            _plan(),
            _staging(),
            preview,
            ingest_checkout=tmp_path,
            runtime=lambda _checkout: (spark, apply_table, lambda *_a, **_k: {"status": "success"}),
            checkout_verifier=lambda _checkout, _revision: None,
        )

    assert len(reads) == 2, "the probe and the read-back must be two separate reads"


def test_the_outcome_distinguishes_a_written_table_description_from_a_skipped_one(tmp_path: Path) -> None:
    """ "verified" alone cannot tell a resumed run from a fresh one for a table-level plan."""

    def run(existing: str | None):
        stored = {"nmdc.metadata_staging_20260820.biosample_set": existing}

        class Catalog:
            def getTable(self, name):
                return SimpleNamespace(description=stored.get(name))

            def listColumns(self, _name):
                return [SimpleNamespace(name="id", description="Stable biosample identifier")]

        def apply_table(_spark, table, value, **_kwargs):
            stored[table] = value
            return {"status": "success"}

        spark = SimpleNamespace(catalog=Catalog())
        preview = build_berdl_metadata_preview(
            _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
        )
        return apply_berdl_staging_metadata(
            _plan(),
            _staging(),
            preview,
            ingest_checkout=tmp_path,
            runtime=lambda _checkout: (spark, apply_table, lambda *_a, **_k: {"status": "success"}),
            checkout_verifier=lambda _checkout, _revision: None,
        )

    written = run(None)
    skipped = run("NMDC biosamples")

    assert written.targets[0].table_description_status == "verified"
    assert skipped.targets[0].table_description_status == "verified"
    assert written.targets[0].table_description_already_correct is False
    assert skipped.targets[0].table_description_already_correct is True


def test_progress_rates_the_run_on_columns_written_not_columns_verified(tmp_path: Path) -> None:
    """A skip costs a read and a write costs a commit, so counting them together misreports both."""
    messages: list[str] = []

    class Catalog:
        def getTable(self, _name):
            return SimpleNamespace(description="NMDC biosamples")

        def listColumns(self, _name):
            return [SimpleNamespace(name="id", description="Stable biosample identifier")]

    spark = SimpleNamespace(catalog=Catalog())
    preview = build_berdl_metadata_preview(
        _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    apply_berdl_staging_metadata(
        _plan(),
        _staging(),
        preview,
        ingest_checkout=tmp_path,
        runtime=lambda _checkout: (
            spark,
            lambda *_a, **_k: {"status": "success"},
            lambda *_a, **_k: {"status": "success"},
        ),
        checkout_verifier=lambda _checkout, _revision: None,
        progress=messages.append,
    )

    summary = next(m for m in messages if "verified" in m and "written" in m)
    assert "1/1 verified" in summary
    assert "0 written" in summary, f"an all-skipped run must not report written work: {summary}"


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


def test_apply_skips_column_readback_without_planned_columns(tmp_path: Path) -> None:
    plan = _plan()
    plan.supported_operations = [
        operation
        for operation in plan.supported_operations
        if operation.kind is not MetadataOperationKind.COLUMN_DESCRIPTION
    ]
    descriptions: dict[str, str] = {}

    class Catalog:
        def getTable(self, name):
            return SimpleNamespace(description=descriptions.get(name))

        def listColumns(self, _name):
            pytest.fail("column descriptions must not be read back without planned columns")

    spark = SimpleNamespace(catalog=Catalog())

    def apply_table(_spark, table, value, **_kwargs):
        descriptions[table] = value
        return {"status": "success"}

    preview = build_berdl_metadata_preview(
        plan, _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    outcome = apply_berdl_staging_metadata(
        plan,
        _staging(),
        preview,
        ingest_checkout=tmp_path,
        runtime=lambda _checkout: (
            spark,
            apply_table,
            lambda *_args, **_kwargs: pytest.fail("column descriptions must not be applied"),
        ),
        checkout_verifier=lambda *_args: None,
    )

    assert outcome.targets[0].table_description_status == "verified"
    assert outcome.targets[0].columns_verified == []


def test_checkout_verification_rejects_a_modified_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    revision = "a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3"
    commands: list[tuple[str, ...]] = []

    def runner(args):
        command = tuple(args)
        commands.append(command)
        if "rev-parse" in command and command[-1] == "HEAD":
            return subprocess.CompletedProcess(args, 0, revision + "\n", "")
        if "status" in command:
            return subprocess.CompletedProcess(args, 0, " M utils/delta_comments.py\n", "")
        pytest.fail(f"unexpected command after a dirty checkout: {command}")

    monkeypatch.setattr(berdl_metadata, "_run_command", runner)

    with pytest.raises(BerdlMetadataError, match="does not match the verified ingest revision"):
        berdl_metadata._verify_ingest_checkout(tmp_path, revision)

    assert commands[-1][:4] == ("git", "-C", str(tmp_path.resolve()), "status")


def test_application_reports_per_table_progress(tmp_path: Path) -> None:
    """A run measured in tens of minutes must not be silent; #256."""
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

    messages: list[str] = []
    preview = build_berdl_metadata_preview(
        _plan(), _staging(), metadata_plan_sha256="5" * 64, staging_outcome_sha256="6" * 64
    )
    apply_berdl_staging_metadata(
        _plan(),
        _staging(),
        preview,
        ingest_checkout=tmp_path,
        runtime=lambda _checkout: (spark, apply_table, apply_columns),
        checkout_verifier=lambda *_args: None,
        progress=messages.append,
    )

    joined = "\n".join(messages)
    assert "applying descriptions to 1 table and 1 column in" in joined
    assert "[1/1] biosample_set: applying 1 column description" in joined
    assert "verified 1 column (" in joined
    assert "1/1 verified" in joined
    assert "1 written" in joined
    assert "min elapsed" in joined


def test_progress_goes_to_stderr_so_stdout_stays_parseable(capsys) -> None:
    from nmdc_lakehouse.berdl_metadata import _default_progress

    _default_progress("halfway through biosample_set")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "halfway through biosample_set" in captured.err


def test_progress_text_pluralises_correctly() -> None:
    """Operator-facing output should not read as 1 columns."""
    from nmdc_lakehouse.berdl_metadata import _plural

    assert _plural(0, "column") == "0 columns"
    assert _plural(1, "column") == "1 column"
    assert _plural(2, "column") == "2 columns"
    assert _plural(1, "column description") == "1 column description"
    assert _plural(1393, "column description") == "1393 column descriptions"
