"""Exercise preview, recorded status and metadata-only recovery without a live pod."""

import json
import subprocess
from functools import partial

import pytest
from click.testing import CliRunner

from nmdc_lakehouse import berdl_metadata, berdl_staging
from nmdc_lakehouse import publication_staging as staging
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.publication_planning import plan_publication
from nmdc_lakehouse.publication_prepare import PreparationError, file_digest
from tests.test_berdl_staging import GitRunner, _upstream_outcome
from tests.test_publication_planning import prepared as prepared_fixture

prepared = prepared_fixture
FRESH_DESTINATION = staging._fresh_destination


@pytest.fixture
def planned(prepared, monkeypatch):
    root, configuration = prepared
    plan = plan_publication(root, configuration, runner=GitRunner())
    calls = []
    failure = {"data": False, "metadata": False}
    monkeypatch.setattr(
        berdl_staging,
        "revalidate_berdl_staging_plan",
        partial(berdl_staging.revalidate_berdl_staging_plan, runner=GitRunner()),
    )
    monkeypatch.setattr(berdl_metadata, "_verify_ingest_checkout", lambda *a: None)

    def upload(command):
        calls.append("data")
        print("private runtime diagnostic")
        if failure["data"]:
            raise RuntimeError("private runtime error")
        document = _upstream_outcome(plan).model_dump(mode="json")
        document["verification"]["tables"] = [
            {
                "table": a.table,
                "status": "verified",
                "source_rows": a.rows,
                "destination_rows": a.rows,
                "source_basis": "source parquet",
                "source_sha256": a.sha256,
            }
            for a in plan.artifacts
        ]
        (root / "evidence/kbase-ingest-outcome.json").write_text(json.dumps(document))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(
        berdl_metadata,
        "execute_berdl_staging",
        partial(berdl_staging.execute_berdl_staging, checkout_runner=GitRunner(), staging_runner=upload),
    )

    def apply(metadata, data, preview, *, ingest_checkout):
        calls.append("metadata")
        if failure["metadata"]:
            raise berdl_metadata.BerdlMetadataError("private metadata error")
        tables, columns, _ = berdl_metadata._description_operations(metadata)
        return berdl_metadata.BerdlMetadataOutcome(
            outcome_format_version=3,
            status="metadata-verified",
            snapshot_id=data.snapshot_id,
            destination_id=data.destination_id,
            staging_namespace=data.staging_namespace,
            staging_outcome_sha256=preview.staging_outcome_sha256,
            metadata_plan_sha256=preview.metadata_plan_sha256,
            deferred_namespace_operations=preview.deferred_namespace_operations,
            targets=[
                berdl_metadata.AppliedMetadataTarget(
                    table=table,
                    table_description_status="verified" if table in tables else "not-planned",
                    columns_verified=[column for column, _ in columns[table]],
                    schema_properties_status="verified",
                )
                for table in metadata.tables
            ],
        )

    monkeypatch.setattr(berdl_metadata, "apply_berdl_staging_metadata", apply)
    monkeypatch.setattr(staging, "_fresh_destination", lambda plan: calls.append("empty-destination"))
    authorization = dict(
        authorize_snapshot=plan.snapshot_id,
        authorize_plan_sha256=file_digest(root / "evidence/berdl-staging-plan.json"),
        execute=True,
    )
    return root, authorization, calls, failure


def test_status_and_preview_are_offline_then_completed_retry_is_noop(planned, capsys):
    root, authorization, calls, _ = planned
    state = staging.publication_status(root)
    assert state["status"] == "planned"
    assert state["tables"] == 2
    assert authorization["authorize_plan_sha256"] in state["next_command"]
    assert staging.stage_publication(root)["status"] == "preview-only"
    assert calls == []
    state = staging.stage_publication(root, **authorization)
    assert state["status"] == "data-and-table-metadata-verified"
    assert state["columns_verified"] == 15
    assert state["next_command"] is None
    assert calls == ["empty-destination", "data", "metadata"]
    assert staging.stage_publication(root, **authorization) == state
    assert calls == ["empty-destination", "data", "metadata"]
    assert "private runtime diagnostic" not in capsys.readouterr().out
    log = next((root / "evidence").glob("staging-*.log"))
    assert "private runtime diagnostic" in log.read_text()
    assert log.stat().st_mode & 0o077 == 0


def test_metadata_failure_retries_without_upload_and_requires_same_authorization(planned):
    root, authorization, calls, failure = planned
    failure["metadata"] = True
    with pytest.raises(PreparationError, match="Staging stopped"):
        staging.stage_publication(root, **authorization)
    assert staging.publication_status(root)["status"] == "data-verified-metadata-pending"
    assert staging.stage_publication(root)["phase"] == "metadata-only"
    calls.clear()
    with pytest.raises(PreparationError, match="exact reviewed"):
        staging.stage_publication(root, execute=True)
    assert not calls
    failure["metadata"] = False
    assert staging.stage_publication(root, **authorization)["status"] == "data-and-table-metadata-verified"
    assert calls == ["metadata"]


def test_uncertain_upload_is_never_automatically_repeated(planned):
    root, authorization, calls, failure = planned
    failure["data"] = True
    with pytest.raises(PreparationError, match="Staging stopped"):
        staging.stage_publication(root, **authorization)
    assert staging.publication_status(root)["status"] == "partial-staging"
    calls.clear()
    with pytest.raises(PreparationError, match="inspect the private staging log"):
        staging.stage_publication(root, **authorization)
    assert not calls


@pytest.mark.parametrize("change", ["table", "column", "property", "data", "upstream", "metadata-hash"])
def test_status_rejects_unbound_or_incomplete_success_evidence(planned, change):
    root, authorization, _, _ = planned
    staging.stage_publication(root, **authorization)
    evidence = root / "evidence"
    path = evidence / "nmdc-staging-metadata-outcome.json"
    if change == "data":
        path = evidence / "nmdc-staging-outcome.json"
    elif change == "upstream":
        path = evidence / "kbase-ingest-outcome.json"
    value = json.loads(path.read_text())
    if change == "table":
        value["targets"].pop()
    elif change == "column":
        value["targets"][0]["columns_verified"].pop()
    elif change == "property":
        value["targets"][0]["schema_properties_status"] = "not-planned"
    elif change == "metadata-hash":
        value["metadata_plan_sha256"] = "0" * 64
    else:
        value["status"] = "unverified"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        staging.publication_status(root)


def test_prepared_status_and_changed_receipt(prepared):
    root, _ = prepared
    assert staging.publication_status(root)["status"] == "prepared"
    with pytest.raises(PreparationError, match="Send to the pod"):
        staging.stage_publication(root)
    path = root / "preparation.json"
    value = json.loads(path.read_text())
    value["status"] = "incomplete"
    path.write_text(json.dumps(value))
    with pytest.raises(PreparationError, match="incomplete"):
        staging.publication_status(root)


@pytest.mark.parametrize("occupied", ["namespace", "prefix", "neither"])
def test_fresh_destination_checks_namespace_and_objects(planned, monkeypatch, occupied):
    root, _, _, _ = planned
    fresh = FRESH_DESTINATION
    plan = berdl_staging.load_berdl_staging_plan(root / "evidence/berdl-staging-plan.json")

    class Runtime:
        def sql(self, sql):
            assert sql == "SHOW NAMESPACES IN `nmdc`"
            return self

        def collect(self):
            return [{"namespace": plan.dataset}] if occupied == "namespace" else []

        def list_objects(self, bucket, **kw):
            assert bucket == plan.bucket and kw["prefix"] == plan.bronze_prefix + "/"
            return [object()] if occupied == "prefix" else []

    runtime = Runtime()
    monkeypatch.setattr(berdl_metadata, "_runtime", lambda path: (runtime, None, None))
    monkeypatch.setattr(staging.berdl_adapter, "_runtime", lambda path: (None, runtime))
    if occupied == "neither":
        assert fresh(plan) is runtime
    else:
        with pytest.raises(PreparationError, match="select a new destination"):
            fresh(plan)


def test_cli_replaces_old_commands_and_emits_parseable_status(planned):
    root, authorization, calls, _ = planned
    runner = CliRunner()
    for old in ("berdl-upload", "berdl-apply-metadata"):
        assert runner.invoke(cli, [old, "--help"]).exit_code != 0
    result = runner.invoke(cli, ["stage-publication", str(root)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "preview-only"
    result = runner.invoke(cli, ["stage-publication", str(root), "--execute"])
    assert result.exit_code != 0 and not calls
    result = runner.invoke(
        cli,
        [
            "stage-publication",
            str(root),
            "--execute",
            "--authorize-snapshot",
            authorization["authorize_snapshot"],
            "--authorize-plan-sha256",
            authorization["authorize_plan_sha256"],
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "data-and-table-metadata-verified"
    result = runner.invoke(cli, ["publication-status", str(root)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["columns_verified"] == 15


def test_staging_cannot_run_concurrently(planned):
    import fcntl

    root, authorization, calls, _ = planned
    with (root / "evidence/.stage.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(PreparationError, match="Another staging"):
            staging.stage_publication(root, **authorization)
    assert not calls


@pytest.mark.parametrize("name", ["berdl-staging-plan.json", "nmdc-staging-outcome.json", "staging-attempt.json"])
def test_status_refuses_redirected_evidence(planned, name):
    root, authorization, calls, _ = planned
    path = root / "evidence" / name
    if path.exists():
        path.rename(path.with_suffix(".retained"))
    path.symlink_to(root / "absent")
    with pytest.raises(PreparationError, match="ordinary files"):
        staging.stage_publication(root, **authorization)
    assert not calls
