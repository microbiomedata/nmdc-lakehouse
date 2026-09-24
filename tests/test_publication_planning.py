"""Verify combined planning retains the established evidence and execution guards."""

import json
from importlib.metadata import version

import pytest
from click.testing import CliRunner

from nmdc_lakehouse import publication_planning as planning
from nmdc_lakehouse.berdl_staging import BerdlStagingPlanError
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.local_provenance import derive_provenance
from nmdc_lakehouse.metadata_application import load_metadata_application_plan
from nmdc_lakehouse.publication_plan import Disposition, load_publication_plan
from nmdc_lakehouse.publication_prepare import PreparationError, prepare_publication
from tests.test_berdl_staging import REVISION, GitRunner, _checkout
from tests.test_local_provenance import snapshot


@pytest.fixture
def prepared(tmp_path):
    source = tmp_path / "derived"
    derive_provenance(snapshot(tmp_path / "parent"), source)
    config = tmp_path / "prepare.json"
    config.write_text(
        json.dumps(
            {
                "source_version": version("nmdc-schema"),
                "snapshot": "derived",
                "namespace": {"name": "nmdc.metadata", "title": "NMDC", "description": "Reviewed description."},
            }
        )
    )
    root = tmp_path / "prepared"
    prepare_publication(config, root)
    inventory = tmp_path / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "inventory_format_version": 2,
                "destination_id": "nmdc-production",
                "observed_at": "2026-09-23T12:00:00Z",
                "provider": "nmdc",
                "table_format": "iceberg",
                "metadata_capabilities": ["namespace", "table", "column"],
                "tables": [
                    {"name": name, "rows": 5, "physical_schema_sha256": "a" * 64, "observed_table_format": "iceberg"}
                    for name in ("graph_edges", "biosample_set")
                ],
            }
        )
    )
    destination = tmp_path / "destination.json"
    destination.write_text(
        json.dumps(
            {
                "inventory": inventory.name,
                "ingest_checkout": _checkout(tmp_path).name,
                "ingest_revision": REVISION,
                "staging_namespace": "nmdc.nmdc_provenance_staging_test",
                "bucket": "cdm-lake",
                "bronze_prefix": "tenant-general-warehouse/nmdc/staging/test",
            }
        )
    )
    return root, destination


def test_all_plans_bind_same_prepared_evidence_and_resume(prepared):
    root, config = prepared
    before = {p.name: p.read_bytes() for p in (root / "snapshot").iterdir()}
    plan = planning.plan_publication(root, config, runner=GitRunner())
    assert {a.table for a in plan.artifacts} == {"graph_edges", "biosample_to_workflow_run"}
    assert plan.target_validation.requested_mode == "full"
    publication = load_publication_plan(root / "evidence/publication-plan.json")
    assert {e.table: e.disposition for e in publication.tables} == {
        "graph_edges": Disposition.REPLACE,
        "biosample_to_workflow_run": Disposition.ADD,
        "biosample_set": Disposition.PRESERVE,
    }
    metadata = load_metadata_application_plan(root / "evidence/metadata-application-plan.json")
    assert metadata.snapshot_id == plan.snapshot_id == publication.candidate_snapshot_id
    assert not metadata.missing_descriptions
    assert metadata.staging_namespace == plan.staging_namespace
    assert {p.name: p.read_bytes() for p in (root / "snapshot").iterdir()} == before
    evidence = {p.name: p.read_bytes() for p in (root / "evidence").iterdir()}
    assert planning.plan_publication(root, config, runner=GitRunner()) == plan
    assert {p.name: p.read_bytes() for p in (root / "evidence").iterdir()} == evidence


@pytest.mark.parametrize("file", ["metadata-bundle.json", "target-validation.json"])
def test_changed_prepared_evidence_stops_before_plan(prepared, file):
    root, config = prepared
    with (root / "evidence" / file).open("a") as stream:
        stream.write(" ")
    with pytest.raises(PreparationError, match="Prepared evidence changed"):
        planning.plan_publication(root, config, runner=GitRunner())
    assert not (root / "evidence/berdl-staging-plan.json").exists()


def test_new_destination_refuses_to_replace_reviewed_plan(prepared):
    root, config = prepared
    planning.plan_publication(root, config, runner=GitRunner())
    saved = (root / "evidence/berdl-staging-plan.json").read_bytes()
    config.write_text(config.read_text().replace("staging_test", "staging_different"))
    with pytest.raises(PreparationError, match="planning-inputs.json differs"):
        planning.plan_publication(root, config, runner=GitRunner())
    assert (root / "evidence/berdl-staging-plan.json").read_bytes() == saved


def test_dirty_runtime_can_be_fixed_without_repreparing(prepared):
    root, config = prepared
    with pytest.raises(BerdlStagingPlanError, match="no tracked or untracked changes"):
        planning.plan_publication(root, config, runner=GitRunner(dirty=" M core.py"))
    assert not (root / "evidence/berdl-staging-plan.json").exists()
    assert planning.plan_publication(root, config, runner=GitRunner()).status == "plan-only"


@pytest.mark.parametrize(
    "change", ["root", "receipt", "parent", "namespace", "plan-link", "inventory", "ingest_checkout"]
)
def test_unbound_or_redirected_inputs_are_refused(prepared, tmp_path, change):
    root, config = prepared
    receipt = root / "preparation.json"
    if change == "root":
        link = tmp_path / "linked"
        link.symlink_to(root)
        root = link
    elif change in {"receipt", "parent"}:
        value = json.loads(receipt.read_text())
        value["status" if change == "receipt" else "parent_snapshot_id"] = "wrong"
        receipt.write_text(json.dumps(value))
    elif change == "namespace":
        config.write_text(config.read_text().replace("nmdc.nmdc_provenance", "nmdc_provenance"))
    elif change in {"inventory", "ingest_checkout"}:
        value = json.loads(config.read_text())
        link = tmp_path / "linked-input"
        link.symlink_to(config.parent / value[change])
        value[change] = link.name
        config.write_text(json.dumps(value))
    else:
        (root / "evidence/berdl-staging-plan.json").symlink_to(receipt)
    with pytest.raises(ValueError):
        planning.plan_publication(root, config, runner=GitRunner())


def test_cli_replaces_four_commands_and_sanitizes_invalid_config(prepared, monkeypatch):
    root, config = prepared
    original = planning.plan_publication
    monkeypatch.setattr(planning, "plan_publication", lambda *a: original(*a, runner=GitRunner()))
    runner = CliRunner()
    result = runner.invoke(cli, ["plan-publication", str(root), str(config)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "plan-only"
    assert "plan_sha256=" in result.stderr
    for old in ("publication-plan", "publication-preflight", "metadata-application-plan", "berdl-upload-plan"):
        assert runner.invoke(cli, [old, "--help"]).exit_code != 0
    config.write_text('{"credential": "secret-example"}')
    result = runner.invoke(cli, ["plan-publication", str(root), str(config)])
    assert result.exit_code != 0
    assert "secret-example" not in result.output
    assert "Invalid planning configuration" in result.output


@pytest.mark.parametrize("change", ["receipt", "installed", "flat-pair"])
def test_source_alignment_is_required_for_derived_planning(prepared, monkeypatch, change):
    root, config = prepared
    if change == "receipt":
        path = root / "preparation.json"
        receipt = json.loads(path.read_text())
        receipt["source_version"] = "0.0.0"
        path.write_text(json.dumps(receipt))
    elif change == "installed":
        monkeypatch.setattr(planning, "version", lambda name: "0.0.0")
    else:

        def mismatch():
            raise PreparationError("source/flat mismatch")

        monkeypatch.setattr(planning, "assert_source_schema_aligned", mismatch)
    with pytest.raises(PreparationError):
        planning.plan_publication(root, config, runner=GitRunner())
    assert not (root / "evidence/planning-inputs.json").exists()
    assert not (root / "evidence/berdl-staging-plan.json").exists()


@pytest.mark.parametrize("capabilities", [["table"], ["column"], []])
def test_missing_mandatory_metadata_capability_refuses_planning(prepared, capabilities):
    root, config = prepared
    path = config.parent / "inventory.json"
    value = json.loads(path.read_text())
    value["metadata_capabilities"] = capabilities
    path.write_text(json.dumps(value))
    with pytest.raises(PreparationError, match="requires table and column"):
        planning.plan_publication(root, config, runner=GitRunner())
    assert not (root / "evidence/planning-inputs.json").exists()
    assert not (root / "evidence/metadata-application-plan.json").exists()


def test_failed_planning_status_retains_verified_preparation_and_correct_recovery(prepared, monkeypatch):
    from functools import partial

    from nmdc_lakehouse import berdl_staging
    from nmdc_lakehouse.publication_staging import publication_status

    root, config = prepared
    receipt = (root / "preparation.json").read_bytes()
    with pytest.raises(BerdlStagingPlanError):
        planning.plan_publication(root, config, runner=GitRunner(dirty=" M core.py"))
    status = publication_status(root)
    assert status["status"] == "prepared"
    assert "planning has not completed" in status["next_action"]
    assert "run or repeat plan-publication" in status["next_action"]
    assert "Send" not in status["next_action"]
    planning.plan_publication(root, config, runner=GitRunner())
    monkeypatch.setattr(
        berdl_staging,
        "revalidate_berdl_staging_plan",
        partial(berdl_staging.revalidate_berdl_staging_plan, runner=GitRunner()),
    )
    assert publication_status(root)["status"] == "planned"
    assert (root / "preparation.json").read_bytes() == receipt
