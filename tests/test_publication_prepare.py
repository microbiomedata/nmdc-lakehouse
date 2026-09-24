"""Exercise preparation reuse, source identity, metadata, and failure recovery."""

from __future__ import annotations

import json
import os
from importlib.metadata import version
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from nmdc_lakehouse import publication_prepare as preparation
from nmdc_lakehouse import target_validation as validation
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.local_provenance import derive_provenance
from nmdc_lakehouse.metadata_bundle import load_metadata_bundle
from tests.test_local_provenance import snapshot


@pytest.fixture
def inputs(tmp_path):
    source = tmp_path / "derived input"
    manifest = derive_provenance(snapshot(tmp_path / "parent"), source)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "source_version": version("nmdc-schema"),
                "snapshot": source.name,
                "namespace": {
                    "name": "nmdc.metadata",
                    "title": "NMDC provenance",
                    "description": "Reviewed provenance.",
                },
            }
        )
    )
    return config, source, manifest, tmp_path / "prepared"


def no_revalidation(*_args, **_kwargs):
    pytest.fail("Completed full row validation must not be repeated")


def test_prepare_and_resume_retain_exact_inputs_and_metadata(inputs, monkeypatch):
    config, source, manifest, output = inputs
    original = {p.name: p.read_bytes() for p in source.iterdir()}
    receipt = preparation.prepare_publication(config, output)
    assert receipt["snapshot_id"] == manifest.snapshot_id
    assert receipt["parent_snapshot_id"] == manifest.parent_snapshot_id
    assert receipt["tables"] == 2
    assert receipt["rows"] == 14
    bundle = load_metadata_bundle(output / "evidence/metadata-bundle.json")
    assert len(bundle.tables) == 2
    assert all(t.description.value and all(c.description.value for c in t.columns) for t in bundle.tables)
    assert {p.name: p.read_bytes() for p in source.iterdir()} == original
    before = {p.name: p.read_bytes() for p in (output / "evidence").iterdir()}
    monkeypatch.setattr(validation, "validate_target_snapshot", no_revalidation)
    assert preparation.prepare_publication(config, output) == receipt
    assert {p.name: p.read_bytes() for p in (output / "evidence").iterdir()} == before


def test_reuse_full_report_and_reviewed_profile(inputs, monkeypatch):
    config, source, manifest, output = inputs
    report_path = config.parent / "existing-validation.json"
    report = validation.validate_target_snapshot(source, requested_mode="full")
    validation.write_target_validation_report(report_path, report, snapshot_root=source)
    profile_path = config.parent / "profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "profile_format_version": 1,
                "profile_id": "reviewed",
                "snapshot_id": manifest.snapshot_id,
                "namespace": {
                    "name": "nmdc.metadata",
                    "title": "Reviewed title",
                    "description": "Reviewed description.",
                },
                "overrides": [
                    {
                        "table": "graph_edges",
                        "description": "Approved edges.",
                        "rationale": "Reviewed meaning.",
                        "source": "Schema review",
                    }
                ],
            }
        )
    )
    data = json.loads(config.read_text())
    del data["namespace"]
    data.update(target_validation=report_path.name, profile=profile_path.name)
    config.write_text(json.dumps(data))
    monkeypatch.setattr(validation, "validate_target_snapshot", no_revalidation)
    preparation.prepare_publication(config, output)
    bundle = load_metadata_bundle(output / "evidence/metadata-bundle.json")
    assert bundle.profile_id == "reviewed"
    assert (output / "evidence/target-validation.json").read_bytes() == report_path.read_bytes()
    assert next(t for t in bundle.tables if t.name == "graph_edges").description.value == "Approved edges."
    profile_path.write_text(profile_path.read_text().replace("Approved edges.", "Unreviewed edges."))
    with pytest.raises(preparation.PreparationError, match="Existing preparation-inputs.json differs"):
        preparation.prepare_publication(config, output)


def test_a_bounded_report_is_not_accepted_as_full(inputs):
    config, source, _, output = inputs
    path = config.parent / "bounded.json"
    report = validation.validate_target_snapshot(source, requested_mode="bounded")
    validation.write_target_validation_report(path, report, snapshot_root=source)
    data = json.loads(config.read_text())
    data["target_validation"] = path.name
    config.write_text(json.dumps(data))
    with pytest.raises(preparation.PreparationError, match="requires a full"):
        preparation.prepare_publication(config, output)
    assert not (output / "preparation.json").exists()


def test_changed_config_refuses_without_replacing_evidence(inputs):
    config, _, _, output = inputs
    preparation.prepare_publication(config, output)
    before = (output / "preparation.json").read_bytes()
    config.write_text(config.read_text().replace("Reviewed provenance.", "Changed meaning."))
    with pytest.raises(preparation.PreparationError, match="use a new preparation directory"):
        preparation.prepare_publication(config, output)
    assert (output / "preparation.json").read_bytes() == before


def test_wrong_installed_source_fails_before_creating_outputs(inputs, monkeypatch):
    config, _, _, output = inputs
    monkeypatch.setattr(preparation, "version", lambda _name: "0.0.0")
    with pytest.raises(preparation.PreparationError, match="Installed nmdc-schema differs"):
        preparation.prepare_publication(config, output)
    assert not output.exists()


@pytest.mark.parametrize("component", ["root", "evidence", "snapshot", ".prepare.lock"])
def test_output_symlinks_are_refused(inputs, tmp_path, component):
    config, _, _, output = inputs
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    if component == "root":
        output.symlink_to(elsewhere, target_is_directory=True)
    else:
        output.mkdir()
        (output / component).symlink_to(elsewhere)
    with pytest.raises(preparation.PreparationError):
        preparation.prepare_publication(config, output)
    assert list(elsewhere.iterdir()) == []


def test_corrupt_copied_artifact_refuses_resume(inputs):
    config, _, _, output = inputs
    preparation.prepare_publication(config, output)
    (output / "snapshot/graph_edges.parquet").write_bytes(b"changed")
    with pytest.raises(preparation.PreparationError, match="Prepared input differs"):
        preparation.prepare_publication(config, output)


def test_invalid_metadata_can_resume_without_revalidating_rows(inputs, monkeypatch):
    config, _, _, output = inputs
    real_builder = preparation.build_metadata_bundle
    monkeypatch.setattr(
        preparation, "build_metadata_bundle", lambda *_a, **_k: (_ for _ in ()).throw(ValueError("interrupted"))
    )
    with pytest.raises(ValueError, match="interrupted"):
        preparation.prepare_publication(config, output)
    assert (output / "evidence/target-validation.json").is_file()
    assert not (output / "preparation.json").exists()
    monkeypatch.setattr(preparation, "build_metadata_bundle", real_builder)
    monkeypatch.setattr(validation, "validate_target_snapshot", no_revalidation)
    assert preparation.prepare_publication(config, output)["status"] == "prepared"


def test_export_clears_skips_keeps_empty_columns_and_stops_on_failure(tmp_path, monkeypatch):
    calls = []

    def fail(command, **kwargs):
        calls.append((command, kwargs["env"]))
        (tmp_path / "snapshot").mkdir()
        return SimpleNamespace(returncode=1)

    monkeypatch.setenv("LAKEHOUSE_SKIP_COLLECTIONS", "biosample_set")
    monkeypatch.setenv("LAKEHOUSE_DROP_EMPTY_COLS", "true")
    monkeypatch.setattr(preparation.subprocess, "run", fail)
    with pytest.raises(preparation.PreparationError, match="run-job failed"):
        preparation._export(tmp_path, "production")
    assert len(calls) == 1
    assert calls[0][1]["LAKEHOUSE_SKIP_COLLECTIONS"] == ""
    assert calls[0][1]["LAKEHOUSE_DROP_EMPTY_COLS"] == "false"
    assert os.stat(tmp_path / "export.log").st_mode & 0o777 == 0o600
    with pytest.raises(preparation.PreparationError, match="incomplete dump"):
        preparation._export(tmp_path, "production")
    assert len(calls) == 1


def test_preparation_cli_replaces_the_two_metadata_commands(inputs):
    config, _, _, output = inputs
    runner = CliRunner()
    result = runner.invoke(cli, ["prepare-publication", str(config), str(output)])
    assert result.exit_code == 0, result.output
    assert '"status": "prepared"' in result.output
    for old in ("metadata-profile", "metadata-bundle"):
        assert runner.invoke(cli, [old, "--help"]).exit_code != 0


def test_source_selector_reads_configuration_before_loading_package(tmp_path, monkeypatch):
    from scripts.python import prepare_publication as entry

    config = tmp_path / "config with spaces.json"
    config.write_text(json.dumps({"source_version": "11.23.0"}))
    monkeypatch.setattr(entry.sys, "argv", ["prepare_publication.py", str(config), str(tmp_path / "output")])
    monkeypatch.setenv("NMDC_SCHEMA_VERSION", "11.24.0")
    seen = []

    def invoke(command, **kwargs):
        seen.append((command, kwargs))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(entry.subprocess, "run", invoke)
    assert entry.main() == 0
    command, options = seen[0]
    assert options["env"]["NMDC_SCHEMA_VERSION"] == "11.23.0"
    assert "--locked" in command
    assert command[-2] == str(config)
    assert command[-1] == str(tmp_path / "output")
    assert (options["cwd"] / "scripts/uv_with_source.sh").is_file()
