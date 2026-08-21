"""CLI-level smoke tests."""

import importlib
import sys

from click.testing import CliRunner

from nmdc_lakehouse import __version__
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.doctor import CheckStatus, DoctorCheck, DoctorReport


def test_cli_help_exits_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "nmdc-lakehouse" in result.output.lower()


def test_cli_version_matches_package_version():
    result = CliRunner().invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert __version__ in result.output


def test_cli_and_doctor_load_without_importing_etl_jobs(monkeypatch):
    monkeypatch.setitem(sys.modules, "nmdc_lakehouse.jobs", None)
    monkeypatch.delitem(sys.modules, "nmdc_lakehouse.cli")

    lightweight_cli = importlib.import_module("nmdc_lakehouse.cli")

    assert "doctor" in lightweight_cli.cli.commands
    assert "berdl-doctor" in lightweight_cli.cli.commands


def test_doctor_cli_renders_sanitized_failure_and_exit_code(monkeypatch):
    report = DoctorReport(
        checks=(
            DoctorCheck(
                name="locked-environment",
                status=CheckStatus.FAIL,
                summary="The installed environment is stale.",
                remediation="Run just bootstrap.",
            ),
        )
    )
    monkeypatch.setattr("nmdc_lakehouse.doctor.run_doctor", lambda **_kwargs: report)

    result = CliRunner().invoke(cli, ["doctor"])

    assert result.exit_code == 1
    assert "[FAIL] locked-environment: The installed environment is stale." in result.output
    assert "remedy: Run just bootstrap." in result.output


def test_doctor_cli_forwards_explicit_service_checks(monkeypatch):
    captured: dict[str, tuple[str, ...]] = {}

    def fake_doctor(*, service_checks: tuple[str, ...]) -> DoctorReport:
        captured["service_checks"] = service_checks
        return DoctorReport(checks=())

    monkeypatch.setattr("nmdc_lakehouse.doctor.run_doctor", fake_doctor)

    result = CliRunner().invoke(
        cli,
        ["doctor", "--service-check", "gcp-tunnel", "--service-check", "mongo-ping"],
    )

    assert result.exit_code == 0
    assert captured["service_checks"] == ("gcp-tunnel", "mongo-ping")


def test_berdl_doctor_cli_forwards_paths_and_service_checks(monkeypatch):
    captured: dict[str, object] = {}

    def fake_doctor(snapshot_root, *, project_root, checkout, service_checks):
        captured.update(
            snapshot_root=snapshot_root,
            project_root=project_root,
            checkout=checkout,
            service_checks=service_checks,
        )
        return DoctorReport(checks=())

    monkeypatch.setattr("nmdc_lakehouse.berdl_doctor.run_berdl_doctor", fake_doctor)

    result = CliRunner().invoke(
        cli,
        [
            "berdl-doctor",
            "snapshot",
            "--beril-checkout",
            "beril",
            "--service-check",
            "berdl-proxy",
        ],
    )

    assert result.exit_code == 0
    assert str(captured["snapshot_root"]) == "snapshot"
    assert str(captured["checkout"]) == "beril"
    assert captured["service_checks"] == ("berdl-proxy",)


def test_berdl_doctor_runs_without_a_beril_checkout(tmp_path, monkeypatch) -> None:
    """The bug was `required=True` on the CLI option, so the regression test belongs at the CLI.

    The library already accepted `checkout=None`, so a library-level test would not have caught
    the original bug and would not catch someone reinstating the flag. Click exits 2 on a usage
    error, which is what this asserts against. See #267.
    """
    monkeypatch.delenv("BERIL_CHECKOUT", raising=False)

    result = CliRunner().invoke(cli, ["berdl-doctor", str(tmp_path / "no-such-snapshot")])

    assert "Missing option" not in result.output
    assert result.exit_code != 2, "exit 2 is Click's usage error, which is the bug this closes"
