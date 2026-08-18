"""CLI-level smoke tests."""

import importlib
import sys

from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.doctor import CheckStatus, DoctorCheck, DoctorReport


def test_cli_help_exits_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "nmdc-lakehouse" in result.output.lower()


def test_cli_and_doctor_load_without_importing_etl_jobs(monkeypatch):
    monkeypatch.setitem(sys.modules, "nmdc_lakehouse.jobs", None)
    monkeypatch.delitem(sys.modules, "nmdc_lakehouse.cli")

    lightweight_cli = importlib.import_module("nmdc_lakehouse.cli")

    assert "doctor" in lightweight_cli.cli.commands


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
    monkeypatch.setattr("nmdc_lakehouse.doctor.run_doctor", lambda: report)

    result = CliRunner().invoke(cli, ["doctor"])

    assert result.exit_code == 1
    assert "[FAIL] locked-environment: The installed environment is stale." in result.output
    assert "remedy: Run just bootstrap." in result.output
