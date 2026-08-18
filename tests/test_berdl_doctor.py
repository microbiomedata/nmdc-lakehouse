"""Tests for non-mutating BERDL publication readiness checks."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from nmdc_lakehouse import berdl_doctor
from nmdc_lakehouse.berdl_doctor import run_berdl_doctor
from nmdc_lakehouse.doctor import CheckStatus
from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError


class FakeRunner:
    """Return sanitized deterministic command results."""

    def __init__(self, checkout: Path, *, python_version: str = "Python 3.13.13") -> None:
        self.checkout = checkout
        self.python = checkout / ".venv-berdl" / "bin" / "python"
        self.python_version = python_version
        self.missing_distributions: set[str] = set()

    def __call__(self, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        command = tuple(args)
        if command[:3] == ("git", "-C", str(self.checkout.resolve())):
            return subprocess.CompletedProcess(args, 0, "a" * 40 + "\n", "")
        if command == (str(self.python), "--version"):
            return subprocess.CompletedProcess(args, 0, self.python_version, "")
        if command[:4] == (str(self.python), "-m", "pip", "show"):
            returncode = int(command[-1] in self.missing_distributions)
            return subprocess.CompletedProcess(args, returncode, "", "TOP-SECRET-RAW-OUTPUT")
        if command == ("/tools/mc", "--version"):
            return subprocess.CompletedProcess(args, 0, "mc version RELEASE.TEST", "")
        return subprocess.CompletedProcess(args, 127, "", "TOP-SECRET-RAW-OUTPUT")


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "beril"
    for relative in berdl_doctor.REQUIRED_BERIL_PATHS:
        path = checkout / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("test resource\n", encoding="utf-8")
    python = checkout / ".venv-berdl" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("test interpreter\n", encoding="utf-8")
    return checkout


def _configuration(**overrides: str) -> dict[str, str]:
    configured = {
        "KBASE_AUTH_TOKEN": "TOP-SECRET-SENTINEL",
        "BERDL_DESTINATION_ID": "nmdc-production",
        "BERDL_CATALOG": "nmdc",
        "BERDL_TABLE_FORMAT": "discovered-format",
    }
    configured.update(overrides)
    return configured


@pytest.fixture(autouse=True)
def valid_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = SimpleNamespace(snapshot_id="sha256:test-snapshot", artifacts=(object(), object()))
    monkeypatch.setattr(berdl_doctor, "validate_snapshot", lambda _root: manifest)


def _run(
    tmp_path: Path,
    checkout: Path | None,
    *,
    service_checks: Sequence[str] = (),
    socket_probe: Callable[[str, int, float], bool] = lambda _host, _port, _timeout: True,
    timeout: float = 1.0,
):
    runner_checkout = checkout or tmp_path / "missing"
    return run_berdl_doctor(
        tmp_path / "snapshot",
        project_root=tmp_path,
        checkout=checkout,
        environ=_configuration(),
        runner=FakeRunner(runner_checkout),
        finder=lambda name: "/tools/mc" if name == "mc" else None,
        service_checks=service_checks,
        socket_probe=socket_probe,
        timeout=timeout,
    )


def test_valid_snapshot_and_compatible_environment_pass(tmp_path: Path) -> None:
    checkout = _checkout(tmp_path)
    report = _run(tmp_path, checkout)

    assert report.exit_code == 0
    assert all(check.status is CheckStatus.PASS for check in report.checks)
    assert "sha256:test-snapshot" in report.checks[0].summary
    assert any("revision aaaaaaaaaaaa" in check.summary for check in report.checks)
    assert any("table-format=discovered-format" in check.summary for check in report.checks)
    assert "TOP-SECRET-SENTINEL" not in repr(report)


def test_invalid_snapshot_fails_without_disclosing_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    checkout = _checkout(tmp_path)

    def reject(_root: Path) -> None:
        raise SnapshotManifestError("TOP-SECRET-SENTINEL")

    monkeypatch.setattr(berdl_doctor, "validate_snapshot", reject)
    report = _run(tmp_path, checkout)

    check = next(check for check in report.checks if check.name == "candidate-snapshot")
    assert check.status is CheckStatus.FAIL
    assert "TOP-SECRET-SENTINEL" not in repr(report)


def test_missing_checkout_has_actionable_failures(tmp_path: Path) -> None:
    report = _run(tmp_path, None)

    checkout_check = next(check for check in report.checks if check.name == "beril-checkout")
    environment_check = next(check for check in report.checks if check.name == "berdl-environment")
    assert checkout_check.status is CheckStatus.FAIL
    assert "BERIL_CHECKOUT" in (checkout_check.remediation or "")
    assert environment_check.status is CheckStatus.FAIL


def test_incompatible_python_and_missing_package_fail(tmp_path: Path) -> None:
    checkout = _checkout(tmp_path)
    runner = FakeRunner(checkout, python_version="Python 3.14.1")
    report = run_berdl_doctor(
        tmp_path / "snapshot",
        project_root=tmp_path,
        checkout=checkout,
        environ=_configuration(),
        runner=runner,
        finder=lambda _name: "/tools/mc",
    )

    check = next(check for check in report.checks if check.name == "berdl-environment")
    assert check.status is CheckStatus.FAIL
    assert "Python 3.13" in (check.remediation or "")

    runner = FakeRunner(checkout)
    runner.missing_distributions.add("data-lakehouse-ingest")
    report = run_berdl_doctor(
        tmp_path / "snapshot",
        project_root=tmp_path,
        checkout=checkout,
        environ=_configuration(),
        runner=runner,
        finder=lambda _name: "/tools/mc",
    )
    package = next(check for check in report.checks if check.name == "data-lakehouse-ingest")
    assert package.status is CheckStatus.FAIL
    assert "TOP-SECRET-RAW-OUTPUT" not in repr(report)


def test_missing_mc_and_configuration_names_fail_safely(tmp_path: Path) -> None:
    checkout = _checkout(tmp_path)
    configured = _configuration(KBASE_AUTH_TOKEN="", BERDL_TABLE_FORMAT="")
    report = run_berdl_doctor(
        tmp_path / "snapshot",
        project_root=tmp_path,
        checkout=checkout,
        environ=configured,
        runner=FakeRunner(checkout),
        finder=lambda _name: None,
    )

    assert next(check for check in report.checks if check.name == "mc").status is CheckStatus.FAIL
    token = next(check for check in report.checks if check.name == "kbase-auth-token")
    destination = next(check for check in report.checks if check.name == "berdl-destination")
    assert token.status is CheckStatus.FAIL
    assert destination.status is CheckStatus.FAIL
    assert "BERDL_TABLE_FORMAT" in destination.summary


def test_proxy_check_is_explicit_bounded_and_non_mutating(tmp_path: Path) -> None:
    checkout = _checkout(tmp_path)
    probes: list[tuple[str, int, float]] = []

    def unavailable(host: str, port: int, timeout: float) -> bool:
        probes.append((host, port, timeout))
        return False

    report = _run(
        tmp_path,
        checkout,
        service_checks=("berdl-proxy",),
        socket_probe=unavailable,
        timeout=0.25,
    )

    proxy = next(check for check in report.checks if check.name == "berdl-proxy")
    assert proxy.status is CheckStatus.FAIL
    assert probes == [("127.0.0.1", 8123, 0.25)]


def test_checkout_dotenv_values_are_used_but_never_reported(tmp_path: Path) -> None:
    checkout = _checkout(tmp_path)
    secret = "DOTENV-SECRET-SENTINEL"
    (checkout / ".env").write_text(f"KBASE_AUTH_TOKEN={secret}\n", encoding="utf-8")
    environment = _configuration()
    environment.pop("KBASE_AUTH_TOKEN")

    report = run_berdl_doctor(
        tmp_path / "snapshot",
        project_root=tmp_path,
        checkout=checkout,
        environ=environment,
        runner=FakeRunner(checkout),
        finder=lambda _name: "/tools/mc",
    )

    assert next(check for check in report.checks if check.name == "kbase-auth-token").status is CheckStatus.PASS
    assert secret not in repr(report)
