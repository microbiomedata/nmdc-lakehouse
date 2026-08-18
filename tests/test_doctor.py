"""Tests for credential-free local environment diagnostics."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

from nmdc_lakehouse.doctor import CheckStatus, run_doctor


class FakeRunner:
    """Return deterministic command results without running tools."""

    def __init__(self, responses: dict[tuple[str, ...], tuple[int, str]] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        key = tuple(args)
        self.calls.append(key)
        returncode, stdout = self.responses.get(key, (0, ""))
        return subprocess.CompletedProcess(args=args, returncode=returncode, stdout=stdout, stderr="")


def _healthy_runner(hook_path: Path) -> FakeRunner:
    hook_path.write_text("#!/bin/sh\n", encoding="utf-8")
    hook_path.chmod(0o700)
    return FakeRunner(
        {
            ("uv", "--version"): (0, "uv 0.12.3"),
            ("just", "--version"): (0, "just 1.58.0"),
            ("git", "--version"): (0, "git version 2.50.1"),
            ("git", "config", "--get", "core.hooksPath"): (1, ""),
            ("git", "rev-parse", "--git-path", "hooks/pre-commit"): (0, str(hook_path)),
        }
    )


def _all_commands(_: str) -> str:
    return "/usr/bin/tool"


def test_healthy_unit_environment_exits_zero(tmp_path: Path) -> None:
    report = run_doctor(
        project_root=tmp_path,
        environ={"LAKEHOUSE_ROOT": "./lakehouse"},
        runner=_healthy_runner(tmp_path / "pre-commit"),
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    assert report.exit_code == 0
    assert not [check for check in report.checks if check.status is CheckStatus.FAIL]
    assert any(check.name == "live-mongo-configuration" and check.status is CheckStatus.WARN for check in report.checks)


def test_missing_command_has_targeted_failure(tmp_path: Path) -> None:
    runner = _healthy_runner(tmp_path / "pre-commit")
    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=runner,
        finder=lambda name: None if name == "uv" else "/usr/bin/tool",
        python_version=(3, 13, 13),
    )

    uv_check = next(check for check in report.checks if check.name == "uv")
    assert report.exit_code == 1
    assert uv_check.status is CheckStatus.FAIL
    assert uv_check.remediation == "Install uv, then run just bootstrap."
    assert not any(call[:3] == ("uv", "sync", "--check") for call in runner.calls)
    locked_check = next(check for check in report.checks if check.name == "locked-environment")
    assert "not evaluated" in locked_check.summary


def test_missing_git_skips_hook_commands(tmp_path: Path) -> None:
    runner = _healthy_runner(tmp_path / "pre-commit")
    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=runner,
        finder=lambda name: None if name == "git" else "/usr/bin/tool",
        python_version=(3, 13, 13),
    )

    hook_check = next(check for check in report.checks if check.name == "pre-commit-hook")
    assert hook_check.status is CheckStatus.FAIL
    assert "not evaluated" in hook_check.summary
    assert not any(call[:2] == ("git", "config") for call in runner.calls)


def test_stale_environment_fails_offline_with_bootstrap_remedy(tmp_path: Path) -> None:
    runner = _healthy_runner(tmp_path / "pre-commit")
    sync_args = (
        "uv",
        "sync",
        "--check",
        "--locked",
        "--offline",
        "--extra",
        "dev",
        "--extra",
        "docs",
    )
    runner.responses[sync_args] = (1, "SECRET-IN-RAW-OUTPUT")

    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=runner,
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    check = next(check for check in report.checks if check.name == "locked-environment")
    assert check.status is CheckStatus.FAIL
    assert check.remediation == "Run just bootstrap while package indexes are available."
    assert "--offline" in sync_args
    assert "SECRET-IN-RAW-OUTPUT" not in repr(report)


def test_custom_hooks_path_warns_without_disclosing_path(tmp_path: Path) -> None:
    runner = _healthy_runner(tmp_path / "pre-commit")
    runner.responses[("git", "config", "--get", "core.hooksPath")] = (0, "/secret/hooks")

    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=runner,
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    check = next(check for check in report.checks if check.name == "pre-commit-hook")
    assert check.status is CheckStatus.WARN
    assert "/secret/hooks" not in repr(report)


def test_secret_values_are_never_emitted(tmp_path: Path) -> None:
    secret = "TOP-SECRET-SENTINEL"
    (tmp_path / ".env").write_text(
        f"MONGO_USERNAME=user\nMONGO_PASSWORD={secret}\nNMDC_JUMP_KEY={secret}\nLAKEHOUSE_ROOT=./lakehouse\n",
        encoding="utf-8",
    )

    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=_healthy_runner(tmp_path / "pre-commit"),
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    assert secret not in repr(report)


def test_unsafe_lakehouse_root_fails_without_echoing_value(tmp_path: Path) -> None:
    secret_path = str(tmp_path / ".git")
    report = run_doctor(
        project_root=tmp_path,
        environ={"LAKEHOUSE_ROOT": secret_path},
        runner=_healthy_runner(tmp_path / "pre-commit"),
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    check = next(check for check in report.checks if check.name == "lakehouse-root")
    assert check.status is CheckStatus.FAIL
    assert secret_path not in repr(report)


def test_invalid_path_values_are_sanitized(tmp_path: Path) -> None:
    invalid_path = "TOP-SECRET-SENTINEL\0invalid"
    report = run_doctor(
        project_root=tmp_path,
        environ={"LAKEHOUSE_ROOT": invalid_path, "NMDC_JUMP_KEY": invalid_path},
        runner=_healthy_runner(tmp_path / "pre-commit"),
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    lakehouse_check = next(check for check in report.checks if check.name == "lakehouse-root")
    jump_check = next(check for check in report.checks if check.name == "jump-key-path")
    assert lakehouse_check.status is CheckStatus.FAIL
    assert jump_check.status is CheckStatus.WARN
    assert invalid_path not in repr(report)


def test_malformed_dotenv_fails_without_source_text(tmp_path: Path) -> None:
    secret = "MALFORMED-SECRET-SENTINEL"
    (tmp_path / ".env").write_text(secret, encoding="utf-8")

    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=_healthy_runner(tmp_path / "pre-commit"),
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    check = next(check for check in report.checks if check.name == "unit-configuration")
    assert check.status is CheckStatus.FAIL
    assert "invalid assignment" in check.summary
    assert secret not in repr(report)


def test_unreadable_dotenv_has_targeted_sanitized_failure(tmp_path: Path) -> None:
    secret = "TOP-SECRET-SENTINEL"
    dotenv = tmp_path / ".env"
    dotenv.mkdir()
    (dotenv / "value").write_text(secret, encoding="utf-8")

    report = run_doctor(
        project_root=tmp_path,
        environ={},
        runner=_healthy_runner(tmp_path / "pre-commit"),
        finder=_all_commands,
        python_version=(3, 13, 13),
    )

    check = next(check for check in report.checks if check.name == "unit-configuration")
    assert check.status is CheckStatus.FAIL
    assert "could not be read" in check.summary
    assert "file type and read permissions" in (check.remediation or "")
    assert secret not in repr(report)
