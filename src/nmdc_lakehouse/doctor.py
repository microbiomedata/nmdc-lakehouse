"""Credential-free, read-only diagnostics for a development checkout."""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

MIN_UV_VERSION = (0, 12, 3)
REQUIRED_JUST_VERSION = (1, 58, 0)
REQUIRED_PYTHON_MINOR = (3, 13)
OPTIONAL_LIVE_MONGO_NAMES = ("MONGO_USERNAME", "MONGO_PASSWORD")

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
CommandFinder = Callable[[str], str | None]


class CheckStatus(str, Enum):
    """Severity and exit-policy status for one diagnostic."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class DotenvProblem(str, Enum):
    """Sanitized reason an optional dotenv file could not be used."""

    SYNTAX = "syntax"
    UNREADABLE = "unreadable"


@dataclass(frozen=True)
class DoctorCheck:
    """One sanitized diagnostic result."""

    name: str
    status: CheckStatus
    summary: str
    remediation: str | None = None


@dataclass(frozen=True)
class DoctorReport:
    """Complete local-development readiness report."""

    checks: tuple[DoctorCheck, ...]

    @property
    def exit_code(self) -> int:
        """Return nonzero only when a required check fails."""
        return int(any(check.status is CheckStatus.FAIL for check in self.checks))


def _run_command(args: Sequence[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a local command without exposing its output to the caller's terminal."""
    try:
        return subprocess.run(args, cwd=cwd, text=True, capture_output=True, check=False)  # noqa: S603
    except OSError:
        return subprocess.CompletedProcess(args=args, returncode=127, stdout="", stderr="")


def _version_tuple(output: str) -> tuple[int, ...] | None:
    match = re.search(r"\b(\d+)\.(\d+)(?:\.(\d+))?\b", output)
    if not match:
        return None
    return tuple(int(part) for part in match.groups(default="0"))


def _command_version_check(
    *,
    name: str,
    args: Sequence[str],
    finder: CommandFinder,
    runner: CommandRunner,
    minimum: tuple[int, ...] | None = None,
    exact: tuple[int, ...] | None = None,
) -> DoctorCheck:
    if finder(name) is None:
        return DoctorCheck(
            name=name,
            status=CheckStatus.FAIL,
            summary=f"Required command '{name}' is unavailable.",
            remediation=f"Install {name}, then run just bootstrap.",
        )

    completed = runner(args)
    version = _version_tuple(completed.stdout)
    if completed.returncode != 0 or version is None:
        return DoctorCheck(
            name=name,
            status=CheckStatus.FAIL,
            summary=f"Could not determine the installed {name} version.",
            remediation=f"Reinstall {name} and rerun just doctor.",
        )
    if minimum is not None and version < minimum:
        required = ".".join(str(part) for part in minimum)
        return DoctorCheck(
            name=name,
            status=CheckStatus.FAIL,
            summary=f"The installed {name} version is older than the supported minimum.",
            remediation=f"Upgrade {name} to {required} or newer.",
        )
    if exact is not None and version != exact:
        required = ".".join(str(part) for part in exact)
        return DoctorCheck(
            name=name,
            status=CheckStatus.FAIL,
            summary=f"The installed {name} version does not match repository policy.",
            remediation=f"Install {name} {required}.",
        )
    return DoctorCheck(name=name, status=CheckStatus.PASS, summary=f"{name} version satisfies repository policy.")


def _python_check(version_info: tuple[int, int, int]) -> DoctorCheck:
    if version_info[:2] != REQUIRED_PYTHON_MINOR:
        return DoctorCheck(
            name="python",
            status=CheckStatus.FAIL,
            summary="The active Python minor version is unsupported.",
            remediation="Run just bootstrap so uv selects the checked-in Python 3.13 policy.",
        )
    return DoctorCheck(
        name="python", status=CheckStatus.PASS, summary="The active interpreter uses the supported Python 3.13 minor."
    )


def _environment_sync_check(runner: CommandRunner) -> DoctorCheck:
    completed = runner(
        (
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
    )
    if completed.returncode != 0:
        return DoctorCheck(
            name="locked-environment",
            status=CheckStatus.FAIL,
            summary="The installed environment is missing or differs from the lock file.",
            remediation="Run just bootstrap while package indexes are available.",
        )
    return DoctorCheck(
        name="locked-environment",
        status=CheckStatus.PASS,
        summary="The installed development and documentation environment matches the lock file.",
    )


def _prerequisite_failure_check(*, name: str, prerequisite: str) -> DoctorCheck:
    return DoctorCheck(
        name=name,
        status=CheckStatus.FAIL,
        summary=f"This check was not evaluated because the required {prerequisite} command failed its diagnostic.",
        remediation=f"Resolve the {prerequisite} diagnostic above, then rerun just doctor.",
    )


def _pre_commit_check(runner: CommandRunner, *, project_root: Path) -> DoctorCheck:
    configured = runner(("git", "config", "--get", "core.hooksPath"))
    if configured.returncode == 0:
        return DoctorCheck(
            name="pre-commit-hook",
            status=CheckStatus.WARN,
            summary="A custom Git hooks path is active; repository hook installation is intentionally not assumed.",
            remediation="Run uv run pre-commit run --all-files, or explicitly chain it from the custom hook policy.",
        )

    hook_path_result = runner(("git", "rev-parse", "--git-path", "hooks/pre-commit"))
    if hook_path_result.returncode != 0:
        return DoctorCheck(
            name="pre-commit-hook",
            status=CheckStatus.FAIL,
            summary="The repository pre-commit hook path could not be determined.",
            remediation="Run just bootstrap from a Git checkout.",
        )
    hook_path = Path(hook_path_result.stdout.strip())
    if not hook_path.is_absolute():
        hook_path = project_root / hook_path
    if not hook_path.is_file() or not os.access(hook_path, os.X_OK):
        return DoctorCheck(
            name="pre-commit-hook",
            status=CheckStatus.FAIL,
            summary="The repository pre-commit hook is not installed and executable.",
            remediation="Run just bootstrap.",
        )
    return DoctorCheck(
        name="pre-commit-hook", status=CheckStatus.PASS, summary="The repository pre-commit hook is installed."
    )


def _read_dotenv(path: Path) -> tuple[dict[str, str], DotenvProblem | None]:
    """Read dotenv names and internal values without returning source text."""
    if not path.exists():
        return {}, None
    values: dict[str, str] = {}
    malformed = False
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return {}, DotenvProblem.UNREADABLE
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if "=" not in stripped:
            malformed = True
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            malformed = True
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values, DotenvProblem.SYNTAX if malformed else None


def _configuration_checks(*, project_root: Path, environ: Mapping[str, str]) -> list[DoctorCheck]:
    dotenv_values, problem = _read_dotenv(project_root / ".env")
    configured = {**dotenv_values, **environ}
    checks: list[DoctorCheck] = []

    if problem is DotenvProblem.UNREADABLE:
        checks.append(
            DoctorCheck(
                name="unit-configuration",
                status=CheckStatus.FAIL,
                summary="The optional .env file exists but could not be read.",
                remediation="Check the .env file type and read permissions without committing or sharing its values.",
            )
        )
    elif problem is DotenvProblem.SYNTAX:
        checks.append(
            DoctorCheck(
                name="unit-configuration",
                status=CheckStatus.FAIL,
                summary="The optional .env file contains an invalid assignment.",
                remediation="Fix the .env syntax without committing the file or its values.",
            )
        )
    elif (project_root / ".env").exists():
        checks.append(
            DoctorCheck(
                name="unit-configuration",
                status=CheckStatus.PASS,
                summary="The optional .env file is readable; unit development requires no variable names.",
            )
        )
    else:
        checks.append(
            DoctorCheck(
                name="unit-configuration",
                status=CheckStatus.PASS,
                summary="No .env file is present, which is valid for unit development.",
            )
        )

    missing_optional = tuple(name for name in OPTIONAL_LIVE_MONGO_NAMES if not configured.get(name))
    if missing_optional:
        checks.append(
            DoctorCheck(
                name="live-mongo-configuration",
                status=CheckStatus.WARN,
                summary="Optional live-MongoDB variable names are absent or empty: "
                + ", ".join(missing_optional)
                + ".",
                remediation="Configure only the production-data profile you intend to use; see issue #155.",
            )
        )
    else:
        checks.append(
            DoctorCheck(
                name="live-mongo-configuration",
                status=CheckStatus.PASS,
                summary="Optional live-MongoDB variable names are present; values were not inspected or tested.",
            )
        )

    checks.append(_lakehouse_root_check(configured.get("LAKEHOUSE_ROOT", "./lakehouse"), project_root))
    checks.append(_jump_key_check(configured.get("NMDC_JUMP_KEY")))
    return checks


def _lakehouse_root_check(value: str, project_root: Path) -> DoctorCheck:
    remediation = "Set LAKEHOUSE_ROOT to a dedicated local directory, such as ./lakehouse."
    if not value or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value):
        return DoctorCheck(
            name="lakehouse-root",
            status=CheckStatus.FAIL,
            summary="LAKEHOUSE_ROOT is not a supported local directory path.",
            remediation=remediation,
        )
    try:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = project_root / candidate
        candidate = candidate.resolve(strict=False)
        project_root = project_root.resolve()
    except (OSError, RuntimeError, ValueError):
        return DoctorCheck(
            name="lakehouse-root",
            status=CheckStatus.FAIL,
            summary="LAKEHOUSE_ROOT is not a valid local directory path.",
            remediation=remediation,
        )
    git_path = project_root / ".git"
    unsafe = (
        candidate == Path(candidate.anchor)
        or candidate == project_root
        or candidate == git_path
        or git_path in candidate.parents
    )
    if unsafe or (candidate.exists() and not candidate.is_dir()):
        return DoctorCheck(
            name="lakehouse-root",
            status=CheckStatus.FAIL,
            summary="LAKEHOUSE_ROOT targets an unsafe or non-directory location.",
            remediation=remediation,
        )
    return DoctorCheck(
        name="lakehouse-root", status=CheckStatus.PASS, summary="LAKEHOUSE_ROOT is a safe local directory path."
    )


def _jump_key_check(value: str | None) -> DoctorCheck:
    if not value:
        return DoctorCheck(
            name="jump-key-path",
            status=CheckStatus.WARN,
            summary="NMDC_JUMP_KEY is not configured; it is optional for unit development.",
            remediation="Configure it only for the opt-in GCP tunnel checks in issue #155.",
        )
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value):
        return DoctorCheck(
            name="jump-key-path",
            status=CheckStatus.WARN,
            summary="NMDC_JUMP_KEY is not a local file path.",
            remediation="Set NMDC_JUMP_KEY to a private local key file before live-service checks.",
        )
    try:
        key_path = Path(value).expanduser()
        key_is_file = key_path.is_file()
    except (OSError, RuntimeError, ValueError):
        key_is_file = False
        key_path = None
    if not key_is_file or key_path is None:
        return DoctorCheck(
            name="jump-key-path",
            status=CheckStatus.WARN,
            summary="The configured NMDC_JUMP_KEY file is unavailable.",
            remediation="Install the key with owner-only access before live-service checks.",
        )
    try:
        permissions = stat.S_IMODE(key_path.stat().st_mode)
    except (OSError, ValueError):
        permissions = 0o777
    if permissions & 0o077:
        return DoctorCheck(
            name="jump-key-path",
            status=CheckStatus.WARN,
            summary="The configured NMDC_JUMP_KEY permissions are broader than owner-only.",
            remediation="Restrict the key to owner-only access before opening a tunnel.",
        )
    return DoctorCheck(
        name="jump-key-path",
        status=CheckStatus.PASS,
        summary="The optional NMDC_JUMP_KEY path exists with owner-only permissions.",
    )


def run_doctor(
    *,
    project_root: Path | None = None,
    environ: Mapping[str, str] | None = None,
    runner: CommandRunner | None = None,
    finder: CommandFinder = shutil.which,
    python_version: tuple[int, int, int] | None = None,
    service_checks: Sequence[str] = (),
) -> DoctorReport:
    """Run offline diagnostics plus any explicitly requested live-service checks."""
    root = (project_root or Path.cwd()).resolve()
    command_runner = runner or (lambda args: _run_command(args, cwd=root))
    environment = dict(os.environ if environ is None else environ)
    version_info = python_version or (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)

    uv_check = _command_version_check(
        name="uv",
        args=("uv", "--version"),
        finder=finder,
        runner=command_runner,
        minimum=MIN_UV_VERSION,
    )
    just_check = _command_version_check(
        name="just",
        args=("just", "--version"),
        finder=finder,
        runner=command_runner,
        exact=REQUIRED_JUST_VERSION,
    )
    git_check = _command_version_check(name="git", args=("git", "--version"), finder=finder, runner=command_runner)
    environment_check = (
        _environment_sync_check(command_runner)
        if uv_check.status is CheckStatus.PASS
        else _prerequisite_failure_check(name="locked-environment", prerequisite="uv")
    )
    hook_check = (
        _pre_commit_check(command_runner, project_root=root)
        if git_check.status is CheckStatus.PASS
        else _prerequisite_failure_check(name="pre-commit-hook", prerequisite="git")
    )
    checks = [uv_check, just_check, git_check, _python_check(version_info), environment_check, hook_check]
    checks.extend(_configuration_checks(project_root=root, environ=environment))
    if service_checks:
        from nmdc_lakehouse.service_doctor import run_service_checks

        dotenv_values, dotenv_problem = _read_dotenv(root / ".env")
        if dotenv_problem is not None:
            checks.append(
                DoctorCheck(
                    name="live-service-checks",
                    status=CheckStatus.FAIL,
                    summary="Requested live-service checks were not run because .env is invalid.",
                    remediation="Repair or remove .env, then rerun the requested service checks.",
                )
            )
        else:
            configured = {**dotenv_values, **environment}
            checks.extend(run_service_checks(service_checks, configured=configured))
    return DoctorReport(checks=tuple(checks))
