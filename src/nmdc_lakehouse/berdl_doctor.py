"""Credential-safe, non-mutating readiness checks for BERDL publication."""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from pathlib import Path

from nmdc_lakehouse.doctor import CheckStatus, DoctorCheck, DoctorReport, DotenvProblem, _read_dotenv
from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError, validate_snapshot

BERDL_SERVICE_CHECKS = ("berdl-proxy",)
DEFAULT_TIMEOUT_SECONDS = 1.0
REQUIRED_BERIL_PATHS = (
    "scripts/bootstrap_client.sh",
    "scripts/bootstrap_ingest.sh",
    "scripts/configure_mc.sh",
    "scripts/ingest_preflight.py",
)
REQUIRED_DESTINATION_NAMES = ("BERDL_DESTINATION_ID", "BERDL_CATALOG", "BERDL_TABLE_FORMAT")

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
CommandFinder = Callable[[str], str | None]
SocketProbe = Callable[[str, int, float], bool]


def _run_command(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(args, text=True, capture_output=True, check=False, timeout=10)  # noqa: S603
    except (OSError, subprocess.TimeoutExpired):
        return subprocess.CompletedProcess(args=args, returncode=127, stdout="", stderr="")


def _snapshot_check(root: Path) -> DoctorCheck:
    try:
        manifest = validate_snapshot(root)
    except SnapshotManifestError:
        return DoctorCheck(
            name="candidate-snapshot",
            status=CheckStatus.FAIL,
            summary="The candidate snapshot failed offline validation.",
            remediation="Run nmdc-lakehouse validate-snapshot and resolve its sanitized diagnostic.",
        )
    return DoctorCheck(
        name="candidate-snapshot",
        status=CheckStatus.PASS,
        summary=(
            f"Snapshot {manifest.snapshot_id} passed offline validation with {len(manifest.artifacts)} artifact(s)."
        ),
    )


def _checkout_check(checkout: Path | None, runner: CommandRunner) -> DoctorCheck:
    if checkout is None:
        return DoctorCheck(
            name="beril-checkout",
            status=CheckStatus.FAIL,
            summary="No BERIL research-observatory checkout is configured.",
            remediation=("Set BERIL_CHECKOUT to the selected checkout; the doctor will not clone or change it."),
        )
    try:
        resolved = checkout.expanduser().resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        resolved = None
    if resolved is None or not resolved.is_dir():
        return DoctorCheck(
            name="beril-checkout",
            status=CheckStatus.FAIL,
            summary="The configured BERIL checkout is unavailable or is not a directory.",
            remediation="Set BERIL_CHECKOUT to the root of BERIL-research-observatory.",
        )
    missing = [relative for relative in REQUIRED_BERIL_PATHS if not (resolved / relative).is_file()]
    if missing:
        return DoctorCheck(
            name="beril-checkout",
            status=CheckStatus.FAIL,
            summary="The configured checkout lacks required ingest resource paths: " + ", ".join(missing) + ".",
            remediation="Use a current BERIL-research-observatory checkout containing the documented ingest scripts.",
        )
    completed = runner(("git", "-C", str(resolved), "rev-parse", "--verify", "HEAD"))
    revision = completed.stdout.strip()
    if completed.returncode != 0 or re.fullmatch(r"[0-9a-fA-F]{40}", revision) is None:
        return DoctorCheck(
            name="beril-checkout",
            status=CheckStatus.FAIL,
            summary="The BERIL checkout revision could not be identified.",
            remediation="Repair the checkout with Git outside this command, then rerun the doctor.",
        )
    return DoctorCheck(
        name="beril-checkout",
        status=CheckStatus.PASS,
        summary=f"The BERIL checkout has the required ingest resource paths at revision {revision[:12]}.",
    )


def _environment_python(checkout: Path) -> Path | None:
    candidates = (checkout / ".venv-berdl" / "bin" / "python", checkout / ".venv-berdl" / "Scripts" / "python.exe")
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def _ingest_environment_checks(checkout: Path | None, runner: CommandRunner) -> list[DoctorCheck]:
    if checkout is None:
        return [
            DoctorCheck(
                name="berdl-environment",
                status=CheckStatus.FAIL,
                summary="The dedicated BERDL environment cannot be checked without a configured checkout.",
                remediation="Configure BERIL_CHECKOUT, then rerun the doctor.",
            )
        ]
    python = _environment_python(checkout.expanduser())
    if python is None:
        return [
            DoctorCheck(
                name="berdl-environment",
                status=CheckStatus.FAIL,
                summary="The checkout has no dedicated .venv-berdl Python interpreter.",
                remediation=(
                    "Provision the BERDL client and ingest environment with Python 3.13 in the external checkout."
                ),
            )
        ]
    version_result = runner((str(python), "--version"))
    version_match = re.search(r"\b(\d+)\.(\d+)(?:\.\d+)?\b", version_result.stdout + version_result.stderr)
    if version_result.returncode != 0 or version_match is None or tuple(map(int, version_match.groups())) != (3, 13):
        return [
            DoctorCheck(
                name="berdl-environment",
                status=CheckStatus.FAIL,
                summary="The dedicated BERDL environment does not use the required Python 3.13 minor.",
                remediation="Recreate .venv-berdl with Python 3.13, then rerun both BERIL bootstrap scripts.",
            )
        ]

    checks = [
        DoctorCheck(
            name="berdl-environment",
            status=CheckStatus.PASS,
            summary="The dedicated BERDL environment uses Python 3.13.",
        )
    ]
    for distribution in ("data-lakehouse-ingest", "berdl-remote"):
        completed = runner((str(python), "-m", "pip", "show", distribution))
        status = CheckStatus.PASS if completed.returncode == 0 else CheckStatus.FAIL
        checks.append(
            DoctorCheck(
                name=distribution,
                status=status,
                summary=(
                    f"The dedicated environment contains {distribution}."
                    if status is CheckStatus.PASS
                    else f"The dedicated environment is missing {distribution}."
                ),
                remediation=(
                    None
                    if status is CheckStatus.PASS
                    else "Run the appropriate BERIL bootstrap script in the external checkout, then retry."
                ),
            )
        )
    return checks


def _mc_check(finder: CommandFinder, runner: CommandRunner) -> DoctorCheck:
    executable = finder("mc")
    if executable is None:
        return DoctorCheck(
            name="mc",
            status=CheckStatus.FAIL,
            summary="The MinIO client command is unavailable.",
            remediation=(
                "Install mc through a trusted package manager and configure it separately in the BERIL checkout."
            ),
        )
    completed = runner((executable, "--version"))
    if completed.returncode != 0:
        return DoctorCheck(
            name="mc",
            status=CheckStatus.FAIL,
            summary="The MinIO client command did not report a usable version.",
            remediation="Repair or reinstall mc without changing this repository.",
        )
    return DoctorCheck(name="mc", status=CheckStatus.PASS, summary="The MinIO client command is available.")


def _configuration_checks(configured: Mapping[str, str], dotenv_problem: DotenvProblem | None) -> list[DoctorCheck]:
    if dotenv_problem is not None:
        return [
            DoctorCheck(
                name="berdl-configuration",
                status=CheckStatus.FAIL,
                summary="A relevant .env file is unreadable or malformed; no partial configuration was used.",
                remediation="Repair the local .env syntax without sharing or committing its values.",
            )
        ]
    checks: list[DoctorCheck] = []
    token_present = bool(configured.get("KBASE_AUTH_TOKEN", "").strip())
    checks.append(
        DoctorCheck(
            name="kbase-auth-token",
            status=CheckStatus.PASS if token_present else CheckStatus.FAIL,
            summary=(
                "KBASE_AUTH_TOKEN is present; its value and freshness were not inspected."
                if token_present
                else "KBASE_AUTH_TOKEN is absent or blank."
            ),
            remediation=(
                "Refresh the short-lived token through the supported KBase workflow immediately before publication."
                if token_present
                else "Configure KBASE_AUTH_TOKEN through the supported KBase workflow; do not commit it."
            ),
        )
    )
    missing = [name for name in REQUIRED_DESTINATION_NAMES if not configured.get(name, "").strip()]
    if missing:
        checks.append(
            DoctorCheck(
                name="berdl-destination",
                status=CheckStatus.FAIL,
                summary="Required destination configuration names are absent or blank: " + ", ".join(missing) + ".",
                remediation="Set the logical destination, discovered catalog, and discovered table format explicitly.",
            )
        )
    else:
        destination = configured["BERDL_DESTINATION_ID"].strip()
        catalog = configured["BERDL_CATALOG"].strip()
        table_format = configured["BERDL_TABLE_FORMAT"].strip()
        checks.append(
            DoctorCheck(
                name="berdl-destination",
                status=CheckStatus.PASS,
                summary=f"Destination={destination}; catalog={catalog}; table-format={table_format}.",
            )
        )
    return checks


def _default_socket_probe(host: str, port: int, timeout: float) -> bool:
    try:
        with closing(socket.create_connection((host, port), timeout=timeout)):
            return True
    except OSError:
        return False


def _proxy_check(configured: Mapping[str, str], probe: SocketProbe, timeout: float) -> DoctorCheck:
    host = configured.get("BERDL_PROXY_HOST", "127.0.0.1").strip()
    try:
        port = int(configured.get("BERDL_PROXY_PORT", "8123"))
        if host not in {"localhost", "127.0.0.1", "::1"} or not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        return DoctorCheck(
            name="berdl-proxy",
            status=CheckStatus.FAIL,
            summary="The BERDL proxy settings are not a valid loopback host and TCP port.",
            remediation="Configure BERDL_PROXY_HOST as loopback and BERDL_PROXY_PORT as the local proxy port.",
        )
    if not probe(host, port, timeout):
        return DoctorCheck(
            name="berdl-proxy",
            status=CheckStatus.FAIL,
            summary="No service is reachable on the configured local BERDL proxy port.",
            remediation="Start the externally managed BERDL proxy, then rerun this explicit service check.",
        )
    return DoctorCheck(
        name="berdl-proxy",
        status=CheckStatus.PASS,
        summary="A TCP service is reachable on the configured local BERDL proxy port.",
    )


def run_berdl_doctor(
    snapshot_root: Path,
    *,
    project_root: Path,
    checkout: Path | None,
    environ: Mapping[str, str] | None = None,
    service_checks: Sequence[str] = (),
    runner: CommandRunner = _run_command,
    finder: CommandFinder = shutil.which,
    socket_probe: SocketProbe = _default_socket_probe,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DoctorReport:
    """Inspect publication readiness without changing local or remote state."""
    unknown = set(service_checks) - set(BERDL_SERVICE_CHECKS)
    if unknown:
        raise ValueError("Unknown BERDL service check(s): " + ", ".join(sorted(unknown)) + ".")
    environment = dict(os.environ if environ is None else environ)
    checkout_values: dict[str, str] = {}
    checkout_problem = None
    if checkout is not None:
        checkout_values, checkout_problem = _read_dotenv(checkout.expanduser() / ".env")
    project_values, project_problem = _read_dotenv(project_root / ".env")
    dotenv_problem = checkout_problem or project_problem
    configured = {**checkout_values, **project_values, **environment} if dotenv_problem is None else {}

    checks = [_snapshot_check(snapshot_root), _checkout_check(checkout, runner)]
    checks.extend(_ingest_environment_checks(checkout, runner))
    checks.append(_mc_check(finder, runner))
    checks.extend(_configuration_checks(configured, dotenv_problem))
    if "berdl-proxy" in service_checks:
        checks.append(_proxy_check(configured, socket_probe, timeout))
    return DoctorReport(checks=tuple(checks))
