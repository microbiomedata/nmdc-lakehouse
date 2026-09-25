"""Build the isolated pod runtime that plans and stages a prepared publication.

Run it with the pod's own Python 3.13 from a fresh clone that is already checked
out at the reviewed commit. It uses only the standard library, because no project
environment exists yet. It refuses to reuse an existing runtime: after a failure,
start again in a new directory.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

INGEST_URL = "https://github.com/kbase/data-lakehouse-ingest.git"
# Keep in step with _SUPPORTED_INGEST_REVISIONS in src/nmdc_lakehouse/berdl_staging.py;
# the planner refuses any other revision.
INGEST_REVISION = "a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3"
UV_VERSION = "0.12.17"
SOURCE_VERSIONS = ("11.23.0", "11.24.0")
IMPORT_CHECK = (
    "from berdl_notebook_utils.setup_spark_session import get_spark_session; "
    "from berdl_notebook_utils.clients import get_s3_client; "
    "import nmdc_lakehouse"
)
COMMIT = re.compile(r"[0-9a-f]{40}")


class SetupError(Exception):
    """A precondition or step failed; the message says which."""


def run(step: str, command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
    """Run one setup step, naming it if it fails."""
    print(f"==> {step}", flush=True)
    result = subprocess.run(command, cwd=cwd, env=env, check=False)
    if result.returncode != 0:
        raise SetupError(f"{step} failed with exit status {result.returncode}.")


def git_output(checkout: Path, *arguments: str) -> str:
    """Return the stdout of a read-only git command."""
    result = subprocess.run(["git", "-C", str(checkout), *arguments], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise SetupError(f"git {' '.join(arguments)} failed in {checkout}.")
    return result.stdout


def check_interpreter() -> Path:
    """Require the pod's base Python 3.13, not a virtual environment's copy."""
    if sys.version_info[:2] != (3, 13):
        raise SetupError("Run this with the pod's Python 3.13.")
    if sys.prefix != sys.base_prefix:
        raise SetupError(
            "Run this with the pod's own python3, not from inside a virtual environment; "
            "the runtime inherits the pod's Spark and object-store packages from it."
        )
    return Path(sys.executable)


def check_checkout(checkout: Path, commit: str) -> None:
    """Require a clean clone checked out at exactly the reviewed commit."""
    head = git_output(checkout, "rev-parse", "HEAD").strip()
    if head != commit:
        raise SetupError(f"The checkout is at {head}, not the reviewed commit {commit}.")
    if git_output(checkout, "status", "--porcelain", "-z", "--untracked-files=all"):
        raise SetupError("The checkout has local changes or untracked files; use a fresh clone.")
    for name in (".tools", ".venv"):
        if (checkout / name).exists():
            raise SetupError(f"{checkout / name} already exists; set up a new runtime directory.")


def setup(checkout: Path, commit: str, source_version: str) -> Path:
    """Build the runtime and return the ingest checkout path."""
    python = check_interpreter()
    check_checkout(checkout, commit)
    ingest = checkout.parent / "data-lakehouse-ingest"
    if ingest.exists():
        raise SetupError(f"{ingest} already exists; set up a new runtime directory.")

    run("Clone the official ingest code", ["git", "clone", INGEST_URL, str(ingest)], cwd=checkout.parent)
    run(
        f"Check out ingest revision {INGEST_REVISION}",
        ["git", "-C", str(ingest), "checkout", "--detach", INGEST_REVISION],
        cwd=checkout,
    )
    if git_output(ingest, "rev-parse", "HEAD").strip() != INGEST_REVISION:
        raise SetupError("The ingest checkout is not at the approved revision.")

    run("Create the tool environment", [str(python), "-m", "venv", ".tools"], cwd=checkout)
    tools = checkout / ".tools" / "bin"
    run(
        f"Install uv {UV_VERSION}",
        [str(tools / "python"), "-m", "pip", "install", "--no-user", f"uv=={UV_VERSION}"],
        cwd=checkout,
    )
    run(
        "Create the project environment with the pod's packages",
        [str(tools / "uv"), "venv", "--system-site-packages", "--python", str(python), ".venv"],
        cwd=checkout,
    )
    env = dict(os.environ, NMDC_SCHEMA_VERSION=source_version, PATH=f"{tools}{os.pathsep}{os.environ.get('PATH', '')}")
    run(
        f"Install the locked dependencies for source {source_version}",
        ["bash", "scripts/uv_with_source.sh", "sync", "--locked"],
        cwd=checkout,
        env=env,
    )
    run(
        "Check the runtime imports",
        [str(checkout / ".venv" / "bin" / "python"), "-c", IMPORT_CHECK],
        cwd=checkout,
    )
    return ingest


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, build the runtime, and report where it is."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--commit", required=True, help="Reviewed 40-character nmdc-lakehouse commit.")
    parser.add_argument(
        "--source-version", required=True, choices=SOURCE_VERSIONS, help="Source version recorded in preparation.json."
    )
    args = parser.parse_args(argv)
    if not COMMIT.fullmatch(args.commit):
        parser.error("--commit must be a full 40-character lowercase commit hash.")
    checkout = Path(__file__).resolve().parents[2]
    try:
        ingest = setup(checkout, args.commit, args.source_version)
    except SetupError as error:
        print(f"Setup stopped: {error}", file=sys.stderr)
        return 1
    print(f"Runtime ready: {checkout}")
    print(f"Ingest checkout for destination.json: {ingest}")
    cli = checkout / ".venv" / "bin" / "nmdc-lakehouse"
    print(f"Plan with: {cli} plan-publication PUBLICATION_ROOT DESTINATION_JSON")
    return 0


if __name__ == "__main__":
    sys.exit(main())
