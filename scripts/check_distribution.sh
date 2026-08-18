#!/usr/bin/env bash
# Build release archives, install only the wheel, and verify public metadata.
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT

uv build "$repo_root" --out-dir "$scratch/dist"

wheels=("$scratch"/dist/*.whl)
sdists=("$scratch"/dist/*.tar.gz)
if [[ ${#wheels[@]} -ne 1 || ! -f "${wheels[0]}" ]]; then
    echo "Expected exactly one wheel in the temporary build directory." >&2
    exit 1
fi
if [[ ${#sdists[@]} -ne 1 || ! -f "${sdists[0]}" ]]; then
    echo "Expected exactly one source distribution in the temporary build directory." >&2
    exit 1
fi

cd -- "$scratch"
uv run --isolated --no-project --python 3.13 --with "${wheels[0]}" \
    python - "${wheels[0]}" "${sdists[0]}" <<'PY'
from __future__ import annotations

import subprocess
import sys
import tarfile
import zipfile
from importlib.metadata import metadata, version
from pathlib import Path

import nmdc_lakehouse


def require(condition: bool, message: str) -> None:
    """Fail distribution validation even when Python optimization is enabled."""
    if not condition:
        raise SystemExit(message)


wheel = Path(sys.argv[1])
sdist = Path(sys.argv[2])
installed_version = version("nmdc-lakehouse")
project_metadata = metadata("nmdc-lakehouse")

require(installed_version == nmdc_lakehouse.__version__, "Import and distribution versions differ.")
require(installed_version != "0.0.0", "The distribution still uses the placeholder version.")
require(project_metadata["License-Expression"] == "MIT", "The distribution license expression is not MIT.")
require("LICENSE" in project_metadata.get_all("License-File", []), "The distribution does not declare LICENSE.")

cli = subprocess.run(
    ["nmdc-lakehouse", "--version"],
    check=True,
    capture_output=True,
    text=True,
)
require(installed_version in cli.stdout, "The CLI does not report the installed distribution version.")

with zipfile.ZipFile(wheel) as archive:
    wheel_names = archive.namelist()
    require(
        any(name.endswith(".dist-info/licenses/LICENSE") for name in wheel_names),
        "The wheel does not contain LICENSE.",
    )
    require(
        "nmdc_lakehouse/schemas/nmdc_metadata.yaml" in wheel_names,
        "The wheel does not contain the canonical NMDC metadata schema.",
    )

with tarfile.open(sdist) as archive:
    sdist_names = archive.getnames()
    require(
        any(name.endswith("/LICENSE") for name in sdist_names),
        "The source distribution does not contain LICENSE.",
    )
    require(
        any(name.endswith("/src/nmdc_lakehouse/schemas/nmdc_metadata.yaml") for name in sdist_names),
        "The source distribution does not contain the canonical NMDC metadata schema.",
    )

print(f"Verified wheel and sdist for nmdc-lakehouse {installed_version}.")
PY
