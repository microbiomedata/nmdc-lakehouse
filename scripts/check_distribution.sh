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

wheel = Path(sys.argv[1])
sdist = Path(sys.argv[2])
installed_version = version("nmdc-lakehouse")
project_metadata = metadata("nmdc-lakehouse")

assert installed_version == nmdc_lakehouse.__version__
assert installed_version != "0.0.0"
assert project_metadata["License-Expression"] == "MIT"
assert "LICENSE" in project_metadata.get_all("License-File", [])

cli = subprocess.run(
    ["nmdc-lakehouse", "--version"],
    check=True,
    capture_output=True,
    text=True,
)
assert installed_version in cli.stdout

with zipfile.ZipFile(wheel) as archive:
    assert any(name.endswith(".dist-info/licenses/LICENSE") for name in archive.namelist())

with tarfile.open(sdist) as archive:
    assert any(name.endswith("/LICENSE") for name in archive.getnames())

print(f"Verified wheel and sdist for nmdc-lakehouse {installed_version}.")
PY
