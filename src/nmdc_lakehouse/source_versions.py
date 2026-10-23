"""Read the supported nmdc-schema source releases from pyproject.toml.

Each supported release has one optional extra, ``source-<major>-<minor>``, that pins
``nmdc-schema`` exactly. Those extras are the only list of supported releases:
``scripts/uv_with_source.sh``, ``scripts/python/setup_pod_runtime.py`` and the doctor
read them instead of keeping their own copies. Adding a release means adding its
extra (and its ``[tool.uv] conflicts`` entry) and updating ``uv.lock``.

Versions are compared as PEP 440 releases, not strings: nmdc-schema 11.25 was published
as ``11.25``, and ``11.25`` and ``11.25.0`` name the same release.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

SOURCE_EXTRA = re.compile(r"source-(\d+)-(\d+)")
SOURCE_PIN = re.compile(r"nmdc-schema==((\d+)\.(\d+)(?:\.\d+)?(?:rc\d+)?)")


def source_extras(pyproject: Path) -> dict[str, str]:
    """Map each supported nmdc-schema version to the extra that installs it.

    Raises ``ValueError`` when a ``source-*`` extra does not hold exactly one pin
    whose major and minor match its name.
    """
    extras = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["optional-dependencies"]
    supported: dict[str, str] = {}
    for name, requirements in extras.items():
        extra = SOURCE_EXTRA.fullmatch(name)
        if not extra:
            continue
        pin = SOURCE_PIN.fullmatch(requirements[0]) if len(requirements) == 1 else None
        if pin is None or pin.group(2, 3) != extra.group(1, 2):
            raise ValueError(f"Extra {name} must pin exactly one matching nmdc-schema release.")
        supported[pin.group(1)] = name
    if not supported:
        raise ValueError("pyproject.toml declares no source-* extras.")
    return supported


def default_source_version(supported: dict[str, str]) -> str:
    """Return the newest supported final release; release candidates are never the default."""
    finals = [version for version in supported if "rc" not in version]
    if not finals:
        raise ValueError("pyproject.toml declares no final source release.")
    return max(finals, key=lambda version: tuple(int(part) for part in version.split(".")))


def same_release(left: str, right: str) -> bool:
    """Return whether two nmdc-schema versions name the same PEP 440 release.

    Only versions already written in normal form compare equal: ``11.25`` and
    ``11.25.0`` match, but ``v11.25.0`` or ``11.25.0 `` (from MongoDB, say) do not.
    ``packaging`` is imported here rather than at module level so that
    scripts/python/setup_pod_runtime.py can load this module before any project
    environment exists; it calls only ``source_extras``.
    """
    from packaging.version import InvalidVersion, Version

    try:
        parsed = Version(left), Version(right)
    except InvalidVersion:
        return False
    return str(parsed[0]) == left and str(parsed[1]) == right and parsed[0] == parsed[1]


def supported_release(selected: str, supported: dict[str, str]) -> str | None:
    """Return the pinned version in ``supported`` that names the same release as ``selected``."""
    return next((pinned for pinned in supported if same_release(pinned, selected)), None)
