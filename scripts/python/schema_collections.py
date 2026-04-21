#!/usr/bin/env python3
"""Print the nmdc-schema-specified collection names, one per line.

These are the slot names of the `Database` class in the installed
``nmdc_materialized_patterns.yaml`` — the authoritative list of what lives at
the top level of the NMDC MongoDB. Driven by the installed ``nmdc-schema``
package, so the list stays current with whatever version the project is pinned
to.
"""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path

import yaml


def schema_collections() -> list[str]:
    """Return the sorted list of Database slot names from nmdc-schema."""
    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        raise RuntimeError("nmdc_schema package is not installed")
    pkg_dir = Path(spec.submodule_search_locations[0])
    schema_path = pkg_dir / "nmdc_materialized_patterns.yaml"
    with schema_path.open() as f:
        schema = yaml.safe_load(f)
    slots = schema["classes"]["Database"].get("slots", []) or []
    return sorted(slots)


def main() -> None:
    """CLI entrypoint — print one slot name per line."""
    for name in schema_collections():
        print(name)


if __name__ == "__main__":
    main()
