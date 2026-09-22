"""Producer-identity labels stamped into Parquet footers and the snapshot manifest.

A producer identity is ``<package name>==<version>`` (e.g. ``nmdc-lakehouse-schema==0.2.0``): it
records which released code, at which version, wrote a table, for provenance and reproducibility. A
Python import path names a symbol without saying which release produced the bytes, which is the gap
#333 (advancing #146 / #293) closed.

Producer identity is per-write provenance carried by the data (footer + manifest), never by the
flattened schema, which is purely structural (#336).

Two producers exist: the schema-driven flattener, which lives in the ``nmdc-lakehouse-schema``
package and writes both primary and side tables (one identity: same code, same version); and the
direct loader, which is this repo's own code (``nmdc-lakehouse``).
"""

from __future__ import annotations

from importlib.metadata import version

FLATTENER_PACKAGE = "nmdc-lakehouse-schema"
DIRECT_PACKAGE = "nmdc-lakehouse"


def flattener_mapping_id() -> str:
    """Producer identity written for every flattener-produced table (primary and side)."""
    return f"{FLATTENER_PACKAGE}=={version(FLATTENER_PACKAGE)}"


def direct_mapping_id() -> str:
    """Producer identity written for every direct-loaded table."""
    return f"{DIRECT_PACKAGE}=={version(DIRECT_PACKAGE)}"
