"""Producer-identity labels stamped into Parquet footers and the snapshot manifest.

A producer identity answers "what code, at what version, wrote this table" for provenance and
reproducibility. It is ``<package name>==<version>`` (e.g. ``nmdc-lakehouse-schema==0.2.0``), not a
Python import path: a code path names a symbol without saying which release produced the bytes,
which is exactly the gap #333 (advancing #146 / #293) closed.

Producer identity is per-write provenance carried by the data (footer + manifest), never by the
flattened schema, which is purely structural (#336).

Two producers exist:

- the schema-driven flattener, which lives in the ``nmdc-lakehouse-schema`` package and writes both
  primary and side tables (one identity: the two are the same code at the same version); and
- the direct loader, which is this repo's own code (``nmdc-lakehouse``).

Validation checks the *package* (which producer wrote a table — the routing invariant), not the
exact version, so a snapshot written by one release still validates under another. The version
still rides along in the footer for provenance. Identities persisted before this switch used the
old import-path spelling; those are accepted on read so already-published snapshots keep validating.
"""

from __future__ import annotations

from importlib.metadata import version

FLATTENER_PACKAGE = "nmdc-lakehouse-schema"
DIRECT_PACKAGE = "nmdc-lakehouse"

# Import-path identities persisted before the package==version switch (#333). Accepted on read.
LEGACY_FLATTENER_IDS = frozenset(
    {
        "nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener",
        "nmdc_lakehouse.transforms.flatteners.side_table_rows",
    }
)
LEGACY_DIRECT_IDS = frozenset({"nmdc_lakehouse.jobs.direct_mongo_to_parquet.DirectMongoToParquetJob"})


def flattener_mapping_id() -> str:
    """Producer identity written for every flattener-produced table (primary and side)."""
    return f"{FLATTENER_PACKAGE}=={version(FLATTENER_PACKAGE)}"


def direct_mapping_id() -> str:
    """Producer identity written for every direct-loaded table."""
    return f"{DIRECT_PACKAGE}=={version(DIRECT_PACKAGE)}"


def _package_of(mapping_id: str) -> str | None:
    """Return the package name in a ``package==version`` identity, or None for a legacy label."""
    name, separator, _ = mapping_id.partition("==")
    return name if separator else None


def is_flattener_identity(mapping_id: str) -> bool:
    """True when ``mapping_id`` names the flattener package (or its legacy import-path label)."""
    return mapping_id in LEGACY_FLATTENER_IDS or _package_of(mapping_id) == FLATTENER_PACKAGE


def is_direct_identity(mapping_id: str) -> bool:
    """True when ``mapping_id`` names the direct-loader package (or its legacy import-path label)."""
    return mapping_id in LEGACY_DIRECT_IDS or _package_of(mapping_id) == DIRECT_PACKAGE
