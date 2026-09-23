"""Read MongoDB's recorded migration state before accepting a source contract."""

from __future__ import annotations

from importlib.metadata import version

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

from nmdc_lakehouse.target_validation import assert_source_schema_aligned

MIGRATION_VERSION_VIEW = "_migration_latest_schema_version"

# Every intervening upgrade in the locked nmdc-schema releases is explicitly a
# no-op. A completed migration can therefore precede the deployed source version.
# Keep this reviewed exception exact: 11.17.1 -> 11.18.0 and 11.23.0 -> 11.24.0
# require real migration work. Tests inspect the packaged no-op chain without
# executing it. See docs/source-schema-1124-rollout.md for the production evidence.
NOOP_PREDECESSORS_11_23 = frozenset(
    {"11.18.0", "11.18.1", "11.19.0", "11.19.1", "11.20.0", "11.20.1", "11.20.2", "11.21.0", "11.22.0"}
)


class SourceSchemaError(ValueError):
    """The migration state is not compatible with the installed projection source."""


def assert_mongodb_source_aligned(uri: str) -> str:
    """Require one completed migration version compatible with the installed source.

    NMDC's migration CLI maintains a view that returns ``schema_version: null``
    unless the latest event is MIGRATION_COMPLETED. Read the existing view only;
    never instantiate its Bookkeeper, which would create it. Missing, ambiguous,
    inaccessible, or incomplete bookkeeping fails closed. Exact equality or the
    reviewed no-op predecessors of 11.23.0 are accepted. This is not a snapshot
    read, proof of the deployed API version, or validation of individual records.
    """
    assert_source_schema_aligned()
    expected = version("nmdc-schema")
    client: MongoClient[dict[str, object]]
    try:
        with MongoClient(
            uri, serverSelectionTimeoutMS=10_000, connectTimeoutMS=10_000, socketTimeoutMS=10_000
        ) as client:
            # Match the direct exporter's fallback, while honoring an explicit URI database.
            database: Database[dict[str, object]] = client.get_default_database(default="nmdc")
            rows = list(database[MIGRATION_VERSION_VIEW].find({}, {"_id": 0, "schema_version": 1}).limit(2))
    except PyMongoError:
        raise SourceSchemaError(
            "Cannot read MongoDB migration state; verify the connection and read access to the version view."
        ) from None
    if len(rows) != 1 or not isinstance(rows[0].get("schema_version"), str):
        raise SourceSchemaError(
            "MongoDB has no unambiguous completed schema migration. Verify migration bookkeeping before exporting."
        )
    recorded = rows[0]["schema_version"]
    noop_compatible = expected == "11.23.0" and recorded in NOOP_PREDECESSORS_11_23
    if recorded != expected and not noop_compatible:
        raise SourceSchemaError(
            f"MongoDB's recorded migration version is not compatible with installed nmdc-schema {expected}. "
            "Select a compatible source/flat pair, or verify that the required source migration has completed."
        )
    return expected
