"""Read MongoDB's recorded migration state before accepting a source contract."""

from __future__ import annotations

from importlib.metadata import version

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

from nmdc_lakehouse.target_validation import assert_source_schema_aligned

MIGRATION_VERSION_VIEW = "_migration_latest_schema_version"


class SourceSchemaError(ValueError):
    """The database cannot be shown to match the installed projection source."""


def assert_mongodb_source_aligned(uri: str) -> str:
    """Require one completed migration version matching the installed source.

    NMDC's migration CLI maintains a view that returns ``schema_version: null``
    unless the latest event is MIGRATION_COMPLETED. Read the existing view only;
    never instantiate its Bookkeeper, which would create it. Missing, ambiguous,
    inaccessible, or incomplete bookkeeping fails closed. This is not a snapshot
    read or validation of individual source records.
    """
    assert_source_schema_aligned()
    expected = version("nmdc-schema")
    client: MongoClient[dict[str, object]]
    try:
        with MongoClient(
            uri, serverSelectionTimeoutMS=10_000, connectTimeoutMS=10_000, socketTimeoutMS=10_000
        ) as client:
            database: Database[dict[str, object]] = client.get_default_database()
            rows = list(database[MIGRATION_VERSION_VIEW].find({}, {"_id": 0, "schema_version": 1}).limit(2))
    except PyMongoError:
        raise SourceSchemaError(
            "Cannot read MongoDB migration state; verify the connection and read access to the version view."
        ) from None
    if len(rows) != 1 or not isinstance(rows[0].get("schema_version"), str):
        raise SourceSchemaError(
            "MongoDB has no unambiguous completed schema migration. Verify migration bookkeeping before exporting."
        )
    if rows[0]["schema_version"] != expected:
        raise SourceSchemaError(
            f"MongoDB's recorded schema version does not match installed nmdc-schema {expected}. "
            "Select the matching source/flat pair, or wait for the source migration."
        )
    return expected
