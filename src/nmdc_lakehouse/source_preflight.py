"""Read MongoDB's recorded migration state before accepting a source contract."""

from __future__ import annotations

import ast
from importlib.metadata import version
from importlib.resources import files

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

from nmdc_lakehouse.target_validation import assert_source_schema_aligned

MIGRATION_VERSION_VIEW = "_migration_latest_schema_version"


class SourceSchemaError(ValueError):
    """The migration state is not compatible with the installed projection source."""


def _without_docstring(node: ast.Module | ast.ClassDef | ast.FunctionDef) -> list[ast.stmt]:
    return node.body[1:] if ast.get_docstring(node) is not None else node.body


def _explicit_noop(tree: ast.Module, migrator: ast.ClassDef, declarations: list[ast.Assign]) -> bool:
    """Recognize the package's plain no-upgrade declaration; reject unfamiliar code.

    Matching syntax, rather than importing or calling a migration, keeps discovery
    read-only. A constructor, decorator, helper, or other executable statement is
    deliberately outside this conservative contract.
    """
    module_body = _without_docstring(tree)
    expected_import = ast.parse("from nmdc_schema.migrators.migrator_base import MigratorBase").body[0]
    if len(module_body) != 2 or ast.dump(module_body[0]) != ast.dump(expected_import) or module_body[1] is not migrator:
        return False
    if (
        migrator.decorator_list
        or migrator.keywords
        or len(migrator.bases) != 1
        or not isinstance(migrator.bases[0], ast.Name)
        or migrator.bases[0].id != "MigratorBase"
    ):
        return False
    methods = [node for node in _without_docstring(migrator) if node not in declarations]
    if len(methods) != 1 or not isinstance(methods[0], ast.FunctionDef):
        return False
    method = methods[0]
    # The complete signature is intentional: defaults and decorators can execute
    # code even when the method body is just `pass`. Unknown forms fail closed.
    expected_method = ast.parse("def upgrade(self, commit_changes: bool = False) -> None:\n    pass").body[0]
    method.body = _without_docstring(method)
    return ast.dump(method) == ast.dump(expected_method)


def _has_noop_migration_path(recorded: str, selected: str) -> bool:
    """Walk an unambiguous chain of explicit no-upgrade steps in the installed package."""
    predecessors: dict[str, list[tuple[str, bool]]] = {}
    try:
        for resource in files("nmdc_schema.migrators").iterdir():
            if not resource.name.startswith("migrator_from_") or not resource.name.endswith(".py"):
                continue
            tree = ast.parse(resource.read_text(encoding="utf-8"))
            classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Migrator"]
            if len(classes) != 1:
                raise ValueError("Unrecognized migration class")
            migrator = classes[0]
            declarations = [
                node
                for node in migrator.body
                if isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in {"_from_version", "_to_version"}
            ]
            versions = {
                node.targets[0].id: node.value.value
                for node in declarations
                if isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
                and node.value.value
            }
            if len(declarations) != 2 or set(versions) != {"_from_version", "_to_version"}:
                raise ValueError("Unrecognized migration versions")
            predecessors.setdefault(versions["_to_version"], []).append(
                (versions["_from_version"], _explicit_noop(tree, migrator, declarations))
            )
    except (OSError, ImportError, SyntaxError, ValueError):
        raise SourceSchemaError(
            "Cannot inspect the installed nmdc-schema migration history. Verify the selected package installation."
        ) from None

    visited: set[str] = set()
    current = selected
    while current != recorded:
        if current in visited:
            return False
        visited.add(current)
        steps = predecessors.get(current, [])
        if len(steps) != 1:
            return False
        previous, noop = steps[0]
        if not noop:
            return False
        current = previous
    return True


def assert_mongodb_source_aligned(uri: str) -> str:
    """Require one completed migration version compatible with the installed source.

    NMDC's migration CLI maintains a view that returns ``schema_version: null``
    unless the latest event is MIGRATION_COMPLETED. Read the existing view only;
    never instantiate its Bookkeeper, which would create it. Missing, ambiguous,
    inaccessible, or incomplete bookkeeping fails closed. Exact equality or an
    unambiguous series of explicit no-upgrade steps is accepted. This is not a snapshot
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
    recorded = rows[0].get("schema_version") if len(rows) == 1 else None
    if not isinstance(recorded, str):
        raise SourceSchemaError(
            "MongoDB has no unambiguous completed schema migration. Verify migration bookkeeping before exporting."
        )
    if recorded != expected and not _has_noop_migration_path(recorded, expected):
        raise SourceSchemaError(
            f"MongoDB's recorded migration version is not compatible with installed nmdc-schema {expected}. "
            "Select a compatible source/flat pair, or verify that the required source migration has completed."
        )
    return expected
