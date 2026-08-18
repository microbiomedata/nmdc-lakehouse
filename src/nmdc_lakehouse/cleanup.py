"""Previewable cleanup for recognized local metadata Parquet products."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from functools import cache
from importlib.util import find_spec
from pathlib import Path
from typing import AbstractSet


class UnsafeCleanupRoot(ValueError):
    """Raised when a cleanup root is outside the repository output policy."""


@dataclass(frozen=True)
class CleanupPlan:
    """Resolved cleanup root and the recognized files selected beneath it."""

    root: Path
    targets: tuple[Path, ...]


def find_project_root(start: Path) -> Path:
    """Find the enclosing nmdc-lakehouse checkout, or fail closed."""
    current = start.expanduser().resolve()
    if not current.is_dir():
        current = current.parent
    for candidate in (current, *current.parents):
        pyproject = candidate / "pyproject.toml"
        if not pyproject.is_file() or not (candidate / ".git").exists():
            continue
        try:
            project = tomllib.loads(pyproject.read_text(encoding="utf-8")).get("project", {})
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if project.get("name") == "nmdc-lakehouse":
            return candidate
    raise UnsafeCleanupRoot("Run cleanup from inside an nmdc-lakehouse Git checkout.")


@cache
def metadata_output_names() -> frozenset[str]:
    """Return every primary or potential side-table name for the locked NMDC schema."""
    from linkml_runtime import SchemaView

    from nmdc_lakehouse.jobs.collection_to_parquet import _db_collection_map
    from nmdc_lakehouse.transforms.schema_generator import side_table_class_defs

    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        raise RuntimeError("nmdc_schema package is not installed")
    schema_path = Path(spec.submodule_search_locations[0]) / "nmdc_materialized_patterns.yaml"
    schema_view = SchemaView(str(schema_path))
    collection_map = _db_collection_map()
    names = set(collection_map)
    for collection, root_class in collection_map.items():
        names.update(name for name, _ in side_table_class_defs(schema_view, root_class, collection))
    return frozenset(names)


def _resolve_cleanup_root(root: Path, project_root: Path) -> Path:
    project = project_root.expanduser().resolve()
    raw_root = root.expanduser()
    lexical_root = raw_root if raw_root.is_absolute() else project / raw_root
    lexical_root = Path(os.path.abspath(lexical_root))

    try:
        relative = lexical_root.relative_to(project)
    except ValueError as error:
        raise UnsafeCleanupRoot("Cleanup root must be inside the repository.") from error
    if lexical_root == project:
        raise UnsafeCleanupRoot("Cleanup root must not be the repository root.")
    allowed = relative.parts[0] in {"lakehouse", "local"} or relative.parts[0].startswith("lakehouse_backup")
    if not allowed:
        raise UnsafeCleanupRoot("Cleanup root must be beneath lakehouse/, lakehouse_backup*/, or local/.")

    cursor = project
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise UnsafeCleanupRoot("Cleanup root must not contain symlinked path components.")

    resolved = lexical_root.resolve(strict=False)
    try:
        resolved.relative_to(project)
    except ValueError as error:
        raise UnsafeCleanupRoot("Cleanup root resolves outside the repository.") from error
    if resolved.exists() and not resolved.is_dir():
        raise UnsafeCleanupRoot("Cleanup root must be a directory when it exists.")
    return resolved


def plan_metadata_parquet_cleanup(
    root: Path,
    *,
    project_root: Path,
    generated_names: AbstractSet[str],
) -> CleanupPlan:
    """Select recognized top-level metadata Parquet files without mutating them."""
    resolved_root = _resolve_cleanup_root(root, project_root)
    if not resolved_root.exists():
        return CleanupPlan(root=resolved_root, targets=())
    targets = tuple(
        sorted(
            (
                path
                for path in resolved_root.iterdir()
                if not path.is_symlink()
                and path.is_file()
                and path.suffix == ".parquet"
                and path.stem in generated_names
            ),
            key=lambda path: path.name,
        )
    )
    return CleanupPlan(root=resolved_root, targets=targets)


def apply_cleanup(plan: CleanupPlan) -> int:
    """Delete exactly the regular files captured by ``plan``."""
    validated: list[Path] = []
    for target in plan.targets:
        try:
            resolved_parent = target.resolve(strict=True).parent
        except OSError as error:
            raise UnsafeCleanupRoot("A cleanup target changed or disappeared after preview.") from error
        if target.is_symlink() or not target.is_file() or target.parent != plan.root or resolved_parent != plan.root:
            raise UnsafeCleanupRoot("A cleanup target became unsafe after preview; no files were deleted.")
        validated.append(target)
    for target in validated:
        target.unlink()
    return len(validated)
