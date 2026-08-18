"""Transactional staging and promotion for one collection output set."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path

_SAFE_TABLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*\Z")


class CollectionPromotionError(RuntimeError):
    """Raised when a collection output set cannot be safely promoted."""


class CollectionOutputTransaction:
    """Stage and transactionally replace the owned files for one collection."""

    def __init__(self, root: Path, collection: str, owned_tables: set[str]) -> None:
        """Define the final root and complete set of table names this collection owns."""
        if (
            not _SAFE_TABLE.fullmatch(collection)
            or not owned_tables
            or collection not in owned_tables
            or not all(_SAFE_TABLE.fullmatch(table) for table in owned_tables)
        ):
            raise CollectionPromotionError("Collection and owned table names must be safe local identifiers.")
        self.root = root.expanduser()
        if self.root.is_symlink():
            raise CollectionPromotionError("Collection output root must not be a symlink.")
        self.root = self.root.resolve()
        self.collection = collection
        self.owned_tables = frozenset(owned_tables)
        self.stage_root: Path | None = None
        self._preserve_stage = False

    def __enter__(self) -> CollectionOutputTransaction:
        """Create a same-filesystem staging directory."""
        self.root.mkdir(parents=True, exist_ok=True)
        staging_parent = self.root / ".staging"
        staging_parent.mkdir(exist_ok=True)
        self.stage_root = Path(tempfile.mkdtemp(prefix=f"{self.collection}-", dir=staging_parent))
        return self

    def _stage(self) -> Path:
        if self.stage_root is None:
            raise CollectionPromotionError("Collection output transaction has not been entered.")
        return self.stage_root

    @property
    def stage(self) -> Path:
        """Return the active staging directory."""
        return self._stage()

    def commit(
        self,
        table_rows: tuple[tuple[str, int], ...],
        *,
        source_schema_id: str,
        source_schema_version: str,
    ) -> None:
        """Promote a completed set and remove stale files owned by the collection."""
        stage = self._stage()
        if not source_schema_id or not source_schema_version:
            raise CollectionPromotionError("Source schema identity and version are required.")
        if any(not isinstance(rows, int) or rows < 0 for _table, rows in table_rows):
            raise CollectionPromotionError("Completed table row counts must be non-negative integers.")
        produced = {table for table, _rows in table_rows}
        if self.collection not in produced or not produced <= self.owned_tables or len(produced) != len(table_rows):
            raise CollectionPromotionError(
                "Produced tables must include the primary table and be a unique subset of owned tables."
            )
        expected_stage = {f"{table}.parquet" for table in produced}
        staged_entries = list(stage.iterdir())
        actual_stage = {path.name for path in staged_entries}
        if actual_stage != expected_stage or any(path.is_symlink() or not path.is_file() for path in staged_entries):
            raise CollectionPromotionError("Staged files do not match the completed table inventory.")

        completion_record = {
            "collection": self.collection,
            "status": "complete",
            "source_schema_id": source_schema_id,
            "source_schema_version": source_schema_version,
            "tables": [{"table": table, "rows": rows} for table, rows in sorted(table_rows)],
        }
        (stage / "collection-manifest.json").write_text(
            json.dumps(completion_record, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        backup = stage / ".previous"
        backup.mkdir()
        backed_up: list[str] = []
        promoted: list[str] = []
        try:
            for table in sorted(self.owned_tables):
                destination = self.root / f"{table}.parquet"
                if destination.is_symlink():
                    raise CollectionPromotionError(f"Refusing to replace symlink: {destination.name}")
                if destination.exists() and not destination.is_file():
                    raise CollectionPromotionError(f"Refusing to replace non-file: {destination.name}")
                if destination.exists():
                    os.replace(destination, backup / destination.name)
                    backed_up.append(destination.name)
            for table in sorted(produced):
                name = f"{table}.parquet"
                os.replace(stage / name, self.root / name)
                promoted.append(name)
        except BaseException as original:
            rollback_errors: list[BaseException] = []
            for name in reversed(promoted):
                try:
                    os.replace(self.root / name, stage / name)
                except BaseException as error:
                    rollback_errors.append(error)
            for name in reversed(backed_up):
                try:
                    os.replace(backup / name, self.root / name)
                except BaseException as error:
                    rollback_errors.append(error)
            if rollback_errors:
                self._preserve_stage = True
                original.add_note(
                    f"Collection promotion rollback also failed {len(rollback_errors)} time(s); "
                    "staging was retained for manual recovery."
                )
            raise

    def __exit__(self, _error_type, error, _traceback) -> None:
        """Remove staging content after success or failure."""
        if self.stage_root is None:
            return
        if self._preserve_stage:
            return
        staging_parent = self.stage_root.parent
        try:
            shutil.rmtree(self.stage_root)
        except OSError as cleanup_error:
            if error is None:
                raise
            error.add_note(f"Collection staging cleanup also failed: {cleanup_error}")
        try:
            staging_parent.rmdir()
        except OSError:
            pass
