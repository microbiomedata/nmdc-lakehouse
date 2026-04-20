"""Parquet sink.

Stub module — will use :mod:`pyarrow` to write partitioned Parquet datasets
under ``LAKEHOUSE_ROOT``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


class ParquetSink:
    """Write rows as Parquet files under a root directory / object-store URI."""

    def __init__(self, root: str | Path) -> None:
        """Construct a ParquetSink rooted at ``root``."""
        self.root = Path(root)

    def write(self, rows: Iterable[dict], *, table: str) -> None:
        """Write ``rows`` to ``{root}/{table}``."""
        raise NotImplementedError
