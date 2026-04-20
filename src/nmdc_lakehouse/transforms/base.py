"""Base protocol for transforms."""

from __future__ import annotations

from typing import Iterable, Iterator, Protocol, runtime_checkable


@runtime_checkable
class Transform(Protocol):
    """A transform maps an iterable of records to a flattened iterable of rows."""

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        """Yield flattened rows derived from ``records``."""
        ...
