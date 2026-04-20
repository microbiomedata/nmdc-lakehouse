"""Base protocol for NMDC data sources."""

from __future__ import annotations

from typing import Any, Iterator, Protocol, runtime_checkable


@runtime_checkable
class Source(Protocol):
    """A source yields NMDC records (as dicts) from a linkml-store backend."""

    def iter_records(self, collection: str, **filters: Any) -> Iterator[dict]:
        """Yield records from ``collection`` matching ``filters``."""
        ...
