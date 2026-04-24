"""Base protocol for sinks."""

from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable


@runtime_checkable
class Sink(Protocol):
    """A sink writes an iterable of flat rows to an external location."""

    def write(self, rows: Iterable[dict], *, table: str) -> int | None:
        """Write ``rows`` under the logical ``table`` name.

        Returns the number of rows written, or ``None`` if the sink does not
        track row counts.
        """
        ...
