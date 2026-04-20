"""NMDC MongoDB source using linkml-store.

Stub module — no implementation yet. The intended behaviour is to construct
a :class:`linkml_store.Client`, attach the NMDC database, and expose an
iterator of documents for downstream flattening.
"""

from __future__ import annotations

from typing import Any, Iterator


class MongoSource:
    """linkml-store backed source for the NMDC MongoDB backend."""

    def __init__(self, handle: str, db_name: str) -> None:
        """Construct a MongoSource from a linkml-store handle and database name."""
        self.handle = handle
        self.db_name = db_name

    def iter_records(self, collection: str, **filters: Any) -> Iterator[dict]:
        """Yield records from ``collection`` matching ``filters``."""
        raise NotImplementedError
