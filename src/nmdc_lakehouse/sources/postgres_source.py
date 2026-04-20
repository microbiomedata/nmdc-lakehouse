"""NMDC PostgreSQL source using linkml-store.

Stub module — no implementation yet. Mirrors :mod:`mongo_source`, targeting a
Postgres backend exposed via linkml-store.
"""

from __future__ import annotations

from typing import Any, Iterator


class PostgresSource:
    """linkml-store backed source for an NMDC PostgreSQL backend."""

    def __init__(self, dsn: str) -> None:
        """Construct a PostgresSource from a SQLAlchemy / libpq DSN."""
        self.dsn = dsn

    def iter_records(self, collection: str, **filters: Any) -> Iterator[dict]:
        """Yield records from ``collection`` (table or view) matching ``filters``."""
        raise NotImplementedError
