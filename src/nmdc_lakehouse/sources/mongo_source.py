"""NMDC MongoDB source using linkml-store."""

from __future__ import annotations

from typing import Any, Iterator, Optional

from linkml_store import Client
from linkml_store.api import Database

from nmdc_lakehouse.config import MongoSettings


class MongoSource:
    """linkml-store backed source for an NMDC MongoDB backend."""

    def __init__(self, handle: str, alias: str = "nmdc") -> None:
        """Construct a MongoSource.

        Args:
            handle: linkml-store handle (e.g. ``mongodb://localhost:27017/nmdc``).
                The database name is the path component of the URI.
            alias: Alias under which the database is attached to the client.
        """
        self.handle = handle
        self.alias = alias
        self._client: Optional[Client] = None
        self._db: Optional[Database] = None

    @classmethod
    def from_settings(cls, settings: MongoSettings, alias: str = "nmdc") -> MongoSource:
        """Construct a MongoSource from a :class:`MongoSettings` instance.

        This is the canonical construction path — it plugs into the package's
        pydantic-settings configuration (env vars, ``.env`` file). Direct use
        of ``MongoSource(handle)`` is still supported for one-off calls and
        tests that want to override the URI.
        """
        return cls(handle=settings.uri, alias=alias)

    @property
    def db(self) -> Database:
        """Attach (lazily) and return the linkml-store Database."""
        if self._db is None:
            self._client = Client()
            self._db = self._client.attach_database(self.handle, alias=self.alias)
        return self._db

    def iter_records(
        self,
        collection: str,
        page_size: int = 1000,
        **filters: Any,
    ) -> Iterator[dict]:
        """Yield records from ``collection``.

        Delegates to ``Collection.find_iter``, which paginates via
        limit/offset so full collections do not load into memory.
        Matches the ``Source`` protocol's ``iter_records(collection, **filters)``
        signature; ``filters`` are passed to ``find_iter`` as a ``where`` dict.

        Args:
            collection: Name of the MongoDB collection (e.g. ``biosample_set``).
            page_size: Records per page fetched from the backend.
            **filters: Keyword filters passed as ``where`` to ``find_iter``
                (e.g. ``id="nmdc:bsm-11-..."``).

        Yields:
            One dict per record. Linkml-store's ``find_iter`` strips MongoDB's
            ``_id`` automatically.
        """
        coll = self.db.get_collection(collection, create_if_not_exists=False)
        yield from coll.find_iter(where=dict(filters), page_size=page_size)
