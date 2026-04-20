"""NMDC MongoDB source using linkml-store."""

from __future__ import annotations

from typing import Any, Iterator, Optional

from linkml_store import Client
from linkml_store.api import Database


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
        where: Optional[dict[str, Any]] = None,
        page_size: int = 1000,
    ) -> Iterator[dict]:
        """Yield records from ``collection``.

        Delegates to ``Collection.find_iter``, which paginates via
        limit/offset so full collections do not load into memory.

        Args:
            collection: Name of the MongoDB collection (e.g. ``biosample_set``).
            where: Optional filter dict passed through to ``find_iter``.
            page_size: Records per page fetched from the backend.

        Yields:
            One dict per record, with MongoDB ``_id`` fields stripped.
        """
        coll = self.db.get_collection(collection, create_if_not_exists=False)
        yield from coll.find_iter(where=where or {}, page_size=page_size)
