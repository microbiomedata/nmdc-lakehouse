"""Apache Iceberg sink.

Stub module — will use :mod:`pyiceberg` to append rows to Iceberg tables in
a configured catalog.
"""

from __future__ import annotations

from typing import Iterable


class IcebergSink:
    """Append rows to an Apache Iceberg table."""

    def __init__(self, catalog_name: str, namespace: str) -> None:
        """Construct an IcebergSink bound to a catalog and namespace."""
        self.catalog_name = catalog_name
        self.namespace = namespace

    def write(self, rows: Iterable[dict], *, table: str) -> None:
        """Append ``rows`` to the ``{namespace}.{table}`` Iceberg table."""
        raise NotImplementedError
