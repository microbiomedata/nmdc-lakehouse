"""Helpers for referencing and (optionally) staging large data files.

Stub module. The intended role is to resolve ``DataObject`` references from
the NMDC schema into lakehouse-friendly metadata rows:

* canonical URI (s3://, http(s)://, local path)
* size in bytes
* checksum(s)
* file type / format

without materialising the file contents in Parquet / Iceberg.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BigDataRef:
    """Lakehouse-safe reference to a large external data file."""

    uri: str
    size_bytes: int | None = None
    checksum: str | None = None
    checksum_algo: str | None = None
    media_type: str | None = None
