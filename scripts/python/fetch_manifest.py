#!/usr/bin/env python3
"""Print an rsync ``--files-from`` manifest for schema-specified collections.

Each schema collection contributes two files under ``nmdc/``:
``<name>.bson.gz`` (the documents) and ``<name>.metadata.json.gz`` (indexes).
The collection list is derived from the installed ``nmdc-schema`` package, so
it stays in sync with whatever version this project is pinned to.

Intended usage: ``rsync --files-from=- <src> <dest>`` piped from this script.
"""

from __future__ import annotations

from schema_collections import schema_collections


def main() -> None:
    """Emit two relative paths per collection: .bson.gz and .metadata.json.gz."""
    for name in schema_collections():
        print(f"nmdc/{name}.bson.gz")
        print(f"nmdc/{name}.metadata.json.gz")


if __name__ == "__main__":
    main()
