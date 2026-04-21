#!/usr/bin/env python3
"""Preview the flattener's output against one record from a local MongoDB.

Pulls a single document from a MongoDB collection, flattens it with
``SchemaDrivenFlattener`` using the installed ``nmdc-schema``, and prints
a side-by-side view of nested-input vs flat-output columns.

Usage:
    uv run python scripts/python/flatten_preview.py <collection> [<class>]

Examples:
    uv run python scripts/python/flatten_preview.py biosample_set Biosample
    uv run python scripts/python/flatten_preview.py study_set Study
    uv run python scripts/python/flatten_preview.py data_object_set DataObject

If <class> is omitted, it's inferred from <collection> by stripping the
trailing ``_set`` and converting to PascalCase.
"""

from __future__ import annotations

import json
import os
import sys
from importlib.util import find_spec

from linkml_runtime import SchemaView
from pymongo import MongoClient

from nmdc_lakehouse.transforms.flatteners import flatten_record


def _infer_class(collection: str) -> str:
    """Turn ``biosample_set`` → ``Biosample``, ``data_object_set`` → ``DataObject``."""
    base = collection.removesuffix("_set").removesuffix("_agg")
    return "".join(part.capitalize() for part in base.split("_"))


def main() -> None:
    """Pull one record from <collection> and print nested vs flat."""
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(__doc__)
        sys.exit(1)

    collection = sys.argv[1]
    root_class = sys.argv[2] if len(sys.argv) == 3 else _infer_class(collection)

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc_lakehouse_prep")
    client = MongoClient(mongo_uri)
    db = client.get_default_database()

    doc = db[collection].find_one()
    if doc is None:
        print(f"No documents in {db.name}.{collection}", file=sys.stderr)
        sys.exit(1)
    doc.pop("_id", None)

    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        print("nmdc_schema package is not installed", file=sys.stderr)
        sys.exit(1)
    schema_path = f"{spec.submodule_search_locations[0]}/nmdc_materialized_patterns.yaml"
    sv = SchemaView(schema_path)

    flat = flatten_record(doc, sv, root_class)

    print(f"=== NESTED INPUT ({db.name}.{collection}, class {root_class}) ===")
    print(json.dumps(doc, indent=2, default=str))
    print()
    print("=== FLATTENED OUTPUT ===")
    for k in sorted(flat):
        v = flat[k]
        s = str(v)
        if len(s) > 80:
            s = s[:77] + "..."
        print(f"{k:50s}  {s}")
    print()
    print(f"nested slots: {len(doc)}, flat columns: {len(flat)}")


if __name__ == "__main__":
    main()
