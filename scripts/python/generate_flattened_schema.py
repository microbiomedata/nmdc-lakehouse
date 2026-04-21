#!/usr/bin/env python3
"""Emit the flattened LinkML schema for nmdc-schema's Database class.

Walks the installed ``nmdc-schema`` package, generates a flat
``ClassDefinition`` for each multivalued ``Database`` slot via
:func:`nmdc_lakehouse.transforms.schema_generator.flatten_database_schema`,
and writes the resulting schema to disk as YAML.

Usage:
    uv run python scripts/python/generate_flattened_schema.py [OUTPUT_PATH]

Default OUTPUT_PATH: ./local/nmdc_schema_flattened.yaml
"""

from __future__ import annotations

import sys
from importlib.util import find_spec
from pathlib import Path

from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper

from nmdc_lakehouse.transforms.schema_generator import flatten_database_schema


def main() -> None:
    """Generate the flattened schema and write it to disk."""
    out_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("./local/nmdc_schema_flattened.yaml")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        print("nmdc_schema package is not installed", file=sys.stderr)
        sys.exit(1)
    schema_path = f"{spec.submodule_search_locations[0]}/nmdc_materialized_patterns.yaml"
    sv = SchemaView(schema_path)

    flat_schema = flatten_database_schema(sv)
    yaml_str = yaml_dumper.dumps(flat_schema)
    out_path.write_text(yaml_str)

    print(f"Wrote {out_path}")
    print(f"  classes: {len(flat_schema.classes)}")
    for name, cls in sorted(flat_schema.classes.items()):
        print(f"    {name}: {len(cls.attributes)} attrs")


if __name__ == "__main__":
    main()
