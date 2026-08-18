#!/usr/bin/env python3
"""Generate or check the canonical flattened NMDC metadata LinkML schema.

Walks the installed ``nmdc-schema`` package, generates a flat
``ClassDefinition`` for each multivalued ``Database`` slot via
:func:`nmdc_lakehouse.transforms.schema_generator.flatten_database_schema`,
and writes the complete primary and side-table schema to deterministic YAML.

Usage:
    uv run python scripts/python/generate_flattened_schema.py [--check] [OUTPUT_PATH]

Default OUTPUT_PATH: src/nmdc_lakehouse/schemas/nmdc_metadata.yaml
"""

from __future__ import annotations

import argparse
import os
import tempfile
from importlib.metadata import version
from importlib.util import find_spec
from pathlib import Path

from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper

from nmdc_lakehouse.transforms.schema_generator import flatten_database_schema

CANONICAL_OUTPUT = Path(__file__).resolve().parents[2] / "src/nmdc_lakehouse/schemas/nmdc_metadata.yaml"


class SchemaArtifactError(ValueError):
    """Raised when the canonical target schema cannot be generated or checked."""


def render_installed_schema() -> str:
    """Render the target schema for the locked installed NMDC schema package."""
    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        raise SchemaArtifactError("The nmdc-schema package is not installed.")
    schema_path = Path(spec.submodule_search_locations[0]) / "nmdc_materialized_patterns.yaml"
    schema_view = SchemaView(str(schema_path))
    flat_schema = flatten_database_schema(
        schema_view,
        source_package_version=version("nmdc-schema"),
    )
    return yaml_dumper.dumps(flat_schema)


def check_schema_artifact(path: Path, expected: str) -> None:
    """Fail when a canonical artifact is missing or differs from generation."""
    try:
        observed = path.read_text(encoding="utf-8")
    except OSError as error:
        raise SchemaArtifactError(f"Cannot read generated schema artifact: {path}") from error
    if observed != expected:
        raise SchemaArtifactError(f"Generated schema artifact is stale: {path}. Run `just generate-flat-schema`.")


def write_schema_artifact(path: Path, rendered: str) -> None:
    """Atomically replace one generated schema artifact."""
    temporary: Path | None = None
    descriptor: int | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(temporary_name)
        stream = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = None
        with stream:
            stream.write(rendered)
        temporary.replace(path)
    except OSError as error:
        raise SchemaArtifactError(f"Cannot write generated schema artifact: {path}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> None:
    """Generate the canonical schema or check it for drift."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path, default=CANONICAL_OUTPUT)
    parser.add_argument("--check", action="store_true", help="Fail when OUTPUT differs from current generation.")
    args = parser.parse_args(argv)
    try:
        rendered = render_installed_schema()
        if args.check:
            check_schema_artifact(args.output, rendered)
            print(f"Generated schema artifact is current: {args.output}")
        else:
            write_schema_artifact(args.output, rendered)
            schema_view = SchemaView(str(args.output))
            print(f"Wrote {args.output}")
            print(f"  classes: {len(schema_view.all_classes())}")
    except SchemaArtifactError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
