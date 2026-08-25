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
import hashlib
import os
import re
import tempfile
from importlib.metadata import version
from importlib.util import find_spec
from pathlib import Path

from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper

from nmdc_lakehouse.transforms.schema_generator import (
    UNRESOLVED_CONTENT_SHA256,
    flatten_database_schema,
)

CANONICAL_OUTPUT = Path(__file__).resolve().parents[2] / "src/nmdc_lakehouse/schemas/nmdc_metadata.yaml"


class SchemaArtifactError(ValueError):
    """Raised when the canonical target schema cannot be generated or checked."""


# The dumper quotes a 64-character hex string, so the quotes are part of the line and the digest
# is not. Matching without allowing for them found nothing and reported "declares no content
# digest" on a file that declared one.
_SHA_LINE = re.compile(r"""(?m)^\s*value:\s*['"]?(?P<digest>[0-9a-f]{64})['"]?\s*$""")


def _digest_of(rendered: str) -> str:
    """Hash the document as it reads with the digest field blanked out.

    A document cannot contain its own digest, so the value is computed over the rendered text
    with the placeholder still in place. Verification does the substitution in reverse, which
    makes the check reproducible arithmetic rather than a stored number nobody can recompute.
    """
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def resolve_content_digest(rendered: str) -> str:
    """Replace the placeholder digest with the hash of the document that carries it."""
    if UNRESOLVED_CONTENT_SHA256 not in rendered:
        raise SchemaArtifactError("The rendered schema carries no placeholder digest to resolve.")
    return rendered.replace(UNRESOLVED_CONTENT_SHA256, _digest_of(rendered), 1)


def declared_content_digest(rendered: str) -> str | None:
    """Return the digest an artifact declares, or None when it declares none."""
    for match in _SHA_LINE.finditer(rendered):
        digest = match.group("digest")
        if digest != UNRESOLVED_CONTENT_SHA256:
            return digest
    return None


def verify_content_digest(rendered: str) -> None:
    """Fail when an artifact's declared digest does not describe its own content."""
    declared = declared_content_digest(rendered)
    if declared is None:
        raise SchemaArtifactError("The schema artifact declares no content digest.")
    expected = _digest_of(rendered.replace(declared, UNRESOLVED_CONTENT_SHA256, 1))
    if declared != expected:
        raise SchemaArtifactError(
            f"The schema artifact's declared digest {declared[:16]} does not match its content "
            f"({expected[:16]}). The file was edited by hand, or generation changed without a rerun."
        )


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
    return resolve_content_digest(yaml_dumper.dumps(flat_schema))


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
            verify_content_digest(args.output.read_text(encoding="utf-8"))
            print(f"Generated schema artifact is current: {args.output}")
            print(f"  version: {SchemaView(str(args.output)).schema.version}")
        else:
            write_schema_artifact(args.output, rendered)
            schema_view = SchemaView(str(args.output))
            print(f"Wrote {args.output}")
            print(f"  classes: {len(schema_view.all_classes())}")
    except SchemaArtifactError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
