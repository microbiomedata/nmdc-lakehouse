"""Shared provenance definitions and catalog connection helpers.

Build derived Parquet with ``local_provenance`` before staging.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# One row per side table that contributes provenance edges: (table, source column, destination
# column, slot label). Direction is not a field; it is which column goes on which side. The
# has_output row therefore reads backwards on purpose, because output flows from the process to
# the material and the walk goes upstream from a workflow run.
EDGE_SOURCES = (
    ("workflow_execution_set_was_informed_by", "parent_id", "was_informed_by", "was_informed_by"),
    ("data_generation_set_has_input", "parent_id", "has_input", "has_input"),
    ("material_processing_set_has_output", "has_output", "parent_id", "has_output"),
    ("material_processing_set_has_input", "parent_id", "has_input", "has_input"),
)

# The MaterialProcessing classes recorded as booleans on each biosample-to-workflow pair.
PROCESSING_TYPES = {
    "nmdc:Extraction": "has_extraction",
    "nmdc:LibraryPreparation": "has_library_prep",
    "nmdc:SubSamplingProcess": "has_subsampling",
    "nmdc:Pooling": "has_pooling",
    "nmdc:ChromatographicSeparationProcess": "has_chromatographic_separation",
    "nmdc:DissolvingProcess": "has_dissolving",
    "nmdc:ChemicalConversionProcess": "has_chemical_conversion",
    "nmdc:FiltrationProcess": "has_filtration",
}

DEFAULT_MAX_DEPTH = 15

_QUALIFIED = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")


class DerivedTableError(ValueError):
    """Raised when provenance inputs or catalog access are invalid."""


def check_namespace(namespace: str) -> str:
    """Require an explicit catalog to avoid resolving names in the wrong destination."""
    if not _QUALIFIED.fullmatch(namespace):
        raise DerivedTableError(f"Namespace {namespace!r} must be catalog-qualified as <catalog>.<namespace>.")
    return namespace


def spark_session(checkout: Path) -> object:
    """A Spark session from the reviewed BERDL checkout, not from whatever is importable.

    Same shape as `berdl_metadata._runtime` and for the same reason: the helper has to come from
    the checkout that was reviewed, so an unrelated copy on the path cannot decide what runs.
    """
    source_root = (checkout.expanduser() / "src").resolve()
    sys.path.insert(0, str(source_root))
    try:
        import berdl_notebook_utils.setup_spark_session as session_module
    except ImportError as error:
        raise DerivedTableError("The selected BERDL runtime is not importable.") from error
    finally:
        sys.path.remove(str(source_root))
    # Where it came from, not merely that it imported. Putting the checkout first on sys.path does
    # not displace a copy installed in the environment, and an already-imported module is returned
    # from sys.modules without consulting the path at all. Without this the docstring's claim that
    # the session comes from the reviewed checkout was a hope.
    module_file = getattr(session_module, "__file__", None)
    if module_file is None or not Path(module_file).resolve().is_relative_to(source_root):
        raise DerivedTableError(
            f"berdl_notebook_utils was imported from {module_file!r}, which is not inside "
            f"{source_root}. The session must come from the reviewed checkout."
        )
    return session_module.get_spark_session()
