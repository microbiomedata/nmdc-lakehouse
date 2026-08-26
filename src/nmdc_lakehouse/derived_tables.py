"""Rebuild the two lakehouse tables that no MongoDB dump can reproduce.

`graph_edges` and `biosample_to_workflow_run` are derived inside the lakehouse from provenance
side tables, so a reload replaces everything they are built from and leaves them describing data
that no longer exists. Nothing in the namespace says they are stale, which is why a promotion
that replaces tables and stops is worse than one that also rebuilds these.

Ported from `notebooks/build_biosample_to_workflow_run.ipynb`, with three changes that matter for
running it unattended:

- One engine. The notebook built `graph_edges` through Spark and then walked it through a Trino
  cursor, so an automated run needed two connections and two sets of credentials.
- No driver-side accumulation. The walk collected every hop into pandas frames in the driver.
- No identifier lists in SQL text. Each hop inlined every frontier id into an `IN (...)` clause,
  which for 33,234 workflow runs is a statement megabytes wide and grows with the data.

The walk is the same algorithm: breadth-first from every workflow run, upstream along provenance
edges, recording biosamples as they are reached and the processing classes seen on the way.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# One row per side table that contributes provenance edges: (table, source column, destination
# column, slot label). Direction is not a field; it is which column goes on which side. The
# has_output row therefore reads backwards on purpose, because output flows from the process to
# the material and the walk goes upstream from a workflow run.
_EDGE_SOURCES = (
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

# Rebuilt from the provenance side tables rather than loaded, because no MongoDB dump contains
# them. Named here, beside the code that rebuilds them, so promotion can ask what it must rebuild
# rather than keeping a second list that has to be remembered.
DERIVED_TABLES = ("graph_edges", "biosample_to_workflow_run")

DEFAULT_MAX_DEPTH = 15
BIOSAMPLE_PREFIX = "nmdc:bsm"

_QUALIFIED = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")


class DerivedTableError(ValueError):
    """Raised when a derived table cannot be rebuilt or verified."""


@dataclass(frozen=True)
class RebuildOutcome:
    """What one rebuild produced, read back from the catalog rather than counted in flight."""

    table: str
    rows: int
    depth_reached: int = 0


def _check_namespace(namespace: str) -> str:
    """Require `<catalog>.<namespace>`, for the reason the rest of this repository does.

    An unqualified name resolves in whatever catalog the session points at, and these statements
    replace tables. Writing 87,617 rows into the wrong catalog because a name was ambiguous is
    the kind of mistake that is only obvious afterwards.
    """
    if not _QUALIFIED.fullmatch(namespace):
        raise DerivedTableError(f"Namespace {namespace!r} must be catalog-qualified as <catalog>.<namespace>.")
    return namespace


def graph_edges_statement(namespace: str) -> str:
    """Return the single statement that rebuilds `graph_edges` from its four side tables."""
    _check_namespace(namespace)
    unions = "\n    UNION ALL\n".join(
        f"    SELECT {src} AS src, {dst} AS next_id, '{slot}' AS slot\n    FROM {namespace}.{table}"
        for table, src, dst, slot in _EDGE_SOURCES
    )
    return f"CREATE OR REPLACE TABLE {namespace}.graph_edges AS\n{unions}"


def seed_frontier_statement(namespace: str) -> str:
    """Every workflow run is its own origin at depth zero."""
    _check_namespace(namespace)
    return f"SELECT id AS origin, id AS id FROM {namespace}.workflow_execution_set WHERE id IS NOT NULL"


def hop_statement(namespace: str, frontier_view: str) -> str:
    """One hop upstream: join the current frontier to its outgoing edges.

    A join, not an `IN (...)` list. The notebook inlined every frontier id into the statement
    text, so the statement grew with the data and at 33,234 workflow runs was megabytes wide.
    """
    _check_namespace(namespace)
    return (
        f"SELECT DISTINCT f.origin AS origin, e.next_id AS id "
        f"FROM {frontier_view} f JOIN {namespace}.graph_edges e ON e.src = f.id "
        f"WHERE e.next_id IS NOT NULL"
    )


def reached_biosamples_statement(step_view: str, depth: int) -> str:
    """Rows of this hop that landed on a biosample, stamped with the hop count."""
    return (
        f"SELECT origin AS workflow_run_id, id AS biosample_id, {depth} AS n_hops "
        f"FROM {step_view} WHERE id LIKE '{BIOSAMPLE_PREFIX}%'"
    )


def continuing_frontier_statement(step_view: str) -> str:
    """Rows of this hop that are not biosamples, which is what the next hop walks from."""
    return f"SELECT DISTINCT origin, id FROM {step_view} WHERE id NOT LIKE '{BIOSAMPLE_PREFIX}%'"


def processing_types_statement(namespace: str, frontier_view: str) -> str:
    """Which MaterialProcessing classes the current frontier passes through."""
    _check_namespace(namespace)
    return (
        f"SELECT DISTINCT f.origin AS workflow_run_id, m.type AS processing_type "
        f"FROM {frontier_view} f JOIN {namespace}.material_processing_set m ON m.id = f.id "
        f"WHERE m.type IS NOT NULL"
    )


def pair_statement(namespace: str, reached_view: str, processing_view: str) -> str:
    """Collapse to one row per biosample and workflow run, with the processing booleans.

    `MIN(n_hops)` because a biosample can be reached by more than one path and the documented
    meaning of the column is the minimum number of edges.
    """
    _check_namespace(namespace)
    flags = ",\n       ".join(
        f"MAX(CASE WHEN p.processing_type = '{nmdc_type}' THEN true ELSE false END) AS {column}"
        for nmdc_type, column in PROCESSING_TYPES.items()
    )
    return (
        f"CREATE OR REPLACE TABLE {namespace}.biosample_to_workflow_run AS\n"
        f"SELECT r.biosample_id,\n"
        f"       r.workflow_run_id,\n"
        f"       w.type AS workflow_type,\n"
        f"       MIN(r.n_hops) AS n_hops,\n"
        f"       {flags}\n"
        f"FROM {reached_view} r\n"
        f"JOIN {namespace}.workflow_execution_set w ON w.id = r.workflow_run_id\n"
        f"LEFT JOIN {processing_view} p ON p.workflow_run_id = r.workflow_run_id\n"
        f"GROUP BY r.biosample_id, r.workflow_run_id, w.type"
    )


def _count(spark: object, table: str) -> int:
    """Read a row count back from the catalog, refusing an answer that is not one."""
    try:
        rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()  # type: ignore[attr-defined]
    except Exception as error:
        raise DerivedTableError(f"Cannot count rows in '{table}'.") from error
    if len(rows) != 1:
        raise DerivedTableError(f"Counting '{table}' returned {len(rows)} rows, expected 1.")
    value = rows[0][0]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DerivedTableError(f"Counting '{table}' returned an invalid count: {value!r}")
    return value


def rebuild_graph_edges(spark: object, namespace: str) -> RebuildOutcome:
    """Rebuild `graph_edges` and report the count the catalog gives back.

    No `USING DELTA`. The notebook pinned the format, which was right for the Hive namespace it
    targeted and wrong for an Iceberg one; leaving it out lets the catalog decide.
    """
    _check_namespace(namespace)
    try:
        spark.sql(graph_edges_statement(namespace))  # type: ignore[attr-defined]
    except Exception as error:
        raise DerivedTableError(f"Rebuilding '{namespace}.graph_edges' failed.") from error
    rows = _count(spark, f"{namespace}.graph_edges")
    if rows == 0:
        # An empty edge table makes every later walk terminate immediately and produce an empty
        # biosample_to_workflow_run, which reads as "no provenance" rather than as a failure.
        raise DerivedTableError(
            f"'{namespace}.graph_edges' rebuilt with zero rows, so its four source side tables "
            "are empty or missing. Refusing, because the walk would then report no provenance."
        )
    return RebuildOutcome(table=f"{namespace}.graph_edges", rows=rows)


def rebuild_biosample_to_workflow_run(
    spark: object,
    namespace: str,
    max_depth: int = DEFAULT_MAX_DEPTH,
    progress: object = None,
) -> RebuildOutcome:
    """Walk upstream from every workflow run to the biosamples behind it.

    Breadth-first, one hop per iteration, entirely inside the engine. Stops when a hop reaches
    nothing new, and refuses rather than truncating if it is still finding paths at `max_depth`,
    because a silently truncated walk loses provenance for the deepest samples only, which is the
    hardest kind of gap to notice.
    """
    _check_namespace(namespace)
    if max_depth < 1:
        raise DerivedTableError("max_depth must be at least 1.")
    say = progress if callable(progress) else (lambda _message: None)

    cached: list[str] = []

    def run(statement: str, view: str) -> int:
        """Build one temp view, materialise it, and return its row count.

        The CACHE is not an optimisation detail. A temp view is lazy, so without it each hop's
        plan contains every earlier hop, the count at the end of each hop re-executes the chain,
        and the final UNION ALL re-executes all of them again. At fifteen hops that is a plan
        that grows with depth and work that grows faster.
        """
        try:
            frame = spark.sql(statement)  # type: ignore[attr-defined]
            frame.createOrReplaceTempView(view)
            spark.sql(f"CACHE TABLE {view}")  # type: ignore[attr-defined]
        except Exception as error:
            raise DerivedTableError(f"The walk failed while building '{view}'.") from error
        cached.append(view)
        return _count(spark, view)

    def release() -> None:
        """Drop the cached hops. Best effort: a failure here has not lost any result."""
        for view in cached:
            try:
                spark.sql(f"UNCACHE TABLE {view}")  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - releasing memory must not fail the rebuild
                pass

    # try/finally, because every refusal below leaves cached hops behind otherwise, and there
    # are four of them. Adding the cache in the previous commit made a leak out of each one.
    try:
        run(seed_frontier_statement(namespace), "walk_frontier_0")
        reached_views: list[str] = []
        processing_views: list[str] = []
        depth_reached = 0

        for depth in range(1, max_depth + 1):
            frontier = f"walk_frontier_{depth - 1}"
            step = f"walk_step_{depth}"
            if run(hop_statement(namespace, frontier), step) == 0:
                say(f"hop {depth}: no further edges, walk complete")
                break

            reached = f"walk_reached_{depth}"
            if run(reached_biosamples_statement(step, depth), reached) > 0:
                reached_views.append(reached)

            next_frontier = f"walk_frontier_{depth}"
            remaining = run(continuing_frontier_statement(step), next_frontier)
            depth_reached = depth
            say(f"hop {depth}: {remaining} node(s) still to walk")
            if remaining == 0:
                break

            processing = f"walk_processing_{depth}"
            if run(processing_types_statement(namespace, next_frontier), processing) > 0:
                processing_views.append(processing)
        else:
            raise DerivedTableError(
                f"The walk was still finding paths at depth {max_depth}. Raise max_depth rather than "
                "accepting a truncated result, which would lose provenance only for the deepest "
                "samples and look like a complete table."
            )

        if not reached_views:
            raise DerivedTableError(
                "The walk reached no biosamples at any depth, so there is nothing to write. Refusing "
                "rather than replacing the table with an empty one."
            )

        run(" UNION ALL ".join(f"SELECT * FROM {view}" for view in reached_views), "walk_reached_all")
        if processing_views:
            run(" UNION ALL ".join(f"SELECT * FROM {view}" for view in processing_views), "walk_processing_all")
        else:
            run(
                "SELECT CAST(NULL AS STRING) AS workflow_run_id, CAST(NULL AS STRING) AS processing_type WHERE false",
                "walk_processing_all",
            )

        try:
            spark.sql(pair_statement(namespace, "walk_reached_all", "walk_processing_all"))  # type: ignore[attr-defined]
        except Exception as error:
            raise DerivedTableError(f"Writing '{namespace}.biosample_to_workflow_run' failed.") from error

        rows = _count(spark, f"{namespace}.biosample_to_workflow_run")
        return RebuildOutcome(table=f"{namespace}.biosample_to_workflow_run", rows=rows, depth_reached=depth_reached)
    finally:
        release()
