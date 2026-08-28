"""Rebuild the two lakehouse tables that no MongoDB dump can reproduce.

`graph_edges` and `biosample_to_workflow_run` are derived inside the lakehouse from provenance
side tables, so a reload replaces everything they are built from and leaves them describing data
that no longer exists. Nothing in the namespace says they are stale, which is why a promotion
that replaces tables and stops is worse than one that also rebuilds these.

Ported from `notebooks/build_biosample_to_workflow_run.ipynb`, deleted on 2026-08-27, with three
changes that matter for running it unattended:

- One engine. The notebook built `graph_edges` through Spark and then walked it through a Trino
  cursor, so an automated run needed two connections and two sets of credentials.
- No driver-side accumulation. The walk collected every hop into pandas frames in the driver.
- No identifier lists in SQL text. Each hop inlined every frontier id into an `IN (...)` clause,
  which for 33,234 workflow runs is a statement megabytes wide and grows with the data.

The walk is the same algorithm: breadth-first from every workflow run, upstream along provenance
edges, recording biosamples as they are reached and the processing classes seen on the way. The
notebook's preflight cell came across as `check_processing_types_are_accounted_for`, which refuses
where the cell printed a warning.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

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


def unaccounted_processing_types_statement(namespace: str) -> str:
    """MaterialProcessing types present in the data but absent from `PROCESSING_TYPES`.

    `pair_statement` emits one boolean column per entry in `PROCESSING_TYPES`. A type in the data
    that is not in that mapping gets no column, and every workflow that passed through it reads
    as false for every processing step it did take. The table is then wrong in the direction that
    looks right: it says the processing did not happen rather than that it was not measured.
    """
    _check_namespace(namespace)
    known = ", ".join(f"'{nmdc_type}'" for nmdc_type in PROCESSING_TYPES)
    return (
        f"SELECT DISTINCT type FROM {namespace}.material_processing_set "
        f"WHERE type IS NOT NULL AND type NOT IN ({known})"
    )


def check_processing_types_are_accounted_for(spark: object, namespace: str) -> None:
    """Refuse to build the table when the mapping no longer covers the data.

    Refuses rather than warns. This runs before a table is written, a warning in a log is not
    seen by whoever reads the table months later, and the failure it prevents is invisible in the
    output. Carried over from the preflight cell of the notebook this replaced, which printed a
    warning and left it to the operator to notice.
    """
    _check_namespace(namespace)
    try:
        rows = spark.sql(unaccounted_processing_types_statement(namespace)).collect()  # type: ignore[attr-defined]
    except Exception as error:
        raise DerivedTableError("Could not read material_processing_set to check processing types.") from error
    unaccounted = sorted(str(row[0]) for row in rows)
    if unaccounted:
        raise DerivedTableError(
            "material_processing_set holds type(s) with no column in PROCESSING_TYPES: "
            + ", ".join(unaccounted)
            + ". Add them to PROCESSING_TYPES, or every workflow that passed through one will "
            "read as false for the steps it did take."
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


def rebuild_all(
    spark: object,
    namespace: str,
    max_depth: int = DEFAULT_MAX_DEPTH,
    progress: object = None,
) -> list[RebuildOutcome]:
    """Rebuild both derived tables, in the order one depends on the other.

    `graph_edges` first, because `biosample_to_workflow_run` walks it. The order comes from
    `DERIVED_TABLES` rather than from this function, so a caller reading either sees the same
    answer, and the promotion plan built in `berdl_promotion` orders its rebuilds the same way.
    """
    _check_namespace(namespace)
    say = progress if callable(progress) else (lambda _message: None)
    # Named rather than defaulted. An `else` branch sent anything that was not `graph_edges` to
    # the walk, so a third entry in DERIVED_TABLES would have been rebuilt by the wrong function
    # and reported as a success. A table this cannot rebuild is refused instead.
    builders = {
        "graph_edges": lambda: rebuild_graph_edges(spark, namespace),
        "biosample_to_workflow_run": lambda: rebuild_biosample_to_workflow_run(
            spark, namespace, max_depth=max_depth, progress=say
        ),
    }
    unknown = sorted(set(DERIVED_TABLES) - set(builders))
    if unknown:
        raise DerivedTableError("No rebuild procedure exists for: " + ", ".join(unknown) + ".")
    outcomes = []
    for table in DERIVED_TABLES:
        say(f"rebuilding {namespace}.{table}")
        outcomes.append(builders[table]())
    return outcomes


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
    # Before the walk, not after. A rebuild that runs to completion and then reports the mapping
    # was incomplete has already replaced the table with the wrong one.
    check_processing_types_are_accounted_for(spark, namespace)
    say("processing types: all accounted for")

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
