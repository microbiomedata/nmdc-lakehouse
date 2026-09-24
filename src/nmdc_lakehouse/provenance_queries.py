"""Compare equivalent provenance queries over local Parquet, without returning IDs."""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any

import duckdb

from nmdc_lakehouse.derived_tables import EDGE_SOURCES, DerivedTableError
from nmdc_lakehouse.snapshot_manifest import validate_snapshot

SOURCE_EDGES_SQL = " UNION ALL ".join(
    f"SELECT {source} AS src, {target} AS next_id FROM {table}" for table, source, target, _ in EDGE_SOURCES
)
PAIR_SQL = """WITH RECURSIVE edges AS ({edges}), walk(origin, id, depth) AS (
    SELECT id, id, 0 FROM workflow_execution_set
    UNION
    SELECT w.origin, e.next_id, w.depth + 1
    FROM walk w JOIN edges e ON e.src = w.id
    WHERE w.depth < {max_depth}
      AND NOT EXISTS (SELECT 1 FROM biosample_set b WHERE b.id = w.id)
)
SELECT w.origin AS workflow_run_id, w.id AS biosample_id, MIN(w.depth) AS n_hops
FROM walk w JOIN biosample_set b ON b.id = w.id
GROUP BY w.origin, w.id ORDER BY w.origin, w.id
"""
SAVED_PAIRS_SQL = """SELECT workflow_run_id, biosample_id, n_hops FROM biosample_to_workflow_run
ORDER BY workflow_run_id, biosample_id
"""
DIRECT_PAIRS_SQL = """SELECT DISTINCT w.parent_id AS workflow_run_id, b.id AS biosample_id, 2 AS n_hops
FROM workflow_execution_set_was_informed_by w
JOIN data_generation_set_has_input d ON d.parent_id = w.was_informed_by
JOIN biosample_set b ON b.id = d.has_input
ORDER BY w.parent_id, b.id
"""


def compare_provenance_queries(snapshot_root: Path, derived_root: Path, *, repeats: int = 3) -> dict[str, Any]:
    """Check all pairs/hops for equality, then time three queries with rotating order.

    Timings include fetching sorted rows, exclude integrity checking and view setup,
    and use warm OS caches. They measure this local engine, not BERDL latency.
    """
    if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats < 1:
        raise DerivedTableError("repeats must be a positive integer.")
    parent = validate_snapshot(snapshot_root)
    derived = validate_snapshot(derived_root)
    if derived.scope != "derived-provenance-snapshot" or derived.parent_snapshot_id != parent.snapshot_id:
        raise DerivedTableError("Derived snapshot must name this input snapshot as its parent.")
    metrics = json.loads((derived_root / derived.performance_record.path).read_text(encoding="utf-8"))
    depth = metrics["max_depth"]
    if isinstance(depth, bool) or not isinstance(depth, int) or depth < 1:
        raise DerivedTableError("Derived metrics must record a positive integer max_depth.")
    queries = {
        "four_source_tables": PAIR_SQL.format(edges=SOURCE_EDGES_SQL, max_depth=depth),
        "graph_edges": PAIR_SQL.format(edges="SELECT src, next_id FROM graph_edges", max_depth=depth),
        "saved_mapping": SAVED_PAIRS_SQL,
    }
    with duckdb.connect(config={"memory_limit": "512MiB", "threads": "2"}) as connection:
        for table in ("workflow_execution_set", "biosample_set", *(edge[0] for edge in EDGE_SOURCES)):
            connection.from_parquet(str(snapshot_root / f"{table}.parquet")).create_view(table)
        for table in ("graph_edges", "biosample_to_workflow_run"):
            connection.from_parquet(str(derived_root / f"{table}.parquet")).create_view(table)
        expected = connection.execute(SAVED_PAIRS_SQL).fetchall()
        timings: dict[str, list[float]] = {name: [] for name in queries}
        names = list(queries)
        for repetition in range(repeats):
            # Rotate which query runs first; this is still a warm-cache local comparison.
            order = names[repetition % len(names) :] + names[: repetition % len(names)]
            for name in order:
                started = time.perf_counter()
                actual = connection.execute(queries[name]).fetchall()
                timings[name].append(time.perf_counter() - started)
                if actual != expected:
                    raise DerivedTableError(f"{name} disagrees with the saved mapping's pairs or minimum hops.")
        direct = connection.execute(DIRECT_PAIRS_SQL).fetchall()
        if not set(direct).issubset(expected):
            raise DerivedTableError("The direct-join result is not a subset of the complete mapping.")
    return {
        "status": "success",
        "input_snapshot_id": parent.snapshot_id,
        "derived_snapshot_id": derived.snapshot_id,
        "engine": "DuckDB",
        "engine_version": duckdb.__version__,
        "memory_limit": "512MiB",
        "threads": 2,
        "measurement": "warm-cache query execution plus sorted result fetch; excludes validation and view setup",
        "max_depth": depth,
        "repeats": repeats,
        "equivalent_pairs_and_hops": len(expected),
        "direct_two_hop_pairs": len(direct),
        "pairs_missed_by_direct_join": len(expected) - len(direct),
        "queries": queries,
        "seconds": timings,
        "median_seconds": {name: statistics.median(values) for name, values in timings.items()},
    }
