"""Tests for rebuilding the two derived tables no MongoDB dump can reproduce.

What these cover: the statements produced, the walk's control flow, and every refusal. What they
do NOT cover is whether the SQL computes the right provenance, which needs a live Spark catalog
with real data. That gap is stated in the pull request rather than implied by a green suite.
"""

from __future__ import annotations

import pytest

from nmdc_lakehouse.derived_tables import (
    DEFAULT_MAX_DEPTH,
    DerivedTableError,
    graph_edges_statement,
    hop_statement,
    pair_statement,
    rebuild_biosample_to_workflow_run,
    rebuild_graph_edges,
    seed_frontier_statement,
)

NAMESPACE = "nmdc.metadata"


class FakeFrame:
    def __init__(self, rows: list[tuple], recorder: list[str]) -> None:
        self._rows = rows
        self._recorder = recorder

    def collect(self) -> list[tuple]:
        return self._rows

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - Spark's spelling
        self._recorder.append(f"view:{name}")


class FakeSpark:
    """Answers counts from a script and records every statement it is given."""

    def __init__(self, counts: dict[str, int] | None = None, default_count: int = 1) -> None:
        self.statements: list[str] = []
        self.views: list[str] = []
        self._counts = counts or {}
        self._default = default_count

    def sql(self, statement: str):
        self.statements.append(statement)
        if statement.startswith("SELECT COUNT(*)"):
            table = statement.split(" FROM ", 1)[1].strip()
            return FakeFrame([(self._counts.get(table, self._default),)], self.views)
        return FakeFrame([], self.views)


def test_every_statement_refuses_an_unqualified_namespace() -> None:
    """These statements replace tables, so an ambiguous catalog is not a small mistake."""
    for build in (graph_edges_statement, seed_frontier_statement):
        with pytest.raises(DerivedTableError, match="catalog-qualified"):
            build("nmdc_metadata")
    with pytest.raises(DerivedTableError, match="catalog-qualified"):
        hop_statement("nmdc_metadata", "frontier")
    with pytest.raises(DerivedTableError, match="catalog-qualified"):
        pair_statement("nmdc_metadata", "reached", "processing")


def test_graph_edges_unions_its_four_sources_with_has_output_reversed() -> None:
    """The has_output edge points the other way, and reversing it wrongly breaks every walk."""
    statement = graph_edges_statement(NAMESPACE)

    assert statement.count("UNION ALL") == 3
    assert f"FROM {NAMESPACE}.workflow_execution_set_was_informed_by" in statement
    assert f"FROM {NAMESPACE}.data_generation_set_has_input" in statement
    assert f"FROM {NAMESPACE}.material_processing_set_has_input" in statement
    assert "SELECT has_output AS src, parent_id AS next_id" in statement


def test_graph_edges_does_not_pin_a_table_format() -> None:
    """The notebook wrote USING DELTA, which is wrong for the Iceberg catalog this now targets."""
    assert "USING DELTA" not in graph_edges_statement(NAMESPACE).upper()


def test_a_hop_joins_rather_than_listing_identifiers() -> None:
    """The notebook inlined every frontier id, so the statement grew with the data."""
    statement = hop_statement(NAMESPACE, "walk_frontier_0")

    assert " JOIN " in statement
    assert "IN (" not in statement


def test_rebuilding_graph_edges_reports_the_count_the_catalog_gives_back() -> None:
    spark = FakeSpark(counts={f"{NAMESPACE}.graph_edges": 87617})

    outcome = rebuild_graph_edges(spark, NAMESPACE)

    assert outcome.rows == 87617
    assert outcome.table == f"{NAMESPACE}.graph_edges"


def test_an_empty_graph_edges_is_refused() -> None:
    """Zero edges makes every later walk terminate at once and look like absent provenance."""
    spark = FakeSpark(counts={f"{NAMESPACE}.graph_edges": 0})

    with pytest.raises(DerivedTableError, match="zero rows"):
        rebuild_graph_edges(spark, NAMESPACE)


class ScriptedSpark(FakeSpark):
    """Counts driven per view name, so a walk's shape can be scripted hop by hop."""

    def __init__(self, per_view: dict[str, int], default_count: int = 1) -> None:
        super().__init__(default_count=default_count)
        self._per_view = per_view

    def sql(self, statement: str):
        self.statements.append(statement)
        if statement.startswith("SELECT COUNT(*)"):
            name = statement.split(" FROM ", 1)[1].strip()
            return FakeFrame([(self._per_view.get(name, self._default),)], self.views)
        return FakeFrame([], self.views)


def test_the_walk_stops_when_a_hop_finds_no_edges() -> None:
    """Termination on an exhausted frontier, not on running out of patience."""
    spark = ScriptedSpark(
        {
            "walk_frontier_0": 10,
            "walk_step_1": 5,
            "walk_reached_1": 5,
            "walk_frontier_1": 0,
            "walk_reached_all": 5,
            "walk_processing_all": 0,
            f"{NAMESPACE}.biosample_to_workflow_run": 5,
        }
    )

    outcome = rebuild_biosample_to_workflow_run(spark, NAMESPACE)

    assert outcome.rows == 5
    assert outcome.depth_reached == 1
    assert not [s for s in spark.statements if "walk_step_2" in s], "it should not have hopped again"


def test_a_walk_still_finding_paths_at_max_depth_is_refused() -> None:
    """Truncating loses provenance only for the deepest samples, which is the hardest gap to see."""
    spark = ScriptedSpark({}, default_count=7)

    with pytest.raises(DerivedTableError, match="still finding paths at depth 3"):
        rebuild_biosample_to_workflow_run(spark, NAMESPACE, max_depth=3)


def test_a_walk_that_reaches_no_biosamples_is_refused() -> None:
    """Replacing a populated table with an empty one is the failure this exists to prevent."""
    spark = ScriptedSpark(
        {
            "walk_frontier_0": 10,
            "walk_step_1": 5,
            "walk_reached_1": 0,
            "walk_frontier_1": 0,
        }
    )

    with pytest.raises(DerivedTableError, match="reached no biosamples"):
        rebuild_biosample_to_workflow_run(spark, NAMESPACE)


def test_max_depth_below_one_is_refused() -> None:
    with pytest.raises(DerivedTableError, match="at least 1"):
        rebuild_biosample_to_workflow_run(FakeSpark(), NAMESPACE, max_depth=0)


def test_a_count_that_is_not_a_count_is_refused() -> None:
    """A catalog answering something other than a non-negative integer is a failure, not a zero."""

    class OddSpark(FakeSpark):
        def sql(self, statement: str):
            self.statements.append(statement)
            if statement.startswith("SELECT COUNT(*)"):
                return FakeFrame([(None,)], self.views)
            return FakeFrame([], self.views)

    with pytest.raises(DerivedTableError, match="invalid count"):
        rebuild_graph_edges(OddSpark(), NAMESPACE)


def test_progress_is_reported_per_hop_when_a_callback_is_given() -> None:
    messages: list[str] = []
    spark = ScriptedSpark(
        {
            "walk_frontier_0": 10,
            "walk_step_1": 5,
            "walk_reached_1": 5,
            "walk_frontier_1": 0,
            "walk_reached_all": 5,
            "walk_processing_all": 0,
            f"{NAMESPACE}.biosample_to_workflow_run": 5,
        }
    )

    rebuild_biosample_to_workflow_run(spark, NAMESPACE, progress=messages.append)

    assert any("hop 1" in message for message in messages)


def test_the_default_depth_matches_the_notebook_it_replaces() -> None:
    """A silent change here would alter results without changing any statement."""
    assert DEFAULT_MAX_DEPTH == 15
