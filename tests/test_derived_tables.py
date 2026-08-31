"""Tests for rebuilding the two derived tables no MongoDB dump can reproduce.

What these cover: the statements produced, the walk's control flow, and every refusal. What they
do NOT cover is whether the SQL computes the right provenance, which needs a live Spark catalog
with real data. That gap is stated in the pull request rather than implied by a green suite.
"""

from __future__ import annotations

from pathlib import Path

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


class FailingSpark(FakeSpark):
    """Raises on the first statement matching a marker, to prove each failure is a message."""

    def __init__(self, marker: str, per_view: dict[str, int] | None = None, default_count: int = 1) -> None:
        super().__init__(counts=per_view, default_count=default_count)
        self._marker = marker

    def sql(self, statement: str):
        if self._marker in statement:
            raise RuntimeError("engine said no")
        return super().sql(statement)


def test_a_failed_graph_edges_write_is_a_message_not_a_traceback() -> None:
    with pytest.raises(DerivedTableError, match="Rebuilding 'nmdc.metadata.graph_edges' failed"):
        rebuild_graph_edges(FailingSpark("CREATE OR REPLACE TABLE nmdc.metadata.graph_edges"), NAMESPACE)


def test_a_failed_count_is_a_message_not_a_traceback() -> None:
    with pytest.raises(DerivedTableError, match="Cannot count rows in"):
        rebuild_graph_edges(FailingSpark("SELECT COUNT(*)"), NAMESPACE)


def test_a_count_returning_more_than_one_row_is_refused() -> None:
    """A catalog answering a count with two rows is broken, not reporting a number."""

    class TwoRowSpark(FakeSpark):
        def sql(self, statement: str):
            self.statements.append(statement)
            if statement.startswith("SELECT COUNT(*)"):
                return FakeFrame([(1,), (2,)], self.views)
            return FakeFrame([], self.views)

    with pytest.raises(DerivedTableError, match="returned 2 rows, expected 1"):
        rebuild_graph_edges(TwoRowSpark(), NAMESPACE)


def test_a_failed_hop_is_a_message_naming_the_view() -> None:
    with pytest.raises(DerivedTableError, match="failed while building 'walk_frontier_0'"):
        rebuild_biosample_to_workflow_run(FailingSpark("workflow_execution_set WHERE id IS NOT NULL"), NAMESPACE)


def test_a_hop_that_finds_no_edges_ends_the_walk_and_says_so() -> None:
    """Distinct from the frontier emptying: this is graph_edges having nothing further."""
    messages: list[str] = []
    spark = ScriptedSpark(
        {
            "walk_frontier_0": 10,
            "walk_step_1": 3,
            "walk_reached_1": 3,
            "walk_frontier_1": 2,
            "walk_processing_1": 1,
            "walk_step_2": 0,
            "walk_reached_all": 3,
            "walk_processing_all": 1,
            f"{NAMESPACE}.biosample_to_workflow_run": 3,
        }
    )

    outcome = rebuild_biosample_to_workflow_run(spark, NAMESPACE, progress=messages.append)

    assert outcome.rows == 3
    assert any("no further edges" in message for message in messages)
    # The processing views collected on the way are unioned rather than replaced by the empty stub.
    assert any("SELECT * FROM walk_processing_1" in statement for statement in spark.statements)


def test_a_failed_final_write_is_a_message_not_a_traceback() -> None:
    spark = FailingSpark(
        "CREATE OR REPLACE TABLE nmdc.metadata.biosample_to_workflow_run",
        per_view={
            "walk_frontier_0": 10,
            "walk_step_1": 5,
            "walk_reached_1": 5,
            "walk_frontier_1": 0,
            "walk_reached_all": 5,
            "walk_processing_all": 0,
        },
    )

    with pytest.raises(DerivedTableError, match="Writing 'nmdc.metadata.biosample_to_workflow_run' failed"):
        rebuild_biosample_to_workflow_run(spark, NAMESPACE)


def test_each_hop_is_materialised_and_released() -> None:
    """A temp view is lazy, so without this each hop's plan contains every earlier hop.

    The count at the end of each hop would re-execute the chain, and the final UNION ALL would
    re-execute all of them again. At fifteen hops the work grows faster than the depth.
    """
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

    rebuild_biosample_to_workflow_run(spark, NAMESPACE)

    cached = [s for s in spark.statements if s.startswith("CACHE TABLE")]
    uncached = [s for s in spark.statements if s.startswith("UNCACHE TABLE")]
    assert "CACHE TABLE walk_frontier_0" in cached
    assert "CACHE TABLE walk_step_1" in cached
    assert len(uncached) == len(cached), "every cached hop is released"


def test_a_failure_to_release_the_cache_does_not_fail_the_rebuild() -> None:
    """Freeing memory is best effort; by then the result is already computed and verified."""

    class UncacheFails(ScriptedSpark):
        def sql(self, statement: str):
            if statement.startswith("UNCACHE TABLE"):
                raise RuntimeError("nothing to uncache")
            return super().sql(statement)

    spark = UncacheFails(
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

    assert rebuild_biosample_to_workflow_run(spark, NAMESPACE).rows == 5


def test_a_refusing_walk_still_releases_its_cache() -> None:
    """Four of this function's five exits are refusals, and each one used to leak.

    Adding the cache made a leak out of every refusal path, so this asserts the release happens
    on one of them rather than only on the success it was written for.
    """
    spark = ScriptedSpark({}, default_count=7)

    with pytest.raises(DerivedTableError, match="still finding paths"):
        rebuild_biosample_to_workflow_run(spark, NAMESPACE, max_depth=2)

    cached = [s for s in spark.statements if s.startswith("CACHE TABLE")]
    uncached = [s for s in spark.statements if s.startswith("UNCACHE TABLE")]
    assert cached, "the walk cached something before refusing"
    assert len(uncached) == len(cached), "and released all of it despite refusing"


def test_a_walk_that_fails_mid_hop_still_releases_its_cache() -> None:
    """The same guarantee for an engine failure rather than a refusal."""
    # The trigger names the walk's join, not the bare table. The processing-type coverage check
    # reads the same table before the walk starts, so "material_processing_set" alone now fires
    # there instead and this test would pass on the wrong failure.
    spark = FailingSpark(
        f"JOIN {NAMESPACE}.material_processing_set",
        per_view={"walk_frontier_0": 10, "walk_step_1": 5, "walk_reached_1": 2, "walk_frontier_1": 3},
    )

    with pytest.raises(DerivedTableError, match="failed while building"):
        rebuild_biosample_to_workflow_run(spark, NAMESPACE)

    cached = [s for s in spark.statements if s.startswith("CACHE TABLE")]
    uncached = [s for s in spark.statements if s.startswith("UNCACHE TABLE")]
    assert cached and len(uncached) == len(cached)


def test_a_processing_type_with_no_column_refuses_the_rebuild() -> None:
    """The notebook printed a warning here. A warning is not seen by whoever reads the table.

    `pair_statement` emits one boolean per entry in PROCESSING_TYPES, so a type in the data with
    no entry gets no column, and every workflow that passed through it reads as false for the
    steps it did take. The table is wrong in the direction that looks right.
    """

    class SparkWithAnUnknownType(FakeSpark):
        def sql(self, statement: str):
            if "NOT IN" in statement and "material_processing_set" in statement:
                self.statements.append(statement)
                return FakeFrame([("nmdc:Sonication",), ("nmdc:Lyophilization",)], self.views)
            return super().sql(statement)

    with pytest.raises(DerivedTableError, match="nmdc:Lyophilization, nmdc:Sonication"):
        rebuild_biosample_to_workflow_run(SparkWithAnUnknownType(), NAMESPACE)


def test_the_coverage_check_runs_before_anything_is_written() -> None:
    """A rebuild that refuses after finishing has already replaced the table with the wrong one."""

    class SparkWithAnUnknownType(FakeSpark):
        def sql(self, statement: str):
            if "NOT IN" in statement and "material_processing_set" in statement:
                self.statements.append(statement)
                return FakeFrame([("nmdc:Sonication",)], self.views)
            return super().sql(statement)

    spark = SparkWithAnUnknownType()
    with pytest.raises(DerivedTableError):
        rebuild_biosample_to_workflow_run(spark, NAMESPACE)

    assert not [s for s in spark.statements if s.startswith("CREATE OR REPLACE TABLE")]
    assert not [s for s in spark.statements if s.startswith("CACHE TABLE")]


def test_every_processing_type_in_the_mapping_is_excluded_from_the_check() -> None:
    """The check must not report the types it already has columns for."""
    from nmdc_lakehouse.derived_tables import PROCESSING_TYPES, unaccounted_processing_types_statement

    statement = unaccounted_processing_types_statement(NAMESPACE)

    for nmdc_type in PROCESSING_TYPES:
        assert f"'{nmdc_type}'" in statement, nmdc_type
    assert statement.count("'") == 2 * len(PROCESSING_TYPES)


def test_a_check_that_cannot_read_the_table_refuses_rather_than_assuming_coverage() -> None:
    """An unreadable table is not the same as a table with nothing unaccounted for.

    Both produce no rows. Treating a failed read as "all types are covered" would turn the guard
    off in exactly the conditions where it is least safe to assume anything.
    """

    class SparkThatCannotRead(FakeSpark):
        def sql(self, statement: str):
            if "NOT IN" in statement and "material_processing_set" in statement:
                raise RuntimeError("catalog unavailable")
            return super().sql(statement)

    with pytest.raises(DerivedTableError, match="Could not read material_processing_set"):
        rebuild_biosample_to_workflow_run(SparkThatCannotRead(), NAMESPACE)


def test_rebuild_all_runs_graph_edges_before_the_table_that_walks_it() -> None:
    """The order is the dependency, and it comes from DERIVED_TABLES rather than from this call.

    `biosample_to_workflow_run` walks `graph_edges`, so rebuilding the consumer first would walk
    provenance that is about to be replaced. Sorting the names alphabetically gives exactly that
    wrong order, which is why the promotion plan derives its order from the same tuple.
    """
    from nmdc_lakehouse.derived_tables import DERIVED_TABLES, rebuild_all

    # The frontier empties at the first hop, so the walk terminates rather than refusing at depth.
    spark = ScriptedSpark({"walk_frontier_1": 0})
    outcomes = rebuild_all(spark, NAMESPACE)

    assert [outcome.table for outcome in outcomes] == [f"{NAMESPACE}.{t}" for t in DERIVED_TABLES]
    creates = [s for s in spark.statements if s.startswith("CREATE OR REPLACE TABLE")]
    assert "graph_edges" in creates[0]


def test_rebuild_all_refuses_an_unqualified_namespace() -> None:
    """It replaces two tables, so an ambiguous catalog is not a small mistake."""
    from nmdc_lakehouse.derived_tables import rebuild_all

    with pytest.raises(DerivedTableError, match="catalog-qualified"):
        rebuild_all(ScriptedSpark({"walk_frontier_1": 0}), "nmdc_metadata")


def test_the_command_previews_without_authorization() -> None:
    """Default is a description. A destructive default is one typo away from a rebuild."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    result = CliRunner().invoke(cli, ["rebuild-derived-tables", "nmdc.metadata", "--ingest-checkout", "/tmp"])

    assert result.exit_code == 0, result.output
    assert "nothing has been changed" in result.output
    assert "graph_edges then biosample_to_workflow_run" in result.output


def test_the_command_refuses_authorization_for_a_different_namespace() -> None:
    """Naming the namespace twice is what stops a path edited in shell history from executing."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    result = CliRunner().invoke(
        cli,
        [
            "rebuild-derived-tables",
            "nmdc.metadata",
            "--ingest-checkout",
            "/tmp",
            "--authorize-namespace",
            "nmdc.something_else",
        ],
    )

    assert result.exit_code != 0
    assert "nmdc.something_else" in result.output
    assert "Traceback" not in result.output


def test_the_command_executes_when_the_namespace_is_named_twice(monkeypatch) -> None:
    """The authorized path, with the session stubbed so no pod is needed."""
    from click.testing import CliRunner

    from nmdc_lakehouse import cli as cli_module
    from nmdc_lakehouse.cli import cli

    spark = ScriptedSpark({"walk_frontier_1": 0})
    monkeypatch.setattr("nmdc_lakehouse.derived_tables.spark_session", lambda _checkout: spark)

    result = CliRunner().invoke(
        cli,
        [
            "rebuild-derived-tables",
            NAMESPACE,
            "--ingest-checkout",
            "/tmp",
            "--authorize-namespace",
            NAMESPACE,
        ],
    )

    assert result.exit_code == 0, result.output
    assert f"rebuilt {NAMESPACE}.graph_edges" in result.output
    assert f"rebuilt {NAMESPACE}.biosample_to_workflow_run" in result.output
    assert "nothing has been changed" not in result.output, "it did change something"
    assert cli_module is not None


def test_a_refusal_during_execution_is_a_message_not_a_traceback(monkeypatch) -> None:
    """An operator reading a stack trace cannot tell a refusal from a crash."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    monkeypatch.setattr(
        "nmdc_lakehouse.derived_tables.spark_session",
        lambda _checkout: ScriptedSpark({}, default_count=7),
    )

    result = CliRunner().invoke(
        cli,
        [
            "rebuild-derived-tables",
            NAMESPACE,
            "--ingest-checkout",
            "/tmp",
            "--authorize-namespace",
            NAMESPACE,
            "--max-depth",
            "2",
        ],
    )

    assert result.exit_code != 0
    assert "still finding paths" in result.output
    assert "Traceback" not in result.output


def test_an_unimportable_runtime_is_refused_by_name(tmp_path: Path) -> None:
    """The session must come from the reviewed checkout, not from whatever is on the path."""
    from nmdc_lakehouse.derived_tables import spark_session

    with pytest.raises(DerivedTableError, match="not importable"):
        spark_session(tmp_path)


def test_a_derived_table_with_no_rebuild_procedure_is_refused(monkeypatch) -> None:
    """An else branch sent anything that was not graph_edges to the walk.

    So a third entry in DERIVED_TABLES would have been rebuilt by the wrong function and reported
    as a success, which is the shape of failure that looks like it worked.
    """
    import nmdc_lakehouse.derived_tables as dt

    monkeypatch.setattr(dt, "DERIVED_TABLES", ("graph_edges", "biosample_to_workflow_run", "something_new"))

    with pytest.raises(DerivedTableError, match="No rebuild procedure exists for: something_new"):
        dt.rebuild_all(ScriptedSpark({"walk_frontier_1": 0}), NAMESPACE)


def test_a_session_imported_from_outside_the_checkout_is_refused(tmp_path: Path, monkeypatch) -> None:
    """Importing is not evidence of where it came from.

    The checkout going first on `sys.path` does not displace a copy installed in the environment,
    and an already-imported module comes back from `sys.modules` without the path being consulted.
    """
    import sys as sys_module
    import types

    from nmdc_lakehouse.derived_tables import spark_session

    elsewhere = tmp_path / "elsewhere" / "setup_spark_session.py"
    elsewhere.parent.mkdir(parents=True)
    elsewhere.write_text("", encoding="utf-8")

    package = types.ModuleType("berdl_notebook_utils")
    module = types.ModuleType("berdl_notebook_utils.setup_spark_session")
    module.__file__ = str(elsewhere)
    module.get_spark_session = lambda: object()
    monkeypatch.setitem(sys_module.modules, "berdl_notebook_utils", package)
    monkeypatch.setitem(sys_module.modules, "berdl_notebook_utils.setup_spark_session", module)

    checkout = tmp_path / "checkout"
    (checkout / "src").mkdir(parents=True)

    with pytest.raises(DerivedTableError, match="not inside"):
        spark_session(checkout)


def test_the_command_refuses_an_unqualified_namespace_before_previewing() -> None:
    """A preview that renders for a value the rebuild always rejects reads as an actionable plan."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    result = CliRunner().invoke(cli, ["rebuild-derived-tables", "nmdc_metadata", "--ingest-checkout", "/tmp"])

    assert result.exit_code != 0, result.output
    assert "must be catalog-qualified" in result.output
    assert "nothing has been changed" not in result.output


def test_the_command_refuses_a_max_depth_below_one_before_previewing() -> None:
    """rebuild_all refuses max_depth < 1, so accepting it at the boundary previews an impossible run."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    result = CliRunner().invoke(
        cli,
        ["rebuild-derived-tables", "nmdc.metadata", "--ingest-checkout", "/tmp", "--max-depth", "0"],
    )

    assert result.exit_code != 0, result.output
    assert "nothing has been changed" not in result.output


def test_rebuild_all_refuses_a_bad_max_depth_before_replacing_anything() -> None:
    """graph_edges is replaced first, so a depth checked inside the walk is checked too late."""
    from nmdc_lakehouse.derived_tables import DerivedTableError, rebuild_all

    class RecordingSpark:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def sql(self, statement: str) -> object:
            self.statements.append(statement)
            raise AssertionError("no statement should reach the engine")

    spark = RecordingSpark()
    with pytest.raises(DerivedTableError, match="max_depth must be at least 1"):
        rebuild_all(spark, "nmdc.metadata", max_depth=0)

    assert spark.statements == []


def test_rebuilding_a_subset_leaves_the_other_table_alone() -> None:
    """rebuild_all replaced every derived table regardless of what a caller wanted.

    A promotion plan can rebuild one and preserve the other, so the documented follow-up to such a
    promotion replaced a table nobody authorized touching.
    """
    from nmdc_lakehouse.derived_tables import DERIVED_TABLES, rebuild_all

    class RecordingSpark:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def sql(self, statement: str) -> object:
            self.statements.append(statement)
            return FakeFrame([(7,)], self.statements)

    spark = RecordingSpark()
    rebuild_all(spark, "nmdc.metadata", tables=["graph_edges"])

    issued = " ".join(spark.statements)
    assert "graph_edges" in issued
    for untouched in set(DERIVED_TABLES) - {"graph_edges"}:
        assert untouched not in issued, issued


def test_rebuilding_an_empty_selection_is_refused() -> None:
    """An empty list is a caller mistake, and defaulting it to everything is the destructive read."""
    from nmdc_lakehouse.derived_tables import DerivedTableError, rebuild_all

    class RefusingSpark:
        def sql(self, statement: str) -> object:
            raise AssertionError("no statement should reach the engine")

    with pytest.raises(DerivedTableError, match="No derived tables were named"):
        rebuild_all(RefusingSpark(), "nmdc.metadata", tables=[])


def test_the_table_option_reaches_the_rebuild_through_the_command(monkeypatch) -> None:
    """The selection was only ever tested by calling rebuild_all directly.

    So the Click option could stop being passed through and every test would still pass, on the
    one path where the consequence is replacing a table the operator excluded.
    """
    from click.testing import CliRunner

    import nmdc_lakehouse.derived_tables as derived_tables
    from nmdc_lakehouse.cli import cli
    from nmdc_lakehouse.derived_tables import DERIVED_TABLES

    class RecordingSpark:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def sql(self, statement: str) -> object:
            self.statements.append(statement)
            return FakeFrame([(7,)], self.statements)

    spark = RecordingSpark()
    monkeypatch.setattr(derived_tables, "spark_session", lambda _checkout: spark)

    result = CliRunner().invoke(
        cli,
        [
            "rebuild-derived-tables",
            "nmdc.metadata",
            "--ingest-checkout",
            "/tmp",
            "--table",
            "graph_edges",
            "--authorize-namespace",
            "nmdc.metadata",
        ],
    )

    assert result.exit_code == 0, result.output
    issued = " ".join(spark.statements)
    assert "graph_edges" in issued
    for excluded in set(DERIVED_TABLES) - {"graph_edges"}:
        assert excluded not in issued, issued
