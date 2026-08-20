"""Tests for the BERDL promotion and recovery capability probe."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.berdl_promotion_probe import (
    BerdlPromotionProbeError,
    ProbeOperation,
    ProbeVerdict,
    build_promotion_probe_plan,
    load_promotion_probe_plan,
    plan_sha256,
    render_promotion_probe,
    run_promotion_probe,
    write_promotion_probe_outcome,
)
from nmdc_lakehouse.cli import cli

TENANT = "nmdc"
SOURCE = "nmdc.promotion_probe_20260820"
DESTINATION = "nmdc.promotion_probe_20260820_target"


class ParseException(Exception):
    """Stands in for the Spark parser failure without importing Spark."""


class FakeSpark:
    """A small Iceberg-like catalog: tables exist or they do not, and mutations advance snapshots."""

    version = "3.5.1"

    def __init__(self, failures: dict[str, Exception] | None = None, retention: str | None = "604800000") -> None:
        self.failures = failures or {}
        self.retention = retention
        self.settings = {
            "spark.sql.catalogImplementation": "in-memory",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        }
        self.statements: list[str] = []
        self.tables: dict[str, int] = {}
        self.snapshots: dict[str, int] = {}
        self.history: dict[str, dict[int, int]] = {}
        self.next_snapshot = 1000
        self.catalog = SimpleNamespace(listColumns=lambda _table: [SimpleNamespace(name="id", dataType="int")])

    def _commit(self, table: str, rows: int) -> None:
        self.next_snapshot += 1
        self.tables[table] = rows
        self.snapshots[table] = self.next_snapshot
        self.history.setdefault(table, {})[self.next_snapshot] = rows

    def _require(self, table: str) -> None:
        if table not in self.tables:
            raise RuntimeError(f"TABLE_OR_VIEW_NOT_FOUND: {table}")

    def sql(self, statement: str):
        self.statements.append(statement)
        for marker, error in self.failures.items():
            if marker in statement:
                raise error
        return SimpleNamespace(collect=lambda: self._answer(statement))

    def _answer(self, statement: str):
        if statement.startswith("EXPLAIN "):
            return [("plan",)]
        if statement.startswith("CREATE NAMESPACE"):
            return []
        if statement.startswith("DROP TABLE IF EXISTS "):
            table = statement.split()[-1]
            self.tables.pop(table, None)
            self.snapshots.pop(table, None)
            return []
        if statement.startswith("SHOW TABLES IN "):
            namespace = statement.split()[3]
            name = statement.split("LIKE")[1].strip().strip("'")
            return [(namespace, name)] if f"{namespace}.{name}" in self.tables else []
        if statement.startswith("CREATE OR REPLACE TABLE "):
            target = statement.split()[4]
            source = statement.rsplit(" ", 1)[-1]
            self._require(source)
            self._commit(target, self.tables[source])
            return []
        if statement.startswith("CREATE TABLE "):
            self._commit(statement.split()[2], 0)
            return []
        if statement.startswith("INSERT INTO "):
            table = statement.split()[2]
            self._require(table)
            added = statement.count("(") - statement.count("VALUES") + 1 if "VALUES" in statement else 1
            self._commit(table, self.tables[table] + max(added, 1))
            return []
        if statement.startswith("ALTER TABLE ") and "RENAME TO" in statement:
            source, target = statement.split()[2], statement.split()[-1]
            self._require(source)
            rows = self.tables.pop(source)
            self.snapshots.pop(source, None)
            self._commit(target, rows)
            return []
        if statement.startswith("CALL ") and "rollback_to_snapshot" in statement:
            table, snapshot = _call_arguments(statement)
            self._require(table)
            if snapshot not in self.history.get(table, {}):
                raise RuntimeError(f"Cannot roll back to unknown snapshot {snapshot}")
            self._commit(table, self.history[table][snapshot])
            return []
        if statement.startswith("CALL ") and "set_current_snapshot" in statement:
            table, snapshot = _call_arguments(statement)
            self._require(table)
            if snapshot not in self.history.get(table, {}):
                raise RuntimeError(f"Cannot set an unknown snapshot {snapshot}")
            return []
        if statement.startswith("SELECT snapshot_id"):
            table = statement.split("FROM ")[1].split(".snapshots")[0]
            self._require(table)
            return [(self.snapshots[table],)]
        if statement.startswith("SELECT COUNT(*)"):
            table = statement.split("FROM ")[1].strip()
            self._require(table)
            return [(self.tables[table],)]
        if statement.startswith("SHOW TBLPROPERTIES"):
            return [] if self.retention is None else [("history.expire.max-snapshot-age-ms", self.retention)]
        if statement.startswith("SET "):
            return [(statement[4:].strip(), self.settings.get(statement[4:].strip(), "<undefined>"))]
        if statement.startswith("SELECT current_user()"):
            return [("mamillerpa",)]
        raise AssertionError(f"Unexpected statement: {statement}")


def _call_arguments(statement: str) -> tuple[str, int]:
    inner = statement[statement.index("(") + 1 : statement.rindex(")")]
    table, snapshot = inner.split(",")
    return table.strip().strip("'"), int(snapshot)


def _plan():
    return build_promotion_probe_plan(tenant=TENANT, source_namespace=SOURCE, destination_namespace=DESTINATION)


def _run(spark, plan=None):
    plan = plan or _plan()
    return run_promotion_probe(plan, authorize_plan_sha256=plan_sha256(plan), runtime=lambda: spark)


def test_plan_rejects_a_canonical_dataset() -> None:
    with pytest.raises(BerdlPromotionProbeError, match="canonical NMDC dataset"):
        build_promotion_probe_plan(
            tenant=TENANT, source_namespace="nmdc.nmdc_metadata", destination_namespace=DESTINATION
        )


def test_plan_rejects_a_dataset_without_the_disposable_marker() -> None:
    with pytest.raises(BerdlPromotionProbeError, match="disposable"):
        build_promotion_probe_plan(
            tenant=TENANT, source_namespace="nmdc.scratch_area", destination_namespace=DESTINATION
        )


def test_plan_rejects_a_namespace_outside_the_tenant() -> None:
    with pytest.raises(BerdlPromotionProbeError, match="inside the requested tenant"):
        build_promotion_probe_plan(
            tenant=TENANT, source_namespace="other.promotion_probe_1", destination_namespace=DESTINATION
        )


def test_plan_rejects_identical_namespaces() -> None:
    with pytest.raises(BerdlPromotionProbeError, match="must be distinct"):
        build_promotion_probe_plan(tenant=TENANT, source_namespace=SOURCE, destination_namespace=SOURCE)


def test_execution_requires_the_exact_reviewed_plan() -> None:
    plan = _plan()
    with pytest.raises(BerdlPromotionProbeError, match="authorization does not match"):
        run_promotion_probe(plan, authorize_plan_sha256="0" * 64, runtime=lambda: pytest.fail("must not connect"))


def test_supported_rename_skips_the_replacement_attempt() -> None:
    outcome = _run(FakeSpark())

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.CROSS_NAMESPACE_RENAME].verdict is ProbeVerdict.SUPPORTED
    assert steps[ProbeOperation.REPLACEMENT].verdict is ProbeVerdict.NOT_ATTEMPTED
    assert outcome.status == "probe-complete"
    assert outcome.snapshot_retention_ms == 604800000


def test_unsupported_rename_is_classified_and_falls_back_to_replacement() -> None:
    spark = FakeSpark(failures={"RENAME TO": ParseException("mismatched input 'RENAME'")})

    outcome = _run(spark)

    steps = {step.operation: step for step in outcome.steps}
    rename = steps[ProbeOperation.CROSS_NAMESPACE_RENAME]
    assert rename.verdict is ProbeVerdict.UNSUPPORTED_SYNTAX
    assert rename.error_type == "ParseException"
    assert steps[ProbeOperation.REPLACEMENT].verdict is ProbeVerdict.SUPPORTED


def test_denied_rename_is_classified_as_insufficient_grants() -> None:
    spark = FakeSpark(failures={"RENAME TO": RuntimeError("AccessDenied: principal lacks TABLE_WRITE")})

    outcome = _run(spark)

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.CROSS_NAMESPACE_RENAME].verdict is ProbeVerdict.INSUFFICIENT_GRANTS


def test_unrecognized_failure_is_not_reported_as_a_known_cause() -> None:
    spark = FakeSpark(failures={"RENAME TO": RuntimeError("something nobody has seen before")})

    outcome = _run(spark)

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.CROSS_NAMESPACE_RENAME].verdict is ProbeVerdict.UNCLASSIFIED_FAILURE
    assert outcome.status == "probe-incomplete"


def test_missing_retention_is_reported_as_an_unresolved_question() -> None:
    outcome = _run(FakeSpark(retention=None))

    assert outcome.snapshot_retention_ms is None
    assert any("retention" in question for question in outcome.unresolved_questions)


def test_probe_never_names_a_canonical_object_in_any_statement() -> None:
    spark = FakeSpark()

    _run(spark)

    joined = "\n".join(spark.statements)
    for canonical in ("nmdc_metadata", "nmdc_results", "nmdc_ref_data"):
        assert canonical not in joined


def test_outcome_is_written_once_and_never_replaced(tmp_path: Path) -> None:
    outcome = _run(FakeSpark())
    destination = tmp_path / "probe-outcome.json"

    written = write_promotion_probe_outcome(destination, outcome)

    assert json.loads(written.read_text(encoding="utf-8"))["status"] == "probe-complete"
    with pytest.raises(BerdlPromotionProbeError, match="Refusing to replace"):
        write_promotion_probe_outcome(destination, outcome)


def test_plan_round_trips_through_disk(tmp_path: Path) -> None:
    plan = _plan()
    path = tmp_path / "probe-plan.json"
    path.write_text(render_promotion_probe(plan), encoding="utf-8")

    assert load_promotion_probe_plan(path) == plan


def test_plan_loader_rejects_invalid_content(tmp_path: Path) -> None:
    path = tmp_path / "probe-plan.json"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(BerdlPromotionProbeError, match="not valid"):
        load_promotion_probe_plan(path)


def test_cli_preview_is_offline_and_prints_the_plan_digest(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "berdl-promotion-probe",
            TENANT,
            SOURCE,
            DESTINATION,
            "--output",
            str(tmp_path / "probe-outcome.json"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["source_namespace"] == SOURCE
    assert f"plan_sha256={plan_sha256(_plan())}" in result.stderr
    assert not (tmp_path / "probe-outcome.json").exists()


def test_cli_execution_requires_the_exact_plan_digest(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "berdl-promotion-probe",
            TENANT,
            SOURCE,
            DESTINATION,
            "--output",
            str(tmp_path / "probe-outcome.json"),
            "--execute-probe",
            "--authorize-plan-sha256",
            "0" * 64,
        ],
    )

    assert result.exit_code != 0
    assert "exact reviewed probe plan" in result.output


def test_plan_rejects_an_unsafe_tenant() -> None:
    with pytest.raises(BerdlPromotionProbeError, match="safe identifier"):
        build_promotion_probe_plan(
            tenant="1-bad", source_namespace="1-bad.promotion_probe_1", destination_namespace=DESTINATION
        )


def test_capability_failure_is_distinguished_from_syntax_and_grants() -> None:
    spark = FakeSpark(failures={"RENAME TO": RuntimeError("UnsupportedOperationException: rename across namespaces")})

    outcome = _run(spark)

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.CROSS_NAMESPACE_RENAME].verdict is ProbeVerdict.UNAVAILABLE_CAPABILITY
    assert outcome.status == "probe-complete"


def test_namespace_creation_failure_stops_before_any_mutation() -> None:
    spark = FakeSpark(failures={"CREATE NAMESPACE": RuntimeError("AccessDenied")})

    with pytest.raises(BerdlPromotionProbeError, match="disposable probe namespaces"):
        _run(spark)

    assert not any("RENAME TO" in statement for statement in spark.statements)


def test_table_creation_failure_stops_before_any_mutation() -> None:
    spark = FakeSpark(failures={"CREATE TABLE": RuntimeError("AccessDenied")})

    with pytest.raises(BerdlPromotionProbeError, match="disposable probe table"):
        _run(spark)


def test_unobservable_tables_stop_the_probe_before_mutation() -> None:
    class Blind(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("SELECT COUNT(*)"):
                raise RuntimeError("catalog unavailable")
            return super().sql(statement)

    spark = Blind()

    with pytest.raises(BerdlPromotionProbeError, match="observe every disposable table"):
        _run(spark)

    assert not any("RENAME TO" in statement for statement in spark.statements)


def test_missing_snapshot_identifier_reports_recovery_as_untested() -> None:
    class NoSnapshots(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("SELECT snapshot_id"):
                return []
            return super()._answer(statement)

    outcome = _run(NoSnapshots())

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.ROLLBACK_TO_SNAPSHOT].verdict is ProbeVerdict.NOT_ATTEMPTED
    assert steps[ProbeOperation.SET_CURRENT_SNAPSHOT].verdict is ProbeVerdict.NOT_ATTEMPTED
    assert any("no recovery operation" in question for question in outcome.unresolved_questions)


def test_plan_loader_rejects_an_unreadable_file(tmp_path: Path) -> None:
    with pytest.raises(BerdlPromotionProbeError, match="Cannot read"):
        load_promotion_probe_plan(tmp_path / "absent.json")


def test_rollback_uses_the_destination_snapshot_after_a_replacement() -> None:
    """The recovery point must come from the promoted table, not the source it was copied from."""
    spark = FakeSpark(failures={"RENAME TO": ParseException("mismatched input 'RENAME'")})

    outcome = _run(spark)

    steps = {step.operation: step for step in outcome.steps}
    rollback = steps[ProbeOperation.ROLLBACK_TO_SNAPSHOT]
    assert rollback.verdict is ProbeVerdict.SUPPORTED
    assert rollback.independently_verified is True


def test_rollback_is_verified_by_row_count_not_by_the_call_returning() -> None:
    outcome = _run(FakeSpark())

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.RECOVERY_PRECONDITION].verdict is ProbeVerdict.SUPPORTED
    assert steps[ProbeOperation.ROLLBACK_TO_SNAPSHOT].independently_verified is True
    assert not any("did not return to its pre-mutation" in q for q in outcome.unresolved_questions)


def test_a_rollback_that_does_not_restore_rows_is_reported_as_unreliable() -> None:
    class LyingRollback(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("CALL ") and "rollback_to_snapshot" in statement:
                return []
            return super()._answer(statement)

    outcome = _run(LyingRollback())

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.ROLLBACK_TO_SNAPSHOT].independently_verified is False
    assert any("did not return to its pre-mutation" in q for q in outcome.unresolved_questions)


def test_injected_failure_really_fails_and_leaves_a_mixed_state() -> None:
    spark = FakeSpark()

    outcome = _run(spark)

    steps = {step.operation: step for step in outcome.steps}
    injection = steps[ProbeOperation.INJECTED_FAILURE_RECOVERY]
    assert injection.verdict is not ProbeVerdict.SUPPORTED
    assert injection.independently_verified is True
    assert any("partial promotion is observable" in q for q in outcome.unresolved_questions)


def test_setup_clears_leftovers_in_both_namespaces() -> None:
    spark = FakeSpark()
    spark._commit(f"{DESTINATION}.probe_first", 99)

    _run(spark)

    dropped = [s for s in spark.statements if s.startswith("DROP TABLE IF EXISTS")]
    assert f"DROP TABLE IF EXISTS {DESTINATION}.probe_first" in dropped
    assert f"DROP TABLE IF EXISTS {SOURCE}.probe_first" in dropped


def test_environment_records_configuration_values_not_key_names() -> None:
    outcome = _run(FakeSpark())

    assert outcome.environment.catalog_implementation == "in-memory"
    assert outcome.environment.iceberg_version.startswith("org.apache.iceberg")
    assert outcome.environment.spark_version == "3.5.1"


def test_unset_configuration_is_recorded_as_absent_not_as_the_literal_undefined() -> None:
    spark = FakeSpark()
    spark.settings = {}

    outcome = _run(spark)

    assert outcome.environment.catalog_implementation is None
    assert outcome.environment.iceberg_version is None


def test_provider_error_condition_is_recorded_and_used_for_classification() -> None:
    class Denied(Exception):
        def getCondition(self):  # noqa: N802 - mirrors the provider API name
            return "INSUFFICIENT_PRIVILEGES"

    spark = FakeSpark(failures={"RENAME TO": Denied("opaque provider text")})

    outcome = _run(spark)

    rename = {step.operation: step for step in outcome.steps}[ProbeOperation.CROSS_NAMESPACE_RENAME]
    assert rename.error_condition == "INSUFFICIENT_PRIVILEGES"
    assert rename.error_type == "Denied"
    assert rename.verdict is ProbeVerdict.INSUFFICIENT_GRANTS
