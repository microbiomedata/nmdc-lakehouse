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


class TableNotFound(Exception):
    """Mirrors Spark's missing-table failure, which carries a stable error condition."""

    def getCondition(self):  # noqa: N802 - mirrors the provider API name
        return "TABLE_OR_VIEW_NOT_FOUND"


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
            raise TableNotFound(f"missing table {table}")

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

    joined = "\n".join(spark.statements).casefold()
    for canonical in ("nmdc_metadata", "nmdc_results", "nmdc_ref_data"):
        assert canonical.casefold() not in joined


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


def test_a_rollback_whose_state_cannot_be_read_back_is_unknown_not_unverified() -> None:
    """An unreadable read-back must not be reported as a rollback that failed to restore rows."""

    class BlindAfterRollback(FakeSpark):
        rolled_back = False

        def _answer(self, statement: str):
            if statement.startswith("CALL ") and "rollback_to_snapshot" in statement:
                result = super()._answer(statement)
                self.rolled_back = True
                return result
            if self.rolled_back and statement == f"SELECT COUNT(*) FROM {DESTINATION}.probe_first":
                self.rolled_back = False
                return [("not-a-number",)]
            return super()._answer(statement)

    outcome = _run(BlindAfterRollback())

    rollback = {s.operation: s for s in outcome.steps}[ProbeOperation.ROLLBACK_TO_SNAPSHOT]
    assert rollback.verdict is ProbeVerdict.SUPPORTED
    assert rollback.independently_verified is None
    assert any("could not be read back" in q for q in outcome.unresolved_questions)
    assert not any("did not return to its pre-mutation" in q for q in outcome.unresolved_questions)


def test_the_rollback_check_lists_the_namespace_once() -> None:
    """The tri-state is already in hand; listing again costs a round-trip and double-logs. #279."""
    spark = FakeSpark()
    _run(spark)

    after_rollback = False
    listings = 0
    for statement in spark.statements:
        if statement.startswith("CALL ") and "rollback_to_snapshot" in statement:
            after_rollback = True
            continue
        if statement.startswith("CALL ") and "set_current_snapshot" in statement:
            break
        if after_rollback and statement.startswith("SHOW TABLES IN") and "probe_first" in statement:
            listings += 1

    assert listings == 1, f"the rollback read-back listed the namespace {listings} times"


def test_a_table_destroyed_by_rollback_is_not_reported_as_merely_unreadable() -> None:
    """A table gone after a successful rollback is a destroyed table, not an unknown; #279."""

    class DestroyingRollback(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("CALL ") and "rollback_to_snapshot" in statement:
                table, _ = _call_arguments(statement)
                self._require(table)
                # The call reports success and the table is gone: the outcome the probe exists for.
                self.tables.pop(table, None)
                self.snapshots.pop(table, None)
                return []
            return super()._answer(statement)

    outcome = _run(DestroyingRollback())

    rollback = {s.operation: s for s in outcome.steps}[ProbeOperation.ROLLBACK_TO_SNAPSHOT]
    assert rollback.independently_verified is False
    assert rollback.verdict is ProbeVerdict.UNCLASSIFIED_FAILURE
    assert any("no longer exists" in q and "destroyed it" in q for q in outcome.unresolved_questions)
    # And it must not be reported as the softer, unrelated outcome.
    assert not any("could not be read back" in q for q in outcome.unresolved_questions)


def test_an_unreadable_catalog_after_rollback_is_still_only_unknown() -> None:
    """The genuinely unknown case must stay unknown rather than being called destruction."""

    class BlindAfterRollback(FakeSpark):
        rolled_back = False

        def sql(self, statement: str):
            if self.rolled_back and statement.startswith("SHOW TABLES IN"):
                self.statements.append(statement)
                raise RuntimeError("INSUFFICIENT_PRIVILEGES: cannot list namespace")
            return super().sql(statement)

        def _answer(self, statement: str):
            result = super()._answer(statement)
            if statement.startswith("CALL ") and "rollback_to_snapshot" in statement:
                self.rolled_back = True
            return result

    outcome = _run(BlindAfterRollback())

    rollback = {s.operation: s for s in outcome.steps}[ProbeOperation.ROLLBACK_TO_SNAPSHOT]
    assert rollback.independently_verified is None
    assert any("could not be read back" in q for q in outcome.unresolved_questions)
    assert not any("destroyed it" in q for q in outcome.unresolved_questions)


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
    assert outcome.environment.spark_sql_extensions.startswith("org.apache.iceberg")
    assert outcome.environment.spark_version == "3.5.1"


def test_unset_configuration_is_recorded_as_absent_not_as_the_literal_undefined() -> None:
    spark = FakeSpark()
    spark.settings = {}

    outcome = _run(spark)

    assert outcome.environment.catalog_implementation is None
    assert outcome.environment.spark_sql_extensions is None


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


def test_an_unreadable_promoted_table_still_produces_a_report() -> None:
    """A read failure after mutation must not abort the run and discard the evidence gathered so far."""

    class UnreadableSnapshots(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("SELECT snapshot_id") and DESTINATION in statement:
                raise RuntimeError("INSUFFICIENT_PRIVILEGES reading snapshots metadata")
            return super()._answer(statement)

    outcome = _run(UnreadableSnapshots())

    steps = {step.operation: step for step in outcome.steps}
    assert steps[ProbeOperation.ROLLBACK_TO_SNAPSHOT].verdict is ProbeVerdict.NOT_ATTEMPTED
    assert steps[ProbeOperation.RECOVERY_PRECONDITION].verdict is ProbeVerdict.NOT_ATTEMPTED
    assert any("no recovery operation could be tested" in q for q in outcome.unresolved_questions)
    assert outcome.status in {"probe-complete", "probe-incomplete"}


def test_rendered_documents_use_stable_key_ordering() -> None:
    plan = _plan()

    rendered = render_promotion_probe(plan)

    keys = [line.split('"')[1] for line in rendered.splitlines() if line.startswith('  "')]
    assert keys == sorted(keys)


def test_key_ordering_does_not_change_the_plan_digest() -> None:
    """The digest binds the model, not the rendering, so review formatting cannot invalidate authorization."""
    assert plan_sha256(_plan()) == "3aaed84e8decc71ed5944f246be531d36dc82ddd23efaa05d6e0c977d223e302"


def test_unobservable_post_mutation_state_is_named_not_silently_omitted() -> None:
    """An omitted table must never let a partial report read as complete evidence."""

    class HalfBlind(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("SELECT COUNT(*)") and f"{DESTINATION}.probe_first" in statement:
                raise RuntimeError("catalog read failed")
            return super()._answer(statement)

    outcome = _run(HalfBlind())

    assert not any(state.table == "probe_first" for state in outcome.state_after)
    assert any(
        "could not be established" in question and "probe_first" in question
        for question in outcome.unresolved_questions
    )


def test_retention_records_its_statement() -> None:
    outcome = _run(FakeSpark())

    retention = {step.operation: step for step in outcome.steps}[ProbeOperation.SNAPSHOT_RETENTION]
    assert retention.statement == f"SHOW TBLPROPERTIES {DESTINATION}.probe_first"
    assert retention.verdict is ProbeVerdict.SUPPORTED


def test_a_missing_promoted_table_is_not_reported_as_a_missing_platform_capability() -> None:
    spark = FakeSpark(failures={"CREATE OR REPLACE TABLE": RuntimeError("AccessDenied")})
    spark.failures["RENAME TO"] = ParseException("mismatched input 'RENAME'")

    outcome = _run(spark)

    retention = {step.operation: step for step in outcome.steps}[ProbeOperation.SNAPSHOT_RETENTION]
    assert retention.verdict is ProbeVerdict.NOT_ATTEMPTED
    assert retention.statement is not None
    assert any("never created" in question for question in outcome.unresolved_questions)


def test_a_deliberately_absent_table_is_not_reported_as_unreadable() -> None:
    """probe_second is meant to be missing after the injected failure; that is not an omission."""
    outcome = _run(FakeSpark())

    assert not any("could not be established" in question for question in outcome.unresolved_questions)
    assert [state.table for state in outcome.state_after] == ["probe_first"]


def test_the_injected_failure_is_not_reported_as_a_missing_platform_capability() -> None:
    spark = FakeSpark()

    outcome = _run(spark)

    injection = {step.operation: step for step in outcome.steps}[ProbeOperation.INJECTED_FAILURE_RECOVERY]
    assert injection.verdict is ProbeVerdict.FAILED_AS_EXPECTED
    assert injection.independently_verified is True
    assert outcome.status == "probe-complete"


def test_a_missing_input_table_is_not_a_capability_verdict() -> None:
    spark = FakeSpark(failures={"RENAME TO": RuntimeError("TABLE_OR_VIEW_NOT_FOUND: some.table")})

    outcome = _run(spark)

    rename = {step.operation: step for step in outcome.steps}[ProbeOperation.CROSS_NAMESPACE_RENAME]
    assert rename.verdict is ProbeVerdict.UNCLASSIFIED_FAILURE


def test_a_grant_failure_during_injection_is_not_disguised_as_expected() -> None:
    """Only the intended missing-input failure may be recorded as expected."""

    class DeniedInjection(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("CREATE OR REPLACE TABLE") and "_absent" in statement:
                self.statements.append(statement)
                raise RuntimeError("INSUFFICIENT_PRIVILEGES: principal lacks TABLE_WRITE")
            return super().sql(statement)

    outcome = _run(DeniedInjection())

    injection = {step.operation: step for step in outcome.steps}[ProbeOperation.INJECTED_FAILURE_RECOVERY]
    assert injection.verdict is ProbeVerdict.INSUFFICIENT_GRANTS
    assert injection.verdict is not ProbeVerdict.FAILED_AS_EXPECTED


def test_a_denied_retention_read_is_not_reported_as_a_missing_capability() -> None:
    """A grant failure reading retention must not look like the platform lacking retention."""

    class DeniedRetention(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("SHOW TBLPROPERTIES"):
                self.statements.append(statement)
                raise RuntimeError("INSUFFICIENT_PRIVILEGES: cannot read table properties")
            return super().sql(statement)

    outcome = _run(DeniedRetention())

    step = {s.operation: s for s in outcome.steps}[ProbeOperation.SNAPSHOT_RETENTION]
    assert step.verdict is ProbeVerdict.INSUFFICIENT_GRANTS
    assert step.error_type == "RuntimeError"
    assert outcome.snapshot_retention_ms is None
    assert any("insufficient-grants" in q for q in outcome.unresolved_questions)
    assert not any("not readable from table properties" in q for q in outcome.unresolved_questions)


def test_an_absent_retention_property_is_still_a_capability_verdict() -> None:
    outcome = _run(FakeSpark(retention=None))

    step = {s.operation: s for s in outcome.steps}[ProbeOperation.SNAPSHOT_RETENTION]
    assert step.verdict is ProbeVerdict.UNAVAILABLE_CAPABILITY
    assert step.error_type is None
    assert any("not readable from table properties" in q for q in outcome.unresolved_questions)


def test_an_unlistable_catalog_is_not_recorded_as_an_absent_table() -> None:
    """A failed listing must never become evidence that the injected failure was verified."""

    class BlindCatalog(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("SHOW TABLES IN") and DESTINATION in statement:
                self.statements.append(statement)
                raise RuntimeError("INSUFFICIENT_PRIVILEGES: cannot list namespace")
            return super().sql(statement)

    outcome = _run(BlindCatalog())

    injection = {s.operation: s for s in outcome.steps}[ProbeOperation.INJECTED_FAILURE_RECOVERY]
    assert injection.independently_verified is None
    assert any("could not be listed" in q for q in outcome.unresolved_questions)
    assert not any("promotion is not atomic across tables" in q for q in outcome.unresolved_questions)


def test_the_plan_digest_is_canonical_json_not_pydantic_serialization() -> None:
    """A serializer change across dependency versions must not invalidate an operator's digest."""
    import hashlib
    import json

    plan = _plan()
    expected = hashlib.sha256(
        json.dumps(plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()

    assert plan_sha256(plan) == expected


def test_an_unusable_row_count_is_not_recorded_as_zero() -> None:
    """Fabricating a zero would put invented evidence into the report."""

    class OddCount(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("SELECT COUNT(*)") and f"{SOURCE}.probe_first" in statement:
                return [("not-a-number",)]
            return super()._answer(statement)

    with pytest.raises(BerdlPromotionProbeError, match="observe every disposable table"):
        _run(OddCount())


def test_a_disposable_name_may_not_embed_a_canonical_dataset_name() -> None:
    """The pattern alone is not enough: nmdc_metadata_probe_1 matches it and is still unsafe."""
    for unsafe in (
        "nmdc.nmdc_metadata_probe_1",
        "nmdc.nmdc_results_probe_x",
        "nmdc.my_nmdc_ref_data_probe_2",
        "nmdc.NMDC_METADATA_probe_1",
        "nmdc.Nmdc_Results_probe_x",
        "nmdc.MY_NMDC_REF_DATA_probe_2",
    ):
        with pytest.raises(BerdlPromotionProbeError, match="canonical NMDC dataset name"):
            build_promotion_probe_plan(tenant=TENANT, source_namespace=unsafe, destination_namespace=DESTINATION)


def test_an_unusable_row_count_raises_the_promotion_probe_error() -> None:
    from nmdc_lakehouse.berdl_promotion_probe import BerdlPromotionProbeCountError

    assert issubclass(BerdlPromotionProbeCountError, ValueError)
    assert not hasattr(
        __import__("nmdc_lakehouse.berdl_promotion_probe", fromlist=["x"]), "BerdlMetadataProbeCountError"
    )


def test_classification_ignores_provider_capitalisation() -> None:
    """A casing difference must not decide whether a failure is classified."""
    spark = FakeSpark(failures={"RENAME TO": RuntimeError("Insufficient_Privileges on namespace")})

    outcome = _run(spark)

    rename = {s.operation: s for s in outcome.steps}[ProbeOperation.CROSS_NAMESPACE_RENAME]
    assert rename.verdict is ProbeVerdict.INSUFFICIENT_GRANTS
    assert outcome.status == "probe-complete"


def test_the_injected_failure_is_recognised_whatever_its_casing() -> None:
    class LowerCaseNotFound(Exception):
        def getCondition(self):  # noqa: N802 - mirrors the provider API name
            return "table_or_view_not_found"

    class OddCasing(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("CREATE OR REPLACE TABLE") and "_absent" in statement:
                self.statements.append(statement)
                raise LowerCaseNotFound("missing")
            return super().sql(statement)

    outcome = _run(OddCasing())

    injection = {s.operation: s for s in outcome.steps}[ProbeOperation.INJECTED_FAILURE_RECOVERY]
    assert injection.verdict is ProbeVerdict.FAILED_AS_EXPECTED


def test_a_hand_built_plan_naming_canonical_objects_is_refused() -> None:
    """A matching digest proves the caller knows the plan, not that the plan is safe."""
    from nmdc_lakehouse.berdl_promotion_probe import ProbePlan

    hostile = ProbePlan(
        plan_format_version=1,
        tenant=TENANT,
        source_namespace="nmdc.metadata",
        destination_namespace="nmdc.metadata_backup",
        tables=["biosample_set", "study_set"],
        rows_per_table=2,
    )

    # Refused while re-deriving the canonical plan, which rejects a canonical namespace outright.
    with pytest.raises(BerdlPromotionProbeError, match="disposable"):
        run_promotion_probe(
            hostile,
            authorize_plan_sha256=plan_sha256(hostile),
            runtime=lambda: pytest.fail("Spark must not be reached for a non-canonical plan"),
        )


def test_a_plan_with_altered_tables_is_refused_even_with_a_valid_digest() -> None:
    plan = _plan()
    plan.tables = ["probe_first", "something_else"]

    with pytest.raises(BerdlPromotionProbeError, match="not the canonical plan"):
        run_promotion_probe(
            plan,
            authorize_plan_sha256=plan_sha256(plan),
            runtime=lambda: pytest.fail("Spark must not be reached for a non-canonical plan"),
        )


def test_the_replacement_detail_names_the_actual_rename_verdict() -> None:
    """Replacement is attempted for any non-supported rename, so the detail must not assume one."""
    spark = FakeSpark(failures={"RENAME TO": RuntimeError("AccessDenied on namespace")})

    outcome = _run(spark)

    steps = {s.operation: s for s in outcome.steps}
    assert steps[ProbeOperation.CROSS_NAMESPACE_RENAME].verdict is ProbeVerdict.INSUFFICIENT_GRANTS
    detail = steps[ProbeOperation.REPLACEMENT].detail or ""
    assert "insufficient-grants" in detail
    assert "not supported" not in detail


def test_a_syntax_failure_reading_retention_is_not_described_as_a_grant_problem() -> None:
    class BadSyntax(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("SHOW TBLPROPERTIES"):
                self.statements.append(statement)
                raise ParseException("mismatched input 'TBLPROPERTIES'")
            return super().sql(statement)

    outcome = _run(BadSyntax())

    step = {s.operation: s for s in outcome.steps}[ProbeOperation.SNAPSHOT_RETENTION]
    assert step.verdict is ProbeVerdict.UNSUPPORTED_SYNTAX
    assert any("unsupported-syntax" in q for q in outcome.unresolved_questions)
    assert not any("not readable from table properties" in q for q in outcome.unresolved_questions)


def test_unreadable_environment_fields_are_named_not_reported_as_unset() -> None:
    """A field that could not be read is not a field that has no value; #255."""

    class BlindEnvironment(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("SET spark.sql.extensions"):
                self.statements.append(statement)
                raise RuntimeError("INSUFFICIENT_PRIVILEGES reading configuration")
            return super().sql(statement)

    outcome = _run(BlindEnvironment())

    step = {s.operation: s for s in outcome.steps}[ProbeOperation.ENVIRONMENT]
    assert step.verdict is ProbeVerdict.UNCLASSIFIED_FAILURE
    assert "spark_sql_extensions" in (step.detail or "")
    assert any("could not be read" in q and "spark_sql_extensions" in q for q in outcome.unresolved_questions)


def test_a_present_but_unparseable_retention_is_not_reported_as_absent() -> None:
    outcome = _run(FakeSpark(retention="not-a-number"))

    step = {s.operation: s for s in outcome.steps}[ProbeOperation.SNAPSHOT_RETENTION]
    assert step.verdict is ProbeVerdict.UNCLASSIFIED_FAILURE
    assert "present but is not an integer" in (step.detail or "")
    assert outcome.snapshot_retention_ms is None


def test_a_failed_explain_does_not_claim_explain_is_unsupported() -> None:
    class NoExplain(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("EXPLAIN "):
                self.statements.append(statement)
                raise RuntimeError("INSUFFICIENT_PRIVILEGES on EXPLAIN")
            return super().sql(statement)

    outcome = _run(NoExplain())

    rename = {s.operation: s for s in outcome.steps}[ProbeOperation.CROSS_NAMESPACE_RENAME]
    detail = rename.detail or ""
    assert "no read-only plan was produced" in detail
    assert "RuntimeError" in detail or "INSUFFICIENT" in detail
    assert "unsupported" not in detail.lower()


def test_every_guard_rejects_at_least_one_input_it_must_reject() -> None:
    """The audit's second question: a guard exercised only on valid input asserts nothing."""
    rejected = [
        ("canonical dataset", "nmdc.nmdc_metadata", DESTINATION),
        ("canonical substring", "nmdc.nmdc_metadata_probe_1", DESTINATION),
        ("canonical mixed case", "nmdc.NMDC_METADATA_probe_1", DESTINATION),
        ("no disposable marker", "nmdc.scratch_area", DESTINATION),
        ("outside the tenant", "other.promotion_probe_1", DESTINATION),
        ("identical namespaces", SOURCE, SOURCE),
    ]
    for label, source, destination in rejected:
        with pytest.raises(BerdlPromotionProbeError, match=r".+") as caught:
            build_promotion_probe_plan(tenant=TENANT, source_namespace=source, destination_namespace=destination)
        assert str(caught.value), f"guard for {label} raised without a reason"


def test_unreadable_environment_names_match_the_output_model() -> None:
    """A name in an unresolved question must be findable in the environment payload."""
    from nmdc_lakehouse.berdl_promotion_probe import ProbeEnvironment, _environment

    class QueryBlind(FakeSpark):
        """Fails every field read that goes through SQL; `spark_version` is an attribute and survives."""

        def sql(self, statement: str):
            if statement.startswith("SET ") or statement.startswith("SELECT current_user()"):
                raise RuntimeError("INSUFFICIENT_PRIVILEGES")
            return super().sql(statement)

    environment, unreadable = _environment(QueryBlind())

    assert unreadable, "expected the SQL-backed fields to be reported unreadable"
    assert "spark_version" not in unreadable, "spark_version does not use SQL and should still be readable"
    assert environment.spark_version is not None
    for name in unreadable:
        assert name in ProbeEnvironment.model_fields, f"{name} is not a field of ProbeEnvironment"


def test_an_unreadable_principal_is_null_not_false() -> None:
    """False must mean the query returned nothing, not that it could not be run."""
    from nmdc_lakehouse.berdl_promotion_probe import _environment

    class NoPrincipal(FakeSpark):
        def sql(self, statement: str):
            if statement.startswith("SELECT current_user()"):
                raise RuntimeError("INSUFFICIENT_PRIVILEGES")
            return super().sql(statement)

    environment, unreadable = _environment(NoPrincipal())

    assert environment.current_principal_present is None
    assert "current_principal_present" in unreadable


def test_a_readable_but_empty_principal_is_false_not_null() -> None:
    from nmdc_lakehouse.berdl_promotion_probe import _environment

    class EmptyPrincipal(FakeSpark):
        def _answer(self, statement: str):
            if statement.startswith("SELECT current_user()"):
                return []
            return super()._answer(statement)

    environment, unreadable = _environment(EmptyPrincipal())

    assert environment.current_principal_present is False
    assert "current_principal_present" not in unreadable
