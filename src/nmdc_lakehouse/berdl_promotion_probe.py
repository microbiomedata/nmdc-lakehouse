"""Probe BERDL promotion and recovery capability on disposable tables."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

PLAN_FORMAT_VERSION: Literal[1] = 1
OUTCOME_FORMAT_VERSION: Literal[1] = 1

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_PROBE_DATASET = re.compile(r"[A-Za-z_][A-Za-z0-9_]*_probe_[A-Za-z0-9_]+\Z")
_RESERVED_DATASETS = frozenset({"nmdc_metadata", "nmdc_results", "nmdc_ref_data"})
_PROBE_TABLES: tuple[str, str] = ("probe_first", "probe_second")
_PROBE_ROWS: int = 2


class BerdlPromotionProbeCountError(ValueError):
    """Raised when a table's row count is not a usable integer."""


class BerdlPromotionProbeError(ValueError):
    """Raised when the promotion probe cannot run safely or report honestly."""


class ProbeOperation(StrEnum):
    """One capability the probe tries to establish."""

    ENVIRONMENT = "environment"
    CROSS_NAMESPACE_RENAME = "cross-namespace-rename"
    REPLACEMENT = "replacement"
    RECOVERY_PRECONDITION = "recovery-precondition"
    ROLLBACK_TO_SNAPSHOT = "rollback-to-snapshot"
    SET_CURRENT_SNAPSHOT = "set-current-snapshot"
    INJECTED_FAILURE_RECOVERY = "injected-failure-recovery"
    SNAPSHOT_RETENTION = "snapshot-retention"


class ProbeVerdict(StrEnum):
    """What the platform did when the operation was attempted."""

    SUPPORTED = "supported"
    UNSUPPORTED_SYNTAX = "unsupported-syntax"
    INSUFFICIENT_GRANTS = "insufficient-grants"
    UNAVAILABLE_CAPABILITY = "unavailable-capability"
    FAILED_AS_EXPECTED = "failed-as-expected"
    UNCLASSIFIED_FAILURE = "unclassified-failure"
    NOT_ATTEMPTED = "not-attempted"


# Markers are matched against case-folded text, so a provider's capitalisation cannot decide
# whether a failure is classified or falls through to unclassified.
_SYNTAX_MARKERS: tuple[str, ...] = tuple(
    marker.casefold() for marker in ("PARSE_SYNTAX_ERROR", "ParseException", "mismatched input", "extraneous input")
)
_GRANT_MARKERS: tuple[str, ...] = tuple(
    marker.casefold()
    for marker in (
        "AccessDenied",
        "NotAuthorized",
        "Forbidden",
        "PERMISSION_DENIED",
        "INSUFFICIENT_PERMISSIONS",
        "INSUFFICIENT_PRIVILEGES",
        "not authorized",
    )
)
_CAPABILITY_MARKERS: tuple[str, ...] = tuple(
    marker.casefold()
    for marker in (
        "UnsupportedOperationException",
        "UNSUPPORTED_FEATURE",
        "not supported",
        "cannot be performed",
        "PROCEDURE_NOT_FOUND",
    )
)
_MISSING_INPUT_MARKERS: tuple[str, ...] = tuple(marker.casefold() for marker in ("TABLE_OR_VIEW_NOT_FOUND",))


class ProbePlan(BaseModel):
    """The disposable namespaces and tables one probe run may touch."""

    model_config = ConfigDict(extra="forbid", strict=True)

    plan_format_version: Literal[1] = PLAN_FORMAT_VERSION
    tenant: str
    source_namespace: str
    destination_namespace: str
    tables: list[str] = Field(min_length=2, max_length=2)
    rows_per_table: int = Field(ge=1, le=16)


class ProbeStep(BaseModel):
    """One attempted operation and its credential-free result."""

    model_config = ConfigDict(extra="forbid", strict=True)

    operation: ProbeOperation
    verdict: ProbeVerdict
    statement: str | None = None
    error_type: str | None = None
    error_condition: str | None = None
    detail: str | None = None
    independently_verified: bool | None = None


class TableState(BaseModel):
    """Observed state of one probe table at one moment."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    snapshot_id: str | None
    row_count: int
    schema_fingerprint: str


class ProbeEnvironment(BaseModel):
    """Credential-free identification of the observed platform."""

    model_config = ConfigDict(extra="forbid", strict=True)

    spark_version: str | None
    catalog_implementation: str | None
    spark_sql_extensions: str | None
    current_principal_present: bool


class ProbeOutcome(BaseModel):
    """The immutable, credential-free evidence report for issue 240."""

    model_config = ConfigDict(extra="forbid", strict=True)

    outcome_format_version: Literal[1] = OUTCOME_FORMAT_VERSION
    status: Literal["probe-complete", "probe-incomplete"]
    plan_sha256: str
    tenant: str
    source_namespace: str
    destination_namespace: str
    environment: ProbeEnvironment
    steps: list[ProbeStep]
    state_before: list[TableState]
    state_after: list[TableState]
    state_after_recovery: list[TableState]
    snapshot_retention_ms: int | None
    unresolved_questions: list[str]


def _require_disposable(tenant: str, namespace: str, label: str) -> str:
    if not _IDENTIFIER.fullmatch(tenant):
        raise BerdlPromotionProbeError("The tenant must be a safe identifier.")
    prefix = f"{tenant}."
    if not namespace.startswith(prefix):
        raise BerdlPromotionProbeError(f"The {label} must live inside the requested tenant.")
    dataset = namespace[len(prefix) :]
    # Substring and case-folded. 'nmdc_metadata_probe_1' satisfies the disposable pattern, and so
    # does 'NMDC_METADATA_probe_1'; either would put a canonical dataset name into every generated
    # statement and into the report.
    folded = dataset.casefold()
    if any(reserved.casefold() in folded for reserved in _RESERVED_DATASETS):
        raise BerdlPromotionProbeError(f"The {label} must not contain a canonical NMDC dataset name.")
    if not _PROBE_DATASET.fullmatch(dataset):
        raise BerdlPromotionProbeError(f"The {label} must use a disposable <name>_probe_<suffix> identifier.")
    return namespace


def build_promotion_probe_plan(*, tenant: str, source_namespace: str, destination_namespace: str) -> ProbePlan:
    """Bind one probe run to two disposable namespaces in the same tenant."""
    source = _require_disposable(tenant, source_namespace, "source namespace")
    destination = _require_disposable(tenant, destination_namespace, "destination namespace")
    if source == destination:
        raise BerdlPromotionProbeError("The source and destination namespaces must be distinct.")
    return ProbePlan(
        plan_format_version=PLAN_FORMAT_VERSION,
        tenant=tenant,
        source_namespace=source,
        destination_namespace=destination,
        tables=list(_PROBE_TABLES),
        rows_per_table=_PROBE_ROWS,
    )


def plan_sha256(plan: ProbePlan) -> str:
    """Return the content identity of a reviewed probe plan.

    Hashes canonical JSON rather than Pydantic's serialization, matching
    ``snapshot_manifest._json_sha256``. A serializer change across dependency versions must not
    silently invalidate an authorization digest an operator is holding.
    """
    encoded = json.dumps(
        plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _classify(error: BaseException) -> ProbeVerdict:
    # Case-folded: providers vary in how they capitalise conditions and free text, and a casing
    # difference must not decide whether a failure is classified or reported as unclassified.
    text = f"{type(error).__name__}: {_error_condition(error) or ''}: {error}".casefold()
    if any(marker in text for marker in _SYNTAX_MARKERS):
        return ProbeVerdict.UNSUPPORTED_SYNTAX
    if any(marker in text for marker in _GRANT_MARKERS):
        return ProbeVerdict.INSUFFICIENT_GRANTS
    if any(marker in text for marker in _CAPABILITY_MARKERS):
        return ProbeVerdict.UNAVAILABLE_CAPABILITY
    return ProbeVerdict.UNCLASSIFIED_FAILURE


def _rows(spark: Any, statement: str) -> list[Any]:
    return list(spark.sql(statement).collect())


def _scalar(spark: Any, statement: str) -> Any:
    rows = _rows(spark, statement)
    if not rows:
        return None
    first = rows[0]
    values = list(first) if isinstance(first, (list, tuple)) else [first]
    return values[0] if values else None


def _error_condition(error: BaseException) -> str | None:
    """Return the provider's stable error identifier, never its free-text message."""
    for name in ("getCondition", "getErrorClass", "getSqlState"):
        reader = getattr(error, name, None)
        if not callable(reader):
            continue
        try:
            value = reader()
        except Exception:
            continue
        if isinstance(value, str) and value:
            return value
    return None


def _is_missing_input(step: ProbeStep) -> bool:
    """Return whether a step failed for the one reason the injected failure intends.

    Any other failure keeps its classified verdict, so a grant or syntax problem is never
    disguised as an expected outcome.
    """
    text = f"{step.error_condition or ''} {step.error_type or ''}".casefold()
    return any(marker in text for marker in _MISSING_INPUT_MARKERS)


def _attempt(spark: Any, operation: ProbeOperation, statement: str, *, detail: str | None = None) -> ProbeStep:
    """Run one statement and record a credential-free verdict."""
    try:
        _rows(spark, statement)
    except Exception as error:
        return ProbeStep(
            operation=operation,
            verdict=_classify(error),
            statement=statement,
            error_type=type(error).__name__,
            error_condition=_error_condition(error),
            detail=detail,
        )
    return ProbeStep(operation=operation, verdict=ProbeVerdict.SUPPORTED, statement=statement, detail=detail)


def _explain_detail(spark: Any, statement: str) -> str:
    """Describe what a read-only plan for the statement did, without asserting why it failed.

    A failed EXPLAIN is not evidence that EXPLAIN is unsupported; it may equally be a syntax or
    permission problem with the statement itself. The report says what happened, not why.
    """
    try:
        _rows(spark, f"EXPLAIN {statement}")
    except Exception as error:
        condition = _error_condition(error) or type(error).__name__
        return f"no read-only plan was produced ({condition})"
    return "read-only plan accepted"


def _schema_fingerprint(spark: Any, table: str) -> str:
    columns = [(column.name, str(getattr(column, "dataType", ""))) for column in spark.catalog.listColumns(table)]
    payload = json.dumps(columns, separators=(",", ":"), sort_keys=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _snapshot_id(spark: Any, table: str) -> str | None:
    value = _scalar(spark, f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
    return None if value is None else str(value)


def _table_exists(spark: Any, namespace: str, table: str) -> bool | None:
    """Return whether the table exists, or None when the catalog could not be listed.

    A failed listing is not an absent table. Collapsing the two would let a permission or
    catalog failure be recorded as evidence that a table is genuinely missing.
    """
    try:
        return bool(_rows(spark, f"SHOW TABLES IN {namespace} LIKE '{table}'"))
    except Exception:
        return None


def _table_state(spark: Any, namespace: str, table: str) -> TableState:
    full_table = f"{namespace}.{table}"
    count = _scalar(spark, f"SELECT COUNT(*) FROM {full_table}")
    if isinstance(count, bool) or not isinstance(count, int):
        # Raising routes this through the existing unreadable-table pathway rather than
        # recording a fabricated zero as observed evidence.
        raise BerdlPromotionProbeCountError(f"Cannot read a row count for '{full_table}'.")
    return TableState(
        table=table,
        snapshot_id=_snapshot_id(spark, full_table),
        row_count=count,
        schema_fingerprint=_schema_fingerprint(spark, full_table),
    )


def _unreadable_question(unreadable: Sequence[str], namespace: str) -> list[str]:
    """Name tables whose state could not be established, so a real omission is never silent.

    Covers both a table that exists and could not be read, and a table whose existence could not be
    determined because the catalog would not list it. Claiming the table exists would assert more
    than the run established.
    """
    if not unreadable:
        return []
    return [
        f"The state of {', '.join(sorted(unreadable))} in '{namespace}' could not be established, "
        "either because the table could not be read or because its existence could not be "
        "determined, so this report is not complete evidence for those tables."
    ]


def _observed_state(spark: Any, namespace: str, table: str) -> TableState | None:
    """Read one table's state, or return None so a partial run can still emit a report."""
    if _table_exists(spark, namespace, table) is not True:
        return None
    try:
        return _table_state(spark, namespace, table)
    except Exception:
        return None


def _observed_states(spark: Any, namespace: str, tables: Sequence[str]) -> tuple[list[TableState], list[str]]:
    """Return observed states, plus the tables that exist but could not be read.

    A table that is simply absent is an expected outcome, such as the second table after the
    injected failure, and is not reported as an unreadable omission.
    """
    states: list[TableState] = []
    unreadable: list[str] = []
    for table in tables:
        present = _table_exists(spark, namespace, table)
        if present is None:
            unreadable.append(table)
            continue
        if not present:
            continue
        try:
            states.append(_table_state(spark, namespace, table))
        except Exception:
            unreadable.append(table)
    return states, unreadable


def _setting(spark: Any, key: str) -> str | None:
    """Read one Spark configuration value, not the key echoed back beside it."""
    rows = _rows(spark, f"SET {key}")
    if not rows:
        return None
    first = rows[0]
    values = list(first) if isinstance(first, (list, tuple)) else [first]
    if len(values) < 2 or values[1] is None:
        return None
    value = str(values[1])
    return None if value in {"", "<undefined>"} else value


def _environment(spark: Any) -> tuple[ProbeEnvironment, list[str]]:
    """Read the platform identification, naming any field that could not be read.

    Returning None for both "not set" and "could not be read" would let a permission problem look
    like an absent setting, so the unreadable ones are named separately.
    """
    unreadable: list[str] = []

    def _safe(name: str, reader: Callable[[], Any]) -> Any:
        try:
            return reader()
        except Exception:
            unreadable.append(name)
            return None

    version = _safe("spark_version", lambda: getattr(spark, "version", None))
    catalog_impl = _safe("catalog_implementation", lambda: _setting(spark, "spark.sql.catalogImplementation"))
    extensions = _safe("spark_sql_extensions", lambda: _setting(spark, "spark.sql.extensions"))
    # Named for the field it populates, so an unresolved question can be correlated with the payload.
    principal = _safe("current_principal_present", lambda: _scalar(spark, "SELECT current_user()"))
    environment = ProbeEnvironment(
        spark_version=str(version) if version is not None else None,
        catalog_implementation=str(catalog_impl) if catalog_impl is not None else None,
        spark_sql_extensions=str(extensions) if extensions is not None else None,
        current_principal_present=principal is not None,
    )
    return environment, unreadable


def _runtime() -> Any:
    try:
        from berdl_notebook_utils.setup_spark_session import get_spark_session
    except ImportError as error:
        raise BerdlPromotionProbeError("The BERDL Spark runtime is not importable.") from error
    try:
        return get_spark_session()
    except Exception as error:
        raise BerdlPromotionProbeError("Cannot initialize the BERDL Spark session.") from error


def _create_probe_tables(spark: Any, plan: ProbePlan) -> None:
    """Create the disposable source namespace and its tiny synthetic tables."""
    try:
        _rows(spark, f"CREATE NAMESPACE IF NOT EXISTS {plan.source_namespace}")
        _rows(spark, f"CREATE NAMESPACE IF NOT EXISTS {plan.destination_namespace}")
    except Exception as error:
        raise BerdlPromotionProbeError("Cannot create the disposable probe namespaces.") from error
    values = ", ".join(f"({index}, 'probe-{index}')" for index in range(plan.rows_per_table))
    for namespace in (plan.source_namespace, plan.destination_namespace):
        for table in plan.tables:
            try:
                _rows(spark, f"DROP TABLE IF EXISTS {namespace}.{table}")
            except Exception as error:
                raise BerdlPromotionProbeError(f"Cannot clear the disposable probe table '{table}'.") from error
    for table in plan.tables:
        full_table = f"{plan.source_namespace}.{table}"
        try:
            _rows(spark, f"CREATE TABLE {full_table} (id INT, label STRING) USING iceberg")
            _rows(spark, f"INSERT INTO {full_table} VALUES {values}")
        except Exception as error:
            raise BerdlPromotionProbeError(f"Cannot create the disposable probe table '{table}'.") from error


def _retention_step(spark: Any, statement: str) -> tuple[ProbeStep, int | None]:
    """Read the retention property, distinguishing a failed call from an absent property.

    Swallowing the failure would report a grant or syntax problem as a missing platform
    capability, which is the distinction this command exists to make.
    """
    try:
        rows = _rows(spark, statement)
    except Exception as error:
        step = ProbeStep(
            operation=ProbeOperation.SNAPSHOT_RETENTION,
            verdict=_classify(error),
            statement=statement,
            error_type=type(error).__name__,
            error_condition=_error_condition(error),
            detail="the retention property could not be read",
        )
        return step, None
    retention: int | None = None
    unparseable: str | None = None
    for row in rows:
        values = list(row) if isinstance(row, (list, tuple)) else [row]
        if len(values) >= 2 and str(values[0]) == "history.expire.max-snapshot-age-ms":
            try:
                retention = int(str(values[1]))
            except ValueError:
                # Present but not a number: a different situation from the property being absent.
                unparseable = "the retention property is present but is not an integer"
            break
    step = ProbeStep(
        operation=ProbeOperation.SNAPSHOT_RETENTION,
        verdict=ProbeVerdict.SUPPORTED
        if retention is not None
        else (ProbeVerdict.UNCLASSIFIED_FAILURE if unparseable else ProbeVerdict.UNAVAILABLE_CAPABILITY),
        statement=statement,
        detail=unparseable,
    )
    return step, retention


def run_promotion_probe(
    plan: ProbePlan,
    *,
    authorize_plan_sha256: str,
    runtime: Callable[[], Any] = _runtime,
) -> ProbeOutcome:
    """Establish which promotion and recovery operations BERDL actually supports."""
    expected = plan_sha256(plan)
    if authorize_plan_sha256 != expected:
        raise BerdlPromotionProbeError("The probe authorization does not match the reviewed plan.")
    # A matching digest proves the caller knows the plan, not that the plan is safe. The
    # disposable-namespace and fixed-table constraints live in build_promotion_probe_plan, so a plan
    # loaded from disk would otherwise reach Spark without ever having been through them.
    canonical = build_promotion_probe_plan(
        tenant=plan.tenant,
        source_namespace=plan.source_namespace,
        destination_namespace=plan.destination_namespace,
    )
    if plan != canonical:
        raise BerdlPromotionProbeError(
            "The probe plan is not the canonical plan for its namespaces; refusing to run it."
        )
    spark = runtime()
    environment, unreadable_environment = _environment(spark)
    _create_probe_tables(spark, plan)
    state_before, unreadable_before = _observed_states(spark, plan.source_namespace, plan.tables)
    if unreadable_before or len(state_before) != len(plan.tables):
        raise BerdlPromotionProbeError("The probe could not observe every disposable table before mutation.")

    steps: list[ProbeStep] = [
        ProbeStep(
            operation=ProbeOperation.ENVIRONMENT,
            verdict=ProbeVerdict.SUPPORTED if not unreadable_environment else ProbeVerdict.UNCLASSIFIED_FAILURE,
            detail=(f"could not read: {', '.join(sorted(unreadable_environment))}" if unreadable_environment else None),
        )
    ]
    unresolved: list[str] = []
    if unreadable_environment:
        unresolved.append(
            f"The platform fields {', '.join(sorted(unreadable_environment))} could not be read, so this "
            "report does not identify the deployment completely."
        )

    first, second = plan.tables
    source_first = f"{plan.source_namespace}.{first}"
    destination_first = f"{plan.destination_namespace}.{first}"
    destination_second = f"{plan.destination_namespace}.{second}"

    rename = f"ALTER TABLE {source_first} RENAME TO {destination_first}"
    rename_step = _attempt(
        spark,
        ProbeOperation.CROSS_NAMESPACE_RENAME,
        rename,
        detail=_explain_detail(spark, rename),
    )
    steps.append(rename_step)

    if rename_step.verdict is ProbeVerdict.SUPPORTED:
        steps.append(ProbeStep(operation=ProbeOperation.REPLACEMENT, verdict=ProbeVerdict.NOT_ATTEMPTED))
    else:
        steps.append(
            _attempt(
                spark,
                ProbeOperation.REPLACEMENT,
                f"CREATE OR REPLACE TABLE {destination_first} USING iceberg AS SELECT * FROM {source_first}",
                detail=f"attempted because cross-namespace rename returned {rename_step.verdict.value}",
            )
        )

    state_after, unreadable_after = _observed_states(spark, plan.destination_namespace, plan.tables)

    catalog = plan.tenant
    promoted = _observed_state(spark, plan.destination_namespace, first)
    if promoted is None or promoted.snapshot_id is None:
        for operation in (
            ProbeOperation.RECOVERY_PRECONDITION,
            ProbeOperation.ROLLBACK_TO_SNAPSHOT,
            ProbeOperation.SET_CURRENT_SNAPSHOT,
        ):
            steps.append(ProbeStep(operation=operation, verdict=ProbeVerdict.NOT_ATTEMPTED))
        unresolved.append(
            "The promoted table or its Iceberg snapshot identifier was not readable, so no recovery "
            "operation could be tested."
        )
    else:
        recovery_point = promoted.snapshot_id
        precondition = _attempt(
            spark,
            ProbeOperation.RECOVERY_PRECONDITION,
            f"INSERT INTO {destination_first} VALUES (-1, 'probe-rollback-marker')",
            detail="a second mutation, so rollback has a real earlier snapshot to return to",
        )
        steps.append(precondition)
        if precondition.verdict is not ProbeVerdict.SUPPORTED:
            for operation in (ProbeOperation.ROLLBACK_TO_SNAPSHOT, ProbeOperation.SET_CURRENT_SNAPSHOT):
                steps.append(ProbeStep(operation=operation, verdict=ProbeVerdict.NOT_ATTEMPTED))
            unresolved.append("The promoted table could not be mutated, so recovery could not be tested.")
        else:
            rollback = _attempt(
                spark,
                ProbeOperation.ROLLBACK_TO_SNAPSHOT,
                f"CALL {catalog}.system.rollback_to_snapshot('{destination_first}', {recovery_point})",
            )
            if rollback.verdict is ProbeVerdict.SUPPORTED:
                recovered = _observed_state(spark, plan.destination_namespace, first)
                rollback.independently_verified = recovered is not None and recovered.row_count == promoted.row_count
                if not rollback.independently_verified:
                    unresolved.append(
                        "The rollback call reported success but the table did not return to its pre-mutation "
                        "row count, or its state could not be read back, so the recovery operation cannot be "
                        "relied on."
                    )
            steps.append(rollback)
            steps.append(
                _attempt(
                    spark,
                    ProbeOperation.SET_CURRENT_SNAPSHOT,
                    f"CALL {catalog}.system.set_current_snapshot('{destination_first}', {recovery_point})",
                )
            )

    injection = _attempt(
        spark,
        ProbeOperation.INJECTED_FAILURE_RECOVERY,
        (
            f"CREATE OR REPLACE TABLE {destination_second} USING iceberg "
            f"AS SELECT * FROM {plan.source_namespace}.{second}_absent"
        ),
        detail="deliberately sourced from a table that does not exist, to fail between two table mutations",
    )
    first_present = _table_exists(spark, plan.destination_namespace, first)
    second_present = _table_exists(spark, plan.destination_namespace, second)
    injection.independently_verified = None if second_present is None else not second_present
    if injection.verdict is not ProbeVerdict.SUPPORTED and _is_missing_input(injection):
        injection.verdict = ProbeVerdict.FAILED_AS_EXPECTED
    steps.append(injection)
    if second_present is None:
        unresolved.append(
            "The destination catalog could not be listed after the injected failure, so whether a partial "
            "promotion is observable was not established."
        )
    if injection.verdict is ProbeVerdict.SUPPORTED:
        unresolved.append(
            "The injected failure did not fail, so this run does not establish partial-promotion behavior."
        )
    elif first_present is True and second_present is False:
        unresolved.append(
            "After a failure between two table mutations the destination held the first table and not the "
            "second, so a partial promotion is observable and promotion is not atomic across tables."
        )

    retention_statement = f"SHOW TBLPROPERTIES {destination_first}"
    if first_present is not True:
        steps.append(
            ProbeStep(
                operation=ProbeOperation.SNAPSHOT_RETENTION,
                verdict=ProbeVerdict.NOT_ATTEMPTED,
                statement=retention_statement,
                detail="the promoted table does not exist, so retention could not be read",
            )
        )
        retention = None
        unresolved.append(
            "The promoted table was never created, so the snapshot retention window is unknown for a "
            "reason unrelated to platform capability."
        )
    else:
        retention_step, retention = _retention_step(spark, retention_statement)
        steps.append(retention_step)
        if retention_step.verdict is ProbeVerdict.UNAVAILABLE_CAPABILITY:
            unresolved.append(
                "Snapshot retention is not readable from table properties, so recovery time is unbounded."
            )
        elif retention is None:
            unresolved.append(
                "The retention property read returned "
                f"{retention_step.verdict.value}, so the recovery window is unknown and this run does "
                "not establish whether the platform provides one."
            )

    state_after_recovery, unreadable_recovery = _observed_states(spark, plan.destination_namespace, plan.tables)
    unresolved.extend(_unreadable_question(unreadable_after, plan.destination_namespace))
    unresolved.extend(_unreadable_question(unreadable_recovery, plan.destination_namespace))
    complete = all(step.verdict is not ProbeVerdict.UNCLASSIFIED_FAILURE for step in steps)
    return ProbeOutcome(
        outcome_format_version=OUTCOME_FORMAT_VERSION,
        status="probe-complete" if complete else "probe-incomplete",
        plan_sha256=expected,
        tenant=plan.tenant,
        source_namespace=plan.source_namespace,
        destination_namespace=plan.destination_namespace,
        environment=environment,
        steps=steps,
        state_before=state_before,
        state_after=state_after,
        state_after_recovery=state_after_recovery,
        snapshot_retention_ms=retention,
        unresolved_questions=unresolved,
    )


def render_promotion_probe(document: ProbePlan | ProbeOutcome) -> str:
    """Render canonical reviewable JSON for stdout or a file."""
    return json.dumps(document.model_dump(mode="json"), indent=2, sort_keys=True)


def write_promotion_probe_outcome(path: Path, outcome: ProbeOutcome) -> Path:
    """Atomically create probe evidence without replacing an earlier outcome."""
    destination = path.expanduser()
    if destination.exists() or destination.is_symlink():
        raise BerdlPromotionProbeError("Refusing to replace an existing promotion probe outcome.")
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise BerdlPromotionProbeError("The promotion probe outcome parent must be an ordinary directory.")
    destination = parent.resolve() / destination.name
    descriptor: int | None = None
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
        temporary = Path(temporary_name)
        stream = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = None
        with stream:
            stream.write(render_promotion_probe(outcome))
            stream.write("\n")
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise BerdlPromotionProbeError("Refusing to replace an existing promotion probe outcome.") from error
        except OSError as error:
            raise BerdlPromotionProbeError("Cannot publish the promotion probe outcome atomically.") from error
    except OSError as error:
        raise BerdlPromotionProbeError("Cannot write the promotion probe outcome.") from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def load_promotion_probe_plan(path: Path) -> ProbePlan:
    """Load one reviewed probe plan without trusting its origin."""
    try:
        payload = path.expanduser().read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise BerdlPromotionProbeError("Cannot read the promotion probe plan.") from error
    try:
        return ProbePlan.model_validate_json(payload)
    except ValidationError as error:
        raise BerdlPromotionProbeError("The promotion probe plan is not valid.") from error
