"""Plan a canonical promotion without performing one.

Staging proved that candidate tables load and verify. It authorizes nothing about the canonical
namespace, and this module keeps that separation: it reads verified evidence and writes an
immutable description of what a promotion would do, touching nothing.

The split mirrors `berdl_staging`, which has now run end to end, and it exists for the same
reason: the point where a human authorizes a destructive act should be a distinct, reviewable
artifact rather than a flag on the command that performs it.
"""

from __future__ import annotations

import re
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nmdc_lakehouse.derived_tables import DERIVED_TABLES
from nmdc_lakehouse.publication_plan import Disposition, PublicationPlan

PROMOTION_PLAN_FORMAT_VERSION: Literal[1] = 1

# Dispositions this planner can express as a step. `retire` is absent on purpose: it removes
# canonical tables, nothing here implements that, and a plan whose header counts a disposition its
# sequence omits is a silent omission the operator authorizes without seeing.
_PLANNED_DISPOSITIONS = frozenset({Disposition.REPLACE, Disposition.ADD, Disposition.REBUILD, Disposition.PRESERVE})

_QUALIFIED = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")


class StagedTableLike(Protocol):
    """One staged table, as the staging outcome reports it."""

    table: str
    destination_rows: int


class StagingOutcomeLike(Protocol):
    """The fields promotion reads from a verified staging outcome.

    A protocol rather than the concrete model, so this module does not import the staging
    machinery to read four attributes, and so a test can supply a stand-in without constructing a
    full outcome. Typed rather than `object`, because reaching into an untyped value is how the
    caller finds out at runtime that the shape changed.
    """

    status: str
    snapshot_id: str
    staging_namespace: str
    destination_id: str
    tables: list[StagedTableLike]


class MetadataOutcomeLike(Protocol):
    """The fields promotion reads from a verified metadata outcome."""

    status: str
    snapshot_id: str
    staging_namespace: str


class PromotionPlanError(ValueError):
    """Raised when verified evidence does not authorize a promotion plan."""


class PromotionOperation(BaseModel):
    """One canonical object and what promotion would do to it."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    disposition: Disposition
    expected_rows: int | None = Field(default=None, ge=0)
    rationale: str = Field(min_length=1, max_length=1000)


class BerdlPromotionPlan(BaseModel):
    """Credential-free description of a promotion, produced without performing one."""

    model_config = ConfigDict(extra="forbid", strict=True)

    plan_format_version: Literal[1]
    status: Literal["plan-only"]
    snapshot_id: str
    staging_namespace: str
    canonical_namespace: str
    destination_id: str
    staging_outcome_sha256: str
    metadata_outcome_sha256: str
    publication_plan_sha256: str
    operations: list[PromotionOperation]
    # Dropped before the replacements and rebuilt after them, which is the ordering Mark chose on
    # 2026-08-26. These tables are absent, and queries against them fail, for the whole run.
    derived_rebuilds: list[str]
    recovery: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_namespaces(self) -> "BerdlPromotionPlan":
        """Both namespaces must name a catalog, and promotion must not target the staging one."""
        for value, label in ((self.staging_namespace, "staging"), (self.canonical_namespace, "canonical")):
            if not _QUALIFIED.fullmatch(value):
                raise ValueError(f"The {label} namespace must be catalog-qualified as <catalog>.<namespace>.")
        if self.staging_namespace == self.canonical_namespace:
            raise ValueError("Promotion cannot target the staging namespace it reads from.")
        return self


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PromotionPlanError(message)


def build_berdl_promotion_plan(
    *,
    publication_plan: PublicationPlan,
    staging_outcome: StagingOutcomeLike,
    metadata_outcome: MetadataOutcomeLike,
    canonical_namespace: str,
    staging_outcome_sha256: str,
    metadata_outcome_sha256: str,
    publication_plan_sha256: str,
    recovery: str,
) -> BerdlPromotionPlan:
    """Cross-check verified evidence and describe the promotion it authorizes.

    Every check here refuses rather than warns. A promotion built from evidence that does not
    agree with itself is the failure this whole chain exists to prevent, and by the time it is
    running the canonical namespace is what is being changed.
    """
    _require(
        staging_outcome.status == "data-verified",
        "The staging outcome is not data-verified, so no candidate data is proven to have loaded.",
    )
    _require(
        metadata_outcome.status == "metadata-verified",
        "The metadata outcome is not metadata-verified, so the descriptions are not proven applied.",
    )

    snapshot_id = staging_outcome.snapshot_id
    _require(
        metadata_outcome.snapshot_id == snapshot_id,
        "The staging and metadata outcomes describe different snapshots.",
    )
    _require(
        publication_plan.candidate_snapshot_id == snapshot_id,
        "The publication plan was built for a different snapshot than the staging outcome.",
    )
    _require(
        metadata_outcome.staging_namespace == staging_outcome.staging_namespace,
        "The metadata was applied to a different namespace than the one that was staged.",
    )
    # The dispositions were decided against one destination's contents. Promoting them into a
    # different destination promotes decisions made about tables that are not the ones being
    # replaced, and every other check here would still pass.
    _require(
        publication_plan.destination_id == staging_outcome.destination_id,
        f"The publication plan describes destination '{publication_plan.destination_id}' but the "
        f"staging outcome describes '{staging_outcome.destination_id}'.",
    )

    # Row counts, table by table. The publication plan decided what to do on the strength of
    # candidate row counts, and the staging outcome is what actually landed. Promoting on a plan
    # whose numbers no longer match the data is promoting on a stale decision.
    staged_rows = {table.table: table.destination_rows for table in staging_outcome.tables}
    operations: list[PromotionOperation] = []
    for entry in publication_plan.tables:
        if entry.disposition in (Disposition.REPLACE, Disposition.ADD):
            _require(
                entry.table in staged_rows,
                f"'{entry.table}' is planned as {entry.disposition.value} but was not staged.",
            )
            # Absent, not zero. candidate_rows is optional on PlanEntry, and comparing None
            # produced "planned with None rows but 27352 were staged", which reads as a count
            # mismatch when the real problem is that the plan never recorded a count to decide on.
            _require(
                entry.candidate_rows is not None,
                f"'{entry.table}' is planned as {entry.disposition.value} with no candidate row "
                "count, so there is nothing to check the staged data against.",
            )
            _require(
                entry.candidate_rows == staged_rows[entry.table],
                f"'{entry.table}' was planned with {entry.candidate_rows} rows but "
                f"{staged_rows[entry.table]} were staged.",
            )
        operations.append(
            PromotionOperation(
                table=entry.table,
                disposition=entry.disposition,
                expected_rows=entry.candidate_rows,
                rationale=entry.rationale,
            )
        )

    _require(bool(operations), "The publication plan describes no canonical objects.")

    unsupported = sorted(
        {operation.disposition.value for operation in operations if operation.disposition not in _PLANNED_DISPOSITIONS}
    )
    _require(
        not unsupported,
        "No promotion step exists for disposition(s): " + ", ".join(unsupported) + ".",
    )

    # A rebuild disposition names a table this repository knows how to rebuild, or the plan is
    # describing work nothing can perform.
    # Ordered by DERIVED_TABLES, not alphabetically. biosample_to_workflow_run walks graph_edges,
    # so sorting put them in exactly the wrong order: the plan said to rebuild the consumer first,
    # while derived_tables and its own documentation both say graph_edges goes first. A plan that
    # contradicts the module it plans for is worse than one that says nothing about ordering.
    planned_rebuilds = {operation.table for operation in operations if operation.disposition is Disposition.REBUILD}
    rebuilds = [table for table in DERIVED_TABLES if table in planned_rebuilds]
    unknown = sorted(planned_rebuilds.difference(DERIVED_TABLES))
    _require(
        not unknown,
        "No rebuild procedure exists for: " + ", ".join(unknown) + ".",
    )

    # Every staged table must have a disposition. A staged table nobody decided about would be
    # silently left behind in staging, which reads afterwards as though it was never loaded.
    planned = {operation.table for operation in operations}
    undecided = sorted(set(staged_rows) - planned)
    _require(
        not undecided,
        "Staged tables have no disposition in the publication plan: " + ", ".join(undecided) + ".",
    )

    return BerdlPromotionPlan(
        plan_format_version=PROMOTION_PLAN_FORMAT_VERSION,
        status="plan-only",
        snapshot_id=snapshot_id,
        staging_namespace=staging_outcome.staging_namespace,
        canonical_namespace=canonical_namespace,
        destination_id=staging_outcome.destination_id,
        staging_outcome_sha256=staging_outcome_sha256,
        metadata_outcome_sha256=metadata_outcome_sha256,
        publication_plan_sha256=publication_plan_sha256,
        operations=operations,
        derived_rebuilds=rebuilds,
        recovery=recovery,
    )


def promotion_steps(plan: BerdlPromotionPlan) -> list[str]:
    """The ordered steps, so the operator authorizes a sequence rather than a set of counts.

    Dropping first is deliberate. Leaving the derived tables in place while the tables they are
    computed from are replaced underneath would leave them returning biosample-to-workflow
    mappings built from provenance that no longer exists, and those answers look normal. An
    absent table fails a query in seconds; a stale one is found out much later, by someone else.
    """
    counts: dict[str, int] = {}
    for operation in plan.operations:
        counts[operation.disposition.value] = counts.get(operation.disposition.value, 0) + 1
    steps = []
    if plan.derived_rebuilds:
        steps.append(f"drop {len(plan.derived_rebuilds)} derived table(s): " + ", ".join(plan.derived_rebuilds))
    if counts.get(Disposition.REPLACE.value):
        steps.append(f"replace {counts[Disposition.REPLACE.value]} table(s) from staging")
    if counts.get(Disposition.ADD.value):
        steps.append(f"add {counts[Disposition.ADD.value]} table(s) absent from the destination")
    if counts.get(Disposition.PRESERVE.value):
        steps.append(f"leave {counts[Disposition.PRESERVE.value]} table(s) untouched")
    if plan.derived_rebuilds:
        steps.append("rebuild those derived table(s) from the replaced provenance side tables")
    steps.append(f"verify all {len(plan.operations)} object(s) by read-back")
    return steps


def render_promotion_plan(plan: BerdlPromotionPlan) -> str:
    """Render the plan as the operator will read it before authorizing anything."""
    counts: dict[str, int] = {}
    for operation in plan.operations:
        counts[operation.disposition.value] = counts.get(operation.disposition.value, 0) + 1
    lines = [
        f"promotion plan for {plan.canonical_namespace}",
        f"  from staging   {plan.staging_namespace}",
        f"  snapshot       {plan.snapshot_id}",
        f"  objects        {len(plan.operations)}",
    ]
    lines.extend(f"    {name:10s} {count}" for name, count in sorted(counts.items()))
    lines.append("")
    lines.extend(f"  {index}. {step}" for index, step in enumerate(promotion_steps(plan), start=1))
    if plan.derived_rebuilds:
        # Said before authorization rather than in a runbook nobody reads at 2am. This is the
        # consequence of the ordering, and the operator is the person who can postpone it.
        lines.extend(
            [
                "",
                "  OUTAGE: " + ", ".join(plan.derived_rebuilds) + " are dropped at step 1 and do",
                "  not exist again until the rebuild. Queries against them, and joins from",
                "  biosample_to_workflow_run into the results tables, fail for the whole run.",
                "  That is deliberate: leaving them in place would return provenance that no",
                "  longer exists, and those answers look correct.",
            ]
        )
    lines.append("")
    lines.append(f"  recovery       {plan.recovery}")
    lines.append("  nothing has been changed; this plan is a description")
    return "\n".join(lines)
