"""Plan a canonical promotion without performing one.

Staging proved that candidate tables load and verify. It authorizes nothing about the canonical
namespace, and this module keeps that separation: it reads verified evidence and writes an
immutable description of what a promotion would do, touching nothing.

The split mirrors `berdl_staging`, which has now run end to end, and it exists for the same
reason: the point where a human authorizes a destructive act should be a distinct, reviewable
artifact rather than a flag on the command that performs it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import textwrap
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from nmdc_lakehouse.derived_tables import DERIVED_TABLES
from nmdc_lakehouse.metadata_application import catalog_of_namespace
from nmdc_lakehouse.publication_plan import Disposition, PublicationPlan

PROMOTION_PLAN_FORMAT_VERSION: Literal[1] = 1

# Dispositions this planner can express as a step. `retire` is absent on purpose: it removes
# canonical tables, nothing here implements that, and a plan whose header counts a disposition its
# sequence omits is a silent omission the operator authorizes without seeing.
_PLANNED_DISPOSITIONS = frozenset({Disposition.REPLACE, Disposition.ADD, Disposition.REBUILD, Disposition.PRESERVE})

_QUALIFIED = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")
# One unquoted table name. `\Z` and not `$`, because `$` matches before a trailing newline and a
# name ending in one would pass while carrying a line break into the statement.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class StagedTableLike(Protocol):
    """One staged table, as the staging outcome reports it."""

    @property
    def table(self) -> str:
        """Canonical table name, matched against the publication plan."""
        ...

    @property
    def destination_rows(self) -> int:
        """Rows that actually landed in staging, checked against the planned count."""
        ...


class StagingOutcomeLike(Protocol):
    """The fields promotion reads from a verified staging outcome.

    A protocol rather than the concrete model, so this module does not import the staging
    machinery to read four attributes, and so a test can supply a stand-in without constructing a
    full outcome. Typed rather than `object`, because reaching into an untyped value is how the
    caller finds out at runtime that the shape changed.
    """

    # Read-only properties, not plain attributes. A protocol attribute is mutable and so matched
    # invariantly, which rejects a model whose status is Literal["data-verified"] against str, and
    # rejects list[StagedTable] against list[StagedTableLike]. This module only reads these, so
    # properties and a Sequence state the actual contract instead of an accidentally stricter one.
    @property
    def status(self) -> str:
        """Verification status; promotion refuses anything but the verified value."""
        ...

    @property
    def snapshot_id(self) -> str:
        """Snapshot this evidence describes, cross-checked across all three inputs."""
        ...

    @property
    def staging_namespace(self) -> str:
        """Namespace the evidence describes, cross-checked across all three inputs."""
        ...

    @property
    def destination_id(self) -> str:
        """Destination the dispositions were decided against."""
        ...

    @property
    def tables(self) -> Sequence[StagedTableLike]:
        """Every table staging reported on, in the order the outcome recorded them."""
        ...


class MetadataOutcomeLike(Protocol):
    """The fields promotion reads from a verified metadata outcome."""

    @property
    def destination_id(self) -> str:
        """Destination the metadata was applied against."""
        ...

    @property
    def status(self) -> str:
        """Verification status; promotion refuses anything but the verified value."""
        ...

    @property
    def snapshot_id(self) -> str:
        """Snapshot this evidence describes, cross-checked across all three inputs."""
        ...

    @property
    def staging_namespace(self) -> str:
        """Namespace the evidence describes, cross-checked across all three inputs."""
        ...


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
    #: Catalog the destination evidence describes. Persisted rather than derived, so the file
    #: carries the same fact the builder checked; `validate_namespaces` binds it to the namespace
    #: this actually writes into, the way `BerdlStagingPlan` binds its own.
    destination_provider: str
    staging_outcome_sha256: str
    metadata_outcome_sha256: str
    publication_plan_sha256: str
    operations: list[PromotionOperation]
    # Dropped before the replacements and rebuilt after them, which is the ordering Mark chose on
    # 2026-08-26. These tables are absent, and queries against them fail, for the whole run.
    derived_rebuilds: list[str]
    recovery: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_table_names(self) -> "BerdlPromotionPlan":
        """Refuse any table name that is not a plain identifier.

        Every name here ends up interpolated into SQL.

        The plan is JSON read from disk. Nothing about that file is authenticated beyond the
        digest an operator types, and the digest is of the file as it is, not of a file anyone
        vouched for. A name carrying a semicolon or a backtick would become extra statements
        inside a DROP or a CREATE OR REPLACE, which is the one place in this repository where
        that is unrecoverable.
        """
        for table in self.derived_rebuilds:
            if not _IDENTIFIER.fullmatch(table):
                raise ValueError(f"Derived rebuild {table!r} is not a plain table identifier.")
        for operation in self.operations:
            if not _IDENTIFIER.fullmatch(operation.table):
                raise ValueError(f"Operation table {operation.table!r} is not a plain table identifier.")
        return self

    @model_validator(mode="after")
    def validate_namespaces(self) -> "BerdlPromotionPlan":
        """Both namespaces must name a catalog, and promotion must not target the staging one."""
        for value, label in ((self.staging_namespace, "staging"), (self.canonical_namespace, "canonical")):
            if not _QUALIFIED.fullmatch(value):
                raise ValueError(f"The {label} namespace must be catalog-qualified as <catalog>.<namespace>.")
        if self.staging_namespace == self.canonical_namespace:
            raise ValueError("Promotion cannot target the staging namespace it reads from.")
        # The same binding `BerdlStagingPlan` makes. A provider is a label and nothing addresses a
        # table with it, which is exactly why it drifts: without this a plan whose evidence
        # describes provider `nmdc` can name, authorize and destroy `other.metadata`.
        if self.destination_provider != catalog_of_namespace(self.canonical_namespace, "canonical namespace"):
            raise ValueError("The destination provider must name the catalog the promotion writes into.")
        return self

    @model_validator(mode="after")
    def validate_operations_and_rebuilds(self) -> "BerdlPromotionPlan":
        """Re-establish what the builder established, because a file is not a builder.

        `load_promotion_plan` validates this model and nothing else. Every invariant that lived
        only in `build_berdl_promotion_plan` was therefore absent for an edited plan, which could
        name an unrelated table as a derived rebuild and drop it, duplicate an operation, or carry
        a `retire` that the statements silently skip while the header counts it.
        """
        if not self.operations:
            raise ValueError("A promotion plan must describe at least one operation.")
        tables = [operation.table for operation in self.operations]
        if len(tables) != len(set(tables)):
            raise ValueError("A promotion plan must not name the same table twice.")
        unsupported = sorted(
            {operation.disposition.value for operation in self.operations} - {d.value for d in _PLANNED_DISPOSITIONS}
        )
        if unsupported:
            raise ValueError("A promotion plan cannot express: " + ", ".join(unsupported) + ".")
        # Exactly the rebuild operations, in `DERIVED_TABLES` order, which is how the builder
        # derives this list. Anything else means the file was edited: a name that is not a derived
        # table would be dropped by a statement nobody planned.
        expected = [
            table
            for table in DERIVED_TABLES
            if table in {op.table for op in self.operations if op.disposition is Disposition.REBUILD}
        ]
        if self.derived_rebuilds != expected:
            raise ValueError(
                f"derived_rebuilds must be exactly the rebuild operations in DERIVED_TABLES order, which is {expected}."
            )
        return self


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PromotionPlanError(message)


def _require_named_once(names: list[str], source: str) -> None:
    """Refuse evidence that names one table twice, rather than keeping whichever entry came last.

    Collapsing by name is the tempting fix and the wrong one. A duplicate is not a formatting
    quirk, it means the file describes the same table twice and the two descriptions may disagree,
    so the count that survives is decided by list order. This is an authorization artifact: the
    operator has to be told the evidence is malformed, not handed the last row silently.
    """
    duplicated = sorted({name for name, count in Counter(names).items() if count > 1})
    _require(
        not duplicated,
        f"{source} names the same table more than once: " + ", ".join(duplicated) + ".",
    )


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
    # All three inputs, not two. The check above was added for the plan and the staging outcome,
    # and the metadata outcome carries a destination as well. Guarding two of three leaves the
    # column descriptions to have been applied somewhere other than where the data landed.
    _require(
        publication_plan.destination_id == metadata_outcome.destination_id,
        f"The publication plan describes destination '{publication_plan.destination_id}' but the "
        f"metadata outcome describes '{metadata_outcome.destination_id}'.",
    )

    # A provider is optional on a publication plan and required here, because promotion is what
    # binds it to a catalog. Defaulting it would put an unchecked label in the file and make the
    # binding vacuous, which is the failure the binding is for.
    _require(
        publication_plan.destination_provider is not None,
        "The publication plan does not name a destination provider, so nothing can bind the promotion to a catalog.",
    )

    # Row counts, table by table. The publication plan decided what to do on the strength of
    # candidate row counts, and the staging outcome is what actually landed. Promoting on a plan
    # whose numbers no longer match the data is promoting on a stale decision.
    _require_named_once([table.table for table in staging_outcome.tables], "The staging outcome")
    _require_named_once([entry.table for entry in publication_plan.tables], "The publication plan")
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
        # Narrowed by the `_require` above; mypy cannot see through it.
        destination_provider=str(publication_plan.destination_provider),
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
        # A rebuild reads whatever the destination holds when it runs, which is only "the
        # replaced tables" if this plan replaced any. A plan can rebuild with zero replacements,
        # and telling the operator otherwise describes a step that is not the one about to run.
        source = (
            "the replaced provenance side tables"
            if counts.get(Disposition.REPLACE.value)
            else "the provenance side tables already in the destination"
        )
        steps.append(f"rebuild those derived table(s) from {source}")
    # Named as a step because it is one, and because the plan consumes the metadata outcome as
    # evidence. Consuming it says the staging tables were verified; it does not say the verified
    # metadata arrives here. `CREATE OR REPLACE TABLE ... AS SELECT` builds a table from a query,
    # and a table comment and TBLPROPERTIES are not part of a query result. An operator reading a
    # plan that cites a metadata outcome will otherwise assume promotion carries it.
    if counts.get(Disposition.REPLACE.value) or counts.get(Disposition.ADD.value):
        steps.append("note that table comments and properties do not travel with these statements")
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
        # consequence of the ordering, and the operator is the person who can postpone it. The
        # sentence names the tables this plan drops, not the ones a two-table plan would drop.
        # Written as one sentence and wrapped, rather than as hand-split lines. Splitting it by
        # hand is what let the first fix change the verbs and leave the pronouns behind, and it
        # makes every later edit a re-wrapping job.
        single = len(plan.derived_rebuilds) == 1
        is_are, does_do, them = ("is", "does", "it") if single else ("are", "do", "them")
        outage = (
            f"OUTAGE: {', '.join(plan.derived_rebuilds)} {is_are} dropped at step 1 and {does_do} "
            f"not exist again until the rebuild. Queries against {them}, and joins from {them} "
            "into the results tables, fail for the whole run. That is deliberate: leaving "
            f"{them} in place would return provenance that no longer exists, and those answers "
            "look correct."
        )
        lines.append("")
        lines.extend(textwrap.wrap(outage, width=78, initial_indent="  ", subsequent_indent="  "))
    lines.append("")
    lines.append(f"  recovery       {plan.recovery}")
    lines.append("  nothing has been changed; this plan is a description")
    return "\n".join(lines)


# Generic, so the caller keeps the concrete model type. Returning a bare BaseModel would hand
# build_berdl_promotion_plan something the type checker cannot match to its protocols.
_Evidence = TypeVar("_Evidence", bound=BaseModel)


def _read_evidence(path: Path, model: type[_Evidence], label: str) -> tuple[_Evidence, str]:
    """Read one evidence file and hash the exact bytes that were parsed.

    The digest comes from the same `contents` that pydantic validated, not from a second read.
    Hashing the file again would let a file change between the two reads and produce a plan whose
    recorded digest belongs to bytes nobody checked.
    """
    document = path.expanduser()
    if not document.is_file() or document.is_symlink():
        raise PromotionPlanError(f"The {label} must be an ordinary file.")
    try:
        contents = document.read_bytes()
        parsed = model.model_validate_json(contents, strict=True)
    except ValidationError as error:
        # Naming the reason, not only the file. An operator holding a plan this refuses cannot act
        # on "not valid": the whole point of refusing is that they go and look at what is wrong.
        reasons = "; ".join(
            f"{'.'.join(str(part) for part in problem['loc']) or '<plan>'}: {problem['msg']}"
            for problem in error.errors()
        )
        raise PromotionPlanError(f"The {label} is not valid. {reasons}") from error
    except (OSError, UnicodeDecodeError) as error:
        raise PromotionPlanError(f"The {label} could not be read.") from error
    return parsed, hashlib.sha256(contents).hexdigest()


def plan_berdl_promotion_from_files(
    *,
    publication_plan_path: Path,
    staging_outcome_path: Path,
    metadata_outcome_path: Path,
    canonical_namespace: str,
    recovery: str,
) -> BerdlPromotionPlan:
    """Read the three pieces of evidence from disk and build the plan they authorize.

    Each digest recorded in the plan is of the bytes this call actually parsed, so the plan names
    the exact evidence it was built from rather than whatever those paths hold later.
    """
    from nmdc_lakehouse.berdl_metadata import BerdlMetadataOutcome
    from nmdc_lakehouse.berdl_staging import BerdlStagingOutcome

    publication_plan, publication_plan_sha256 = _read_evidence(
        publication_plan_path, PublicationPlan, "publication plan"
    )
    staging_outcome, staging_outcome_sha256 = _read_evidence(
        staging_outcome_path, BerdlStagingOutcome, "BERDL staging outcome"
    )
    metadata_outcome, metadata_outcome_sha256 = _read_evidence(
        metadata_outcome_path, BerdlMetadataOutcome, "BERDL metadata outcome"
    )
    return build_berdl_promotion_plan(
        publication_plan=publication_plan,
        staging_outcome=staging_outcome,
        metadata_outcome=metadata_outcome,
        canonical_namespace=canonical_namespace,
        staging_outcome_sha256=staging_outcome_sha256,
        metadata_outcome_sha256=metadata_outcome_sha256,
        publication_plan_sha256=publication_plan_sha256,
        recovery=recovery,
    )


def render_berdl_promotion_plan_json(plan: BerdlPromotionPlan) -> str:
    """Render the plan as stable, credential-free JSON."""
    return json.dumps(plan.model_dump(mode="json"), indent=2, sort_keys=True)


def write_berdl_promotion_plan(path: Path, plan: BerdlPromotionPlan) -> Path:
    """Create the plan file without replacing an earlier one.

    This follows the create-without-replacing contract used by the staging and metadata outcomes
    rather than the overwrite contract used by regenerable plans. A promotion plan is the artifact
    a human authorizes against, and silently replacing one would mean an operator can approve a
    digest that no longer describes the file at that path.
    """
    destination = path.expanduser()
    if destination.exists() or destination.is_symlink():
        raise PromotionPlanError("Refusing to replace an existing BERDL promotion plan.")
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise PromotionPlanError("The BERDL promotion plan parent must be an ordinary directory.")
    destination = parent.resolve() / destination.name
    descriptor: int | None = None
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
        temporary = Path(temporary_name)
        stream = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = None
        with stream:
            stream.write(render_berdl_promotion_plan_json(plan))
            stream.write("\n")
        try:
            # os.link rather than replace: it fails if the destination appeared since the check
            # above, so the refusal holds even when two runs race for the same path.
            os.link(temporary, destination)
        except FileExistsError as error:
            raise PromotionPlanError("Refusing to replace an existing BERDL promotion plan.") from error
        except OSError as error:
            raise PromotionPlanError("Cannot publish the BERDL promotion plan atomically.") from error
    except OSError as error:
        raise PromotionPlanError("Cannot write the BERDL promotion plan.") from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
    return destination


class PromotionRefused(PromotionPlanError):
    """Raised when a promotion is asked for and the evidence or authorization does not allow it."""


def load_promotion_plan(path: Path) -> tuple[BerdlPromotionPlan, str]:
    """Read a written plan and hash the exact bytes read, so authorization names this file."""
    return _read_evidence(path, BerdlPromotionPlan, "BERDL promotion plan")


def promotion_statements(plan: BerdlPromotionPlan) -> list[tuple[str, str, str]]:
    """The SQL this promotion runs, in order, paired with the step and the table each belongs to.

    Built from the plan rather than decided here, so what runs and what an operator authorized
    cannot diverge. The order is `promotion_steps`, and the derived tables are dropped before the
    replacements for the reason recorded there: leaving them in place while the tables they are
    computed from change underneath returns provenance that no longer exists, and those answers
    look correct.

    `preserve` contributes nothing, which is the point of it: a table nobody decided to touch is
    a table this must not touch.
    """
    statements: list[tuple[str, str, str]] = []
    for table in plan.derived_rebuilds:
        statements.append(("drop", table, f"DROP TABLE IF EXISTS {plan.canonical_namespace}.{table}"))
    for operation in plan.operations:
        if operation.disposition not in (Disposition.REPLACE, Disposition.ADD):
            continue
        # `add` is CREATE TABLE and `replace` is CREATE OR REPLACE TABLE. They were the same
        # statement, on the reasoning that the difference is only whether the destination already
        # held the table. That reasoning is about when the plan was built. The inventory proved the
        # table was absent then; nothing proves it is absent now, and `CREATE OR REPLACE` would
        # overwrite whatever appeared in between without saying so. A plain `CREATE TABLE` fails,
        # which is the outcome an operator who authorized an `add` should get.
        verb = "CREATE OR REPLACE TABLE" if operation.disposition is Disposition.REPLACE else "CREATE TABLE"
        statements.append(
            (
                operation.disposition.value,
                operation.table,
                f"{verb} {plan.canonical_namespace}.{operation.table} "
                f"AS SELECT * FROM {plan.staging_namespace}.{operation.table}",
            )
        )
    return statements


def execute_promotion(
    spark: object,
    plan: BerdlPromotionPlan,
    *,
    plan_sha256: str,
    authorize_plan_sha256: str,
    authorize_canonical_namespace: str,
    authorize_destination_id: str,
    progress: object = None,
) -> list[str]:
    """Perform the promotion this plan describes, or refuse.

    Two authorizations, because they fail differently. The digest binds this run to the exact plan
    a human read: a plan regenerated after the evidence moved has a different digest and is
    refused, even though it may describe the same tables. The namespace is typed again because a
    digest is copied from a previous command and a namespace is not, so an operator promoting into
    the wrong place gets caught by the argument they had to write themselves.

    The destination is the third, and it is the weakest of them, deliberately. Nothing here can
    verify which deployment a session actually reaches: the runtime comes from a checkout named at
    execution time, and `spark_session` establishes only that the helper was imported from that
    checkout, not what the checkout is configured to talk to. So the operator asserts it. That
    turns an unchecked assumption into a stated one, which is all this can honestly do offline.

    Refuses rather than warns on all three, and the read-back afterwards is the check, not the
    counts this returns: a statement that succeeded is not a table that holds what it should.
    """
    say = progress if callable(progress) else (lambda _message: None)
    if plan.status != "plan-only":
        raise PromotionRefused(f"The plan status is {plan.status!r}, not 'plan-only'.")
    if authorize_plan_sha256 != plan_sha256:
        raise PromotionRefused("Execution requires --authorize-plan-sha256 with the digest of the plan being run.")
    if authorize_canonical_namespace != plan.canonical_namespace:
        raise PromotionRefused(
            f"--authorize-canonical-namespace is {authorize_canonical_namespace!r} but the plan "
            f"promotes into {plan.canonical_namespace!r}."
        )
    if authorize_destination_id != plan.destination_id:
        raise PromotionRefused(
            f"--authorize-destination-id is {authorize_destination_id!r} but the dispositions were "
            f"decided against {plan.destination_id!r}. The same namespace name exists in more than "
            f"one deployment, and which one this session reaches is decided by the checkout, not "
            f"by anything checked here."
        )

    performed: list[str] = []
    for step, table, statement in promotion_statements(plan):
        say(f"{step}: {plan.canonical_namespace}.{table}")
        try:
            spark.sql(statement)  # type: ignore[attr-defined]
        # BaseException, not Exception. Ctrl-C raises KeyboardInterrupt, which does not derive from
        # Exception, so an operator interrupting this loop got a generic abort and no record of
        # what had already run. That is the moment the record matters most: the interrupt lands
        # between statements or inside one, and nothing here can tell which, so the in-flight
        # statement is reported as unknown rather than as failed or as skipped.
        except BaseException as error:
            # Named, and with what already ran. A promotion that stops part way leaves the
            # namespace in a state nobody planned, and the operator's first question is which
            # objects moved.
            stopped = "was interrupted during" if isinstance(error, KeyboardInterrupt) else "failed during"
            raise PromotionRefused(
                f"The promotion {stopped} {step}: {statement}. That statement may or may not have "
                f"taken effect. {len(performed)} statement(s) had already run: "
                f"{', '.join(performed) or 'none'}."
            ) from error
        performed.append(statement)
    return performed
