"""Tests for planning a canonical promotion without performing one.

Every case here is a refusal, apart from the two that prove the happy path still works. That
balance is deliberate: this artifact authorizes changing the canonical namespace, so what it
declines to authorize is the whole point of it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from nmdc_lakehouse.berdl_promotion import (
    BerdlPromotionPlan,
    PromotionPlanError,
    build_berdl_promotion_plan,
    render_promotion_plan,
)
from nmdc_lakehouse.publication_plan import Disposition, PlanEntry, PublicationPlan

SNAPSHOT = "sha256:" + "a" * 64
STAGING = "nmdc.nmdc_metadata_staging_20260824"
CANONICAL = "nmdc.metadata"


def _entry(table: str, disposition: Disposition, rows: int | None) -> PlanEntry:
    return PlanEntry(
        table=table,
        disposition=disposition,
        rationale=f"{disposition.value} because the test says so.",
        decision_source="generated" if rows is not None else "policy",
        candidate_path=f"{table}.parquet" if rows is not None else None,
        candidate_rows=rows,
        candidate_target_schema_id="https://w3id.org/nmdc/nmdc-schema-flattened" if rows is not None else None,
        candidate_mapping_id="nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener" if rows is not None else None,
        destination_rows=None,
        candidate_physical_schema_sha256="c" * 64 if rows is not None else None,
        destination_physical_schema_sha256=None,
    )


def _publication_plan(*entries: PlanEntry) -> PublicationPlan:
    return PublicationPlan(
        plan_format_version=1,
        candidate_snapshot_id=SNAPSHOT,
        destination_id="nmdc-production",
        destination_observed_at="2026-08-24T20:49:38+00:00",
        destination_provider="nmdc",
        destination_table_format="iceberg",
        destination_metadata_capabilities=[],
        tables=list(entries),
    )


def _staging(*tables: tuple[str, int], status: str = "data-verified"):
    return SimpleNamespace(
        status=status,
        snapshot_id=SNAPSHOT,
        staging_namespace=STAGING,
        destination_id="nmdc-production",
        tables=[SimpleNamespace(table=name, destination_rows=rows) for name, rows in tables],
    )


def _metadata(status: str = "metadata-verified", snapshot_id: str = SNAPSHOT, namespace: str = STAGING):
    return SimpleNamespace(status=status, snapshot_id=snapshot_id, staging_namespace=namespace)


def _build(publication_plan, staging, metadata=None, canonical: str = CANONICAL):
    return build_berdl_promotion_plan(
        publication_plan=publication_plan,
        staging_outcome=staging,
        metadata_outcome=metadata if metadata is not None else _metadata(),
        canonical_namespace=canonical,
        staging_outcome_sha256="1" * 64,
        metadata_outcome_sha256="2" * 64,
        publication_plan_sha256="3" * 64,
        recovery="Reload the immutable snapshot into a fresh staging namespace, measured at 8m56s.",
    )


def test_a_plan_describes_every_object_and_changes_nothing() -> None:
    plan = _build(
        _publication_plan(
            _entry("biosample_set", Disposition.REPLACE, 27352),
            _entry("organism_set", Disposition.ADD, 0),
            _entry("graph_edges", Disposition.REBUILD, None),
        ),
        _staging(("biosample_set", 27352), ("organism_set", 0)),
    )

    assert plan.status == "plan-only"
    assert len(plan.operations) == 3
    assert plan.derived_rebuilds == ["graph_edges"]
    assert "nothing has been changed" in render_promotion_plan(plan)


def test_an_empty_add_is_planned_rather_than_skipped() -> None:
    """Confirmed with Mark on 2026-08-26: the six zero-row additions are wanted."""
    plan = _build(
        _publication_plan(_entry("organism_set", Disposition.ADD, 0)),
        _staging(("organism_set", 0)),
    )

    assert plan.operations[0].expected_rows == 0


def test_unverified_staging_is_refused() -> None:
    with pytest.raises(PromotionPlanError, match="not data-verified"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1), status="data-failed"),
        )


def test_unverified_metadata_is_refused() -> None:
    """Promoting descriptions that were never proven applied is how the canonical namespace
    ends up with the data and not the documentation."""
    with pytest.raises(PromotionPlanError, match="not metadata-verified"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
            _metadata(status="preview-only"),
        )


def test_evidence_describing_different_snapshots_is_refused() -> None:
    with pytest.raises(PromotionPlanError, match="different snapshots"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
            _metadata(snapshot_id="sha256:" + "b" * 64),
        )


def test_metadata_applied_to_another_namespace_is_refused() -> None:
    with pytest.raises(PromotionPlanError, match="different namespace"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
            _metadata(namespace="nmdc.somewhere_else"),
        )


def test_a_row_count_that_moved_since_planning_is_refused() -> None:
    """The plan decided on candidate counts; staging is what landed. A mismatch means the
    decision was made about different data."""
    with pytest.raises(PromotionPlanError, match="planned with 27352 rows but 27000 were staged"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 27352)),
            _staging(("biosample_set", 27000)),
        )


def test_a_planned_table_that_was_never_staged_is_refused() -> None:
    with pytest.raises(PromotionPlanError, match="was not staged"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("study_set", 1)),
        )


def test_a_staged_table_with_no_disposition_is_refused() -> None:
    """It would otherwise be left behind in staging, which afterwards looks like it never loaded."""
    with pytest.raises(PromotionPlanError, match="no disposition"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1), ("forgotten_set", 5)),
        )


def test_a_rebuild_nothing_can_perform_is_refused() -> None:
    """A rebuild disposition has to name a table this repository knows how to rebuild."""
    with pytest.raises(PromotionPlanError, match="No rebuild procedure exists for: mystery_set"):
        _build(
            _publication_plan(_entry("mystery_set", Disposition.REBUILD, None)),
            _staging(),
        )


def test_promotion_cannot_target_the_namespace_it_reads_from() -> None:
    with pytest.raises(ValidationError, match="cannot target the staging namespace"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
            canonical=STAGING,
        )


def test_an_unqualified_canonical_namespace_is_refused() -> None:
    with pytest.raises(ValidationError, match="canonical namespace must be catalog-qualified"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
            canonical="nmdc_metadata",
        )


def test_a_plan_with_no_objects_is_refused() -> None:
    with pytest.raises(PromotionPlanError, match="no canonical objects"):
        _build(_publication_plan(), _staging())


def test_the_rendered_plan_counts_every_disposition() -> None:
    plan = _build(
        _publication_plan(
            _entry("a_set", Disposition.REPLACE, 1),
            _entry("b_set", Disposition.REPLACE, 2),
            _entry("c_set", Disposition.ADD, 0),
        ),
        _staging(("a_set", 1), ("b_set", 2), ("c_set", 0)),
    )

    rendered = render_promotion_plan(plan)

    assert "replace    2" in rendered
    assert "add        1" in rendered


def test_a_plan_document_round_trips() -> None:
    plan = _build(
        _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
        _staging(("biosample_set", 1)),
    )

    assert BerdlPromotionPlan.model_validate(plan.model_dump()) == plan


def _full_plan():
    return _build(
        _publication_plan(
            _entry("biosample_set", Disposition.REPLACE, 27352),
            _entry("study_set", Disposition.REPLACE, 41),
            _entry("organism_set", Disposition.ADD, 0),
            _entry("graph_edges", Disposition.REBUILD, None),
            _entry("biosample_to_workflow_run", Disposition.REBUILD, None),
        ),
        _staging(("biosample_set", 27352), ("study_set", 41), ("organism_set", 0)),
    )


def test_the_derived_tables_are_dropped_before_the_replacements_and_rebuilt_after() -> None:
    """Mark's decision on 2026-08-26, and the ordering is the decision.

    Leaving them in place while the tables they are computed from are replaced underneath would
    leave them returning provenance that no longer exists, and those answers look correct.
    """
    from nmdc_lakehouse.berdl_promotion import promotion_steps

    steps = promotion_steps(_full_plan())

    assert "drop" in steps[0]
    assert "replace" in steps[1]
    assert "rebuild" in steps[3]
    assert steps.index([s for s in steps if s.startswith("drop")][0]) < steps.index(
        [s for s in steps if s.startswith("replace")][0]
    )


def test_the_rendered_plan_states_the_outage_before_authorization() -> None:
    """Prominent where the operator is, not only in a runbook."""
    rendered = render_promotion_plan(_full_plan())

    assert "OUTAGE" in rendered
    assert "graph_edges" in rendered
    assert "fail for the whole run" in rendered


def test_a_plan_with_nothing_derived_says_nothing_about_an_outage() -> None:
    """A warning that appears when it does not apply is one people learn to skip."""
    rendered = render_promotion_plan(
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
        )
    )

    assert "OUTAGE" not in rendered
