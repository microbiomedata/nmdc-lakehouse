"""Tests for planning a canonical promotion without performing one.

Most cases here are refusals, and that is deliberate: this artifact authorizes changing the
canonical namespace, so what it declines to authorize is most of what it is for. The rest prove
the accepted path still works and that the rendering says what an operator needs.

No count of them appears in this sentence on purpose. The previous version said "apart from the
two", which stopped being true as soon as cases were added and had to be re-read to notice.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from nmdc_lakehouse.berdl_promotion import (
    BerdlPromotionPlan,
    PromotionPlanError,
    PromotionRefused,
    build_berdl_promotion_plan,
    execute_promotion,
    load_promotion_plan,
    promotion_statements,
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


def _metadata(
    status: str = "metadata-verified",
    snapshot_id: str = SNAPSHOT,
    namespace: str = STAGING,
    destination_id: str = "nmdc-production",
):
    return SimpleNamespace(
        status=status,
        snapshot_id=snapshot_id,
        staging_namespace=namespace,
        destination_id=destination_id,
    )


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


def test_rebuilds_are_ordered_by_dependency_not_alphabetically() -> None:
    """biosample_to_workflow_run walks graph_edges, so sorting reversed the required order.

    The plan said to rebuild the consumer first while `derived_tables` and its documentation both
    say graph_edges goes first, which is a plan contradicting the module it plans for.
    """
    from nmdc_lakehouse.derived_tables import DERIVED_TABLES

    plan = _full_plan()

    assert plan.derived_rebuilds == list(DERIVED_TABLES)
    assert plan.derived_rebuilds[0] == "graph_edges"
    assert plan.derived_rebuilds != sorted(plan.derived_rebuilds), "alphabetical would be wrong here"


def test_a_disposition_with_no_step_is_refused() -> None:
    """A disposition counted in the header but absent from the sequence is a silent omission.

    `retire` removes canonical tables and nothing here implements that, so a plan containing one
    is refused rather than summarised and skipped.
    """
    with pytest.raises(PromotionPlanError, match="No promotion step exists for disposition"):
        _build(
            _publication_plan(
                _entry("biosample_set", Disposition.REPLACE, 1),
                _entry("old_set", Disposition.RETIRE, None),
            ),
            _staging(("biosample_set", 1)),
        )


def test_preserve_appears_in_the_steps_rather_than_only_in_the_counts() -> None:
    """It is a real no-op, and the operator should read it rather than infer it from a total."""
    from nmdc_lakehouse.berdl_promotion import promotion_steps

    plan = _build(
        _publication_plan(
            _entry("biosample_set", Disposition.REPLACE, 1),
            _entry("functional_annotation_agg", Disposition.PRESERVE, None),
        ),
        _staging(("biosample_set", 1)),
    )

    assert any("leave 1 table(s) untouched" in step for step in promotion_steps(plan))


def test_every_counted_disposition_appears_in_the_steps() -> None:
    """The property behind the two cases above: the header and the sequence cannot disagree."""
    from nmdc_lakehouse.berdl_promotion import promotion_steps

    plan = _full_plan()
    steps = " ".join(promotion_steps(plan))

    for operation in plan.operations:
        assert operation.disposition.value in steps or operation.table in steps, operation.disposition


def test_evidence_describing_different_destinations_is_refused() -> None:
    """Every other check passes on mismatched destinations, which is what makes this one needed.

    The dispositions were decided against one destination's contents. Promoting them into another
    promotes decisions about tables that are not the ones being replaced.
    """
    publication_plan = _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1))
    publication_plan.destination_id = "somewhere-else"

    with pytest.raises(PromotionPlanError, match="describes destination 'somewhere-else'"):
        _build(publication_plan, _staging(("biosample_set", 1)))


def test_a_missing_candidate_row_count_is_refused_as_missing_not_as_a_mismatch() -> None:
    """`candidate_rows` is optional on PlanEntry, and None read as a count that disagreed.

    The message said "planned with None rows but 1 were staged", which points at the data when
    the real problem is that the plan recorded no count to decide on.
    """
    with pytest.raises(PromotionPlanError, match="no candidate row count"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, None)),
            _staging(("biosample_set", 1)),
        )


def test_a_rebuild_with_no_replacements_does_not_claim_the_sources_were_replaced() -> None:
    """The step text described a plan that is not the one about to run.

    A rebuild reads whatever the destination holds at the moment it runs. Calling those "the
    replaced provenance side tables" when this plan replaced nothing tells the operator a
    replacement is part of the sequence they are authorizing, and it is not.
    """
    from nmdc_lakehouse.berdl_promotion import promotion_steps

    plan = _build(
        _publication_plan(
            _entry("functional_annotation_agg", Disposition.PRESERVE, None),
            _entry("graph_edges", Disposition.REBUILD, None),
        ),
        _staging(),
    )
    steps = " ".join(promotion_steps(plan))

    assert "rebuild" in steps
    assert "replaced provenance side tables" not in steps
    assert "already in the destination" in steps


def test_the_outage_names_the_tables_this_plan_drops() -> None:
    """It named biosample_to_workflow_run whatever the plan actually dropped.

    An operator dropping only graph_edges was warned about joins from a table this plan does not
    touch, which is both wrong and the kind of wrong that teaches people to skim the block.
    """
    rendered = render_promotion_plan(
        _build(
            _publication_plan(
                _entry("biosample_set", Disposition.REPLACE, 1),
                _entry("graph_edges", Disposition.REBUILD, None),
            ),
            _staging(("biosample_set", 1)),
        )
    )

    # Normalised, because the block is wrapped and a phrase can straddle two lines.
    outage = " ".join(rendered[rendered.index("OUTAGE") :].split())

    assert "graph_edges is dropped" in outage
    assert "biosample_to_workflow_run" not in outage
    # The verbs were made conditional and the pronouns were not, which reads as a half-edit
    # rather than as a sentence. Asserted here so the number agreement holds across the block.
    assert "does not exist again" in outage
    assert "against it, and joins from it" in outage
    assert "leaving it in place" in outage
    assert "them" not in outage


def test_the_outage_agrees_with_itself_when_both_derived_tables_are_dropped() -> None:
    """The singular case is the new one, so the plural is the control that it did not break."""
    outage = " ".join(render_promotion_plan(_full_plan()).split())

    assert "graph_edges, biosample_to_workflow_run are dropped" in outage
    assert "do not exist again" in outage
    assert "against them, and joins from them" in outage
    assert "leaving them in place" in outage
    assert " it " not in outage.split("OUTAGE")[1].split("recovery")[0]


def test_a_staging_outcome_naming_one_table_twice_is_refused() -> None:
    """The row check read a dict, so the second entry quietly replaced the first.

    A duplicate is not a formatting quirk. It means the evidence describes the same table twice
    and the two descriptions may disagree, so which count gets checked was decided by list order.
    """
    with pytest.raises(PromotionPlanError, match="staging outcome names the same table more than once"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 27352)),
            _staging(("biosample_set", 27352), ("biosample_set", 1)),
        )


def test_a_publication_plan_naming_one_table_twice_is_refused() -> None:
    """The same shape on the other input, which nobody flagged.

    A duplicated plan entry built two operations for one table, so the object count in the header
    the operator authorizes was larger than the number of tables the promotion touches.
    """
    with pytest.raises(PromotionPlanError, match="publication plan names the same table more than once"):
        _build(
            _publication_plan(
                _entry("biosample_set", Disposition.REPLACE, 27352),
                _entry("biosample_set", Disposition.PRESERVE, None),
            ),
            _staging(("biosample_set", 27352)),
        )


def test_a_metadata_outcome_describing_a_different_destination_is_refused() -> None:
    """The destination guard covered two of the three inputs.

    It was added for the publication plan and the staging outcome. The metadata outcome carries a
    destination too, so descriptions could have been applied somewhere other than where the data
    landed and every other check would still pass.
    """
    with pytest.raises(PromotionPlanError, match="metadata outcome describes 'somewhere-else'"):
        _build(
            _publication_plan(_entry("biosample_set", Disposition.REPLACE, 1)),
            _staging(("biosample_set", 1)),
            metadata=_metadata(destination_id="somewhere-else"),
        )


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _plan_document() -> dict:
    return {
        "plan_format_version": 1,
        "candidate_snapshot_id": SNAPSHOT,
        "destination_id": "nmdc-production",
        "destination_observed_at": "2026-08-24T20:49:38+00:00",
        "destination_provider": "nmdc",
        "destination_table_format": "iceberg",
        "destination_metadata_capabilities": [],
        "tables": [
            json.loads(_entry("biosample_set", Disposition.REPLACE, 27352).model_dump_json()),
            json.loads(_entry("graph_edges", Disposition.REBUILD, None).model_dump_json()),
        ],
    }


def _staging_document() -> dict:
    return {
        "outcome_format_version": 1,
        "status": "data-verified",
        "snapshot_id": SNAPSHOT,
        "staging_namespace": STAGING,
        "destination_id": "nmdc-production",
        "bucket": "berdl-bucket",
        "bronze_prefix": "bronze/nmdc",
        "progress_key": "progress.json",
        "config_key": "config.json",
        "ingest_revision": "f" * 40,
        "staging_plan_sha256": "1" * 64,
        "upstream_outcome_sha256": "2" * 64,
        "upstream_started_at": "2026-08-24T20:00:00+00:00",
        "upstream_finished_at": "2026-08-24T20:08:56+00:00",
        "tables": [
            {
                "table": "biosample_set",
                "artifact_sha256": "3" * 64,
                "rows": 27352,
                "destination_rows": 27352,
                "source_basis": "snapshot",
            }
        ],
    }


def _metadata_document() -> dict:
    return {
        "outcome_format_version": 1,
        "status": "metadata-verified",
        "snapshot_id": SNAPSHOT,
        "destination_id": "nmdc-production",
        "staging_namespace": STAGING,
        "staging_outcome_sha256": "4" * 64,
        "metadata_plan_sha256": "5" * 64,
        "deferred_namespace_operations": 0,
        "targets": [],
    }


def _files(root: Path) -> dict[str, Path]:
    return {
        "publication_plan_path": _write(root / "plan.json", _plan_document()),
        "staging_outcome_path": _write(root / "staging.json", _staging_document()),
        "metadata_outcome_path": _write(root / "metadata.json", _metadata_document()),
    }


RECOVERY = "Reload the immutable snapshot into a fresh staging namespace, measured at 8m56s."


def test_the_plan_records_the_digest_of_the_bytes_it_actually_read(tmp_path: Path, monkeypatch) -> None:
    """The recorded digests must identify the evidence, or they identify nothing.

    Hashing each file in a second pass would let it change between the read and the hash, and the
    plan would then name bytes nobody validated. Asserting the digest against a later read of the
    same path cannot detect that, because a double read agrees with itself while the file is
    still. So each path is read once here and returns different bytes on any second read: if the
    planner reads twice, the digest it recorded is of bytes this test never saw.
    """
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files

    paths = _files(tmp_path)
    first_read: dict[Path, bytes] = {}
    reads: dict[Path, int] = {}
    real_read_bytes = Path.read_bytes

    def counting_read_bytes(self: Path) -> bytes:
        resolved = self.resolve()
        reads[resolved] = reads.get(resolved, 0) + 1
        if reads[resolved] == 1:
            first_read[resolved] = real_read_bytes(self)
            return first_read[resolved]
        return b'{"tampered": true}'

    monkeypatch.setattr(Path, "read_bytes", counting_read_bytes)
    plan = plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **paths)

    for path, recorded in (
        (paths["publication_plan_path"], plan.publication_plan_sha256),
        (paths["staging_outcome_path"], plan.staging_outcome_sha256),
        (paths["metadata_outcome_path"], plan.metadata_outcome_sha256),
    ):
        resolved = path.resolve()
        assert reads[resolved] == 1, f"{path.name} was read {reads[resolved]} times"
        assert recorded == hashlib.sha256(first_read[resolved]).hexdigest()


def test_evidence_that_is_not_an_ordinary_file_is_refused(tmp_path: Path) -> None:
    """A symlink means the bytes hashed are not the bytes at the path the operator reviewed."""
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files

    paths = _files(tmp_path)
    real = paths["staging_outcome_path"]
    link = tmp_path / "staging-link.json"
    link.symlink_to(real)
    paths["staging_outcome_path"] = link

    with pytest.raises(PromotionPlanError, match="staging outcome must be an ordinary file"):
        plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **paths)


def test_the_plan_file_is_never_replaced(tmp_path: Path) -> None:
    """A promotion plan is what a human authorizes against.

    Silently replacing one lets an operator approve a digest that no longer describes the file at
    that path.
    """
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files, write_berdl_promotion_plan

    plan = plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **_files(tmp_path))
    output = tmp_path / "promotion.json"
    written = write_berdl_promotion_plan(output, plan)
    assert written.is_file()

    with pytest.raises(PromotionPlanError, match="Refusing to replace"):
        write_berdl_promotion_plan(output, plan)


def test_the_command_writes_a_plan_and_changes_nothing(tmp_path: Path) -> None:
    """The module was reachable only from its own tests until this command existed."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    paths = _files(tmp_path)
    output = tmp_path / "promotion.json"
    result = CliRunner().invoke(
        cli,
        [
            "berdl-promotion-plan",
            "--plan",
            str(paths["publication_plan_path"]),
            "--staging-outcome",
            str(paths["staging_outcome_path"]),
            "--metadata-outcome",
            str(paths["metadata_outcome_path"]),
            "--canonical-namespace",
            CANONICAL,
            "--recovery",
            RECOVERY,
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "nothing has been changed" in result.output
    assert "OUTAGE" in result.output
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["status"] == "plan-only"
    assert written["derived_rebuilds"] == ["graph_edges"]


def test_the_command_reports_a_refusal_as_a_usage_error_not_a_traceback(tmp_path: Path) -> None:
    """An operator reading a stack trace cannot tell a refused plan from a crashed one."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    paths = _files(tmp_path)
    broken = _staging_document()
    broken["destination_id"] = "somewhere-else"
    _write(paths["staging_outcome_path"], broken)

    result = CliRunner().invoke(
        cli,
        [
            "berdl-promotion-plan",
            "--plan",
            str(paths["publication_plan_path"]),
            "--staging-outcome",
            str(paths["staging_outcome_path"]),
            "--metadata-outcome",
            str(paths["metadata_outcome_path"]),
            "--canonical-namespace",
            CANONICAL,
            "--recovery",
            RECOVERY,
            "--output",
            str(tmp_path / "promotion.json"),
        ],
    )

    assert result.exit_code != 0
    assert "somewhere-else" in result.output
    assert "Traceback" not in result.output
    assert not (tmp_path / "promotion.json").exists(), "a refused plan must leave no artifact"


def test_evidence_that_is_not_valid_json_is_refused_by_name(tmp_path: Path) -> None:
    """The message has to name which of the three files is wrong.

    All three are read the same way, so "the evidence is not valid" would send the operator to
    check all of them.
    """
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files

    paths = _files(tmp_path)
    paths["metadata_outcome_path"].write_text("{not json", encoding="utf-8")

    with pytest.raises(PromotionPlanError, match="BERDL metadata outcome is not valid"):
        plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **paths)


def test_an_output_directory_that_does_not_exist_is_refused(tmp_path: Path) -> None:
    """The writer does not create the parent.

    Creating it would let a typo in the output path silently produce a plan somewhere nobody is
    looking, which for an authorization artifact is worse than failing.
    """
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files, write_berdl_promotion_plan

    plan = plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **_files(tmp_path))

    with pytest.raises(PromotionPlanError, match="parent must be an ordinary directory"):
        write_berdl_promotion_plan(tmp_path / "no-such-dir" / "promotion.json", plan)


def test_the_refusal_survives_a_file_appearing_after_the_check(tmp_path: Path, monkeypatch) -> None:
    """The pre-check alone is a race; `os.link` is what actually enforces the refusal.

    Simulated by making the existence check blind, which is what a second process writing between
    the check and the link looks like from inside this function.
    """
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files, write_berdl_promotion_plan

    plan = plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **_files(tmp_path))
    output = tmp_path / "promotion.json"
    output.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(Path, "exists", lambda self: False)

    with pytest.raises(PromotionPlanError, match="Refusing to replace"):
        write_berdl_promotion_plan(output, plan)

    assert output.read_text(encoding="utf-8") == "{}", "the existing file must be untouched"
    leftovers = [item.name for item in tmp_path.iterdir() if item.name.startswith(".promotion.json.")]
    assert not leftovers, f"the temporary file must be cleaned up, found {leftovers}"


class _RecordingSpark:
    """Records what was issued, and can be told to fail on the nth statement."""

    def __init__(self, fail_on: int | None = None) -> None:
        self.statements: list[str] = []
        self._fail_on = fail_on

    def sql(self, statement: str) -> object:
        self.statements.append(statement)
        if self._fail_on is not None and len(self.statements) == self._fail_on:
            raise RuntimeError("the engine refused this statement")
        return object()


def _executable_plan(tmp_path: Path) -> tuple[BerdlPromotionPlan, str]:
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files

    plan = plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **_files(tmp_path))
    path = _write(tmp_path / "promotion.json", json.loads(plan.model_dump_json()))
    return load_promotion_plan(path)


def test_the_drops_precede_every_replacement(tmp_path: Path) -> None:
    """Order is the decision this encodes. A derived table left standing over replaced provenance
    answers questions about rows that no longer exist, and those answers look correct."""
    plan, _digest = _executable_plan(tmp_path)

    steps = [step for step, _table, _statement in promotion_statements(plan)]

    assert "drop" in steps, steps
    assert max(index for index, step in enumerate(steps) if step == "drop") < min(
        index for index, step in enumerate(steps) if step != "drop"
    ), steps


def test_promotion_refuses_a_digest_that_does_not_match_the_plan(tmp_path: Path) -> None:
    """The digest binds the run to the exact plan a human read, not to one that resembles it."""
    plan, digest = _executable_plan(tmp_path)
    spark = _RecordingSpark()

    with pytest.raises(PromotionRefused, match="--authorize-plan-sha256"):
        execute_promotion(
            spark,
            plan,
            plan_sha256=digest,
            authorize_plan_sha256="0" * 64,
            authorize_canonical_namespace=CANONICAL,
            authorize_destination_id=plan.destination_id,
        )

    assert spark.statements == []


def test_promotion_refuses_a_namespace_that_is_not_the_one_the_plan_promotes_into(tmp_path: Path) -> None:
    """A digest is copied from a previous command; a namespace is typed. This catches the typing."""
    plan, digest = _executable_plan(tmp_path)
    spark = _RecordingSpark()

    with pytest.raises(PromotionRefused, match="nmdc.somewhere_else"):
        execute_promotion(
            spark,
            plan,
            plan_sha256=digest,
            authorize_plan_sha256=digest,
            authorize_canonical_namespace="nmdc.somewhere_else",
            authorize_destination_id=plan.destination_id,
        )

    assert spark.statements == []


def test_promotion_runs_exactly_the_statements_the_plan_describes(tmp_path: Path) -> None:
    """What runs and what an operator authorized cannot diverge, so they are compared directly."""
    plan, digest = _executable_plan(tmp_path)
    spark = _RecordingSpark()

    performed = execute_promotion(
        spark,
        plan,
        plan_sha256=digest,
        authorize_plan_sha256=digest,
        authorize_canonical_namespace=CANONICAL,
        authorize_destination_id=plan.destination_id,
    )

    expected = [statement for _step, _table, statement in promotion_statements(plan)]
    assert spark.statements == expected
    assert performed == expected


def test_a_promotion_that_stops_part_way_names_what_already_ran(tmp_path: Path) -> None:
    """The operator's first question is which objects moved, so the refusal answers it."""
    plan, digest = _executable_plan(tmp_path)
    spark = _RecordingSpark(fail_on=2)
    expected = [statement for _step, _table, statement in promotion_statements(plan)]
    assert len(expected) >= 2, "this test needs a plan with at least two statements"

    with pytest.raises(PromotionRefused) as refusal:
        execute_promotion(
            spark,
            plan,
            plan_sha256=digest,
            authorize_plan_sha256=digest,
            authorize_canonical_namespace=CANONICAL,
            authorize_destination_id=plan.destination_id,
        )

    message = str(refusal.value)
    assert expected[0] in message, message
    assert "1 statement(s) had already run" in message, message
    # The one that failed is named as the failure, not as something that ran.
    assert f"failed during {promotion_statements(plan)[1][0]}" in message, message


def _promotion_plan_file(tmp_path: Path) -> Path:
    from nmdc_lakehouse.berdl_promotion import plan_berdl_promotion_from_files

    plan = plan_berdl_promotion_from_files(canonical_namespace=CANONICAL, recovery=RECOVERY, **_files(tmp_path))
    return _write(tmp_path / "promotion.json", json.loads(plan.model_dump_json()))


def test_the_promote_command_previews_without_both_authorizations(tmp_path: Path) -> None:
    """Half an authorization is not one. Neither flag alone reaches the destructive path."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    path = _promotion_plan_file(tmp_path)
    _plan, digest = load_promotion_plan(path)
    runner = CliRunner()

    for extra in (
        [],
        ["--authorize-plan-sha256", digest],
        ["--authorize-canonical-namespace", CANONICAL],
    ):
        result = runner.invoke(cli, ["berdl-promote", str(path), "--ingest-checkout", str(tmp_path), *extra])

        assert result.exit_code == 0, result.output
        assert "nothing has been changed" in result.output, extra
        # The statements are shown so a reviewer reads what would run, and the digest is shown
        # because it is what the next invocation has to name.
        assert digest in result.output, extra
        assert "DROP TABLE IF EXISTS" in result.output, extra


def test_the_promote_command_executes_and_refuses_to_call_it_verified(tmp_path: Path, monkeypatch) -> None:
    """The plan's last step is a read-back this command does not perform.

    Reporting only a statement count lets the output stand in for a verification nobody has run,
    which is the failure mode where a promotion is announced complete and is not.
    """
    from click.testing import CliRunner

    import nmdc_lakehouse.derived_tables as derived_tables
    from nmdc_lakehouse.cli import cli

    path = _promotion_plan_file(tmp_path)
    plan, digest = load_promotion_plan(path)
    spark = _RecordingSpark()
    monkeypatch.setattr(derived_tables, "spark_session", lambda _checkout: spark)

    result = CliRunner().invoke(
        cli,
        [
            "berdl-promote",
            str(path),
            "--ingest-checkout",
            str(tmp_path),
            "--authorize-plan-sha256",
            digest,
            "--authorize-canonical-namespace",
            CANONICAL,
            "--authorize-destination-id",
            plan.destination_id,
        ],
    )

    assert result.exit_code == 0, result.output
    assert spark.statements == [statement for _step, _table, statement in promotion_statements(plan)]
    assert "NOT VERIFIED" in result.output, result.output
    assert "rerun with all three --authorize- options" not in result.output, result.output
    for table in plan.derived_rebuilds:
        assert table in result.output, result.output


def test_a_plan_naming_a_table_that_is_not_an_identifier_is_refused(tmp_path: Path) -> None:
    """The plan is JSON on disk, and its digest is of the file as it is, not of one anyone vouched
    for. A name carrying a semicolon becomes extra statements inside a DROP."""
    path = _promotion_plan_file(tmp_path)
    document = json.loads(path.read_text())
    document["derived_rebuilds"] = ["graph_edges; DROP TABLE nmdc.metadata.biosample_set"]
    tampered = _write(tmp_path / "tampered.json", document)

    with pytest.raises(PromotionPlanError, match="not a plain table identifier"):
        load_promotion_plan(tampered)


def test_a_plan_whose_operation_names_a_bad_table_is_refused(tmp_path: Path) -> None:
    """Both lists of table names reach SQL, so both are checked. Fixing one would leave the other."""
    path = _promotion_plan_file(tmp_path)
    document = json.loads(path.read_text())
    document["operations"][0]["table"] = "biosample_set`; DROP TABLE x"
    tampered = _write(tmp_path / "tampered-operation.json", document)

    with pytest.raises(PromotionPlanError, match="not a plain table identifier"):
        load_promotion_plan(tampered)


def test_the_plan_says_promotion_does_not_carry_table_metadata(tmp_path: Path) -> None:
    """The plan consumes a metadata outcome, which is evidence about staging, not about here.

    `CREATE OR REPLACE TABLE ... AS SELECT` builds a table from a query result, and a table
    comment and TBLPROPERTIES are not part of one. An operator reading a plan that cites a
    verified metadata outcome would otherwise assume promotion carries it.
    """
    plan, _digest = _executable_plan(tmp_path)

    rendered = render_promotion_plan(plan)

    assert "re-apply table comments and properties" in rendered, rendered


def test_the_promote_command_says_the_metadata_did_not_come_with_it(tmp_path: Path, monkeypatch) -> None:
    """Same claim, at the point where someone would otherwise call the promotion finished."""
    from click.testing import CliRunner

    import nmdc_lakehouse.derived_tables as derived_tables
    from nmdc_lakehouse.cli import cli

    path = _promotion_plan_file(tmp_path)
    plan, digest = load_promotion_plan(path)
    monkeypatch.setattr(derived_tables, "spark_session", lambda _checkout: _RecordingSpark())

    result = CliRunner().invoke(
        cli,
        [
            "berdl-promote",
            str(path),
            "--ingest-checkout",
            str(tmp_path),
            "--authorize-plan-sha256",
            digest,
            "--authorize-canonical-namespace",
            CANONICAL,
            "--authorize-destination-id",
            plan.destination_id,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "METADATA NOT CARRIED" in result.output, result.output
    assert "berdl-apply-metadata" in result.output, result.output


def test_promotion_refuses_a_destination_the_plan_was_not_decided_against(tmp_path: Path) -> None:
    """Nothing here can verify which deployment a session reaches, so the operator asserts it.

    The runtime comes from a checkout named at execution time and `spark_session` establishes only
    that the helper was imported from it, not what it is configured to talk to. The same namespace
    name exists in more than one deployment.
    """
    plan, digest = _executable_plan(tmp_path)
    spark = _RecordingSpark()

    with pytest.raises(PromotionRefused, match="nmdc-somewhere-else"):
        execute_promotion(
            spark,
            plan,
            plan_sha256=digest,
            authorize_plan_sha256=digest,
            authorize_canonical_namespace=CANONICAL,
            authorize_destination_id="nmdc-somewhere-else",
        )

    assert spark.statements == []
