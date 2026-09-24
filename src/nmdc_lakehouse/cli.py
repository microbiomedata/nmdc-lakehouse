"""Click-based CLI entry point for nmdc-lakehouse.

This is the default "job runner". It can be replaced or complemented later
by an external orchestrator (Dagster, Prefect, Snakemake, ...) without
changing the source / transform / sink modules.
"""

from __future__ import annotations

import logging
import shlex
import time
from datetime import UTC, datetime
from pathlib import Path

import click

from nmdc_lakehouse.service_doctor import SERVICE_CHECKS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(package_name="nmdc-lakehouse")
def cli() -> None:
    """nmdc-lakehouse: NMDC -> lakehouse ETL."""


@cli.command("list-jobs")
def list_jobs() -> None:
    """List all ETL jobs registered with the runner."""
    import nmdc_lakehouse.jobs  # noqa: F401 -- register built-in jobs only when needed
    from nmdc_lakehouse.jobs.registry import list_names

    for name in list_names():
        click.echo(name)


@cli.command("doctor")
@click.option(
    "--service-check",
    type=click.Choice(SERVICE_CHECKS),
    multiple=True,
    help="Run an explicit optional-service check; repeat to combine checks.",
)
@click.pass_context
def doctor(context: click.Context, service_check: tuple[str, ...]) -> None:
    """Check local readiness; service checks are explicit opt-ins."""
    from nmdc_lakehouse.doctor import run_doctor

    report = run_doctor(service_checks=service_check)
    for check in report.checks:
        click.echo(f"[{check.status.value}] {check.name}: {check.summary}")
        if check.remediation:
            click.echo(f"       remedy: {check.remediation}")
    context.exit(report.exit_code)


@cli.command("clean-parquet")
@click.option(
    "--root",
    type=click.Path(path_type=Path),
    default=None,
    help="Output root to inspect; defaults to LAKEHOUSE_ROOT.",
)
@click.option("--delete", is_flag=True, help="Delete the previewed files; preview is the default.")
def clean_parquet(root: Path | None, delete: bool) -> None:
    """Preview or delete recognized local metadata Parquet products."""
    from nmdc_lakehouse.cleanup import (
        UnsafeCleanupRoot,
        apply_cleanup,
        find_project_root,
        metadata_output_names,
        plan_metadata_parquet_cleanup,
    )
    from nmdc_lakehouse.config import LakehouseSettings

    output_root = root if root is not None else LakehouseSettings().root
    try:
        project_root = find_project_root(Path.cwd())
        plan = plan_metadata_parquet_cleanup(
            output_root,
            project_root=project_root,
            generated_names=metadata_output_names(),
        )
    except UnsafeCleanupRoot as error:
        raise click.ClickException(str(error)) from error

    action = "Removing" if delete else "Would remove"
    for target in plan.targets:
        click.echo(f"{action}: {target.relative_to(plan.root)}")
    if delete:
        try:
            removed = apply_cleanup(plan)
        except UnsafeCleanupRoot as error:
            raise click.ClickException(str(error)) from error
        click.echo(f"Removed {removed} recognized metadata Parquet file(s).")
    else:
        click.echo(f"Previewed {len(plan.targets)} recognized metadata Parquet file(s); no files were deleted.")
        if plan.targets:
            click.echo("Rerun with --delete to remove exactly these files.")


@cli.command("create-snapshot-manifest")
@click.argument("root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--metrics",
    "metrics_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Successful all-collections metrics JSON inside ROOT.",
)
@click.option(
    "--source-label",
    required=True,
    envvar="LAKEHOUSE_SOURCE_LABEL",
    help="Sanitized logical source environment, such as nmdc-production.",
)
def create_snapshot_manifest(root: Path, metrics_path: Path, source_label: str) -> None:
    """Create the completion manifest for one successful full snapshot."""
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError, build_manifest, write_manifest

    try:
        manifest = build_manifest(root, metrics_path, source_label)
        destination = write_manifest(root, manifest)
    except SnapshotManifestError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"snapshot_id={manifest.snapshot_id}")
    click.echo(f"artifacts={len(manifest.artifacts)}")
    click.echo(f"manifest={destination}")


@cli.command("validate-snapshot")
@click.argument("root", type=click.Path(path_type=Path, file_okay=False))
def validate_snapshot_command(root: Path) -> None:
    """Validate a manifested snapshot entirely offline."""
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError, validate_snapshot

    try:
        manifest = validate_snapshot(root)
    except SnapshotManifestError as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Validated {manifest.snapshot_id}: {len(manifest.artifacts)} Parquet artifact(s).")


@cli.command("validate-target-rows")
@click.argument("root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--output",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="New JSON evidence path outside the immutable snapshot.",
)
@click.option(
    "--mode",
    "requested_mode",
    type=click.Choice(["bounded", "full"]),
    default="bounded",
    show_default=True,
    help="Validate all rows, or all small tables plus deterministic samples.",
)
@click.option(
    "--full-table-max-rows",
    type=click.IntRange(min=0),
    default=10_000,
    show_default=True,
    help="In bounded mode, validate every row in tables no larger than this.",
)
@click.option(
    "--sample-rows",
    type=click.IntRange(min=1),
    default=100,
    show_default=True,
    help="In bounded mode, deterministically select this many rows from each larger table.",
)
def validate_target_rows_command(
    root: Path,
    output: Path,
    requested_mode: str,
    full_table_max_rows: int,
    sample_rows: int,
) -> None:
    """Validate manifested Parquet rows against the matching packaged target schema."""
    from typing import Literal, cast

    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError
    from nmdc_lakehouse.target_validation import (
        TargetValidationError,
        validate_target_snapshot,
        write_target_validation_report,
    )

    try:
        report = validate_target_snapshot(
            root,
            requested_mode=cast(Literal["bounded", "full"], requested_mode),
            full_table_max_rows=full_table_max_rows,
            sample_rows=sample_rows,
        )
        destination = write_target_validation_report(output, report, snapshot_root=root)
    except (SnapshotManifestError, TargetValidationError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"status={report.status}")
    click.echo(f"snapshot_id={report.snapshot_id}")
    click.echo(f"selected_rows={report.selected_rows}/{report.eligible_rows}")
    click.echo(f"invalid_rows={report.invalid_rows}")
    click.echo(f"report={destination}")
    if report.status != "success":
        raise click.ClickException("Target LinkML validation found invalid rows; inspect the sanitized report.")


@cli.command("snapshot-manifest-schema")
def snapshot_manifest_schema_command() -> None:
    """Print the current snapshot-manifest JSON Schema."""
    import json

    from nmdc_lakehouse.snapshot_manifest import manifest_json_schema

    click.echo(json.dumps(manifest_json_schema(), indent=2, sort_keys=True))


@cli.command("publication-plan-schema")
@click.argument("document", type=click.Choice(["inventory", "policy", "plan"]))
def publication_plan_schema_command(document: str) -> None:
    """Print a publication inventory, policy, or plan JSON Schema."""
    import json
    from typing import Literal, cast

    from nmdc_lakehouse.publication_plan import publication_json_schema

    selected = cast(Literal["inventory", "policy", "plan"], document)
    click.echo(json.dumps(publication_json_schema(selected), indent=2, sort_keys=True))


@cli.command("metadata-application-plan-schema")
def metadata_application_plan_schema_command() -> None:
    """Print the metadata application plan JSON Schema."""
    import json

    from nmdc_lakehouse.metadata_application import metadata_application_json_schema

    click.echo(json.dumps(metadata_application_json_schema(), indent=2, sort_keys=True))


@cli.command("plan-publication")
@click.argument("root", type=click.Path(path_type=Path, file_okay=False))
@click.argument("configuration", type=click.Path(path_type=Path, dir_okay=False))
def plan_publication_command(root: Path, configuration: Path) -> None:
    """Build disposition, metadata and staging plans from a prepared directory."""
    from pydantic import ValidationError

    from nmdc_lakehouse.berdl_staging import render_berdl_staging_plan
    from nmdc_lakehouse.publication_planning import plan_publication
    from nmdc_lakehouse.publication_prepare import file_digest

    try:
        plan = plan_publication(root, configuration)
    except ValidationError as error:
        raise click.ClickException("Invalid planning configuration or publication evidence.") from error
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    output = root.expanduser().resolve() / "evidence/berdl-staging-plan.json"
    click.echo(render_berdl_staging_plan(plan))
    click.echo(f"plan={output}\nplan_sha256={file_digest(output)}", err=True)


@cli.command("stage-publication")
@click.argument("root", type=click.Path(path_type=Path, file_okay=False))
@click.option("--authorize-snapshot", help="Exact snapshot ID approved for this invocation.")
@click.option("--authorize-plan-sha256", help="Exact SHA-256 of the reviewed staging plan.")
@click.option("--execute", is_flag=True, help="Stage or retry metadata; otherwise only preview.")
def stage_publication_command(
    root: Path, authorize_snapshot: str | None, authorize_plan_sha256: str | None, execute: bool
) -> None:
    """Stage a prepared publication, resuming metadata alone when data is verified."""
    import json

    from pydantic import ValidationError

    from nmdc_lakehouse.publication_staging import stage_publication

    try:
        result = stage_publication(
            root,
            authorize_snapshot=authorize_snapshot,
            authorize_plan_sha256=authorize_plan_sha256,
            execute=execute,
        )
    except ValidationError as error:
        raise click.ClickException("Invalid publication evidence.") from error
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(result, indent=2, sort_keys=True))


@cli.command("publication-status")
@click.argument("root", type=click.Path(path_type=Path, file_okay=False))
def publication_status_command(root: Path) -> None:
    """Verify local publication evidence and show the next action; no service access."""
    import json

    from pydantic import ValidationError

    from nmdc_lakehouse.publication_staging import publication_status

    try:
        result = publication_status(root)
    except ValidationError as error:
        raise click.ClickException("Invalid publication evidence.") from error
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(result, indent=2, sort_keys=True))


@cli.command("derive-provenance")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.argument("output_root", type=click.Path(path_type=Path, file_okay=False))
@click.option("--max-depth", type=click.IntRange(min=1), default=15, show_default=True)
def derive_provenance_command(snapshot_root: Path, output_root: Path, max_depth: int) -> None:
    """Build described graph and biosample/workflow Parquet from a local snapshot."""
    from nmdc_lakehouse.derived_tables import DerivedTableError
    from nmdc_lakehouse.local_provenance import derive_provenance
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError

    try:
        manifest = derive_provenance(
            snapshot_root, output_root, max_depth=max_depth, progress=lambda message: click.echo(message, err=True)
        )
    except (DerivedTableError, SnapshotManifestError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"snapshot_id={manifest.snapshot_id}")
    click.echo(f"parent_snapshot_id={manifest.parent_snapshot_id}")
    for artifact in manifest.artifacts:
        click.echo(f"{artifact.table}={artifact.rows}")
    click.echo(f"manifest={output_root.expanduser().resolve() / 'snapshot-manifest.json'}")


@cli.command("compare-provenance-queries")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.argument("derived_root", type=click.Path(path_type=Path, file_okay=False))
@click.argument("output", type=click.Path(path_type=Path, dir_okay=False))
@click.option("--repeats", type=click.IntRange(min=1), default=3, show_default=True)
def compare_provenance_queries_command(snapshot_root: Path, derived_root: Path, output: Path, repeats: int) -> None:
    """Check answer equality and time local queries with and without derived tables."""
    import json

    from nmdc_lakehouse.derived_tables import DerivedTableError
    from nmdc_lakehouse.provenance_queries import compare_provenance_queries
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError

    output = output.expanduser()
    if (
        output.exists()
        or output.is_symlink()
        or any(output.resolve().is_relative_to(root.expanduser().resolve()) for root in (snapshot_root, derived_root))
    ):
        raise click.ClickException("Report must be a new file outside both snapshots.")
    try:
        report = compare_provenance_queries(snapshot_root.expanduser(), derived_root.expanduser(), repeats=repeats)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
    except (DerivedTableError, SnapshotManifestError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"equivalent_pairs_and_hops={report['equivalent_pairs_and_hops']}")
    click.echo(f"pairs_missed_by_direct_join={report['pairs_missed_by_direct_join']}")
    for name, seconds in report["median_seconds"].items():
        click.echo(f"{name}_median_seconds={seconds:.6f}")
    click.echo(f"report={output.resolve()}")


@cli.command("berdl-promotion-plan")
@click.argument("metadata_root", type=click.Path(path_type=Path, file_okay=False))
@click.argument("derived_root", type=click.Path(path_type=Path, file_okay=False))
@click.argument("output", type=click.Path(path_type=Path, dir_okay=False))
@click.option("--ingest-checkout", type=click.Path(path_type=Path, file_okay=False), required=True)
@click.option("--recovery", required=True, help="Reviewed manual response to partial canonical changes.")
def berdl_promotion_plan_command(
    metadata_root: Path, derived_root: Path, output: Path, ingest_checkout: Path, recovery: str
) -> None:
    """Read both staged snapshots and current catalog state into one reviewable plan."""
    from nmdc_lakehouse.berdl_promotion import plan_promotion, render_promotion_plan
    from nmdc_lakehouse.publication_prepare import file_digest

    try:
        plan = plan_promotion(metadata_root, derived_root, output, ingest_checkout=ingest_checkout, recovery=recovery)
    except (ValueError, OSError) as error:
        raise click.ClickException("Promotion preview failed; inspect the private log if one was created.") from error
    click.echo(render_promotion_plan(plan))
    click.echo(f"plan={output.resolve()}")
    click.echo(f"plan_sha256={file_digest(output)}")
    click.echo(f"destination_id={plan.sources[0].destination_id}")


@cli.command("berdl-promote")
@click.argument("plan_path", type=click.Path(path_type=Path, dir_okay=False))
@click.option("--authorize-plan-sha256", help="Exact SHA-256 of the reviewed combined plan.")
@click.option("--authorize-canonical-namespace", help="Exact canonical namespace from the reviewed plan.")
@click.option("--authorize-destination-id", help="Exact destination identity from the reviewed plan.")
def berdl_promote_command(
    plan_path: Path,
    authorize_plan_sha256: str | None,
    authorize_canonical_namespace: str | None,
    authorize_destination_id: str | None,
) -> None:
    """Preview the saved plan; all three authorizations enable execution and read-back."""
    import json

    from nmdc_lakehouse.berdl_promotion import (
        PromotionPlanError,
        execute_promotion,
        load_promotion_plan,
        render_promotion_plan,
    )

    try:
        plan, digest = load_promotion_plan(plan_path)
        click.echo(render_promotion_plan(plan))
        click.echo(f"plan_sha256={digest}")
        click.echo(f"destination_id={plan.sources[0].destination_id}")
        if not all((authorize_plan_sha256, authorize_canonical_namespace, authorize_destination_id)):
            click.echo("Preview only. Supply all three --authorize- options after exact-plan review to execute.")
            return
        assert authorize_plan_sha256 and authorize_canonical_namespace and authorize_destination_id
        result = execute_promotion(
            plan_path,
            authorize_plan_sha256=authorize_plan_sha256,
            authorize_canonical_namespace=authorize_canonical_namespace,
            authorize_destination_id=authorize_destination_id,
        )
    except PromotionPlanError as error:
        raise click.ClickException(str(error)) from error
    except (ValueError, OSError) as error:
        raise click.ClickException(
            "Promotion refused or incomplete; inspect the execution journal and private log if present. "
            "No automatic recovery was attempted."
        ) from error
    click.echo(json.dumps(result, sort_keys=True, indent=2))


@cli.command("berdl-promotion-probe")
@click.argument("tenant")
@click.argument("source_namespace")
@click.argument("destination_namespace")
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option("--authorize-plan-sha256", help="Exact SHA-256 of the reviewed probe plan.")
@click.option(
    "--execute-probe",
    is_flag=True,
    help="Create and mutate disposable probe tables; the default only previews the plan.",
)
def berdl_promotion_probe_command(
    tenant: str,
    source_namespace: str,
    destination_namespace: str,
    output: Path,
    authorize_plan_sha256: str | None,
    execute_probe: bool,
) -> None:
    """Establish which BERDL promotion and recovery operations exist, on disposable tables."""
    from nmdc_lakehouse.berdl_promotion_probe import (
        BerdlPromotionProbeError,
        build_promotion_probe_plan,
        plan_sha256,
        render_promotion_probe,
        run_promotion_probe,
        write_promotion_probe_outcome,
    )

    try:
        plan = build_promotion_probe_plan(
            tenant=tenant,
            source_namespace=source_namespace,
            destination_namespace=destination_namespace,
        )
        digest = plan_sha256(plan)
        if not execute_probe:
            click.echo(render_promotion_probe(plan))
            click.echo(f"plan_sha256={digest}", err=True)
            return
        if authorize_plan_sha256 != digest:
            raise BerdlPromotionProbeError("Execution requires the exact reviewed probe plan SHA-256.")
        outcome = run_promotion_probe(plan, authorize_plan_sha256=digest)
        destination = write_promotion_probe_outcome(output, outcome)
    except (BerdlPromotionProbeError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_promotion_probe(outcome))
    click.echo(f"outcome={destination.resolve()}", err=True)


@cli.command("berdl-doctor")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--beril-checkout",
    type=click.Path(path_type=Path, file_okay=False),
    envvar="BERIL_CHECKOUT",
    required=False,
    default=None,
    help=(
        "BERIL-research-observatory checkout to inspect. Optional: the maintained pod-resident "
        "path does not use BERIL, and its checks are reported as skipped when this is absent."
    ),
)
@click.option(
    "--service-check",
    type=click.Choice(["berdl-proxy"]),
    multiple=True,
    help="Run an explicit bounded local proxy check.",
)
@click.pass_context
def berdl_doctor(
    context: click.Context,
    snapshot_root: Path,
    beril_checkout: Path | None,
    service_check: tuple[str, ...],
) -> None:
    """Check BERDL publication readiness without changing it."""
    from nmdc_lakehouse.berdl_doctor import run_berdl_doctor

    report = run_berdl_doctor(
        snapshot_root,
        project_root=Path.cwd(),
        checkout=beril_checkout,
        service_checks=service_check,
    )
    for check in report.checks:
        click.echo(f"[{check.status.value}] {check.name}: {check.summary}")
        if check.remediation:
            click.echo(f"       remedy: {check.remediation}")
    context.exit(report.exit_code)


@cli.command("prepare-publication")
@click.argument("configuration", type=click.Path(path_type=Path, dir_okay=False))
@click.argument("output", type=click.Path(path_type=Path, file_okay=False))
def prepare_publication_command(configuration: Path, output: Path) -> None:
    """Prepare a snapshot, full validation, and reviewed metadata together."""
    import json

    from pydantic import ValidationError

    from nmdc_lakehouse.publication_prepare import PreparationConfig, prepare_publication

    try:
        receipt = prepare_publication(configuration, output)
    except ValidationError as error:
        schema = PreparationConfig.model_json_schema()
        fields = set(schema["properties"])
        for definition in schema.get("$defs", {}).values():
            fields.update(definition.get("properties", {}))
        details = []
        for item in error.errors(include_input=False, include_context=False, include_url=False):
            parts = list(item["loc"])
            if item["type"] == "extra_forbidden" and parts:
                parts[-1] = "<item>"
            elif "properties" in parts[:-1]:
                parts[parts.index("properties") + 1 :] = ["<item>"]
            location = ".".join(str(part) if isinstance(part, int) or part in fields else "<item>" for part in parts)
            # Free-form validator messages and dictionary keys can contain submitted values.
            details.append(f"{location or '<document>'}: {item['type'].replace('_', ' ')}")
        raise click.ClickException("Invalid preparation configuration or evidence:\n" + "\n".join(details)) from error
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(json.dumps(receipt, indent=2, sort_keys=True))
    click.echo(f"prepared_directory={output.expanduser().resolve()}", err=True)


@cli.command("metadata-bundle-schema")
@click.argument("document", type=click.Choice(["profile", "bundle"]))
def metadata_bundle_schema_command(document: str) -> None:
    """Print a metadata profile or bundle JSON Schema."""
    import json
    from typing import Literal, cast

    from nmdc_lakehouse.metadata_bundle import metadata_json_schema

    selected = cast(Literal["profile", "bundle"], document)
    click.echo(json.dumps(metadata_json_schema(selected), indent=2, sort_keys=True))


@cli.command("run-job")
@click.argument("job_name")
@click.option("--dry-run", is_flag=True, help="Plan the job but do not write output.")
@click.option(
    "--drop-empty-cols",
    is_flag=True,
    envvar="LAKEHOUSE_DROP_EMPTY_COLS",
    help="Remove all-null columns from the output Parquet file.",
)
@click.option(
    "--skip",
    "skip",
    multiple=True,
    help="Collection to skip (repeatable). Only honored by 'all-collections'.",
)
@click.option(
    "--metrics",
    "metrics_path",
    type=click.Path(path_type=Path, dir_okay=False),
    envvar="LAKEHOUSE_METRICS_PATH",
    help="Write an atomic JSON performance/resource record to this local path.",
)
def run_job(
    job_name: str,
    dry_run: bool,
    drop_empty_cols: bool,
    skip: tuple[str, ...],
    metrics_path: Path | None,
) -> None:
    """Run a named ETL job from the registry."""
    import os

    import nmdc_lakehouse.jobs  # noqa: F401 -- register built-in jobs only when needed
    from nmdc_lakehouse.config import LakehouseSettings
    from nmdc_lakehouse.jobs.registry import get
    from nmdc_lakehouse.metrics import failure_record, stamp_result, success_record, write_record

    if drop_empty_cols:
        os.environ["LAKEHOUSE_DROP_EMPTY_COLS"] = "true"
    if skip:
        os.environ["LAKEHOUSE_SKIP_COLLECTIONS"] = ",".join(skip)
    applied_skips: tuple[str, ...] = ()
    started_at = datetime.now(UTC).isoformat()
    t0 = time.monotonic()
    try:
        job = get(job_name)
        applied_skips = tuple(sorted(getattr(job, "skip", ())))
        result = job.run(dry_run=dry_run)
        configured_output_root = getattr(job, "out_root", None)
        output_root = Path(configured_output_root) if configured_output_root is not None else LakehouseSettings().root
        stamp_result(
            result,
            output_root=output_root,
            started_at=started_at,
            finished_at=datetime.now(UTC).isoformat(),
            elapsed_seconds=time.monotonic() - t0,
        )
        if metrics_path is not None:
            write_record(metrics_path, success_record(result, skipped_collections=applied_skips, dry_run=dry_run))
    except (Exception, KeyboardInterrupt) as error:
        if metrics_path is not None:
            try:
                write_record(
                    metrics_path,
                    failure_record(
                        job_name=job_name,
                        started_at=started_at,
                        finished_at=datetime.now(UTC).isoformat(),
                        elapsed_seconds=time.monotonic() - t0,
                        error=error,
                        skipped_collections=applied_skips,
                        dry_run=dry_run,
                    ),
                )
            except Exception:
                logger.exception("The failed-run metrics record could not be written.")
        raise
    click.echo(f"rows_read={result.rows_read}")
    click.echo(f"rows_written={result.rows_written}")
    if result.tables_written:
        click.echo(f"tables={', '.join(result.tables_written)}")


@cli.command("source-preflight")
def source_preflight_command() -> None:
    """Check MongoDB migration compatibility with the selected source/flat pair."""
    from nmdc_lakehouse.config import MongoSettings
    from nmdc_lakehouse.source_preflight import SourceSchemaError, assert_mongodb_source_aligned
    from nmdc_lakehouse.target_validation import TargetValidationError

    try:
        selected = assert_mongodb_source_aligned(MongoSettings().uri)
    except (SourceSchemaError, TargetValidationError) as error:
        raise click.ClickException(str(error)) from None
    click.echo(
        f"MongoDB migration metadata is compatible with the installed source/flat pair for nmdc-schema {selected}."
    )


@cli.command("data-object-manifest")
@click.option("--type", "types", multiple=True, required=True, help="data_object_type to fetch. Repeatable.")
@click.option(
    "--data-object-set",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Snapshot Parquet to read. Needs no pod. One of this and --ingest-checkout is required.",
)
@click.option(
    "--ingest-checkout", type=click.Path(path_type=Path, file_okay=False), help="Read a live catalog instead."
)
@click.option("--namespace", default="nmdc.metadata", show_default=True, help="Catalog namespace for the live read.")
@click.option(
    "--host",
    help=("Restrict to URLs served by this host, given with or without a scheme. No restriction by default."),
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
def data_object_manifest_command(
    types: tuple[str, ...],
    data_object_set: Path | None,
    ingest_checkout: Path | None,
    namespace: str,
    host: str | None,
    output: Path,
) -> None:
    """Build the download manifest for one or more data object types.

    This is the fetch stage the notebook triples shared, and it only builds the manifest;
    downloading is `scripts/download_to_cache.py`, which reads what this writes.

    Types are resolved against nmdc-schema, so a typo fails here rather than producing an empty
    manifest that downloads nothing and reports success. An empty result is refused for the same
    reason, and what was dropped on the way is printed rather than only logged.
    """
    from nmdc_lakehouse.data_object_manifest import (
        DataObjectManifestError,
        build_manifest,
        read_data_object_set,
        read_data_object_set_from_spark,
        write_manifest,
    )

    if (data_object_set is None) == (ingest_checkout is None):
        raise click.UsageError("Name exactly one source: --data-object-set or --ingest-checkout.")
    # Refused before the source is read. Writing the manifest over the snapshot would truncate the
    # Parquet this just read from and replace it with a CSV, which is unrecoverable if that
    # snapshot is the only copy.
    if data_object_set is not None and output.expanduser().resolve() == data_object_set.expanduser().resolve():
        raise click.UsageError("--output would overwrite --data-object-set. Name a different path.")

    source_hint = str(data_object_set) if data_object_set is not None else f"{namespace}.data_object_set"
    try:
        if data_object_set is not None:
            records = read_data_object_set(data_object_set)
            source = str(data_object_set)
        else:
            from nmdc_lakehouse.derived_tables import spark_session

            if ingest_checkout is None:  # pragma: no cover - the exclusivity check above forbids it
                raise click.UsageError("Name exactly one source: --data-object-set or --ingest-checkout.")
            records = read_data_object_set_from_spark(spark_session(ingest_checkout), namespace, types=list(types))
            source = f"{namespace}.data_object_set"
        outcome = build_manifest(records, list(types), host=host)
    except (DataObjectManifestError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    except OSError as error:
        # Reading, not writing. The handler used to cover both and reported a source-side failure
        # as "Writing the manifest failed", which sends the reader to the wrong file.
        raise click.ClickException(f"Reading {source_hint} failed: {error}") from error

    try:
        written = write_manifest(outcome, output).resolve()
    except OSError as error:
        # A full disk or an unwritable destination is an ordinary outcome here, not a defect, and
        # a traceback for one reads as the command breaking rather than the filesystem refusing.
        raise click.ClickException(f"Writing the manifest failed: {error}") from error

    click.echo(f"manifest from {source}")
    for name, count in sorted(outcome.per_type.items()):
        click.echo(f"  {count:>8,}  {name}")
    click.echo(f"  {outcome.total:>8,}  total, {outcome.total_bytes / 1024**3:,.1f} GiB")
    dropped = (
        f"{outcome.dropped_no_url} no URL, {outcome.dropped_not_fetchable} not http(s), "
        f"{outcome.dropped_other_host} other host, {outcome.dropped_duplicate} duplicate, "
        f"{outcome.dropped_zero_byte} zero-byte"
    )
    click.echo(f"  dropped: {dropped}")
    click.echo(f"  written: {written}")
    click.echo("")
    click.echo("  download it with:")
    # Quoted, because a checkout or output path containing a space makes the pasted command run
    # as different arguments rather than fail, which is the worse of the two outcomes.
    click.echo(f"    uv run python {shlex.quote(_downloader_path())} --manifest {shlex.quote(str(written))} \\")
    click.echo("        --cache-dir PATH_TO_CACHE --workers 8")


def _downloader_path() -> str:
    """Where `scripts/download_to_cache.py` is, as an absolute path when it can be found.

    The notebooks walked up from the working directory to find it, and printed the resolved path.
    Printing a relative one instead means the advertised command fails from anywhere but the
    checkout root, which includes `notebooks/`, where its readers are. Resolved from this module
    rather than the working directory, because that is where the checkout actually is.
    """
    candidate = Path(__file__).resolve().parents[2] / "scripts" / "download_to_cache.py"
    # An installed package has no `scripts/` beside it. Saying so beats printing a path that does
    # not exist and looks authoritative because it is absolute.
    return str(candidate) if candidate.is_file() else "PATH_TO_CHECKOUT/scripts/download_to_cache.py"


def _read_run_ids(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


@cli.command("feature-plan")
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
def feature_plan_command(output: Path) -> None:
    """Choose one annotation run per input from the public NMDC API and record its files.

    Reads `workflow_execution_set` and `data_object_set`; writes nothing but OUTPUT.
    """
    import json

    from nmdc_lakehouse.feature_tables import fetch_inventory, plan_runs, plan_to_json

    inventory = fetch_inventory()
    plan = plan_runs(inventory["runs"], inventory["data_objects"], inventory.get("assemblies", ()))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"summary": plan.summary(), **plan_to_json(plan)}, indent=1))
    click.echo(json.dumps(plan.summary(), indent=2))


@cli.command("feature-sample")
@click.argument("plan_path", type=click.Path(path_type=Path, dir_okay=False, exists=True))
@click.option("--count", type=int, default=50, show_default=True)
@click.option("--max-run-gib", type=float, default=None, help="Skip runs larger than this. Biases toward small runs.")
@click.option("--seed", type=int, default=0, show_default=True)
@click.option("--runs", "runs_path", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option("--manifest", type=click.Path(path_type=Path, dir_okay=False), required=True)
def feature_sample_command(
    plan_path: Path, count: int, max_run_gib: float | None, seed: int, runs_path: Path, manifest: Path
) -> None:
    """Pick runs spread across run type and pipeline version, and write their download manifest."""
    import json

    from nmdc_lakehouse.feature_tables import CHECK_TYPES, plan_from_json, sample_runs, write_download_manifest

    plan = plan_from_json(json.loads(plan_path.read_text()))
    limit = int(max_run_gib * 1024**3) if max_run_gib is not None else None
    chosen = sample_runs(plan, count, max_run_bytes=limit, seed=seed)
    if not chosen:
        raise click.ClickException("No run qualifies; nothing written.")
    # The manifest first: it can be refused (cache-path collisions), and a run list written before
    # that would sit beside an older manifest describing a different sample.
    try:
        rows = write_download_manifest(plan, chosen, CHECK_TYPES, manifest)
    except ValueError as error:
        raise click.ClickException(f"Manifest refused, so neither file was written: {error}") from error
    runs_path.parent.mkdir(parents=True, exist_ok=True)
    runs_path.write_text("\n".join(chosen) + "\n")
    size = sum(
        int(plan.selected[r]["files"][t].get("file_size_bytes") or 0)
        for r in chosen
        for t in CHECK_TYPES
        if t in plan.selected[r]["files"]
    )
    click.echo(f"{len(chosen)} runs, {rows} files, {size / 1024**3:,.2f} GiB -> {manifest}")
    click.echo(f"download: uv run python {shlex.quote(_downloader_path())} --manifest {shlex.quote(str(manifest))} \\")
    click.echo("    --cache-dir PATH_TO_CACHE --workers 8")


@cli.command("feature-check")
@click.argument("plan_path", type=click.Path(path_type=Path, dir_okay=False, exists=True))
@click.option("--runs", "runs_path", type=click.Path(path_type=Path, dir_okay=False, exists=True), required=True)
@click.option("--cache-dir", type=click.Path(path_type=Path, file_okay=False, exists=True), required=True)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
def feature_check_command(plan_path: Path, runs_path: Path, cache_dir: Path, output: Path) -> None:
    """Verify checksums and test which file types repeat others, for each downloaded run.

    Exits non-zero when any check fails or any file's MD5 differs from NMDC's record.
    """
    import hashlib
    import json
    from collections import Counter

    from nmdc_lakehouse.feature_tables import CHECK_TYPES, cached_files, check_run, plan_from_json

    plan = plan_from_json(json.loads(plan_path.read_text()))
    run_ids = _read_run_ids(runs_path)
    if not run_ids:
        # An empty or truncated list would otherwise write an empty report and exit 0.
        raise click.ClickException(f"{runs_path} lists no runs; nothing would be checked.")
    report: dict[str, object] = {}
    failures: Counter[str] = Counter()
    passes: Counter[str] = Counter()
    for run_id in run_ids:
        entry = plan.selected[run_id]
        try:
            files, _ = cached_files(entry, cache_dir)
        except ValueError as error:
            raise click.ClickException(str(error)) from error
        bad_md5 = []
        for data_object_type, path in files.items():
            expected = entry["files"][data_object_type].get("md5_checksum")
            with path.open("rb") as handle:
                digest = hashlib.file_digest(handle, lambda: hashlib.md5(usedforsecurity=False)).hexdigest()
            # No recorded digest means nothing was compared, which is not a pass.
            if not expected or digest != expected:
                bad_md5.append(data_object_type)
        # A file that does not match NMDC's checksum is not the file the checks are about, and
        # parsing a truncated or replaced file can raise rather than fail cleanly.
        checks: dict[str, dict[str, object]] = {}
        if not bad_md5:
            try:
                checks = dict(check_run(files))
            except (IndexError, ValueError, UnicodeDecodeError) as error:
                checks = {"parse": {"passed": False, "error": f"{type(error).__name__}: {error}"}}
        checks["md5_matches_nmdc"] = {"passed": not bad_md5, "mismatched": bad_md5, "files": len(files)}
        # A planned file absent from the cache would otherwise only mark its checks skipped.
        # Zero-byte files are left out of the download manifest, so they are not expected here.
        missing = sorted(
            t
            for t, data_object in entry["files"].items()
            if t in CHECK_TYPES and t not in files and int(data_object.get("file_size_bytes") or 0) > 0
        )
        checks["planned_files_present"] = {"passed": not missing, "missing": missing}
        for name, result in checks.items():
            if result.get("skipped"):
                continue
            (passes if result["passed"] else failures)[name] += 1
        report[run_id] = {
            "type": entry["run"].get("type"),
            "version": entry["run"].get("version"),
            "checks": checks,
        }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"runs": report, "passed": passes, "failed": failures}, indent=1, default=list))
    for name in sorted(set(passes) | set(failures)):
        click.echo(f"  {passes[name]:>4} passed  {failures[name]:>4} failed  {name}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    cli()
