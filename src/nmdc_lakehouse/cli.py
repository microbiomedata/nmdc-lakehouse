"""Click-based CLI entry point for nmdc-lakehouse.

This is the default "job runner". It can be replaced or complemented later
by an external orchestrator (Dagster, Prefect, Snakemake, ...) without
changing the source / transform / sink modules.
"""

from __future__ import annotations

import logging
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


@cli.command("snapshot-manifest-schema")
def snapshot_manifest_schema_command() -> None:
    """Print the current snapshot-manifest JSON Schema."""
    import json

    from nmdc_lakehouse.snapshot_manifest import manifest_json_schema

    click.echo(json.dumps(manifest_json_schema(), indent=2, sort_keys=True))


@cli.command("publication-plan")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--inventory",
    "inventory_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Credential-free destination inventory JSON.",
)
@click.option(
    "--policy",
    "policy_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Reviewed publication policy JSON.",
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), help="Also write the generated plan here.")
def publication_plan_command(
    snapshot_root: Path,
    inventory_path: Path,
    policy_path: Path,
    output: Path | None,
) -> None:
    """Generate a complete destination-neutral table disposition plan offline."""
    from nmdc_lakehouse.publication_plan import (
        PublicationPlanError,
        plan_snapshot_publication,
        render_publication_plan,
        write_publication_plan,
    )
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError

    try:
        plan = plan_snapshot_publication(snapshot_root, inventory_path, policy_path)
        if output is not None:
            write_publication_plan(output, plan)
    except (PublicationPlanError, SnapshotManifestError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_publication_plan(plan))


@cli.command("publication-plan-schema")
@click.argument("document", type=click.Choice(["inventory", "policy", "plan"]))
def publication_plan_schema_command(document: str) -> None:
    """Print a publication inventory, policy, or plan JSON Schema."""
    import json
    from typing import Literal, cast

    from nmdc_lakehouse.publication_plan import publication_json_schema

    selected = cast(Literal["inventory", "policy", "plan"], document)
    click.echo(json.dumps(publication_json_schema(selected), indent=2, sort_keys=True))


@cli.command("publication-preflight")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--bundle",
    "bundle_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Approved snapshot-bound metadata bundle JSON.",
)
@click.option(
    "--inventory",
    "inventory_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Credential-free destination inventory JSON.",
)
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Approved publication disposition plan JSON.",
)
def publication_preflight_command(
    snapshot_root: Path,
    bundle_path: Path,
    inventory_path: Path,
    plan_path: Path,
) -> None:
    """Validate all reviewed publication artifacts before staging."""
    from nmdc_lakehouse.metadata_bundle import MetadataBundleError
    from nmdc_lakehouse.publication_plan import PublicationPlanError
    from nmdc_lakehouse.publication_preflight import (
        PublicationPreflightError,
        render_publication_preflight,
        validate_publication_artifacts,
    )
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError

    try:
        report = validate_publication_artifacts(snapshot_root, bundle_path, inventory_path, plan_path)
    except (MetadataBundleError, PublicationPlanError, PublicationPreflightError, SnapshotManifestError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_publication_preflight(report))


@cli.command("berdl-doctor")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--beril-checkout",
    type=click.Path(path_type=Path, file_okay=False),
    envvar="BERIL_CHECKOUT",
    required=True,
    help="Explicit BERIL-research-observatory checkout to inspect.",
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
    beril_checkout: Path,
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


@cli.command("metadata-bundle")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--profile",
    "profile_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Reviewed provider-neutral metadata profile JSON.",
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), help="Also write the generated bundle here.")
def metadata_bundle_command(snapshot_root: Path, profile_path: Path, output: Path | None) -> None:
    """Generate a snapshot-linked metadata bundle entirely offline."""
    from nmdc_lakehouse.metadata_bundle import (
        MetadataBundleError,
        generate_metadata_bundle,
        render_metadata_bundle,
        write_metadata_bundle,
    )
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError

    try:
        bundle = generate_metadata_bundle(snapshot_root, profile_path)
        if output is not None:
            write_metadata_bundle(output, bundle)
    except (MetadataBundleError, SnapshotManifestError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_metadata_bundle(bundle))


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


if __name__ == "__main__":
    cli()
