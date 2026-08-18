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
    job = get(job_name)
    applied_skips = tuple(sorted(getattr(job, "skip", ())))
    started_at = datetime.now(UTC).isoformat()
    t0 = time.monotonic()
    try:
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
