"""Click-based CLI entry point for nmdc-lakehouse.

This is the default "job runner". It can be replaced or complemented later
by an external orchestrator (Dagster, Prefect, Snakemake, ...) without
changing the source / transform / sink modules.
"""

from __future__ import annotations

import logging

import click

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
    type=click.Choice(("mongo-config", "gcp-tunnel", "mongo-ping")),
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
def run_job(job_name: str, dry_run: bool, drop_empty_cols: bool, skip: tuple[str, ...]) -> None:
    """Run a named ETL job from the registry."""
    import os

    import nmdc_lakehouse.jobs  # noqa: F401 -- register built-in jobs only when needed
    from nmdc_lakehouse.jobs.registry import get

    if drop_empty_cols:
        os.environ["LAKEHOUSE_DROP_EMPTY_COLS"] = "true"
    if skip:
        os.environ["LAKEHOUSE_SKIP_COLLECTIONS"] = ",".join(skip)
    job = get(job_name)
    result = job.run(dry_run=dry_run)
    click.echo(f"rows_read={result.rows_read}")
    click.echo(f"rows_written={result.rows_written}")
    if result.tables_written:
        click.echo(f"tables={', '.join(result.tables_written)}")


if __name__ == "__main__":
    cli()
