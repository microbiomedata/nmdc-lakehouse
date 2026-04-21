"""Click-based CLI entry point for nmdc-lakehouse.

This is the default "job runner". It can be replaced or complemented later
by an external orchestrator (Dagster, Prefect, Snakemake, ...) without
changing the source / transform / sink modules.
"""

from __future__ import annotations

import logging

import click

import nmdc_lakehouse.jobs  # noqa: F401 — registers all built-in jobs
from nmdc_lakehouse.jobs.registry import get, list_names

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.group()
@click.version_option(package_name="nmdc-lakehouse")
def cli() -> None:
    """nmdc-lakehouse: NMDC -> lakehouse ETL."""


@cli.command("list-jobs")
def list_jobs() -> None:
    """List all ETL jobs registered with the runner."""
    for name in list_names():
        click.echo(name)


@cli.command("run-job")
@click.argument("job_name")
@click.option("--dry-run", is_flag=True, help="Plan the job but do not write output.")
def run_job(job_name: str, dry_run: bool) -> None:
    """Run a named ETL job from the registry."""
    job = get(job_name)
    result = job.run(dry_run=dry_run)
    click.echo(f"rows_read={result.rows_read}")
    click.echo(f"rows_written={result.rows_written}")
    if result.tables_written:
        click.echo(f"tables={', '.join(result.tables_written)}")


if __name__ == "__main__":
    cli()
