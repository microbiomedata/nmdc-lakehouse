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
    # Implementation will enumerate nmdc_lakehouse.jobs.registry once jobs exist.
    raise NotImplementedError


@cli.command("run-job")
@click.argument("job_name")
@click.option("--dry-run", is_flag=True, help="Plan the job but do not write output.")
def run_job(job_name: str, dry_run: bool) -> None:
    """Run a named ETL job from the registry."""
    raise NotImplementedError


if __name__ == "__main__":
    cli()
