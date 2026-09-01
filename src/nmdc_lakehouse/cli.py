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
from typing import TYPE_CHECKING

import click

from nmdc_lakehouse.service_doctor import SERVICE_CHECKS

if TYPE_CHECKING:
    # Import-time cost is why every command imports its own module inside the function body; this
    # one is only for the annotation.
    from nmdc_lakehouse.berdl_promotion import BerdlPromotionPlan

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
    """Validate manifested Parquet rows against the published target schema."""
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


@cli.command("schema-diff")
@click.argument("before", type=click.Path(path_type=Path, dir_okay=False, exists=True))
@click.argument("after", type=click.Path(path_type=Path, dir_okay=False, exists=True))
@click.option("--limit", default=40, show_default=True, help="Rows per section before truncating.")
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Write the report here instead of printing it.",
)
def schema_diff_command(before: Path, after: Path, limit: int, output_path: Path | None) -> None:
    """Report what changed between two generated flat schemas.

    Materialise an older revision with `git show REV:src/nmdc_lakehouse/schemas/nmdc_metadata.yaml`.
    """
    from nmdc_lakehouse.transforms.schema_diff import SchemaDiffError, diff_schemas, render_diff

    try:
        report = render_diff(diff_schemas(str(before), str(after)), limit=limit)
    except SchemaDiffError as error:
        raise click.ClickException(str(error)) from error
    if output_path is None:
        click.echo(report)
        return
    output_path.write_text(report, encoding="utf-8")
    click.echo(f"wrote {output_path}", err=True)


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


@cli.command("metadata-application-plan")
@click.argument("bundle", type=click.Path(path_type=Path, dir_okay=False))
@click.option(
    "--inventory",
    "inventory_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Credential-free destination inventory JSON.",
)
@click.option(
    "--staging-namespace",
    required=True,
    help="Explicit provider-qualified namespace that will receive staged tables.",
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), help="Also write the generated plan here.")
def metadata_application_plan_command(
    bundle: Path,
    inventory_path: Path,
    staging_namespace: str,
    output: Path | None,
) -> None:
    """Plan provider-neutral metadata operations without mutation."""
    from nmdc_lakehouse.metadata_application import (
        MetadataApplicationError,
        plan_metadata_application,
        render_metadata_application_plan,
        write_metadata_application_plan,
    )
    from nmdc_lakehouse.metadata_bundle import MetadataBundleError
    from nmdc_lakehouse.publication_plan import PublicationPlanError

    try:
        plan = plan_metadata_application(bundle, inventory_path, staging_namespace)
        if output is not None:
            write_metadata_application_plan(output, plan)
    except (MetadataApplicationError, MetadataBundleError, PublicationPlanError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_metadata_application_plan(plan))


@cli.command("metadata-application-plan-schema")
def metadata_application_plan_schema_command() -> None:
    """Print the metadata application plan JSON Schema."""
    import json

    from nmdc_lakehouse.metadata_application import metadata_application_json_schema

    click.echo(json.dumps(metadata_application_json_schema(), indent=2, sort_keys=True))


@cli.command("berdl-upload-plan")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option("--bundle", "bundle_path", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option("--inventory", "inventory_path", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option("--plan", "publication_plan_path", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option(
    "--metadata-plan",
    "metadata_plan_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
)
@click.option(
    "--target-validation",
    "target_validation_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
)
@click.option("--ingest-checkout", type=click.Path(path_type=Path, file_okay=False), required=True)
@click.option("--ingest-revision", required=True, help="Exact KBase ingest Git commit selected for staging.")
@click.option("--tenant", required=True)
@click.option("--dataset", required=True, help="Unique dataset name containing _staging_<suffix>.")
@click.option("--bucket", required=True, help="Explicit S3 bucket selected for staging.")
@click.option("--bronze-prefix", required=True)
@click.option("--progress-key", required=True)
@click.option("--config-key", required=True)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
def berdl_upload_plan_command(
    snapshot_root: Path,
    bundle_path: Path,
    inventory_path: Path,
    publication_plan_path: Path,
    metadata_plan_path: Path,
    target_validation_path: Path,
    ingest_checkout: Path,
    ingest_revision: str,
    tenant: str,
    dataset: str,
    bucket: str,
    bronze_prefix: str,
    progress_key: str,
    config_key: str,
    output: Path,
) -> None:
    """Build an immutable, non-mutating BERDL staging command plan."""
    from nmdc_lakehouse.berdl_staging import (
        BerdlStagingPlanError,
        plan_berdl_staging,
        render_berdl_staging_plan,
        write_berdl_staging_plan,
    )
    from nmdc_lakehouse.metadata_application import MetadataApplicationError
    from nmdc_lakehouse.metadata_bundle import MetadataBundleError
    from nmdc_lakehouse.publication_plan import PublicationPlanError
    from nmdc_lakehouse.publication_preflight import PublicationPreflightError
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError
    from nmdc_lakehouse.target_validation import TargetValidationError

    try:
        plan = plan_berdl_staging(
            snapshot_root,
            bundle_path=bundle_path,
            inventory_path=inventory_path,
            publication_plan_path=publication_plan_path,
            metadata_plan_path=metadata_plan_path,
            target_validation_path=target_validation_path,
            ingest_checkout=ingest_checkout,
            ingest_revision=ingest_revision,
            tenant=tenant,
            dataset=dataset,
            bucket=bucket,
            bronze_prefix=bronze_prefix,
            progress_key=progress_key,
            config_key=config_key,
        )
        destination = write_berdl_staging_plan(output, plan)
    except (
        BerdlStagingPlanError,
        MetadataApplicationError,
        MetadataBundleError,
        PublicationPlanError,
        PublicationPreflightError,
        SnapshotManifestError,
        TargetValidationError,
    ) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_berdl_staging_plan(plan))
    click.echo(f"plan={destination}", err=True)


@cli.command("berdl-upload")
@click.argument("plan_path", type=click.Path(path_type=Path, dir_okay=False))
@click.option(
    "--upstream-outcome",
    "upstream_outcome_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
)
@click.option("--output", "output_path", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option("--authorize-snapshot", help="Exact snapshot ID approved for this invocation.")
@click.option("--authorize-plan-sha256", help="Exact SHA-256 digest of the reviewed staging plan.")
@click.option(
    "--execute-staging",
    is_flag=True,
    help="Run the reviewed staging command; the default only previews it.",
)
def berdl_upload_command(
    plan_path: Path,
    upstream_outcome_path: Path,
    output_path: Path,
    authorize_snapshot: str | None,
    authorize_plan_sha256: str | None,
    execute_staging: bool,
) -> None:
    """Preview or execute and independently verify one reviewed BERDL plan."""
    import json

    from nmdc_lakehouse.berdl_staging import (
        BerdlStagingPlanError,
        execute_berdl_staging,
        render_berdl_staging_outcome,
    )
    from nmdc_lakehouse.metadata_application import MetadataApplicationError
    from nmdc_lakehouse.metadata_bundle import MetadataBundleError
    from nmdc_lakehouse.publication_plan import PublicationPlanError
    from nmdc_lakehouse.publication_preflight import PublicationPreflightError
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError
    from nmdc_lakehouse.target_validation import TargetValidationError

    try:
        command, outcome = execute_berdl_staging(
            plan_path,
            upstream_outcome_path=upstream_outcome_path,
            output_path=output_path,
            authorize_snapshot=authorize_snapshot,
            execute_staging=execute_staging,
            authorize_plan_sha256=authorize_plan_sha256,
        )
    except (
        BerdlStagingPlanError,
        MetadataApplicationError,
        MetadataBundleError,
        PublicationPlanError,
        PublicationPreflightError,
        SnapshotManifestError,
        TargetValidationError,
    ) as error:
        raise click.ClickException(str(error)) from error
    if outcome is None:
        click.echo(json.dumps({"status": "preview-only", "command": command}, indent=2))
    else:
        click.echo(render_berdl_staging_outcome(outcome))
        click.echo(f"outcome={output_path.expanduser().resolve()}", err=True)


@cli.command("berdl-apply-metadata")
@click.argument("metadata_plan_path", type=click.Path(path_type=Path, dir_okay=False))
@click.argument("staging_outcome_path", type=click.Path(path_type=Path, dir_okay=False))
@click.option(
    "--ingest-checkout",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
    help="Clean checkout of the stock KBase ingest revision used for staging.",
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option("--authorize-plan-sha256", help="Exact SHA-256 of the reviewed metadata plan.")
@click.option("--authorize-staging-outcome-sha256", help="Exact SHA-256 of the verified staging outcome.")
@click.option(
    "--execute-metadata",
    is_flag=True,
    help="Apply and read back table and column descriptions; the default only previews them.",
)
def berdl_apply_metadata_command(
    metadata_plan_path: Path,
    staging_outcome_path: Path,
    ingest_checkout: Path,
    output: Path,
    authorize_plan_sha256: str | None,
    authorize_staging_outcome_sha256: str | None,
    execute_metadata: bool,
) -> None:
    """Preview or apply approved descriptions to verified staging tables."""
    from nmdc_lakehouse.berdl_metadata import (
        BerdlMetadataError,
        apply_berdl_staging_metadata,
        load_berdl_metadata_preview,
        render_berdl_metadata,
        write_berdl_metadata_outcome,
    )

    try:
        plan, staging, preview = load_berdl_metadata_preview(metadata_plan_path, staging_outcome_path)
        if not execute_metadata:
            click.echo(render_berdl_metadata(preview))
            return
        if authorize_plan_sha256 != preview.metadata_plan_sha256:
            raise BerdlMetadataError("Execution requires the exact reviewed metadata plan SHA-256.")
        if authorize_staging_outcome_sha256 != preview.staging_outcome_sha256:
            raise BerdlMetadataError("Execution requires the exact verified staging outcome SHA-256.")
        outcome = apply_berdl_staging_metadata(plan, staging, preview, ingest_checkout=ingest_checkout)
        destination = write_berdl_metadata_outcome(output, outcome)
    except (BerdlMetadataError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_berdl_metadata(outcome))
    click.echo(f"outcome={destination.resolve()}", err=True)


@cli.command("rebuild-derived-tables")
@click.argument("namespace")
@click.option("--ingest-checkout", type=click.Path(path_type=Path, file_okay=False), required=True)
@click.option(
    "--max-depth",
    type=click.IntRange(min=1),
    default=None,
    help="Refuse rather than truncate past this many hops.",
)
@click.option(
    "--table",
    "tables",
    multiple=True,
    help="Rebuild only these derived tables. Repeatable. Defaults to all of them.",
)
@click.option(
    "--authorize-namespace",
    help="Exact namespace, required to run. Without it this prints what it would do and stops.",
)
def rebuild_derived_tables_command(
    namespace: str,
    ingest_checkout: Path,
    max_depth: int | None,
    tables: tuple[str, ...],
    authorize_namespace: str | None,
) -> None:
    """Rebuild the derived tables in a namespace.

    Every derived table by default, or only those named by `--table`. The selection exists because
    a promotion plan can rebuild one and preserve the other, and rebuilding both would replace a
    table nobody authorized touching. Whatever is selected is ordered by `DERIVED_TABLES`, since
    the second walks the first.

    Nothing here is incremental, and a reload of the tables they are computed from leaves them
    describing data that no longer exists, which is why they exist as a rebuild rather than as
    something maintained in place.

    Previewing is the default. Execution needs `--authorize-namespace` naming the same namespace,
    so the destructive form cannot be reached by editing a path in a shell history entry.
    """
    from nmdc_lakehouse.derived_tables import (
        DEFAULT_MAX_DEPTH,
        DERIVED_TABLES,
        DerivedTableError,
        check_namespace,
        rebuild_all,
        spark_session,
    )

    # Refused before the preview, not after it. A preview that renders for a namespace the rebuild
    # will always reject reads as an actionable plan for something that can never run.
    try:
        check_namespace(namespace)
    except DerivedTableError as error:
        raise click.ClickException(str(error)) from error

    # An unknown name is refused here rather than after the first table has been replaced, for the
    # same reason the namespace is.
    unknown = sorted(set(tables) - set(DERIVED_TABLES))
    if unknown:
        raise click.ClickException(
            "No rebuild procedure exists for: " + ", ".join(unknown) + ". Known: " + ", ".join(DERIVED_TABLES) + "."
        )
    # Ordered by DERIVED_TABLES whatever order they were typed in, because the second walks the
    # first and that does not stop being true because a caller listed them differently.
    selected = [table for table in DERIVED_TABLES if table in set(tables)] if tables else list(DERIVED_TABLES)

    depth = DEFAULT_MAX_DEPTH if max_depth is None else max_depth
    targets = ", ".join(f"{namespace}.{table}" for table in selected)
    click.echo(f"rebuild plan for {namespace}")
    click.echo(f"  replaces      {targets}")
    click.echo(
        f"  order         {' then '.join(selected)}"
        + (", because the second walks the first" if len(selected) > 1 else "")
    )
    click.echo(f"  max depth     {depth}")

    if authorize_namespace is None:
        click.echo("  nothing has been changed; rerun with --authorize-namespace to execute")
        return
    if authorize_namespace != namespace:
        raise click.ClickException(f"--authorize-namespace is '{authorize_namespace}' but the target is '{namespace}'.")

    try:
        spark = spark_session(ingest_checkout)
        outcomes = rebuild_all(
            spark,
            namespace,
            max_depth=depth,
            progress=lambda message: click.echo(f"  {message}"),
            tables=selected,
        )
    except DerivedTableError as error:
        raise click.ClickException(str(error)) from error
    for outcome in outcomes:
        detail = f", depth {outcome.depth_reached}" if outcome.depth_reached else ""
        # outcome.table is already catalog-qualified. Prefixing it again produced
        # nmdc.metadata.nmdc.metadata.biosample_to_workflow_run, which reads as a real name.
        click.echo(f"  rebuilt {outcome.table}: {outcome.rows} rows{detail}")


@cli.command("berdl-promotion-plan")
@click.option("--plan", "publication_plan_path", type=click.Path(path_type=Path, dir_okay=False), required=True)
@click.option(
    "--staging-outcome",
    "staging_outcome_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Credential-free data-verified outcome from berdl-upload.",
)
@click.option(
    "--metadata-outcome",
    "metadata_outcome_path",
    type=click.Path(path_type=Path, dir_okay=False),
    required=True,
    help="Credential-free metadata-verified outcome from berdl-apply-metadata.",
)
@click.option("--canonical-namespace", required=True, help="Catalog-qualified promotion target, e.g. nmdc.metadata.")
@click.option(
    "--recovery",
    required=True,
    help=(
        "The one recovery operation a human would perform if promotion fails part way. Recorded "
        "for the operator to read and carry out; nothing attempts it automatically."
    ),
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), required=True)
def berdl_promotion_plan_command(
    publication_plan_path: Path,
    staging_outcome_path: Path,
    metadata_outcome_path: Path,
    canonical_namespace: str,
    recovery: str,
    output: Path,
) -> None:
    """Describe the promotion that verified staging authorizes, changing nothing.

    This reads evidence and writes a description. It does not promote, and there is deliberately
    no flag here that makes it promote: the authorization step is a separate reviewable artifact.
    """
    from nmdc_lakehouse.berdl_promotion import (
        PromotionPlanError,
        plan_berdl_promotion_from_files,
        render_promotion_plan,
        write_berdl_promotion_plan,
    )

    try:
        plan = plan_berdl_promotion_from_files(
            publication_plan_path=publication_plan_path,
            staging_outcome_path=staging_outcome_path,
            metadata_outcome_path=metadata_outcome_path,
            canonical_namespace=canonical_namespace,
            recovery=recovery,
        )
        destination = write_berdl_promotion_plan(output, plan)
    except (PromotionPlanError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_promotion_plan(plan))
    click.echo(f"plan={destination}", err=True)


@cli.command("berdl-promote")
@click.argument("plan_path", type=click.Path(path_type=Path, dir_okay=False))
@click.option("--ingest-checkout", type=click.Path(path_type=Path, file_okay=False), required=True)
@click.option("--authorize-plan-sha256", help="Exact SHA-256 of the plan being run.")
@click.option("--authorize-canonical-namespace", help="Exact namespace the plan promotes into.")
@click.option(
    "--authorize-destination-id",
    help="Exact destination the plan's dispositions were decided against.",
)
def berdl_promote_command(
    plan_path: Path,
    ingest_checkout: Path,
    authorize_plan_sha256: str | None,
    authorize_canonical_namespace: str | None,
    authorize_destination_id: str | None,
) -> None:
    """Perform the promotion a reviewed plan describes.

    This is the destructive half. It replaces canonical tables, and when the plan rebuilds derived
    tables it drops those first, which is a deliberate outage: they do not exist again until the
    rebuild. A plan with no rebuild dispositions issues no drop and starts no outage, and a
    preserve-only plan issues nothing at all.

    Previewing is the default and prints the plan, the digest to authorize with, the destination,
    and the exact statements. Execution needs all three authorizations and none is optional: the
    digest binds the run to the plan a human read, the namespace is typed again because a digest
    gets copied from a previous command while a namespace does not, and the destination is
    asserted because nothing here can verify which deployment the session reaches.

    Rebuilding the derived tables is `rebuild-derived-tables`, run after this. Doing it here would
    make one command that cannot be stopped between the drop and the rebuild.
    """
    from nmdc_lakehouse.berdl_promotion import (
        PromotionPlanError,
        execute_promotion,
        load_promotion_plan,
        promotion_statements,
        render_promotion_plan,
    )
    from nmdc_lakehouse.derived_tables import DerivedTableError, spark_session

    try:
        plan, plan_sha256 = load_promotion_plan(plan_path)
    except PromotionPlanError as error:
        raise click.ClickException(str(error)) from error

    click.echo(render_promotion_plan(plan))
    click.echo("")
    click.echo(f"  plan sha256    {plan_sha256}")
    click.echo(f"  destination    {plan.destination_id}")
    for step, _table, statement in promotion_statements(plan):
        click.echo(f"    {step:8s} {statement}")

    if authorize_plan_sha256 is None or authorize_canonical_namespace is None or authorize_destination_id is None:
        click.echo("")
        click.echo("  nothing has been changed; rerun with all three --authorize- options to execute")
        return

    try:
        spark = spark_session(ingest_checkout)
        performed = execute_promotion(
            spark,
            plan,
            plan_sha256=plan_sha256,
            authorize_plan_sha256=authorize_plan_sha256,
            authorize_canonical_namespace=authorize_canonical_namespace,
            authorize_destination_id=authorize_destination_id,
            progress=lambda message: click.echo(f"  {message}"),
        )
    except (PromotionPlanError, DerivedTableError) as error:
        # Saying that nothing was attempted, because the plan names a recovery operation and the
        # option describing it used to promise it would be attempted. An operator reading a
        # failure beside a recorded recovery can reasonably assume it was tried.
        raise click.ClickException(
            f"{error}\n\nNo recovery was attempted; nothing here implements one. The plan records "
            f"what a human would do: {plan.recovery}"
        ) from error

    click.echo(f"  performed {len(performed)} statement(s)")
    if plan.derived_rebuilds:
        # Naming the exact tables, not just the command. A plan can rebuild one derived table and
        # preserve the other, and `rebuild-derived-tables` with no `--table` replaces both, so a
        # bare instruction would have an operator mutate a table this plan preserved.
        selection = " ".join(f"--table {shlex.quote(table)}" for table in plan.derived_rebuilds)
        click.echo("  the derived table(s) are dropped and not yet rebuilt: " + ", ".join(plan.derived_rebuilds) + ".")
        # PATH_TO_CHECKOUT, not <checkout>. Angle brackets are two redirections: `<checkout` reads
        # from a file and `>` takes the next word as an output file, so a shell swallowed the
        # `--table` flag as a redirection target, left the table name in the checkout position, and
        # wrote every message into a file named `--table`. A rebuild with no `--table` replaces
        # every derived table, which is the exact hazard the selection three lines up exists to
        # avoid, and this is the instruction an operator follows while the tables are dropped.
        click.echo(
            f"  run: just rebuild-derived-tables {shlex.quote(plan.canonical_namespace)} PATH_TO_CHECKOUT "
            f"{selection} --authorize-namespace {shlex.quote(plan.canonical_namespace)}"
        )
    # A statement that succeeded is not a table that holds what it should, and the plan's last
    # step is a read-back this command does not perform. Saying only how many statements ran
    # would let the output stand in for the verification nobody has done yet.
    # Only when a table was built. A preserve-only plan issues nothing and a rebuild-only plan
    # issues drops, and telling either operator that "these statements build tables from a query"
    # describes statements that did not run.
    if any(step in ("replace", "add") for step, _table, _statement in promotion_statements(plan)):
        click.echo("")
        _echo_metadata_warning(plan)

    click.echo("")
    # Neutral about whether anything ran, because a preserve-only plan issues no statement and
    # telling that operator "this ran statements" describes work the command did not do. What is
    # true either way is that nothing was read back.
    click.echo("  NOT VERIFIED: no table has been read back. Whatever ran was issued, not checked.")
    click.echo(f"  Verify all {len(plan.operations)} object(s) in {plan.canonical_namespace}")
    click.echo("  before anyone is told the promotion is complete.")


def _echo_metadata_warning(plan: "BerdlPromotionPlan") -> None:
    # A table comment and TBLPROPERTIES are not part of a query result, so the statements above
    # cannot have carried them. There is no follow-up command that fixes this: berdl-apply-metadata
    # refuses a canonical namespace on purpose, because applying descriptions one column at a time
    # stopped partway through biosample_set on 2026-08-20 and left it half described.
    click.echo("  METADATA NOT CARRIED: these statements build tables from a query. Table comments")
    click.echo("  and properties are not part of one, and no command applies them to a canonical")
    click.echo("  namespace afterwards; berdl-apply-metadata refuses one by design. The verified")
    click.echo(f"  metadata is on the staging tables, not on {plan.canonical_namespace}. See issue 320.")


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


@cli.command("metadata-profile")
@click.argument("snapshot_root", type=click.Path(path_type=Path, file_okay=False))
@click.option("--profile-id", required=True, help="Stable credential-free identity for this reviewed profile.")
@click.option("--namespace-name", required=True, help="Logical destination-neutral namespace name.")
@click.option("--title", required=True, help="Human-readable namespace title.")
@click.option("--description", required=True, help="Reviewed namespace description.")
@click.option("--documentation-url", help="Optional HTTPS documentation URL.")
@click.option(
    "--property",
    "properties",
    multiple=True,
    metavar="KEY=VALUE",
    help="Repeatable credential-free namespace property.",
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False), help="Also write the profile draft here.")
def metadata_profile_command(
    snapshot_root: Path,
    profile_id: str,
    namespace_name: str,
    title: str,
    description: str,
    documentation_url: str | None,
    properties: tuple[str, ...],
    output: Path | None,
) -> None:
    """Generate a strict profile draft bound to a validated snapshot."""
    from nmdc_lakehouse.metadata_bundle import (
        MetadataBundleError,
        generate_metadata_profile,
        render_metadata_profile,
        write_metadata_profile,
    )
    from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError

    parsed_properties: dict[str, str] = {}
    for item in properties:
        key, separator, value = item.partition("=")
        if not separator or not key or key in parsed_properties:
            raise click.ClickException("Each namespace property must be a unique KEY=VALUE pair.")
        parsed_properties[key] = value
    try:
        profile = generate_metadata_profile(
            snapshot_root,
            profile_id=profile_id,
            namespace_name=namespace_name,
            title=title,
            description=description,
            documentation_url=documentation_url,
            properties=parsed_properties,
        )
        if output is not None:
            write_metadata_profile(output, profile)
    except (MetadataBundleError, SnapshotManifestError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(render_metadata_profile(profile))


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
@click.option("--host", help="Restrict to URLs starting with this prefix. No restriction by default.")
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
        written = write_manifest(outcome, output)
    except (DataObjectManifestError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    except OSError as error:
        # A full disk or an unwritable destination is an ordinary outcome here, not a defect, and
        # a traceback for one reads as the command breaking rather than the filesystem refusing.
        raise click.ClickException(f"Writing the manifest failed: {error}") from error

    click.echo(f"manifest from {source}")
    for name, count in sorted(outcome.per_type.items()):
        click.echo(f"  {count:>8,}  {name}")
    click.echo(f"  {outcome.total:>8,}  total, {outcome.total_bytes / 1024**3:,.1f} GiB")
    dropped = (
        f"{outcome.dropped_no_url} no URL, {outcome.dropped_other_host} other host, "
        f"{outcome.dropped_duplicate} duplicate, {outcome.dropped_zero_byte} zero-byte"
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


if __name__ == "__main__":
    cli()
