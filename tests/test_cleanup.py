"""Tests for previewable local metadata Parquet cleanup."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cleanup import (
    UnsafeCleanupRoot,
    apply_cleanup,
    find_project_root,
    plan_metadata_parquet_cleanup,
)
from nmdc_lakehouse.cli import cli

GENERATED = {"biosample_set", "biosample_set_has_input"}


def _make_checkout(root: Path) -> None:
    (root / ".git").mkdir()
    (root / "pyproject.toml").write_text('[project]\nname = "nmdc-lakehouse"\n', encoding="utf-8")


def test_plan_selects_only_recognized_top_level_regular_files(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    root.mkdir()
    primary = root / "biosample_set.parquet"
    side_table = root / "biosample_set_has_input.parquet"
    unknown_parquet = root / "personal.parquet"
    manifest = root / "manifest.json"
    nested = root / "nested"
    nested.mkdir()
    nested_generated = nested / "biosample_set.parquet"
    for path in (primary, side_table, unknown_parquet, manifest, nested_generated):
        path.write_text("preserve unless selected", encoding="utf-8")

    plan = plan_metadata_parquet_cleanup(root, project_root=tmp_path, generated_names=GENERATED)

    assert plan.targets == (primary, side_table)
    assert apply_cleanup(plan) == 2
    assert not primary.exists()
    assert not side_table.exists()
    assert unknown_parquet.exists()
    assert manifest.exists()
    assert nested_generated.exists()


def test_plan_preserves_symlink_even_with_generated_name(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    root.mkdir()
    outside = tmp_path / "outside.parquet"
    outside.write_text("outside", encoding="utf-8")
    (root / "biosample_set.parquet").symlink_to(outside)

    plan = plan_metadata_parquet_cleanup(root, project_root=tmp_path, generated_names=GENERATED)

    assert plan.targets == ()
    assert outside.exists()


def test_apply_rejects_target_replaced_by_symlink_after_preview(tmp_path: Path) -> None:
    root = tmp_path / "lakehouse"
    root.mkdir()
    generated = root / "biosample_set.parquet"
    generated.write_text("generated", encoding="utf-8")
    plan = plan_metadata_parquet_cleanup(root, project_root=tmp_path, generated_names=GENERATED)
    generated.unlink()
    outside = tmp_path / "outside.parquet"
    outside.write_text("outside", encoding="utf-8")
    generated.symlink_to(outside)

    with pytest.raises(UnsafeCleanupRoot, match="became unsafe"):
        apply_cleanup(plan)

    assert outside.exists()
    assert generated.is_symlink()


@pytest.mark.parametrize("relative", ["src/output", "tests/output"])
def test_plan_rejects_non_output_roots(tmp_path: Path, relative: str) -> None:
    with pytest.raises(UnsafeCleanupRoot):
        plan_metadata_parquet_cleanup(Path(relative), project_root=tmp_path, generated_names=GENERATED)


def test_plan_rejects_repository_root_with_specific_message(tmp_path: Path) -> None:
    with pytest.raises(UnsafeCleanupRoot, match="must not be the repository root"):
        plan_metadata_parquet_cleanup(Path("."), project_root=tmp_path, generated_names=GENERATED)


def test_find_project_root_walks_up_and_fails_outside_checkout(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    nested = checkout / "src" / "package"
    nested.mkdir(parents=True)
    _make_checkout(checkout)

    assert find_project_root(nested) == checkout
    with pytest.raises(UnsafeCleanupRoot, match="inside an nmdc-lakehouse Git checkout"):
        find_project_root(tmp_path / "elsewhere")


def test_plan_rejects_outside_and_symlinked_roots(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside-lakehouse"
    with pytest.raises(UnsafeCleanupRoot):
        plan_metadata_parquet_cleanup(outside, project_root=tmp_path, generated_names=GENERATED)

    actual = tmp_path / "local" / "actual"
    actual.mkdir(parents=True)
    link = tmp_path / "lakehouse"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(UnsafeCleanupRoot):
        plan_metadata_parquet_cleanup(link, project_root=tmp_path, generated_names=GENERATED)


def test_missing_safe_root_has_empty_plan(tmp_path: Path) -> None:
    plan = plan_metadata_parquet_cleanup(
        Path("local/new-snapshot"),
        project_root=tmp_path,
        generated_names=GENERATED,
    )

    assert plan.root == tmp_path / "local" / "new-snapshot"
    assert plan.targets == ()


def test_plan_reports_directory_enumeration_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "lakehouse"
    root.mkdir()

    def fail_iterdir(_path: Path):
        raise PermissionError

    monkeypatch.setattr(Path, "iterdir", fail_iterdir)

    with pytest.raises(UnsafeCleanupRoot, match="could not be enumerated safely"):
        plan_metadata_parquet_cleanup(root, project_root=tmp_path, generated_names=GENERATED)


def test_apply_reports_unlink_failure_without_traceback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "lakehouse"
    root.mkdir()
    generated = root / "biosample_set.parquet"
    generated.write_text("generated", encoding="utf-8")
    plan = plan_metadata_parquet_cleanup(root, project_root=tmp_path, generated_names=GENERATED)

    def fail_unlink(_path: Path) -> None:
        raise PermissionError

    monkeypatch.setattr(Path, "unlink", fail_unlink)

    with pytest.raises(UnsafeCleanupRoot, match="stopped after removing 0 of 1"):
        apply_cleanup(plan)

    assert generated.exists()


def test_cli_previews_by_default_and_requires_delete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _make_checkout(tmp_path)
    working_directory = tmp_path / "docs"
    working_directory.mkdir()
    monkeypatch.chdir(working_directory)
    root = tmp_path / "lakehouse"
    root.mkdir()
    generated = root / "biosample_set.parquet"
    unknown = root / "personal.parquet"
    generated.write_text("generated", encoding="utf-8")
    unknown.write_text("unknown", encoding="utf-8")
    runner = CliRunner()

    preview = runner.invoke(cli, ["clean-parquet", "--root", str(root)])

    assert preview.exit_code == 0
    assert "Would remove: biosample_set.parquet" in preview.output
    assert "no files were deleted" in preview.output
    assert generated.exists() and unknown.exists()

    deletion = runner.invoke(cli, ["clean-parquet", "--root", str(root), "--delete"])

    assert deletion.exit_code == 0
    assert "Removed 1 recognized metadata Parquet file(s)." in deletion.output
    assert not generated.exists()
    assert unknown.exists()


def test_cli_rejects_cleanup_outside_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(cli, ["clean-parquet", "--root", "lakehouse"])

    assert result.exit_code != 0
    assert "Run cleanup from inside an nmdc-lakehouse Git checkout" in result.output
