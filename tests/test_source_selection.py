"""Exercise the real Just entry points without installing packages or contacting services."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from nmdc_lakehouse.source_versions import default_source_version, source_extras

ROOT = Path(__file__).resolve().parents[1]

_SUPPORTED = source_extras(ROOT / "pyproject.toml")
DEFAULT_EXTRA = _SUPPORTED[default_source_version(_SUPPORTED)]


@pytest.fixture
def recipes(tmp_path):
    shutil.copyfile(ROOT / "justfile", tmp_path / "justfile")
    shutil.copyfile(ROOT / "pyproject.toml", tmp_path / "pyproject.toml")
    (tmp_path / "scripts").mkdir()
    shutil.copyfile(ROOT / "scripts/uv_with_source.sh", tmp_path / "scripts/uv_with_source.sh")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    uv = bin_dir / "uv"
    uv.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$UV_ARGUMENTS"\n')
    uv.chmod(0o755)
    arguments = tmp_path / "uv-arguments.txt"
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "UV_ARGUMENTS": str(arguments)}
    env.pop("NMDC_SCHEMA_VERSION", None)

    def run(*args, selection=None, dotenv=None):
        if selection is not None:
            env["NMDC_SCHEMA_VERSION"] = selection
        if dotenv is not None:
            (tmp_path / ".env").write_text(f"NMDC_SCHEMA_VERSION={dotenv}\n")
        result = subprocess.run(["just", *args], cwd=tmp_path, env=env, capture_output=True, text=True, check=False)
        argv = arguments.read_text().splitlines() if arguments.exists() else []
        return result, argv

    return run


@pytest.mark.parametrize("recipe", ["build", "lock", "test-dist"])
def test_package_only_recipes_render_with_unsupported_source(recipes, recipe):
    result, argv = recipes("--dry-run", recipe, selection="unsupported")
    assert result.returncode == 0, result.stderr
    assert not argv


def test_doctor_reaches_installed_diagnostic_without_selecting_or_syncing_source(recipes):
    result, argv = recipes("doctor", selection="unsupported")
    assert result.returncode == 0, result.stderr
    assert argv == ["run", "--no-sync", "nmdc-lakehouse", "doctor"]


@pytest.mark.parametrize("recipe", ["source-preflight", "install-all"])
@pytest.mark.parametrize("selection", ["unsupported", ""])
def test_source_aware_commands_reject_unsupported_selection_before_uv(recipes, recipe, selection):
    result, argv = recipes(recipe, selection=selection)
    assert result.returncode != 0
    assert "Unsupported NMDC_SCHEMA_VERSION" in result.stderr
    assert not argv


@pytest.mark.parametrize(
    ("selection", "dotenv", "extra"),
    [(None, None, DEFAULT_EXTRA), (None, "11.23.0", "source-11-23"), ("11.24.0", "11.23.0", "source-11-24")],
)
def test_runtime_selection_preserves_default_dotenv_and_shell_precedence(recipes, selection, dotenv, extra):
    result, argv = recipes("cli", "--help", selection=selection, dotenv=dotenv)
    assert result.returncode == 0, result.stderr
    assert argv == ["run", "--extra", extra, "nmdc-lakehouse", "--help"]


def test_install_keeps_locked_dev_docs_and_production_source(recipes):
    result, argv = recipes("install-all", selection="11.23.0")
    assert result.returncode == 0, result.stderr
    assert argv == ["sync", "--extra", "source-11-23", "--locked", "--extra", "dev", "--extra", "docs"]


def _add_release_candidate_extra(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    extras = "[project.optional-dependencies]\n"
    candidate = 'source-11-99 = ["nmdc-schema==11.99.0rc2"]\n'
    pyproject.write_text(pyproject.read_text().replace(extras, extras + candidate, 1))


def test_wrapper_selects_a_release_candidate_extra(recipes, tmp_path):
    _add_release_candidate_extra(tmp_path)
    result, argv = recipes("cli", "--help", selection="11.99.0rc2")
    assert result.returncode == 0, result.stderr
    assert argv == ["run", "--extra", "source-11-99", "nmdc-lakehouse", "--help"]


def test_wrapper_default_ignores_a_release_candidate_extra(recipes, tmp_path):
    _add_release_candidate_extra(tmp_path)
    result, argv = recipes("cli", "--help")
    assert result.returncode == 0, result.stderr
    assert argv[:3] == ["run", "--extra", DEFAULT_EXTRA]


@pytest.mark.parametrize(("pin", "selection"), [("11.99", "11.99.0"), ("11.99.0", "11.99")])
def test_wrapper_treats_two_and_three_part_releases_as_one(recipes, tmp_path, pin, selection):
    pyproject = tmp_path / "pyproject.toml"
    extras = "[project.optional-dependencies]\n"
    pyproject.write_text(pyproject.read_text().replace(extras, f'{extras}source-11-99 = ["nmdc-schema=={pin}"]\n', 1))
    result, argv = recipes("cli", "--help", selection=selection)
    assert result.returncode == 0, result.stderr
    assert argv == ["run", "--extra", "source-11-99", "nmdc-lakehouse", "--help"]
