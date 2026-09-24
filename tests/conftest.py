"""Shared pytest fixtures."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture
def inputs_dir() -> Path:
    """Path to the test fixtures directory."""
    return Path(__file__).parent / "inputs"


@pytest.fixture
def db_tests_enabled() -> bool:
    """True when ENABLE_DB_TESTS=true in the environment."""
    return os.getenv("ENABLE_DB_TESTS", "false").lower() == "true"


@pytest.fixture
def run_files(tmp_path: Path) -> dict[str, Path]:
    """Small made-up annotation files for one run, in NMDC's layout (tests/feature_files.py)."""
    from tests.feature_files import make_run_files

    return make_run_files(tmp_path)
