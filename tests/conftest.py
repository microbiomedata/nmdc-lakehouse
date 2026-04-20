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
