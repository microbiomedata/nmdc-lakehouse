"""Shared catalog connection guards retained after retiring the Spark walk."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.derived_tables import DerivedTableError, check_namespace


def test_rebuild_command_is_removed() -> None:
    result = CliRunner().invoke(cli, ["rebuild-derived-tables", "--help"])
    assert result.exit_code != 0
    assert "No such command" in result.output


def test_catalog_namespace_must_be_qualified() -> None:
    assert check_namespace("nmdc.metadata") == "nmdc.metadata"
    with pytest.raises(DerivedTableError, match="catalog-qualified"):
        check_namespace("nmdc_metadata")


def test_an_unimportable_runtime_is_refused_by_name(tmp_path: Path) -> None:
    """The session must come from the reviewed checkout, not from whatever is on the path."""
    from nmdc_lakehouse.derived_tables import spark_session

    with pytest.raises(DerivedTableError, match="not importable"):
        spark_session(tmp_path)


def test_a_session_imported_from_outside_the_checkout_is_refused(tmp_path: Path, monkeypatch) -> None:
    """Importing is not evidence of where it came from.

    The checkout going first on `sys.path` does not displace a copy installed in the environment,
    and an already-imported module comes back from `sys.modules` without the path being consulted.
    """
    import sys as sys_module
    import types

    from nmdc_lakehouse.derived_tables import spark_session

    elsewhere = tmp_path / "elsewhere" / "setup_spark_session.py"
    elsewhere.parent.mkdir(parents=True)
    elsewhere.write_text("", encoding="utf-8")

    package = types.ModuleType("berdl_notebook_utils")
    module = types.ModuleType("berdl_notebook_utils.setup_spark_session")
    module.__file__ = str(elsewhere)
    module.get_spark_session = lambda: object()
    monkeypatch.setitem(sys_module.modules, "berdl_notebook_utils", package)
    monkeypatch.setitem(sys_module.modules, "berdl_notebook_utils.setup_spark_session", module)

    checkout = tmp_path / "checkout"
    (checkout / "src").mkdir(parents=True)

    with pytest.raises(DerivedTableError, match="not inside"):
        spark_session(checkout)
