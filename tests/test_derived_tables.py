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


def _stub_runtime(monkeypatch, ingest_file: Path, session_file: Path) -> object:
    """Put both runtime pieces in `sys.modules` with the file locations a test wants.

    `berdl_notebook_utils` is supplied by the pod image and lives outside every checkout, so its
    location is deliberately not something a test pins.
    """
    import sys as sys_module
    import types

    session_module = types.ModuleType("berdl_notebook_utils.setup_spark_session")
    session_module.__file__ = str(session_file)
    sentinel = object()
    session_module.get_spark_session = lambda: sentinel
    monkeypatch.setitem(sys_module.modules, "berdl_notebook_utils", types.ModuleType("berdl_notebook_utils"))
    monkeypatch.setitem(sys_module.modules, "berdl_notebook_utils.setup_spark_session", session_module)

    ingest = types.ModuleType("data_lakehouse_ingest")
    ingest.__file__ = str(ingest_file)
    monkeypatch.setitem(sys_module.modules, "data_lakehouse_ingest", ingest)
    return sentinel


def test_a_reviewed_package_imported_from_outside_the_checkout_is_refused(tmp_path: Path, monkeypatch) -> None:
    """Importing is not evidence of where it came from.

    The checkout going first on `sys.path` does not displace a copy installed in the environment,
    and an already-imported module comes back from `sys.modules` without the path being consulted.
    """
    from nmdc_lakehouse.derived_tables import spark_session

    elsewhere = tmp_path / "elsewhere" / "__init__.py"
    elsewhere.parent.mkdir(parents=True)
    elsewhere.write_text("", encoding="utf-8")
    _stub_runtime(monkeypatch, ingest_file=elsewhere, session_file=tmp_path / "pod" / "setup_spark_session.py")

    checkout = tmp_path / "checkout"
    (checkout / "src" / "data_lakehouse_ingest").mkdir(parents=True)

    with pytest.raises(DerivedTableError, match="not inside"):
        spark_session(checkout)


def test_a_session_is_returned_when_the_reviewed_package_is_in_the_checkout(tmp_path: Path, monkeypatch) -> None:
    """The passing path, which nothing exercised before.

    `spark_session` required `berdl_notebook_utils` to live inside the checkout. The BERDL pod
    image supplies it from site-packages and no checkout contains it, so the guard could not be
    satisfied and `berdl-promote` refused on its first real invocation, having never been run
    against a live catalog. A guard nothing proves can pass is how that shipped.
    See https://github.com/microbiomedata/nmdc-lakehouse/issues/339.
    """
    from nmdc_lakehouse.derived_tables import spark_session

    checkout = tmp_path / "checkout"
    package = checkout / "src" / "data_lakehouse_ingest"
    package.mkdir(parents=True)
    ingest_file = package / "__init__.py"
    ingest_file.write_text("", encoding="utf-8")

    # Deliberately outside the checkout, which is where the platform actually puts it.
    sentinel = _stub_runtime(
        monkeypatch, ingest_file=ingest_file, session_file=tmp_path / "site-packages" / "setup_spark_session.py"
    )

    assert spark_session(checkout) is sentinel
