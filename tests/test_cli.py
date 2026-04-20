"""CLI-level smoke tests."""

from click.testing import CliRunner

from nmdc_lakehouse.cli import cli


def test_cli_help_exits_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "nmdc-lakehouse" in result.output.lower()
