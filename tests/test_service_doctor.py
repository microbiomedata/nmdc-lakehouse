"""Tests for explicit live-service readiness checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from nmdc_lakehouse import service_doctor
from nmdc_lakehouse.doctor import CheckStatus
from nmdc_lakehouse.service_doctor import run_service_checks


def _live_configuration(key_path: Path | None = None, **overrides: str) -> dict[str, str]:
    configured = {
        "MONGO_HOST": "localhost",
        "MONGO_PORT": "27124",
        "MONGO_DBNAME": "nmdc",
        "MONGO_USERNAME": "reader",
        "MONGO_PASSWORD": "TOP-SECRET-SENTINEL",
        "MONGO_AUTH_SOURCE": "admin",
        "MONGO_DIRECT_CONNECTION": "true",
    }
    if key_path is not None:
        key_path.write_text("fake test key", encoding="utf-8")
        key_path.chmod(0o600)
        configured["NMDC_JUMP_KEY"] = str(key_path)
    configured.update(overrides)
    return configured


class FakeMongoClient:
    """Record a ping without contacting MongoDB."""

    def __init__(self, failure: Exception | None = None) -> None:
        self.admin = self
        self.failure = failure
        self.commands: list[str] = []
        self.closed = False

    def command(self, name: str) -> None:
        self.commands.append(name)
        if self.failure:
            raise self.failure

    def close(self) -> None:
        self.closed = True


def test_no_requested_service_checks_do_nothing() -> None:
    assert run_service_checks((), configured={}) == []


def test_unknown_service_check_names_invalid_and_valid_choices() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unknown service check\(s\): typo. Valid choices: mongo-config, gcp-tunnel, mongo-ping\.",
    ):
        run_service_checks(("typo",), configured={})


def test_missing_configuration_is_distinct_and_sanitized() -> None:
    checks = run_service_checks(("mongo-config",), configured={"MONGO_PASSWORD": "SECRET"})

    assert len(checks) == 1
    assert checks[0].status is CheckStatus.FAIL
    assert "MONGO_USERNAME" in checks[0].summary
    assert "missing or blank required values" in checks[0].summary
    assert "SECRET" not in repr(checks)


@pytest.mark.parametrize("variable", ["MONGO_USERNAME", "MONGO_PASSWORD"])
def test_whitespace_only_credentials_are_missing(variable: str) -> None:
    checks = run_service_checks(
        ("mongo-config",),
        configured=_live_configuration(**{variable: " \t "}),
    )

    assert len(checks) == 1
    assert checks[0].status is CheckStatus.FAIL
    assert variable in checks[0].summary


@pytest.mark.parametrize(
    "variable",
    [
        "MONGO_HOST",
        "MONGO_DBNAME",
        "MONGO_USERNAME",
        "MONGO_PASSWORD",
        "MONGO_AUTH_SOURCE",
        "MONGO_REPLICA_SET",
    ],
)
def test_surrounding_whitespace_is_rejected_without_changing_values(variable: str) -> None:
    secret = " TOP-SECRET-SENTINEL "
    checks = run_service_checks(
        ("mongo-config",),
        configured=_live_configuration(**{variable: secret}),
    )

    assert len(checks) == 1
    assert checks[0].status is CheckStatus.FAIL
    assert variable in checks[0].summary
    assert "leading or trailing whitespace" in checks[0].summary
    assert secret not in repr(checks)
    assert "values were not displayed or changed" in (checks[0].remediation or "")


def test_tunnel_check_does_not_require_mongo_credentials(tmp_path: Path) -> None:
    key_path = tmp_path / "jump-key"
    configuration = _live_configuration(key_path)
    configuration.pop("MONGO_USERNAME")
    configuration.pop("MONGO_PASSWORD")

    checks = run_service_checks(
        ("gcp-tunnel",),
        configured=configuration,
        socket_probe=lambda _host, _port, _timeout: True,
    )

    assert [check.name for check in checks] == ["gcp-jump-key", "gcp-tunnel"]
    assert all(check.status is CheckStatus.PASS for check in checks)


def test_tunnel_check_requires_coherent_local_settings(tmp_path: Path) -> None:
    probe_called = False

    def probe(_host: str, _port: int, _timeout: float) -> bool:
        nonlocal probe_called
        probe_called = True
        return True

    checks = run_service_checks(
        ("gcp-tunnel",),
        configured=_live_configuration(
            tmp_path / "jump-key", MONGO_HOST="mongo.example.org", MONGO_DIRECT_CONNECTION="false"
        ),
        socket_probe=probe,
    )

    assert checks[-1].name == "gcp-tunnel"
    assert checks[-1].status is CheckStatus.FAIL
    assert not probe_called


def test_tunnel_check_rejects_unrecognized_direct_connection_boolean(tmp_path: Path) -> None:
    probe_called = False

    def probe(_host: str, _port: int, _timeout: float) -> bool:
        nonlocal probe_called
        probe_called = True
        return True

    checks = run_service_checks(
        ("gcp-tunnel",),
        configured=_live_configuration(tmp_path / "jump-key", MONGO_DIRECT_CONNECTION="sometimes"),
        socket_probe=probe,
    )

    assert checks[-1].name == "gcp-tunnel"
    assert checks[-1].status is CheckStatus.FAIL
    assert "not a recognized boolean" in checks[-1].summary
    assert not probe_called


def test_missing_key_prevents_tunnel_probe(tmp_path: Path) -> None:
    probe_called = False

    def probe(_host: str, _port: int, _timeout: float) -> bool:
        nonlocal probe_called
        probe_called = True
        return True

    checks = run_service_checks(
        ("gcp-tunnel",),
        configured=_live_configuration(NMDC_JUMP_KEY=str(tmp_path / "missing-key")),
        socket_probe=probe,
    )

    assert checks[-1].name == "gcp-jump-key"
    assert checks[-1].status is CheckStatus.FAIL
    assert "unavailable" in checks[-1].summary
    assert "owner-only access" in (checks[-1].remediation or "")
    assert not probe_called


def test_unsafe_key_permissions_have_specific_remediation(tmp_path: Path) -> None:
    key_path = tmp_path / "jump-key"
    configuration = _live_configuration(key_path)
    key_path.chmod(0o644)

    checks = run_service_checks(
        ("gcp-tunnel",),
        configured=configuration,
        socket_probe=lambda _host, _port, _timeout: True,
    )

    assert checks[-1].name == "gcp-jump-key"
    assert checks[-1].status is CheckStatus.FAIL
    assert "permissions are broader" in checks[-1].summary
    assert "Restrict the key" in (checks[-1].remediation or "")


@pytest.mark.parametrize("override", ["", " \t "])
def test_blank_jump_key_override_uses_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, override: str) -> None:
    default_key = tmp_path / "default-jump-key"
    configuration = _live_configuration(default_key)
    jump_key_variable = "NMDC_JUMP_" + "KEY"
    configuration[jump_key_variable] = override
    monkeypatch.setattr(service_doctor, "DEFAULT_JUMP_KEY", str(default_key))

    checks = run_service_checks(
        ("gcp-tunnel",),
        configured=configuration,
        socket_probe=lambda _host, _port, _timeout: True,
    )

    assert [check.name for check in checks] == ["gcp-jump-key", "gcp-tunnel"]
    assert all(check.status is CheckStatus.PASS for check in checks)


def test_absent_tunnel_and_reachable_tunnel_are_distinct(tmp_path: Path) -> None:
    configuration = _live_configuration(tmp_path / "jump-key")
    absent = run_service_checks(
        ("gcp-tunnel",),
        configured=configuration,
        socket_probe=lambda _host, _port, _timeout: False,
    )
    reachable = run_service_checks(
        ("gcp-tunnel",),
        configured=configuration,
        socket_probe=lambda _host, _port, _timeout: True,
    )

    assert absent[-1].status is CheckStatus.FAIL
    assert "No service is listening" in absent[-1].summary
    assert reachable[-1].status is CheckStatus.PASS


def test_mongo_ping_is_bounded_read_only_and_closes_client() -> None:
    client = FakeMongoClient()
    captured: dict[str, Any] = {}

    def factory(uri: str, **kwargs: Any) -> FakeMongoClient:
        captured.update(uri=uri, **kwargs)
        return client

    checks = run_service_checks(
        ("mongo-ping",),
        configured=_live_configuration(),
        timeout=0.25,
        mongo_client_factory=factory,
    )

    assert checks[-1].status is CheckStatus.PASS
    assert client.commands == ["ping"]
    assert client.closed
    assert captured["serverSelectionTimeoutMS"] == 250
    assert captured["connectTimeoutMS"] == 250
    assert captured["socketTimeoutMS"] == 250
    assert "TOP-SECRET-SENTINEL" not in repr(checks)


def test_authentication_and_network_failures_are_sanitized_and_distinct() -> None:
    auth = FakeMongoClient(OperationFailure("TOP-SECRET-SENTINEL"))
    network = FakeMongoClient(ServerSelectionTimeoutError("TOP-SECRET-SENTINEL"))

    auth_checks = run_service_checks(
        ("mongo-ping",),
        configured=_live_configuration(),
        mongo_client_factory=lambda *_args, **_kwargs: auth,
    )
    network_checks = run_service_checks(
        ("mongo-ping",),
        configured=_live_configuration(),
        mongo_client_factory=lambda *_args, **_kwargs: network,
    )

    assert "authentication" in auth_checks[-1].summary
    assert "reachable" in network_checks[-1].summary
    assert "TOP-SECRET-SENTINEL" not in repr(auth_checks + network_checks)
    assert auth.closed and network.closed


def test_unexpected_ping_failure_is_sanitized() -> None:
    client = FakeMongoClient(ValueError("TOP-SECRET-SENTINEL"))

    checks = run_service_checks(
        ("mongo-ping",),
        configured=_live_configuration(),
        mongo_client_factory=lambda *_args, **_kwargs: client,
    )

    assert checks[-1].status is CheckStatus.FAIL
    assert "unexpected sanitized error" in checks[-1].summary
    assert "TOP-SECRET-SENTINEL" not in repr(checks)
    assert client.closed


def test_combined_checks_validate_configuration_once(tmp_path: Path) -> None:
    client = FakeMongoClient()
    checks = run_service_checks(
        ("mongo-config", "gcp-tunnel", "mongo-ping"),
        configured=_live_configuration(tmp_path / "jump-key"),
        socket_probe=lambda _host, _port, _timeout: True,
        mongo_client_factory=lambda *_args, **_kwargs: client,
    )

    assert [check.name for check in checks] == [
        "mongo-service-configuration",
        "gcp-jump-key",
        "gcp-tunnel",
        "mongo-ping",
    ]
    assert all(check.status is CheckStatus.PASS for check in checks)


def test_combined_checks_keep_tunnel_result_when_mongo_credentials_are_missing(tmp_path: Path) -> None:
    configuration = _live_configuration(tmp_path / "jump-key")
    configuration.pop("MONGO_PASSWORD")

    checks = run_service_checks(
        ("mongo-config", "gcp-tunnel", "mongo-ping"),
        configured=configuration,
        socket_probe=lambda _host, _port, _timeout: True,
    )

    assert [check.name for check in checks] == [
        "mongo-service-configuration",
        "gcp-jump-key",
        "gcp-tunnel",
    ]
    assert checks[0].status is CheckStatus.FAIL
    assert checks[-1].status is CheckStatus.PASS


def test_the_cli_and_berdl_commands_import_without_the_mongodb_driver() -> None:
    """BERDL commands run where the ETL dependency set is absent, such as a BERDL pod."""
    import builtins
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent(
        """
        import builtins, sys
        _real = builtins.__import__
        def _blocked(name, *a, **k):
            if name == "pymongo" or name.startswith("pymongo."):
                raise ModuleNotFoundError("No module named 'pymongo'")
            return _real(name, *a, **k)
        builtins.__import__ = _blocked
        from click.testing import CliRunner
        from nmdc_lakehouse.cli import cli
        from nmdc_lakehouse.service_doctor import SERVICE_CHECKS
        assert SERVICE_CHECKS
        for cmd in ("berdl-upload", "berdl-upload-plan", "berdl-apply-metadata", "berdl-doctor"):
            result = CliRunner().invoke(cli, [cmd, "--help"])
            assert result.exit_code == 0, (cmd, result.output)
        print("ok")
        """
    )
    assert builtins  # keep the import meaningful to linters
    completed = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr
    assert "ok" in completed.stdout


def test_a_missing_driver_reports_a_remediable_failure(tmp_path: Path) -> None:
    """Without the driver, the check must name the cause instead of an unexplained failure."""

    def _factory(*_args, **_kwargs):
        raise ModuleNotFoundError("No module named 'pymongo'")

    checks = run_service_checks(
        ("mongo-ping",),
        configured=_live_configuration(tmp_path / "jump-key"),
        mongo_client_factory=_factory,
    )

    ping = [check for check in checks if check.name == "mongo-ping"][0]
    assert ping.status is CheckStatus.FAIL
    assert "driver is not installed" in ping.summary
    assert "Install the MongoDB extra" in ping.remediation
