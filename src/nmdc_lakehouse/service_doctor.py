"""Explicit, bounded readiness checks for optional live services."""

from __future__ import annotations

import socket
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from pymongo import MongoClient
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from nmdc_lakehouse.config import MongoSettings
from nmdc_lakehouse.doctor import CheckStatus, DoctorCheck, _jump_key_check

SERVICE_CHECKS = ("mongo-config", "gcp-tunnel", "mongo-ping")
DEFAULT_TIMEOUT_SECONDS = 1.0
DEFAULT_JUMP_KEY = "~/.ssh/jump-dev.microbiomedata.org.private_key"

SocketProbe = Callable[[str, int, float], bool]
MongoClientFactory = Callable[..., Any]


@dataclass(frozen=True)
class LiveMongoConfiguration:
    """Validated live-MongoDB settings; values must never enter a report."""

    settings: MongoSettings


def _parse_boolean(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


def _live_mongo_configuration(configured: Mapping[str, str]) -> tuple[LiveMongoConfiguration | None, DoctorCheck]:
    required = ("MONGO_USERNAME", "MONGO_PASSWORD")
    missing = tuple(name for name in required if not configured.get(name, "").strip())
    if missing:
        return None, DoctorCheck(
            name="mongo-service-configuration",
            status=CheckStatus.FAIL,
            summary="Live MongoDB configuration has missing or blank required values: " + ", ".join(missing) + ".",
            remediation="Set the listed variables in an untracked .env file; see docs/mongodb-connection.md.",
        )

    direct_value = configured.get("MONGO_DIRECT_CONNECTION", "false")
    direct_connection = _parse_boolean(direct_value)
    if direct_connection is None:
        return None, DoctorCheck(
            name="mongo-service-configuration",
            status=CheckStatus.FAIL,
            summary="MONGO_DIRECT_CONNECTION is not a recognized boolean.",
            remediation="Use true for an SSH tunnel or false for a directly reachable replica set.",
        )

    text_settings = {
        "MONGO_HOST": configured.get("MONGO_HOST", "localhost"),
        "MONGO_DBNAME": configured.get("MONGO_DBNAME", "nmdc"),
        "MONGO_USERNAME": configured["MONGO_USERNAME"],
        "MONGO_PASSWORD": configured["MONGO_PASSWORD"],
        "MONGO_AUTH_SOURCE": configured.get("MONGO_AUTH_SOURCE", "admin"),
        "MONGO_REPLICA_SET": configured.get("MONGO_REPLICA_SET", ""),
    }
    padded = tuple(name for name, value in text_settings.items() if value != value.strip())
    if padded:
        return None, DoctorCheck(
            name="mongo-service-configuration",
            status=CheckStatus.FAIL,
            summary="Live MongoDB configuration has leading or trailing whitespace in: " + ", ".join(padded) + ".",
            remediation=(
                "Remove surrounding whitespace from the listed variables; values were not displayed or changed."
            ),
        )

    try:
        port = int(configured.get("MONGO_PORT", "27017"))
        if not 1 <= port <= 65535:
            raise ValueError
        settings = MongoSettings(
            host=text_settings["MONGO_HOST"],
            port=port,
            dbname=text_settings["MONGO_DBNAME"],
            username=text_settings["MONGO_USERNAME"],
            password=text_settings["MONGO_PASSWORD"],
            auth_source=text_settings["MONGO_AUTH_SOURCE"],
            replica_set=text_settings["MONGO_REPLICA_SET"] or None,
            direct_connection=direct_connection,
        )
    except (KeyError, ValueError):
        return None, DoctorCheck(
            name="mongo-service-configuration",
            status=CheckStatus.FAIL,
            summary="The live MongoDB port or settings are invalid.",
            remediation="Use a TCP port from 1 through 65535 and nonempty connection settings.",
        )

    if not settings.host.strip() or not settings.dbname.strip() or not settings.auth_source.strip():
        return None, DoctorCheck(
            name="mongo-service-configuration",
            status=CheckStatus.FAIL,
            summary="A required live MongoDB connection setting is empty.",
            remediation="Set MONGO_HOST, MONGO_DBNAME, and MONGO_AUTH_SOURCE to nonempty values.",
        )

    return LiveMongoConfiguration(settings), DoctorCheck(
        name="mongo-service-configuration",
        status=CheckStatus.PASS,
        summary="Live MongoDB settings form a valid credential-bearing URI; the URI was not displayed or contacted.",
    )


def _default_socket_probe(host: str, port: int, timeout: float) -> bool:
    try:
        with closing(socket.create_connection((host, port), timeout=timeout)):
            return True
    except OSError:
        return False


def _gcp_tunnel_check(configured: Mapping[str, str], probe: SocketProbe, timeout: float) -> DoctorCheck:
    host = configured.get("MONGO_HOST", "localhost").strip()
    direct_connection = _parse_boolean(configured.get("MONGO_DIRECT_CONNECTION", "false"))
    try:
        port = int(configured.get("MONGO_PORT", "27017"))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        return DoctorCheck(
            name="gcp-tunnel",
            status=CheckStatus.FAIL,
            summary="The configured local MongoDB tunnel port is invalid.",
            remediation="Use a TCP port from 1 through 65535 for MONGO_PORT.",
        )
    if direct_connection is None:
        return DoctorCheck(
            name="gcp-tunnel",
            status=CheckStatus.FAIL,
            summary="MONGO_DIRECT_CONNECTION is not a recognized boolean.",
            remediation="Use true for the local GCP SSH tunnel.",
        )
    if host not in {"localhost", "127.0.0.1", "::1"} or direct_connection is not True:
        return DoctorCheck(
            name="gcp-tunnel",
            status=CheckStatus.FAIL,
            summary="MongoDB settings are not coherent with a local SSH tunnel.",
            remediation="Use a loopback MONGO_HOST and MONGO_DIRECT_CONNECTION=true for the GCP tunnel.",
        )
    if not probe(host, port, timeout):
        return DoctorCheck(
            name="gcp-tunnel",
            status=CheckStatus.FAIL,
            summary="No service is listening on the configured local MongoDB tunnel port.",
            remediation="Open the documented GCP tunnel separately, then rerun this check.",
        )
    return DoctorCheck(
        name="gcp-tunnel",
        status=CheckStatus.PASS,
        summary="A TCP service is reachable on the configured local tunnel port.",
    )


def _gcp_jump_key_check(configured: Mapping[str, str]) -> DoctorCheck:
    key_path = configured.get("NMDC_JUMP_KEY") or DEFAULT_JUMP_KEY
    base_check = _jump_key_check(key_path)
    if base_check.status is CheckStatus.PASS:
        return DoctorCheck(
            name="gcp-jump-key",
            status=CheckStatus.PASS,
            summary="The GCP jump-host key exists with owner-only permissions.",
        )
    return DoctorCheck(
        name="gcp-jump-key",
        status=CheckStatus.FAIL,
        summary=base_check.summary,
        remediation=base_check.remediation,
    )


def _mongo_ping_check(
    configuration: LiveMongoConfiguration,
    client_factory: MongoClientFactory,
    timeout: float,
) -> DoctorCheck:
    timeout_ms = max(1, int(timeout * 1000))
    try:
        client = client_factory(
            configuration.settings.uri,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=timeout_ms,
            socketTimeoutMS=timeout_ms,
        )
        try:
            client.admin.command("ping")
        finally:
            client.close()
    except OperationFailure:
        return DoctorCheck(
            name="mongo-ping",
            status=CheckStatus.FAIL,
            summary="MongoDB was reached but rejected authentication or authorization.",
            remediation="Verify the MongoDB username, password, and authentication source.",
        )
    except ConfigurationError:
        return DoctorCheck(
            name="mongo-ping",
            status=CheckStatus.FAIL,
            summary="PyMongo rejected the connection configuration.",
            remediation="Review the decomposed MongoDB settings without sharing their values.",
        )
    except (ServerSelectionTimeoutError, NetworkTimeout, ConnectionFailure):
        return DoctorCheck(
            name="mongo-ping",
            status=CheckStatus.FAIL,
            summary="MongoDB was not reachable within the bounded timeout.",
            remediation="Check the network path or separately managed tunnel, then retry.",
        )
    except Exception:
        return DoctorCheck(
            name="mongo-ping",
            status=CheckStatus.FAIL,
            summary="MongoDB readiness failed with an unexpected sanitized error.",
            remediation="Review local configuration and logs without sharing credential-bearing values.",
        )
    return DoctorCheck(
        name="mongo-ping",
        status=CheckStatus.PASS,
        summary="MongoDB answered a bounded, read-only ping command.",
    )


def run_service_checks(
    requested: Sequence[str],
    *,
    configured: Mapping[str, str],
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    socket_probe: SocketProbe = _default_socket_probe,
    mongo_client_factory: MongoClientFactory = MongoClient,
) -> list[DoctorCheck]:
    """Run explicitly requested checks without starting or changing services."""
    if not requested:
        return []
    unknown = set(requested) - set(SERVICE_CHECKS)
    if unknown:
        unknown_names = ", ".join(sorted(unknown))
        valid_names = ", ".join(SERVICE_CHECKS)
        raise ValueError(f"Unknown service check(s): {unknown_names}. Valid choices: {valid_names}.")

    checks: list[DoctorCheck] = []
    configuration = None
    if "mongo-config" in requested or "mongo-ping" in requested:
        configuration, config_check = _live_mongo_configuration(configured)
        checks.append(config_check)
    if "gcp-tunnel" in requested:
        key_check = _gcp_jump_key_check(configured)
        checks.append(key_check)
        if key_check.status is CheckStatus.PASS:
            checks.append(_gcp_tunnel_check(configured, socket_probe, timeout))
    if "mongo-ping" in requested and configuration is not None:
        checks.append(_mongo_ping_check(configuration, mongo_client_factory, timeout))
    return checks
