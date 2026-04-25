"""Central configuration for nmdc-lakehouse.

Settings are loaded from environment variables (optionally via a `.env` file)
using pydantic-settings. This module only declares the shape of configuration;
actual connection logic lives in :mod:`nmdc_lakehouse.sources`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import quote

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoSettings(BaseSettings):
    """Connection settings for the NMDC MongoDB backend."""

    model_config = SettingsConfigDict(env_prefix="MONGO_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 27017
    dbname: str = "nmdc"
    username: str = "admin"
    password: str = ""
    auth_source: str = "admin"
    replica_set: Optional[str] = None
    direct_connection: bool = False

    @property
    def uri(self) -> str:
        """Assemble a MongoDB connection URI from the decomposed fields.

        Includes credentials only when ``password`` is non-empty (typical
        local-dev MongoDB runs without auth). Username and password are
        percent-escaped. Appends query parameters for ``replicaSet`` and/or
        ``directConnection`` when set.

        ``authSource`` is appended whenever ``password`` is set (almost always
        ``admin`` for NMDC). ``direct_connection=True`` is required when
        connecting through an SSH tunnel to a replica-set MongoDB: the server
        advertises internal hostnames that are unreachable from outside the
        cluster, so pymongo must skip replica-set discovery and use only the
        provided host:port.
        """
        auth = ""
        if self.password:
            auth = f"{quote(self.username, safe='')}:{quote(self.password, safe='')}@"
        base = f"mongodb://{auth}{self.host}:{self.port}/{self.dbname}"
        params: list[str] = []
        if self.password:
            params.append(f"authSource={quote(self.auth_source, safe='')}")
        if self.replica_set:
            params.append(f"replicaSet={quote(self.replica_set, safe='')}")
        if self.direct_connection:
            params.append("directConnection=true")
        if params:
            base += "?" + "&".join(params)
        return base


class PostgresSettings(BaseSettings):
    """Connection settings for an optional NMDC PostgreSQL backend."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", env_file=".env", extra="ignore")

    dsn: Optional[str] = None


class LakehouseSettings(BaseSettings):
    """Settings for the lakehouse output layer."""

    model_config = SettingsConfigDict(env_prefix="LAKEHOUSE_", env_file=".env", extra="ignore")

    root: Path = Field(default=Path("./lakehouse"))


class Settings(BaseSettings):
    """Aggregate settings object used across the package."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo: MongoSettings = Field(default_factory=MongoSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    lakehouse: LakehouseSettings = Field(default_factory=LakehouseSettings)


def get_settings() -> Settings:
    """Return a fresh Settings instance. Wrap in a cache in callers if needed."""
    return Settings()
