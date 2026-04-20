"""Central configuration for nmdc-lakehouse.

Settings are loaded from environment variables (optionally via a `.env` file)
using pydantic-settings. This module only declares the shape of configuration;
actual connection logic lives in :mod:`nmdc_lakehouse.sources`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoSettings(BaseSettings):
    """Connection settings for the NMDC MongoDB backend."""

    model_config = SettingsConfigDict(env_prefix="MONGO_", env_file=".env", extra="ignore")

    host: str = "localhost"
    port: int = 27017
    db: str = "nmdc"
    username: str = "admin"
    password: str = ""
    replica_set: Optional[str] = None


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
