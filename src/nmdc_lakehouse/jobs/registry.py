"""In-process registry of available ETL jobs.

Jobs register themselves (or are registered explicitly) so the CLI can list
and dispatch them by name. This is intentionally lightweight so the
registry can be replaced or wrapped by an external orchestrator later.
"""

from __future__ import annotations

from typing import Callable, Dict

from nmdc_lakehouse.jobs.base import Job

_REGISTRY: Dict[str, Callable[[], Job]] = {}


def register(name: str) -> Callable[[Callable[[], Job]], Callable[[], Job]]:
    """Register a zero-arg factory that constructs a :class:`Job` under ``name``."""

    def _decorator(factory: Callable[[], Job]) -> Callable[[], Job]:
        if name in _REGISTRY:
            raise ValueError(f"Job already registered: {name}")
        _REGISTRY[name] = factory
        return factory

    return _decorator


def get(name: str) -> Job:
    """Instantiate the job registered under ``name``."""
    if name not in _REGISTRY:
        raise KeyError(f"Unknown job: {name}")
    return _REGISTRY[name]()


def list_names() -> list[str]:
    """Return the sorted list of registered job names."""
    return sorted(_REGISTRY)
