"""Tests for the job registry scaffold."""

import pytest

from nmdc_lakehouse.jobs import registry
from nmdc_lakehouse.jobs.base import Job, JobResult


class _DummyJob(Job):
    name = "dummy"

    def run(self, *, dry_run: bool = False) -> JobResult:
        return JobResult(job_name=self.name)


def test_register_and_get(monkeypatch):
    monkeypatch.setattr(registry, "_REGISTRY", {})

    @registry.register("dummy")
    def _factory() -> Job:
        return _DummyJob()

    assert "dummy" in registry.list_names()
    job = registry.get("dummy")
    assert isinstance(job, _DummyJob)


def test_get_unknown_raises(monkeypatch):
    monkeypatch.setattr(registry, "_REGISTRY", {})
    with pytest.raises(KeyError):
        registry.get("nope")
