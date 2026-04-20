"""Base classes for ETL jobs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class JobResult:
    """Summary record emitted by a completed job."""

    job_name: str
    rows_read: int = 0
    rows_written: int = 0
    tables_written: tuple[str, ...] = ()


class Job(ABC):
    """Abstract base class for a single ETL job."""

    name: str

    @abstractmethod
    def run(self, *, dry_run: bool = False) -> JobResult:
        """Execute the job and return a :class:`JobResult`."""
        raise NotImplementedError
