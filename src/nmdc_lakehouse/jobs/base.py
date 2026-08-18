"""Base classes for ETL jobs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class OutputResult:
    """Measured local output produced by a completed job."""

    table: str
    path: str
    rows: int
    bytes: int


@dataclass
class JobResult:
    """Summary record emitted by a completed job."""

    job_name: str
    rows_read: int = 0
    rows_written: int = 0
    tables_written: tuple[str, ...] = ()
    table_rows: tuple[tuple[str, int], ...] = ()
    started_at: str | None = None
    finished_at: str | None = None
    elapsed_seconds: float | None = None
    output_root: str | None = None
    outputs: tuple[OutputResult, ...] = ()
    children: tuple[JobResult, ...] = ()


class Job(ABC):
    """Abstract base class for a single ETL job."""

    name: str

    @abstractmethod
    def run(self, *, dry_run: bool = False) -> JobResult:
        """Execute the job and return a :class:`JobResult`."""
        raise NotImplementedError
