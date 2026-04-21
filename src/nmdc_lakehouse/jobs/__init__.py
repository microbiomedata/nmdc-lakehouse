"""ETL jobs.

A job wires a :class:`~nmdc_lakehouse.sources.base.Source`, one or more
:class:`~nmdc_lakehouse.transforms.base.Transform` s, and a
:class:`~nmdc_lakehouse.sinks.base.Sink` into a runnable unit. Jobs are
registered in :mod:`nmdc_lakehouse.jobs.registry` and dispatched by
:mod:`nmdc_lakehouse.cli` (or by an external orchestrator).

Importing this package registers all built-in jobs.
"""

from nmdc_lakehouse.jobs import biosample_to_parquet as _  # noqa: F401
