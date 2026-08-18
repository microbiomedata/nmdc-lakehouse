"""Schema-directed ETL from NMDC MongoDB to local Parquet artifacts."""

from importlib.metadata import version

__version__ = version("nmdc-lakehouse")

__all__ = ["__version__"]
