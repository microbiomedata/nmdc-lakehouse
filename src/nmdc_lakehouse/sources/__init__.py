"""Data sources.

Thin adapters around :mod:`linkml_store` clients for the NMDC MongoDB and
(optionally) PostgreSQL backends. Each source exposes an iterable of NMDC
records for downstream flattening.
"""
