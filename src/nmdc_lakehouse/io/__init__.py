"""Bulk data-file handling.

Utilities for staging and referencing the large genomic sequence (and other
bulk) files that accompany NMDC metadata records. Flattened lakehouse rows
hold pointers (URIs, checksums, sizes) rather than inlining the payload.
"""
