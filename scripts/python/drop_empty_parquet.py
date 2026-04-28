"""Delete zero-row Parquet files under a root directory.

Usage: uv run python scripts/python/drop_empty_parquet.py <root>
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    """Delete zero-row Parquet files under the given root directory."""
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <root>", file=sys.stderr)
        sys.exit(1)
    root = Path(sys.argv[1])
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        sys.exit(1)
    removed = 0
    for p in sorted(root.rglob("*.parquet")):
        meta = pq.read_metadata(p)
        if meta.num_rows == 0:
            print(f"removing {p.relative_to(root)} (0 rows)")
            p.unlink()
            removed += 1
    print(f"{removed} file(s) removed")


if __name__ == "__main__":
    main()
