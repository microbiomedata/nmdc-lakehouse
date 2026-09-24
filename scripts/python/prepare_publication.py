"""Select the configured source extra before importing the publication command."""

import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Use the existing source selector; keep credentials in the inherited environment."""
    if len(sys.argv) != 3:
        print("Usage: prepare_publication.py CONFIGURATION OUTPUT", file=sys.stderr)
        return 2
    config = Path(sys.argv[1]).expanduser().absolute()
    output = Path(sys.argv[2]).expanduser().absolute()
    try:
        source = json.loads(config.read_text())["source_version"]
        if not isinstance(source, str):
            raise ValueError
    except (OSError, ValueError, KeyError, TypeError):
        print("Configuration must be a JSON object with a string source_version.", file=sys.stderr)
        return 2
    repo = Path(__file__).resolve().parents[2]
    return subprocess.run(
        [
            "bash",
            "scripts/uv_with_source.sh",
            "run",
            "--locked",
            "nmdc-lakehouse",
            "prepare-publication",
            str(config),
            str(output),
        ],
        cwd=repo,
        env=dict(os.environ, NMDC_SCHEMA_VERSION=source),
        check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
