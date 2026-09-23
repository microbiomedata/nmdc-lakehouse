#!/usr/bin/env bash
# Keep source selection out of Just's global evaluation so diagnostics remain usable.
set -euo pipefail

case "${NMDC_SCHEMA_VERSION-11.24.0}" in
  11.23.0) source_extra=source-11-23 ;;
  11.24.0) source_extra=source-11-24 ;;
  *)
    echo "Unsupported NMDC_SCHEMA_VERSION; use 11.23.0 or 11.24.0." >&2
    exit 2
    ;;
esac

case "${1-}" in
  run|sync) uv_command=$1; shift ;;
  *) echo "Usage: uv_with_source.sh {run|sync} [arguments...]" >&2; exit 2 ;;
esac

exec uv "$uv_command" --extra "$source_extra" "$@"
