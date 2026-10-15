#!/usr/bin/env bash
# Keep source selection out of Just's global evaluation so diagnostics remain usable.
# The supported releases are the source-<major>-<minor> extras in pyproject.toml;
# src/nmdc_lakehouse/source_versions.py reads the same extras.
set -euo pipefail

pyproject="$(dirname "$0")/../pyproject.toml"
supported=$(sed -nE 's/^source-[0-9]+-[0-9]+ = \["nmdc-schema==([0-9]+\.[0-9]+\.[0-9]+(rc[0-9]+)?)"\]$/\1/p' "$pyproject")
default=$(printf '%s\n' "$supported" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -t. -k1,1n -k2,2n -k3,3n | tail -n 1 || true)
selected=${NMDC_SCHEMA_VERSION-$default}

if [ -z "$selected" ] || ! printf '%s\n' "$supported" | grep -qxF -- "$selected"; then
  echo "Unsupported NMDC_SCHEMA_VERSION; use one of: ${supported//$'\n'/ }" >&2
  exit 2
fi
IFS=. read -r major minor _ <<<"$selected"
source_extra="source-$major-$minor"

case "${1-}" in
  run|sync) uv_command=$1; shift ;;
  *) echo "Usage: uv_with_source.sh {run|sync} [arguments...]" >&2; exit 2 ;;
esac

exec uv "$uv_command" --extra "$source_extra" "$@"
