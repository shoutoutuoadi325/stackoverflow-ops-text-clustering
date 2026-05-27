#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

LOCAL_RAW="${1:-$LOCAL_DATA_DIR/$RAW_JSON}"

if [[ ! -f "$LOCAL_RAW" ]]; then
  echo "Raw JSON not found: $LOCAL_RAW" >&2
  exit 1
fi

$HDFS -mkdir -p "$HDFS_BASE/raw"
$HDFS -put -f "$LOCAL_RAW" "$HDFS_BASE/raw/"
$HDFS -ls -h "$HDFS_BASE/raw"
