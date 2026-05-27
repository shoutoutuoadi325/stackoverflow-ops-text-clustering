#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

LOCAL_EXPORT="${1:-$PROJECT_HOME/output/hdfs_output}"

rm -rf "$LOCAL_EXPORT"
mkdir -p "$(dirname "$LOCAL_EXPORT")"
$HDFS -get -f "$HDFS_BASE/output" "$LOCAL_EXPORT"
find "$LOCAL_EXPORT" -maxdepth 3 -type f | sort | head -100
