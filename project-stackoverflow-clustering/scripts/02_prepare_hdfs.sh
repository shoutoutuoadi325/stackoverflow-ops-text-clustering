#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

LOCAL_JSONL="${1:-$PROJECT_HOME/output/questions.jsonl}"

if [[ ! -f "$LOCAL_JSONL" ]]; then
  echo "JSONL not found: $LOCAL_JSONL" >&2
  exit 1
fi

$HDFS -mkdir -p "$HDFS_BASE/jsonl" "$HDFS_BASE/parquet" "$HDFS_BASE/output"
$HDFS -put -f "$LOCAL_JSONL" "$HDFS_JSONL"
$HDFS -ls -h "$HDFS_BASE/jsonl"
