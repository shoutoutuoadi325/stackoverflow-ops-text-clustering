#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

spark_submit_project "$PROJECT_HOME/src/eda.py" \
  --input "$HDFS_JSONL" \
  --input-format jsonl \
  --output "$HDFS_EDA" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
