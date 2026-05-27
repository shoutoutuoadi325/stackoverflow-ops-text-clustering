#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

spark_submit_project "$PROJECT_HOME/src/preprocess.py" \
  --input "$HDFS_JSONL" \
  --output "$HDFS_QUESTIONS" \
  --top-answers "$TOP_ANSWERS" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
