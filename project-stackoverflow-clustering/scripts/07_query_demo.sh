#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

spark_submit_project "$PROJECT_HOME/src/query_demo.py" \
  --clusters "$HDFS_CLUSTERS" \
  --pairs "$HDFS_SIMILAR_PAIRS" \
  "$@"
