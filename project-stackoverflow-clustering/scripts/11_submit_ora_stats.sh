#!/usr/bin/env bash
# Compute ORA-XXXXX error-code statistics from the preprocessed questions parquet
# and curate ORA-driven pair samples from the LSH similar_pairs parquet.
#
# Run after scripts/04_submit_preprocess.sh (so questions parquet has ora_codes)
# and scripts/05_submit_cluster.sh (so similar_pairs has ORA columns).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

spark_submit_project "$PROJECT_HOME/src/ora_code_stats.py" \
  --questions "$HDFS_QUESTIONS" \
  --output "${HDFS_ORA_STATS:-/user/bigdata/stackoverflow/output/ora_codes}" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

spark_submit_project "$PROJECT_HOME/src/build_ora_showcase.py" \
  --pairs "$HDFS_SIMILAR_PAIRS" \
  --output "${HDFS_ORA_SHOWCASE:-/user/bigdata/stackoverflow/output/ora_showcase}" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
