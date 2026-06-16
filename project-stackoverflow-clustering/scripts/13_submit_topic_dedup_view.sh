#!/usr/bin/env bash
# Build the per-topic deduplication cross view by joining topic_clusters and
# clusters output. Run after scripts/05_submit_cluster.sh has produced both.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

OUTPUT_DIR="${HDFS_TOPIC_DEDUP_VIEW:-/user/bigdata/stackoverflow/output/topic_dedup_view}"

spark_submit_project "$PROJECT_HOME/src/build_topic_dedup_view.py" \
  --topics "$HDFS_TOPIC_CLUSTERS" \
  --clusters "$HDFS_CLUSTERS" \
  --output "$OUTPUT_DIR" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
