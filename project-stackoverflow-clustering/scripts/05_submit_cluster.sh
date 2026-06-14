#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

spark_submit_project "$PROJECT_HOME/src/feature_engineering.py" \
  --input "$HDFS_QUESTIONS" \
  --output "$HDFS_FEATURES" \
  --num-features "$NUM_FEATURES" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

spark_submit_project "$PROJECT_HOME/src/topic_clustering.py" \
  --features "$HDFS_FEATURES" \
  --output "$HDFS_TOPIC_CLUSTERS" \
  --k "$TOPIC_K" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

spark_submit_project "$PROJECT_HOME/src/cluster_lsh.py" \
  --features "$HDFS_FEATURES" \
  --output "$HDFS_SIMILAR_PAIRS" \
  --distance-threshold "$LSH_DISTANCE_THRESHOLD" \
  --similarity-threshold "$SIMILARITY_THRESHOLD" \
  --num-hash-tables "$MINHASH_TABLES" \
  --max-bucket-size "$LSH_MAX_BUCKET_SIZE" \
  --join-strategy "${LSH_JOIN_STRATEGY:-approx}" \
  --top-n-per-doc "$LSH_TOP_N_PER_DOC" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

spark_submit_project "$PROJECT_HOME/src/connected_components.py" \
  --questions "$HDFS_QUESTIONS" \
  --pairs "$HDFS_SIMILAR_PAIRS" \
  --output "$HDFS_CLUSTERS" \
  --iterations "$CONNECTED_COMPONENT_ITERATIONS" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
