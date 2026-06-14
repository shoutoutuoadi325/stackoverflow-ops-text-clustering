#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

# A higher-recall profile for demos and threshold analysis.
# It reuses existing preprocessed features and writes to parallel output paths.
RECALL_SUFFIX="${RECALL_SUFFIX:-recall065}"
RECALL_DISTANCE_THRESHOLD="${RECALL_DISTANCE_THRESHOLD:-0.35}"
RECALL_SIMILARITY_THRESHOLD="${RECALL_SIMILARITY_THRESHOLD:-0.65}"
RECALL_MINHASH_TABLES="${RECALL_MINHASH_TABLES:-2}"
RECALL_MAX_BUCKET_SIZE="${RECALL_MAX_BUCKET_SIZE:-200}"
RECALL_JOIN_STRATEGY="${RECALL_JOIN_STRATEGY:-bucket}"

RECALL_PAIRS="${HDFS_SIMILAR_PAIRS}_${RECALL_SUFFIX}"
RECALL_CLUSTERS="${HDFS_CLUSTERS}_${RECALL_SUFFIX}"

spark_submit_project "$PROJECT_HOME/src/cluster_lsh.py" \
  --features "$HDFS_FEATURES" \
  --output "$RECALL_PAIRS" \
  --distance-threshold "$RECALL_DISTANCE_THRESHOLD" \
  --similarity-threshold "$RECALL_SIMILARITY_THRESHOLD" \
  --num-hash-tables "$RECALL_MINHASH_TABLES" \
  --max-bucket-size "$RECALL_MAX_BUCKET_SIZE" \
  --join-strategy "$RECALL_JOIN_STRATEGY" \
  --top-n-per-doc "$LSH_TOP_N_PER_DOC" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

spark_submit_project "$PROJECT_HOME/src/connected_components.py" \
  --questions "$HDFS_QUESTIONS" \
  --pairs "$RECALL_PAIRS" \
  --output "$RECALL_CLUSTERS" \
  --iterations "$CONNECTED_COMPONENT_ITERATIONS" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
