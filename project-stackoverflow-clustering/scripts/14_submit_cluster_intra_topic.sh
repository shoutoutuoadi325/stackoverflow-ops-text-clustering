#!/usr/bin/env bash
# Two-stage duplicate detection: BisectingKMeans topic clustering followed by
# intra-cluster MinHashLSH. Designed to be called twice with different K to
# produce K=50 vs K=150 comparison numbers in one go.
#
# Usage:
#   bash scripts/14_submit_cluster_intra_topic.sh k50 50
#   bash scripts/14_submit_cluster_intra_topic.sh k150 150
#
# Reuses an existing topic_clusters parquet when its directory name matches
# (topic_clusters_k50 / topic_clusters_k150). Pass FORCE_RETRAIN=1 to
# unconditionally rerun BisectingKMeans.
#
# Knobs (env, default inherited from conf/app.conf when applicable):
#   INTRA_SIM_THRESHOLD     similarity floor inside each topic (default 0.65)
#   INTRA_DIST_THRESHOLD    LSH distance threshold (default 0.35)
#   INTRA_HASH_TABLES       MinHashLSH numHashTables (default 4)
#   MAX_TOPICS              if >0, only run on the largest N topics (smoke)
#   TOPIC_TRAINING_NUM_FEATURES  fold high-dimensional TF-IDF vectors for
#                                BisectingKMeans training only (0 keeps original)
#   TOPIC_SKIP_SILHOUETTE   1 skips the expensive silhouette pass
#   ORA_BOOST / ORA_RESCUE_FLOOR  forwarded from conf/app.conf
set -euo pipefail

LABEL="${1:-k50}"
TOPIC_K_ARG="${2:-50}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

TOPICS_PATH="${HDFS_BASE:-/user/bigdata/stackoverflow}/output/topic_clusters_${LABEL}"
INTRA_OUT="${HDFS_BASE:-/user/bigdata/stackoverflow}/output/intra_topic"

NEED_TRAIN=1
if [[ "${FORCE_RETRAIN:-0}" != "1" ]]; then
  if $HDFS -test -e "$TOPICS_PATH/_SUCCESS" 2>/dev/null; then
    NEED_TRAIN=0
    echo "Reusing existing topic clusters at hdfs://$TOPICS_PATH"
  fi
fi

if [[ "$NEED_TRAIN" == "1" ]]; then
  echo "Training BisectingKMeans with K=$TOPIC_K_ARG -> $TOPICS_PATH"
  topic_args=(
    --features "$HDFS_FEATURES"
    --output "$TOPICS_PATH"
    --k "$TOPIC_K_ARG"
    --shuffle-partitions "$SHUFFLE_PARTITIONS"
    --training-num-features "${TOPIC_TRAINING_NUM_FEATURES:-0}"
  )
  if [[ "${TOPIC_SKIP_SILHOUETTE:-0}" == "1" ]]; then
    topic_args+=(--skip-silhouette)
  fi
  spark_submit_project "$PROJECT_HOME/src/topic_clustering.py" \
    "${topic_args[@]}"
fi

echo "Running intra-topic MinHashLSH (label=$LABEL, sim>=${INTRA_SIM_THRESHOLD:-0.65})"
spark_submit_project "$PROJECT_HOME/src/cluster_lsh_intra_topic.py" \
  --features "$HDFS_FEATURES" \
  --topics "$TOPICS_PATH" \
  --output "$INTRA_OUT" \
  --label "$LABEL" \
  --similarity-threshold "${INTRA_SIM_THRESHOLD:-0.65}" \
  --distance-threshold "${INTRA_DIST_THRESHOLD:-0.35}" \
  --num-hash-tables "${INTRA_HASH_TABLES:-4}" \
  --top-n-per-doc "${LSH_TOP_N_PER_DOC:-10}" \
  --min-topic-size "${MIN_TOPIC_SIZE:-2}" \
  --max-topics "${MAX_TOPICS:-0}" \
  --ora-boost "${ORA_BOOST:-0.10}" \
  --ora-rescue-floor "${ORA_RESCUE_FLOOR:-0.0}" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

echo "Done. Outputs under hdfs://$INTRA_OUT/$LABEL/"
