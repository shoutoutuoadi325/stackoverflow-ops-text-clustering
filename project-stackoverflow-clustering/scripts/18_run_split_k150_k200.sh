#!/usr/bin/env bash
# Stable large-K experiment without retraining BisectingKMeans.
#
# Builds topic_clusters_k150_split / k200_split by splitting existing low-K
# topics, then runs the unchanged intra-topic LSH and domain rescue.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

export PATH="/opt/bigdata/hadoop/bin:/opt/bigdata/spark/bin:/opt/bigdata/jdk/bin:${PATH:-}"
export RESCUE_TITLE_SIM_FLOOR="${RESCUE_TITLE_SIM_FLOOR:-0.55}"
export RESCUE_TAG_SIM_FLOOR="${RESCUE_TAG_SIM_FLOOR:-0.50}"

BASE_LABEL="${BASE_LABEL:-k50}"
HDFS_BASE_DIR="${HDFS_BASE:-/user/bigdata/stackoverflow}"
BASE_TOPICS="${HDFS_BASE_DIR}/output/topic_clusters_${BASE_LABEL}"
INTRA_OUT="${HDFS_BASE_DIR}/output/intra_topic"

run_one() {
  local target_k="$1"
  local label="k${target_k}_split"
  local topics="${HDFS_BASE_DIR}/output/topic_clusters_${label}"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] START ${label}: split ${BASE_LABEL} -> K=${target_k}"
  spark_submit_project "$PROJECT_HOME/src/split_topic_clusters.py" \
    --base-topics "$BASE_TOPICS" \
    --features "$HDFS_FEATURES" \
    --output "$topics" \
    --target-k "$target_k" \
    --shuffle-partitions "$SHUFFLE_PARTITIONS"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] START ${label}: intra-topic LSH"
  spark_submit_project "$PROJECT_HOME/src/cluster_lsh_intra_topic.py" \
    --features "$HDFS_FEATURES" \
    --topics "$topics" \
    --output "$INTRA_OUT" \
    --label "$label" \
    --similarity-threshold "${INTRA_SIM_THRESHOLD:-0.65}" \
    --distance-threshold "${INTRA_DIST_THRESHOLD:-0.35}" \
    --num-hash-tables "${INTRA_HASH_TABLES:-4}" \
    --top-n-per-doc "${LSH_TOP_N_PER_DOC:-10}" \
    --min-topic-size "${MIN_TOPIC_SIZE:-2}" \
    --max-topics "${MAX_TOPICS:-0}" \
    --ora-boost "${ORA_BOOST:-0.10}" \
    --ora-rescue-floor "${ORA_RESCUE_FLOOR:-0.0}" \
    --shuffle-partitions "$SHUFFLE_PARTITIONS"

  # domain_rescue_graph imports cluster_lsh_intra_topic.py, so keep this step in
  # client mode where the project source directory is on PYTHONPATH.
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] START ${label}: domain rescue title>=${RESCUE_TITLE_SIM_FLOOR}, tag>=${RESCUE_TAG_SIM_FLOOR}"
  DEPLOY_MODE=client DRIVER_MEMORY="${RESCUE_DRIVER_MEMORY:-2g}" EXECUTOR_MEMORY="${RESCUE_EXECUTOR_MEMORY:-4g}" \
    bash "$SCRIPT_DIR/15_submit_domain_rescue.sh" "$label"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUMMARY ${label} baseline"
  hdfs dfs -cat "${INTRA_OUT}/${label}/summary_csv/part-"*.csv 2>/dev/null || true
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] SUMMARY ${label} hybrid"
  hdfs dfs -cat "${INTRA_OUT}/${label}/hybrid/hybrid_summary_csv/part-"*.csv 2>/dev/null || true
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] DONE ${label}"
}

if ! $HDFS -test -e "$BASE_TOPICS/_SUCCESS" 2>/dev/null; then
  echo "Missing base topics: hdfs://${BASE_TOPICS}" >&2
  exit 1
fi

for k in 150 200; do
  run_one "$k"
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] SPLIT PIPELINE COMPLETE"
