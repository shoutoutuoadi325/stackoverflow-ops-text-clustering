#!/usr/bin/env bash
# Domain-aware hybrid graph: cross-topic ORA rescue on top of intra-topic LSH.
# Does NOT rerun MinHashLSH. Requires similar_pairs from scripts/14_submit_cluster_intra_topic.sh.
#
# Usage:
#   bash scripts/15_submit_domain_rescue.sh k50
#   bash scripts/15_submit_domain_rescue.sh k100
set -euo pipefail

LABEL="${1:-k50}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

INTRA_ROOT="${HDFS_INTRA_TOPIC:-${HDFS_BASE:-/user/bigdata/stackoverflow}/output/intra_topic}"
TOPICS_PATH="${HDFS_BASE:-/user/bigdata/stackoverflow}/output/topic_clusters_${LABEL}"
BASELINE_PAIRS="${INTRA_ROOT}/${LABEL}/similar_pairs"
OUTPUT="${INTRA_ROOT}/${LABEL}/hybrid"

if ! $HDFS -test -e "${BASELINE_PAIRS}/_SUCCESS" 2>/dev/null; then
  echo "Missing baseline similar_pairs at hdfs://${BASELINE_PAIRS}"
  echo "Run first: bash scripts/14_submit_cluster_intra_topic.sh ${LABEL} ${LABEL#k}"
  exit 1
fi

echo "Domain hybrid rescue label=${LABEL} (no LSH rerun)"
spark_submit_project "$PROJECT_HOME/src/domain_rescue_graph.py" \
  --label "$LABEL" \
  --features "$HDFS_FEATURES" \
  --topics "$TOPICS_PATH" \
  --baseline-pairs "$BASELINE_PAIRS" \
  --output "$OUTPUT" \
  --intra-topic-root "$INTRA_ROOT" \
  --title-sim-floor "${RESCUE_TITLE_SIM_FLOOR:-0.45}" \
  --tag-rescue-floor "${RESCUE_TAG_SIM_FLOOR:-0.40}" \
  --ora-boost "${ORA_BOOST:-0.10}" \
  --cc-iterations "${CONNECTED_COMPONENT_ITERATIONS:-15}" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"

echo "Done. Outputs under hdfs://${OUTPUT}/"
echo "  hybrid_summary_csv/ baseline_vs_hybrid_csv/ rescued_pairs_samples_csv/"
