#!/usr/bin/env bash
# Reproduce 2x large-K vector-folding controls: k150c2 and k200c2.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

export PATH="/opt/bigdata/hadoop/bin:/opt/bigdata/spark/bin:/opt/bigdata/jdk/bin:${PATH:-}"
export TOPIC_TRAINING_NUM_FEATURES="${TOPIC_TRAINING_NUM_FEATURES:-131072}"
export TOPIC_SKIP_SILHOUETTE="${TOPIC_SKIP_SILHOUETTE:-1}"
export RESCUE_TITLE_SIM_FLOOR="${RESCUE_TITLE_SIM_FLOOR:-0.55}"
export RESCUE_TAG_SIM_FLOOR="${RESCUE_TAG_SIM_FLOOR:-0.50}"

for K in 150 200; do
  LABEL="k${K}c2"
  echo "[$(date ''+%Y-%m-%d %H:%M:%S'')] START ${LABEL}: K=${K}, topic_dims=${TOPIC_TRAINING_NUM_FEATURES}"
  bash "$SCRIPT_DIR/14_submit_cluster_intra_topic.sh" "$LABEL" "$K"
  DEPLOY_MODE=client DRIVER_MEMORY="${RESCUE_DRIVER_MEMORY:-2g}" EXECUTOR_MEMORY="${RESCUE_EXECUTOR_MEMORY:-4g}" \
    bash "$SCRIPT_DIR/15_submit_domain_rescue.sh" "$LABEL"
  echo "[$(date ''+%Y-%m-%d %H:%M:%S'')] DONE ${LABEL}"
done
