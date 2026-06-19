#!/usr/bin/env bash
# Sequential K=150/K=200 intra-topic LSH + domain rescue with tightened ORA thresholds.
#
# Usage (on cluster master):
#   nohup bash scripts/16_run_k150_k200_pipeline.sh > /tmp/k150_k200_pipeline.log 2>&1 &
#
# Env overrides:
#   RESCUE_TITLE_SIM_FLOOR=0.55  RESCUE_TAG_SIM_FLOOR=0.50
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

export PATH="/opt/bigdata/hadoop/bin:/opt/bigdata/spark/bin:/opt/bigdata/jdk/bin:${PATH:-}"
export RESCUE_TITLE_SIM_FLOOR="${RESCUE_TITLE_SIM_FLOOR:-0.55}"
export RESCUE_TAG_SIM_FLOOR="${RESCUE_TAG_SIM_FLOOR:-0.50}"

LOG="${PIPELINE_LOG:-/tmp/k150_k200_pipeline.log}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

summarize() {
  local label="$1"
  log "--- summary ${label} baseline ---"
  hdfs dfs -cat "/user/bigdata/stackoverflow/output/intra_topic/${label}/summary_csv/part-"*.csv 2>/dev/null | tee -a "$LOG" || log "NO baseline summary ${label}"
  log "--- summary ${label} hybrid (ORA floor ${RESCUE_TITLE_SIM_FLOOR}) ---"
  hdfs dfs -cat "/user/bigdata/stackoverflow/output/intra_topic/${label}/hybrid/hybrid_summary_csv/part-"*.csv 2>/dev/null | tee -a "$LOG" || log "NO hybrid summary ${label}"
}

for K in 150 200; do
  LABEL="k${K}"
  log "========== START ${LABEL} (K=${K}) intra_topic LSH =========="
  bash "$SCRIPT_DIR/14_submit_cluster_intra_topic.sh" "$LABEL" "$K" >> "$LOG" 2>&1

  log "========== START ${LABEL} domain rescue (title>=${RESCUE_TITLE_SIM_FLOOR}, tag>=${RESCUE_TAG_SIM_FLOOR}) =========="
  bash "$SCRIPT_DIR/15_submit_domain_rescue.sh" "$LABEL" >> "$LOG" 2>&1

  summarize "$LABEL"
  log "========== DONE ${LABEL} =========="
done

log "========== PIPELINE COMPLETE k150+k200 =========="
