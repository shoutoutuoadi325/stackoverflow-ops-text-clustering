#!/usr/bin/env bash
# Runs ON cluster master — monitors K150/K200 pipeline, auto-resumes on failure.
set -uo pipefail

REMOTE="/opt/bigdata/project-stackoverflow-clustering"
LOG="/tmp/k150_k200_pipeline.log"
MON="/tmp/k150_k200_monitor.log"
POLL=180
MAX_RESTARTS=5
RESTARTS=0

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$MON"; }

apply_safe_config() {
  local drv="${1:-4g}" exe="${2:-4g}" oh="${3:-1024m}"
  sed -i 's/^DEPLOY_MODE=.*/DEPLOY_MODE=${DEPLOY_MODE:-cluster}/' "$REMOTE/conf/app.conf"
  sed -i "s/^DRIVER_MEMORY=.*/DRIVER_MEMORY=\${DRIVER_MEMORY:-${drv}}/" "$REMOTE/conf/app.conf"
  sed -i "s/^EXECUTOR_MEMORY=.*/EXECUTOR_MEMORY=\${EXECUTOR_MEMORY:-${exe}}/" "$REMOTE/conf/app.conf"
  sed -i "s/^EXECUTOR_MEMORY_OVERHEAD=.*/EXECUTOR_MEMORY_OVERHEAD=\${EXECUTOR_MEMORY_OVERHEAD:-${oh}}/" "$REMOTE/conf/app.conf"
  sed -i 's/^NUM_EXECUTORS=.*/NUM_EXECUTORS=${NUM_EXECUTORS:-1}/' "$REMOTE/conf/app.conf"
}

resume_pipeline() {
  export PATH="/opt/bigdata/hadoop/bin:/opt/bigdata/spark/bin:/opt/bigdata/jdk/bin:${PATH:-}"
  export RESCUE_TITLE_SIM_FLOOR=0.55 RESCUE_TAG_SIM_FLOOR=0.50
  source "$REMOTE/scripts/common.sh"
  for K in 150 200; do
    L="k${K}"
    if ! hdfs dfs -test -e "/user/bigdata/stackoverflow/output/intra_topic/${L}/summary_csv/_SUCCESS" 2>/dev/null; then
      log "RESUME ${L} intra_topic"
      bash "$REMOTE/scripts/14_submit_cluster_intra_topic.sh" "$L" "$K" >> "$LOG" 2>&1 || return 1
    fi
    if ! hdfs dfs -test -e "/user/bigdata/stackoverflow/output/intra_topic/${L}/hybrid/hybrid_summary_csv/_SUCCESS" 2>/dev/null; then
      log "RESUME ${L} domain_rescue"
      bash "$REMOTE/scripts/15_submit_domain_rescue.sh" "$L" >> "$LOG" 2>&1 || return 1
    fi
    log "DONE ${L}"
  done
  log "PIPELINE COMPLETE k150+k200"
}

log "cluster monitor started (poll=${POLL}s)"

while true; do
  if grep -q "PIPELINE COMPLETE k150+k200" "$LOG" 2>/dev/null; then
    log "detected PIPELINE COMPLETE — exit 0"
    exit 0
  fi

  RUNNING=$(pgrep -f '16_run_k150_k200_pipeline.sh|14_submit_cluster_intra_topic|15_submit_domain_rescue|topic_clustering|cluster_lsh_intra|domain_rescue_graph' | wc -l)
  AVAIL=$(grep MemAvailable /proc/meminfo | awk '{print int($2/1024)}')
  TAIL=$(tail -5 "$LOG" 2>/dev/null | tr '\n' ' ')

  log "running_procs=${RUNNING} mem_avail_mb=${AVAIL} tail=${TAIL:0:120}"

  FAIL=0
  if tail -60 "$LOG" 2>/dev/null | grep -qiE 'insufficient memory|Cannot allocate memory|Java heap space'; then FAIL=1; fi
  if tail -60 "$LOG" 2>/dev/null | grep -qiE 'ApplicationMaster exited|Job aborted due to stage failure|Py4JJavaError'; then FAIL=1; fi
  if [[ "$RUNNING" -eq 0 ]] && ! grep -q "PIPELINE COMPLETE" "$LOG" 2>/dev/null; then
    # idle >15 min with no process => likely crashed (BisectingKMeans can pause logging)
    LAST=$(stat -c %Y "$LOG" 2>/dev/null || echo 0)
    NOW=$(date +%s)
    if (( NOW - LAST > 900 )); then FAIL=1; fi
  fi

  if [[ "$FAIL" -eq 1 ]] && [[ "$RESTARTS" -lt "$MAX_RESTARTS" ]]; then
    RESTARTS=$((RESTARTS + 1))
    log "FAIL detected — auto-resume attempt ${RESTARTS}/${MAX_RESTARTS}"
    if tail -60 "$LOG" 2>/dev/null | grep -qi 'Java heap space'; then
      apply_safe_config "4g" "4g" "1024m"
    else
      apply_safe_config "4g" "4g" "1024m"
    fi
    pkill -f '16_run_k150_k200_pipeline.sh' 2>/dev/null || true
    sleep 5
    resume_pipeline >> "$LOG" 2>&1 || log "resume failed this round"
  fi

  sleep "$POLL"
done
