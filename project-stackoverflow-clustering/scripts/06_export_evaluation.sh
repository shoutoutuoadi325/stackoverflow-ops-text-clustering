#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

SWEEP_OUTPUT_BASE="${SWEEP_OUTPUT_BASE:-$HDFS_BASE/output/evaluation/sweep}"
LOCAL_EVALUATION_DIR="${LOCAL_EVALUATION_DIR:-$PROJECT_HOME/output/evaluation/sweep}"

mkdir -p "$LOCAL_EVALUATION_DIR"
for run_path in $($HDFS -ls "$SWEEP_OUTPUT_BASE" 2>/dev/null | awk '{print $8}'); do
  run_name="$(basename "$run_path")"
  mkdir -p "$LOCAL_EVALUATION_DIR/$run_name"
  for child in lsh_metrics_csv cluster_metrics_csv runtime_csv similar_pairs_samples_csv clusters_samples_csv; do
    if $HDFS -test -e "$run_path/$child"; then
      rm -rf "$LOCAL_EVALUATION_DIR/$run_name/$child"
      $HDFS -get "$run_path/$child" "$LOCAL_EVALUATION_DIR/$run_name/$child"
    fi
  done
done

python3 "$PROJECT_HOME/src/summarize_sweep_results.py" \
  --input-root "$PROJECT_HOME/output/evaluation/sweep" \
  --output "$PROJECT_HOME/output/evaluation/parameter_sweep_summary.csv"
python3 "$PROJECT_HOME/src/sync_report_samples.py" --project-root "$PROJECT_HOME"
