#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

# Override these env vars for smoke runs, for example:
# SWEEP_SIMILARITIES="0.75 0.85" SWEEP_HASH_TABLES="2" SWEEP_BUCKET_SIZES="200" bash scripts/05_submit_cluster_sweep.sh
SWEEP_SIMILARITIES="${SWEEP_SIMILARITIES:-0.65 0.70 0.75 0.80 0.85}"
SWEEP_HASH_TABLES="${SWEEP_HASH_TABLES:-2 4 8}"
SWEEP_BUCKET_SIZES="${SWEEP_BUCKET_SIZES:-200 300 500}"
SWEEP_OUTPUT_BASE="${SWEEP_OUTPUT_BASE:-$HDFS_BASE/output/evaluation/sweep}"
SWEEP_JOIN_STRATEGY="${SWEEP_JOIN_STRATEGY:-bucket}"

for similarity in $SWEEP_SIMILARITIES; do
  distance="$(python3 -c "print(round(1.0 - float('$similarity'), 4))")"
  sim_tag="$(python3 -c "print(str('$similarity').replace('.', ''))")"
  for hash_tables in $SWEEP_HASH_TABLES; do
    for bucket_size in $SWEEP_BUCKET_SIZES; do
      run_name="sim${sim_tag}_ht${hash_tables}_bucket${bucket_size}"
      run_base="$SWEEP_OUTPUT_BASE/$run_name"
      pairs_path="$run_base/similar_pairs"
      clusters_path="$run_base/clusters"

      echo "=== Running $run_name ==="
      started_at="$(date +%s)"
      spark_submit_project "$PROJECT_HOME/src/cluster_lsh.py" \
        --features "$HDFS_FEATURES" \
        --output "$pairs_path" \
        --metrics-output "$run_base/lsh_metrics_csv" \
        --distance-threshold "$distance" \
        --similarity-threshold "$similarity" \
        --num-hash-tables "$hash_tables" \
        --max-bucket-size "$bucket_size" \
        --join-strategy "$SWEEP_JOIN_STRATEGY" \
        --top-n-per-doc "$LSH_TOP_N_PER_DOC" \
        --shuffle-partitions "$SHUFFLE_PARTITIONS"

      spark_submit_project "$PROJECT_HOME/src/connected_components.py" \
        --questions "$HDFS_QUESTIONS" \
        --pairs "$pairs_path" \
        --output "$clusters_path" \
        --metrics-output "$run_base/cluster_metrics_csv" \
        --iterations "$CONNECTED_COMPONENT_ITERATIONS" \
        --shuffle-partitions "$SHUFFLE_PARTITIONS"
      ended_at="$(date +%s)"
      echo "run_name,elapsed_seconds" > "/tmp/${run_name}_runtime.csv"
      echo "$run_name,$((ended_at - started_at))" >> "/tmp/${run_name}_runtime.csv"
      $HDFS -mkdir -p "$run_base/runtime_csv"
      $HDFS -put -f "/tmp/${run_name}_runtime.csv" "$run_base/runtime_csv/part-00000.csv"
    done
  done
done
