#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

python3 "$PROJECT_HOME/src/local_similarity_sweep.py" \
  --input "$LOCAL_DATA_DIR/$RAW_JSON" \
  --output-dir "$PROJECT_HOME/output/evaluation/local_title_sweep"

python3 "$PROJECT_HOME/src/build_review_candidates.py" \
  --baseline "$PROJECT_HOME/output/hdfs_output/similar_pairs_samples_csv" \
  --local-samples "$PROJECT_HOME/output/evaluation/local_title_sweep/similar_pairs_samples.csv" \
  --input-root "$PROJECT_HOME/output/evaluation" \
  --output "$PROJECT_HOME/evaluation/review_candidates.csv"

python3 "$PROJECT_HOME/src/evaluate_review_labels.py" \
  --input "$PROJECT_HOME/evaluation/review_candidates.csv" \
  --output "$PROJECT_HOME/output/evaluation/review_metrics.json"

python3 "$PROJECT_HOME/src/summarize_sweep_results.py" \
  --input-root "$PROJECT_HOME/output/evaluation/sweep" \
  --output "$PROJECT_HOME/output/evaluation/parameter_sweep_summary.csv"

python3 "$PROJECT_HOME/src/build_demo_assets.py" \
  --output-root "$PROJECT_HOME/output/hdfs_output" \
  --validation "$PROJECT_HOME/output/validation/raw_data_validation.json" \
  --sweep-summary "$PROJECT_HOME/output/evaluation/parameter_sweep_summary.csv" \
  --review-candidates "$PROJECT_HOME/evaluation/review_candidates.csv" \
  --review-metrics "$PROJECT_HOME/output/evaluation/review_metrics.json" \
  --demo-dir "$PROJECT_HOME/demo"
