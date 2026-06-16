#!/usr/bin/env bash
# Submit the tag-partitioned MinHashLSH job. Splits the corpus by primary tag
# (first non-`oracle-database` tag) and runs an independent LSH per bucket,
# then unions the results. See src/cluster_lsh_partitioned.py for the
# motivation and trade-off.
#
# Usage:
#   bash scripts/12_submit_cluster_partitioned.sh [extra spark args]
#
# Override knobs via env vars (see conf/app.conf): SIMILARITY_THRESHOLD,
# LSH_DISTANCE_THRESHOLD, MINHASH_TABLES, MIN_TAG_BUCKET_SIZE,
# PRIMARY_TAG_EXCLUDE, ORA_BOOST, ORA_RESCUE_FLOOR.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

OUTPUT_DIR="${HDFS_SIMILAR_PAIRS_PARTITIONED:-/user/bigdata/stackoverflow/output/similar_pairs_partitioned}"
METRICS_DIR="${HDFS_SIMILAR_PAIRS_PARTITIONED:-/user/bigdata/stackoverflow/output/similar_pairs_partitioned}_metrics_csv"

spark_submit_project "$PROJECT_HOME/src/cluster_lsh_partitioned.py" \
  --features "$HDFS_FEATURES" \
  --output "$OUTPUT_DIR" \
  --metrics-output "$METRICS_DIR" \
  --distance-threshold "$LSH_DISTANCE_THRESHOLD" \
  --similarity-threshold "$SIMILARITY_THRESHOLD" \
  --num-hash-tables "$MINHASH_TABLES" \
  --top-n-per-doc "$LSH_TOP_N_PER_DOC" \
  --min-bucket-size "${MIN_TAG_BUCKET_SIZE:-50}" \
  --exclude-tags "${PRIMARY_TAG_EXCLUDE:-oracle-database}" \
  --max-buckets "${MAX_TAG_BUCKETS:-0}" \
  --ora-boost "${ORA_BOOST:-0.10}" \
  --ora-rescue-floor "${ORA_RESCUE_FLOOR:-0.0}" \
  --shuffle-partitions "$SHUFFLE_PARTITIONS"
