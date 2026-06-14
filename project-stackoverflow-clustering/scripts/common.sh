#!/usr/bin/env bash
set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
CONF_FILE="${CONF_FILE:-$PROJECT_HOME/conf/app.conf}"

if [[ -f "$CONF_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CONF_FILE"
fi

SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
HDFS="${HDFS:-hdfs dfs}"

spark_submit_project() {
  "$SPARK_SUBMIT" \
    --master "${SPARK_MASTER:-yarn}" \
    --deploy-mode "${DEPLOY_MODE:-client}" \
    --driver-memory "${DRIVER_MEMORY:-4g}" \
    --executor-memory "${EXECUTOR_MEMORY:-4g}" \
    --executor-cores "${EXECUTOR_CORES:-2}" \
    --num-executors "${NUM_EXECUTORS:-4}" \
    --conf "spark.sql.adaptive.enabled=${SPARK_ADAPTIVE_ENABLED:-true}" \
    --conf "spark.serializer=${SPARK_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}" \
    --conf "spark.eventLog.enabled=${SPARK_EVENTLOG_ENABLED:-false}" \
    "$@"
}
