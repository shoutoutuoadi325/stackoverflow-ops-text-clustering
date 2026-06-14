#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

python3 "$PROJECT_HOME/src/validate_raw_data.py" \
  --input "$LOCAL_DATA_DIR/$RAW_JSON" \
  --output-dir "$PROJECT_HOME/output/validation"
