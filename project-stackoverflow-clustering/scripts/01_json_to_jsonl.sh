#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"

INPUT="${1:-$LOCAL_DATA_DIR/$RAW_JSON}"
OUTPUT="${2:-$PROJECT_HOME/output/questions.jsonl}"

mkdir -p "$(dirname "$OUTPUT")"
python3 "$PROJECT_HOME/src/json_to_jsonl.py" "$INPUT" "$OUTPUT"
wc -l "$OUTPUT"
