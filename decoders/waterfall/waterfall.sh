#!/usr/bin/env sh
set -eu

: "${INPUT_FILE:?INPUT_FILE is required}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required}"
: "${SAMP_RATE:?SAMP_RATE is required}"

mkdir -p "$OUTPUT_DIR"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

exec python3 "$SCRIPT_DIR/waterfall.py" \
  "$INPUT_FILE" \
  "$OUTPUT_DIR/waterfall.png" \
  --samp-rate "$SAMP_RATE" \
  --center-freq "${FREQUENCY:-0}" \
  --bandwidth "${BANDWIDTH:-0}"
