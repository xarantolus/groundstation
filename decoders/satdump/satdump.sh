#!/bin/bash
set -eoux pipefail

mkdir -p /tmp/satdump-config
export HOME=/tmp/satdump-config

# SatDump v2's `pipeline <name>` registers params as CLI11 `add_flag`, which
# only accepts `--key=value` (no space form), so all params here and in
# config.yml decoder args must use `=`.
satdump \
	pipeline \
	"${PIPELINE_NAME}" \
	"--samplerate=${SAMP_RATE}" \
	--baseband_format=f32 \
	"$@" \
	baseband \
	"${INPUT_FILE:-/data/recording.bin}" \
	"${OUTPUT_DIR:-/output}"
