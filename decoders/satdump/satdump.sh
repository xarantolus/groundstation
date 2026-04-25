#!/bin/bash
set -eoux pipefail

mkdir -p /tmp/satdump-config
export HOME=/tmp/satdump-config

satdump \
	"${PIPELINE_NAME}" \
	baseband \
	"${INPUT_FILE:-/data/recording.bin}" \
	"${OUTPUT_DIR:-/output}" \
	--samplerate "${SAMP_RATE}" \
	--baseband_format f32 \
	"$@"
