#!/bin/bash
set -eoux pipefail

# Run satdump with a specific pipeline
# https://docs.satdump.org/pipelines.html

satdump \
	"${PIPELINE_NAME}" \
	baseband \
	"${INPUT_FILE:-/data/recording.bin}" \
	"${OUTPUT_DIR:-/output}" \
	--samplerate "${SAMP_RATE}" \
	--baseband_format f32 \
	"$@"
