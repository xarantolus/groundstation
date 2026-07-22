#!/bin/bash
set -eoux pipefail

mkdir -p /tmp/gr-satellites-config
export HOME=/tmp/gr-satellites-config

mkdir -p "${HOME}/.gr_satellites"
cp /etc/groundstation/gr-satellites-config.ini "${HOME}/.gr_satellites/config.ini"

mkdir -p "${OUTPUT_DIR:-/output}"

gr_satellites \
	"${SATELLITE}" \
	--rawfile "${INPUT_FILE:-/data/recording.bin}" \
	--samp_rate "${SAMP_RATE}" \
	--iq \
	--ignore_unknown_args \
	--kiss_out "${OUTPUT_DIR:-/output}/frames.kiss" \
	--telemetry_output "${OUTPUT_DIR:-/output}/telemetry.log" \
	"$@" \
	2>&1 | tee "${OUTPUT_DIR:-/output}/gr_satellites.log"
