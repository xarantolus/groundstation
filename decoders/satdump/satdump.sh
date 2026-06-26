#!/bin/bash
set -eoux pipefail

mkdir -p /tmp/satdump-config
export HOME=/tmp/satdump-config

# We only use SatDump as a demodulator/decoder on pre-recorded basebands, so it
# never needs orbital data. Install a settings.json that disables Kepler/TLE
# auto-update ("Never") — otherwise a slow or unreachable celestrak.org blocks
# every pipeline for ~22 min (10 curl retries x ~134s connect timeout). The
# auto-update task is only scheduled when kepler_update_interval != "Never"
# (src-core/db/kepler/kepler_handler.cpp autoUpdateKeplers).
#
# HOME points at a tmpfs (podman --read-only mounts one on /tmp), and SatDump
# also writes its sqlite DB here, so the baked-in config is copied into place at
# runtime rather than living in the read-only image rootfs.
mkdir -p "${HOME}/.config/satdump"
cp /etc/groundstation/satdump-settings.json "${HOME}/.config/satdump/settings.json"

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
