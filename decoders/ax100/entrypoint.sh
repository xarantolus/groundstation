#!/bin/bash
set -eo pipefail

# Run the GNU Radio flow graph: /data/recording.bin -> /output/csp_packets.bin
ax100_decoder.py

# Decode any CSP packets that the flow graph produced.
if [ -s /output/csp_packets.bin ]; then
    cd /app
    uv run --no-sync --offline decode.py /output/csp_packets.bin \
        >/output/decoded.txt 2>&1 || true
fi
