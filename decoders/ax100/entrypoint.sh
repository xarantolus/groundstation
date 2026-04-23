#!/bin/bash

ax100_decoder.py || echo "ax100_decoder.py exited with non-zero status $?" >&2

# Decode any CSP packets that the flow graph produced.
if [ -s /output/csp_packets.bin ]; then
    cd /app
    uv run --no-sync --offline decode.py /output/csp_packets.bin \
        >/output/decoded.txt 2>&1 || true
fi
