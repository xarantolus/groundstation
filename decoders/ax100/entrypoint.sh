#!/bin/bash

ax100_decoder.py || echo "ax100_decoder.py exited with non-zero status $?" >&2

if [ -s /output/csp_packets_kiss.bin ]; then
    cd /app
    uv run --no-sync --offline decode.py /output/csp_packets_kiss.bin \
        >/output/decoded.txt 2>&1 || true
    uv run --no-sync --offline deframe.py /output/csp_packets_kiss.bin /output/messages \
        >>/output/decoded.txt 2>&1 || true
fi
