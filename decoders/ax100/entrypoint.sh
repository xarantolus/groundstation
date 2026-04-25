#!/bin/bash

ax100_decoder.py || echo "ax100_decoder.py exited with non-zero status $?" >&2

if [ -s /output/csp_packets.kiss ]; then
    cd /app
    /app/.venv/bin/python decode.py /output/csp_packets.kiss \
        >/output/decoded.txt 2>&1 || true
    /app/.venv/bin/python deframe.py /output/csp_packets.kiss /output/messages \
        >>/output/decoded.txt 2>&1 || true
fi
