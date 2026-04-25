#!/bin/bash

ax100_decoder.py || echo "ax100_decoder.py exited with non-zero status $?" >&2

if [ -s /output/csp_packets_kiss.bin ]; then
    cd /app
    /app/.venv/bin/python decode.py /output/csp_packets_kiss.bin \
        >/output/decoded.txt 2>&1 || true
    /app/.venv/bin/python deframe.py /output/csp_packets_kiss.bin /output/messages \
        >>/output/decoded.txt 2>&1 || true
fi
