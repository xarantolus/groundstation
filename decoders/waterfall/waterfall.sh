#!/usr/bin/env sh
set -euox

# See https://ffmpeg.org/ffmpeg-filters.html#showspectrumpic
ffmpeg -y \
  -f f32le -ar "$SAMP_RATE" -ac 2 -i "$INPUT_FILE" \
  -lavfi "showspectrumpic=\
s=${WIDTH:-600}x${HEIGHT:-2000}:\
mode=combined:\
color=intensity:\
orientation=horizontal:\
scale=log:\
fscale=log:\
legend=1\
" \
  -frames:v 1 \
  "$OUTPUT_DIR/waterfall.png"
