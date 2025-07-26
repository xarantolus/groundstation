#!/bin/bash
set -eox pipefail

IMAGE_NAME="ghcr.io/xarantolus/groundstation/gs:latest"

XAUTH="$XAUTHORITY"
if [ "$XAUTH" == "" ]; then
  XAUTH="$HOME/.Xauthority"
fi

XARGS="-v ${XAUTH}:/root/.Xauthority:ro"

if which xhost >/dev/null 2>&1; then
  xhost "+SI:localuser:$(whoami)"
fi

WD="$(pwd)"

CONTAINER_TOOL="podman"
if [[ "$*" == *"--docker"* ]]; then
  CONTAINER_TOOL="docker"
fi

ARGS=()
for arg in "$@"; do
  if [[ "$arg" != "--docker" ]]; then
    ARGS+=("$arg")
  fi
done

$CONTAINER_TOOL run -it --rm \
  --privileged \
  -v /dev:/dev \
  --device /dev/bus/usb \
  --device /dev/dri \
  --network host \
  -e "DISPLAY=$DISPLAY" \
  -e "XAUTHORITY=/root/.Xauthority" \
  -v "/tmp/.X11-unix:/tmp/.X11-unix" \
  -v "$WD:/data:Z" \
  -w /data \
  $XARGS \
  "${IMAGE_NAME}" "${ARGS[@]}"
