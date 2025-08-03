#!/bin/bash
set -eox pipefail

REPO_NAME="xarantolus/groundstation"
IMAGE_TAG="latest"

if [ -z "$GITHUB_REPOSITORY_OWNER" ]; then
  REPO_URL=$(git config --get remote.origin.url)

  if [[ $REPO_URL =~ github\.com[:/]([^/]+)/([^/]+)(\.git)?/?$ ]]; then
    REPO_OWNER="$(echo "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]')"
    REPO_NAME_PART="${BASH_REMATCH[2]}"
    # Remove .git suffix if present
    REPO_NAME_PART="${REPO_NAME_PART%.git}"
    REPO_NAME="$REPO_OWNER/$(echo "$REPO_NAME_PART" | tr '[:upper:]' '[:lower:]')"
  else
    echo "Error: Unable to extract repository information from git URL."
    exit 1
  fi
else
  # Use GITHUB_REPOSITORY_OWNER in case it's set (e.g., in GitHub Actions)
  REPO_OWNER="$GITHUB_REPOSITORY_OWNER"
  REPO_NAME="$REPO_OWNER/$(basename "$(git rev-parse --show-toplevel)" | tr '[:upper:]' '[:lower:]')"
fi

IMAGE_TAG="$(git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]' | tr '/' '-')"
if [ "$IMAGE_TAG" == "HEAD" ] || [ "$IMAGE_TAG" == "main" ] || [ "$IMAGE_TAG" == "master" ]; then
  IMAGE_TAG="latest"
fi

IMAGE_PATH_PREFIX="ghcr.io/$REPO_NAME"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    armv7l) ARCH="arm/v7" ;;
    armv6l) ARCH="arm/v6" ;;
    *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

PLATFORM="${PLATFORM:-${OS}/${ARCH}}"
PUSH_MODE=false
CONTAINER_TOOL="podman"

if [[ "$*" == *"--push"* ]]; then
  PUSH_MODE=true
fi

if [[ "$*" == *"--docker"* ]]; then
  CONTAINER_TOOL="docker"
fi

# Build groundstation
BUILD_ARGS="--platform $PLATFORM -t $IMAGE_PATH_PREFIX/gs:$IMAGE_TAG --build-arg TAG=$IMAGE_TAG --build-arg GITHUB_REPOSITORY=$REPO_NAME"
if [ "$PUSH_MODE" = true ]; then
  BUILD_ARGS="$BUILD_ARGS --cache-from=$IMAGE_PATH_PREFIX/gs-cache --cache-to=$IMAGE_PATH_PREFIX/gs-cache"
else
  BUILD_ARGS="$BUILD_ARGS"
fi
$CONTAINER_TOOL build $BUILD_ARGS -f Dockerfile .
if [ "$PUSH_MODE" = true ]; then
  $CONTAINER_TOOL push "$IMAGE_PATH_PREFIX/gs:$IMAGE_TAG"
fi

# Build satellite-recorder
BUILD_ARGS="--platform $PLATFORM -t $IMAGE_PATH_PREFIX/satellite-recorder:$IMAGE_TAG --build-arg TAG=$IMAGE_TAG --build-arg GITHUB_REPOSITORY=$REPO_NAME"
if [ "$PUSH_MODE" = true ]; then
  BUILD_ARGS="$BUILD_ARGS --cache-from=$IMAGE_PATH_PREFIX/satellite-recorder-cache --cache-to=$IMAGE_PATH_PREFIX/satellite-recorder-cache"
else
  BUILD_ARGS="$BUILD_ARGS"
fi
$CONTAINER_TOOL build $BUILD_ARGS -f Dockerfile.recorder .
if [ "$PUSH_MODE" = true ]; then
  $CONTAINER_TOOL push "$IMAGE_PATH_PREFIX/satellite-recorder:$IMAGE_TAG"
fi

# Decoders
cd decoders

# Build NOAA decoder
cd noaa_apt
BUILD_ARGS="--platform $PLATFORM -t $IMAGE_PATH_PREFIX/noaa-decoder:$IMAGE_TAG --build-arg TAG=$IMAGE_TAG --build-arg GITHUB_REPOSITORY=$REPO_NAME"
if [ "$PUSH_MODE" = true ]; then
  BUILD_ARGS="$BUILD_ARGS --cache-from=$IMAGE_PATH_PREFIX/noaa-decoder-cache --cache-to=$IMAGE_PATH_PREFIX/noaa-decoder-cache"
else
  BUILD_ARGS="$BUILD_ARGS"
fi
$CONTAINER_TOOL build $BUILD_ARGS -f Dockerfile .
cd ..
if [ "$PUSH_MODE" = true ]; then
  $CONTAINER_TOOL push "$IMAGE_PATH_PREFIX/noaa-decoder:$IMAGE_TAG"
fi

# Build satdump decoder
cd satdump
BUILD_ARGS="--platform $PLATFORM -t $IMAGE_PATH_PREFIX/satdump-decoder:$IMAGE_TAG --build-arg TAG=$IMAGE_TAG --build-arg GITHUB_REPOSITORY=$REPO_NAME"
if [ "$PUSH_MODE" = true ]; then
  BUILD_ARGS="$BUILD_ARGS --cache-from=$IMAGE_PATH_PREFIX/satdump-decoder-cache --cache-to=$IMAGE_PATH_PREFIX/satdump-decoder-cache"
else
  BUILD_ARGS="$BUILD_ARGS"
fi
$CONTAINER_TOOL build $BUILD_ARGS -f Dockerfile .
cd ..
if [ "$PUSH_MODE" = true ]; then
  $CONTAINER_TOOL push "$IMAGE_PATH_PREFIX/satdump-decoder:$IMAGE_TAG"
fi

# Build waterfall
cd waterfall
BUILD_ARGS="--platform $PLATFORM -t $IMAGE_PATH_PREFIX/waterfall:$IMAGE_TAG --build-arg TAG=$IMAGE_TAG --build-arg GITHUB_REPOSITORY=$REPO_NAME"
if [ "$PUSH_MODE" = true ]; then
  BUILD_ARGS="$BUILD_ARGS --cache-from=$IMAGE_PATH_PREFIX/waterfall-cache --cache-to=$IMAGE_PATH_PREFIX/waterfall-cache"
else
  BUILD_ARGS="$BUILD_ARGS"
fi
$CONTAINER_TOOL build $BUILD_ARGS -f Dockerfile .
if [ "$PUSH_MODE" = true ]; then
  $CONTAINER_TOOL push "$IMAGE_PATH_PREFIX/waterfall:$IMAGE_TAG"
fi
cd ..

cd .. # leave decoders

python -m pip install -r auto/requirements.txt --break-system-packages
