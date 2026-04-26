#!/bin/bash
#set -eox pipefail

if [ -z "$GITHUB_REPOSITORY_OWNER" ]; then
  REPO_URL=$(git config --get remote.origin.url)
  if [[ $REPO_URL =~ github\.com[:/]([^/]+)/([^/]+?)(\.git)?/?$ ]]; then
    REPO_NAME="$(echo "${BASH_REMATCH[1]}/${BASH_REMATCH[2]}" | tr '[:upper:]' '[:lower:]')"
  else
    echo "Error: Unable to extract repository information from git URL." >&2
    exit 1
  fi
else
  REPO_NAME="$(echo "$GITHUB_REPOSITORY_OWNER/$(basename "$(git rev-parse --show-toplevel)")" | tr '[:upper:]' '[:lower:]')"
fi

IMAGE_TAG="$(git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]' | tr '/' '-')"
case "$IMAGE_TAG" in HEAD|main|master) IMAGE_TAG="latest" ;; esac
IMAGE_TAG="${IMAGE_TAG}${TAG_SUFFIX:-}"

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
# Cache is per-arch: a multi-arch manifest tag was unreliable for podman's
# --cache-from, which would pull the default-platform digest and miss every
# step on the Pi. Naming the cache by ARCH gives each builder its own.
CACHE_ARCH="$(echo "${PLATFORM##*/}" | tr '[:upper:]' '[:lower:]')"
PUSH_MODE=false
CONTAINER_TOOL="podman"
[[ "$*" == *"--push"* ]] && PUSH_MODE=true
[[ "$*" == *"--docker"* ]] && CONTAINER_TOOL="docker"

build_image() {
  local name="$1" dockerfile="$2" pull="$3"
  local image="$IMAGE_PATH_PREFIX/$name:$IMAGE_TAG"
  local cache="$IMAGE_PATH_PREFIX/$name-cache-$CACHE_ARCH"
  local cache_args=()

  if [ "$CONTAINER_TOOL" = "docker" ]; then
    cache_args+=(--cache-from="type=registry,ref=$cache")
    [ "$PUSH_MODE" = true ] && cache_args+=(--cache-to="type=registry,ref=$cache,mode=max")
  else
    cache_args+=(--layers --cache-from="$cache")
    [ "$PUSH_MODE" = true ] && cache_args+=(--cache-to="$cache")
  fi

  $CONTAINER_TOOL build \
    --pull="$pull" \
    --platform "$PLATFORM" \
    -t "$image" \
    --build-arg TAG="$IMAGE_TAG" \
    --build-arg GITHUB_REPOSITORY="$REPO_NAME" \
    "${cache_args[@]}" \
    -f "$dockerfile" .

  [ "$PUSH_MODE" = true ] && $CONTAINER_TOOL push "$image"
}

build_image gs Dockerfile newer
build_image satellite-recorder Dockerfile.recorder never

(cd decoders/ax100 && build_image ax100-decoder Dockerfile never)
(cd decoders/waterfall && build_image waterfall Dockerfile missing)
(cd decoders/satdump && build_image satdump-decoder Dockerfile missing)
