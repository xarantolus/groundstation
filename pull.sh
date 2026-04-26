#!/bin/bash
set -euo pipefail

if [ -z "${GITHUB_REPOSITORY_OWNER:-}" ]; then
  REPO_URL=$(git config --get remote.origin.url)
  if [[ $REPO_URL =~ github\.com[:/]([^/]+)/([^/]+) ]]; then
    REPO_NAME="${BASH_REMATCH[1]}/${BASH_REMATCH[2]%.git}"
    REPO_NAME="$(echo "$REPO_NAME" | tr '[:upper:]' '[:lower:]')"
  else
    echo "Error: Unable to extract repository information from git URL." >&2
    exit 1
  fi
else
  REPO_NAME="$(echo "$GITHUB_REPOSITORY_OWNER/$(basename "$(git rev-parse --show-toplevel)")" | tr '[:upper:]' '[:lower:]')"
fi

IMAGE_TAG="$(git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]' | tr '/' '-')"
case "$IMAGE_TAG" in HEAD|main|master) IMAGE_TAG="latest" ;; esac

IMAGE_PATH_PREFIX="ghcr.io/$REPO_NAME"
CONTAINER_TOOL="podman"
[[ "$*" == *"--docker"* ]] && CONTAINER_TOOL="docker"

for name in gs satellite-recorder ax100-decoder waterfall satdump-decoder; do
  $CONTAINER_TOOL pull "$IMAGE_PATH_PREFIX/$name:$IMAGE_TAG"
done
