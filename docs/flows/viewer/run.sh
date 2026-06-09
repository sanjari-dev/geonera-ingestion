#!/usr/bin/env bash
#
# Geonera Ingestion — Flow Diagram Viewer
# Create (or recreate) the container from the published image.
#
# Pulls `sansalfian/geonera-ingestion-docs` from Docker Hub and runs it,
# replacing any previous instance — handy for (re)deploying on a server
# without needing this repo's source checked out.
#
# Usage:
#   ./run.sh                  # defaults: latest tag, port 4040, name geonera-flow-viewer
#   PORT=8080 ./run.sh        # publish on a different host port
#   IMAGE_TAG=1.0 ./run.sh    # pin a specific image tag instead of :latest
#   CONTAINER_NAME=docs-viewer ./run.sh
#
# All knobs are environment variables so this also drops cleanly into CI/CD
# or systemd unit `ExecStart=` lines.

set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:-sansalfian/geonera-ingestion-docs}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"

CONTAINER_NAME="${CONTAINER_NAME:-geonera-flow-viewer}"
HOST_PORT="${PORT:-4040}"
CONTAINER_PORT="4040"
NETWORK="${NETWORK:-}"          # optional: attach to an existing Docker network
RESTART_POLICY="${RESTART_POLICY:-unless-stopped}"

echo "==> Pulling ${IMAGE}"
docker pull "${IMAGE}"

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "==> Removing existing container '${CONTAINER_NAME}'"
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

RUN_ARGS=(
  -d
  --name "${CONTAINER_NAME}"
  --restart "${RESTART_POLICY}"
  -p "${HOST_PORT}:${CONTAINER_PORT}"
  -e "PORT=${CONTAINER_PORT}"
)

if [[ -n "${NETWORK}" ]]; then
  RUN_ARGS+=(--network "${NETWORK}")
fi

echo "==> Starting container '${CONTAINER_NAME}' (host port ${HOST_PORT} -> container ${CONTAINER_PORT})"
docker run "${RUN_ARGS[@]}" "${IMAGE}"

echo
echo "Container '${CONTAINER_NAME}' is up, running image: ${IMAGE}"
echo "  Open:        http://localhost:${HOST_PORT}/viewer/"
echo "  Logs:        docker logs -f ${CONTAINER_NAME}"
echo "  Health:      docker inspect --format='{{.State.Health.Status}}' ${CONTAINER_NAME}"
echo "  Stop:        docker rm -f ${CONTAINER_NAME}"
