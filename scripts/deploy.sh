#!/usr/bin/env bash
# Deploy an already-built, already-tested image with automatic rollback.
#
# Usage: scripts/deploy.sh <image>      e.g. scripts/deploy.sh products-pipeline:42
#
# Run from the repo root (where docker-compose.yml and .env live).
#
# Flow:
#   1. Validate: image exists, network exists, compose file + .env resolve.
#      Any failure here exits BEFORE the running stack is touched.
#   2. Pick the rollback target: products-pipeline:latest (last successful
#      deploy), or, on the first run, whatever image is currently running.
#   3. compose down, then up with the new image, then verify both containers
#      are running the new image and haven't restarted.
#   4. Success: tag the new image as products-pipeline:latest.
#      Failure: redeploy the rollback image and verify it.
#
# Exit codes: 0 deployed | 1 deploy failed, rolled back (or nothing to roll back
# to) | 2 deploy AND rollback failed (manual intervention needed)

set -euo pipefail

NEW_IMAGE="${1:?usage: deploy.sh <image>}"
IMAGE_REPO="products-pipeline"
LATEST_IMAGE="$IMAGE_REPO:latest"
ROLLBACK_IMAGE="$IMAGE_REPO:rollback"
CONTAINERS=(products-pipeline-ec2 products-pipeline-backfill)
VERIFY_WAIT_SECONDS="${VERIFY_WAIT_SECONDS:-20}"

log() { echo "[deploy] $*"; }

image_id() { docker image inspect -f '{{.Id}}' "$1"; }

# Replace the running stack with containers from image $1.
compose_up() {
  local image="$1"
  PIPELINE_IMAGE="$image" docker compose down --remove-orphans || return 1
  # Fixed container_name values: clear leftovers from a stack started under a
  # different compose project name.
  docker rm -f "${CONTAINERS[@]}" >/dev/null 2>&1 || true
  PIPELINE_IMAGE="$image" docker compose up -d --no-build || return 1
}

# Both containers must be running image $1 and must not have restarted
# (restart: always would otherwise hide a crash loop).
verify() {
  local expected_id
  expected_id="$(image_id "$1")" || return 1
  log "waiting ${VERIFY_WAIT_SECONDS}s before health check..."
  sleep "$VERIFY_WAIT_SECONDS"

  local failed=0 c state
  for c in "${CONTAINERS[@]}"; do
    state="$(docker inspect -f '{{.State.Status}} {{.RestartCount}} {{.Image}}' "$c" 2>/dev/null || echo "missing")"
    log "$c: $state"
    if [[ "$state" != "running 0 $expected_id" ]]; then
      failed=1
      log "$c is unhealthy; last 100 log lines:"
      docker logs --tail 100 "$c" 2>&1 || true
    fi
  done
  return "$failed"
}

# ---------- 1. Pre-flight (nothing touched yet) ----------
image_id "$NEW_IMAGE" >/dev/null || { log "image $NEW_IMAGE not found"; exit 1; }
docker network inspect pipeline-net >/dev/null 2>&1 || docker network create pipeline-net >/dev/null
PIPELINE_IMAGE="$NEW_IMAGE" docker compose config -q || { log "docker-compose.yml / .env invalid; aborting"; exit 1; }

# ---------- 2. Rollback target ----------
rollback_available=0
if docker image inspect "$LATEST_IMAGE" >/dev/null 2>&1; then
  docker tag "$LATEST_IMAGE" "$ROLLBACK_IMAGE"
  rollback_available=1
elif running_id="$(docker inspect -f '{{.Image}}' "${CONTAINERS[0]}" 2>/dev/null)"; then
  docker tag "$running_id" "$ROLLBACK_IMAGE"
  rollback_available=1
fi
if (( rollback_available )); then
  log "rollback target: $ROLLBACK_IMAGE ($(image_id "$ROLLBACK_IMAGE"))"
else
  log "no previous deployment found; rollback unavailable"
fi

# ---------- 3. Deploy ----------
log "deploying $NEW_IMAGE ($(image_id "$NEW_IMAGE"))"
if compose_up "$NEW_IMAGE" && verify "$NEW_IMAGE"; then
  docker tag "$NEW_IMAGE" "$LATEST_IMAGE"
  log "deploy succeeded; $LATEST_IMAGE -> $NEW_IMAGE"
  exit 0
fi

# ---------- 4. Rollback ----------
log "deploy of $NEW_IMAGE FAILED"
if (( ! rollback_available )); then
  log "nothing to roll back to"
  exit 1
fi

log "rolling back to $ROLLBACK_IMAGE"
if compose_up "$ROLLBACK_IMAGE" && verify "$ROLLBACK_IMAGE"; then
  log "rollback succeeded; previous version is running again"
  exit 1
fi

log "ROLLBACK FAILED - manual intervention required"
exit 2
