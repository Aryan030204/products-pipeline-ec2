#!/bin/sh
# Fired by crond on the schedule installed in backfill-cron-entrypoint.sh.
# Mirrors the old workflow's `timeout-minutes: 60` (timeout 3600) and its
# `concurrency: cancel-in-progress: false` intent — a 2-day interval against
# a 60-minute run makes overlap effectively impossible, so a non-blocking
# lock (skip + log) is used rather than queuing.
. /app/.cron_env.sh

cd /app

if ! flock -n /tmp/backfill.lock -c "timeout 3600 node pipeline.js"; then
  echo "[backfill-cron] Skipped/failed: previous backfill still running, or run errored/timed out."
fi
