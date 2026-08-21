#!/bin/sh
# Container entrypoint for the retrospective/rolling backfill worker.
#
# Replaces .github/workflows/products-pipeline-backfill.yml's GitHub Actions
# `schedule:` trigger. The workflow ran on GitHub's UTC cron; Alpine
# containers default to UTC too, so the same "40 6 */2 * *" expression fires
# at the identical wall-clock moments here. Business logic (pipeline.js) and
# every env value are untouched — only the trigger mechanism moved.
set -e

# busybox crond spawns jobs with a minimal environment, so snapshot the
# container's env (set via docker-compose) to a file the job script sources.
printenv | sed "s/'/'\\\\''/g; s/^\\([^=]*\\)=\\(.*\\)$/export \\1='\\2'/" > /app/.cron_env.sh
chmod 600 /app/.cron_env.sh

CRON_SCHEDULE="${BACKFILL_CRON_SCHEDULE:-40 6 */2 * *}"
echo "${CRON_SCHEDULE} sh /app/backfill-cron-run.sh >> /proc/1/fd/1 2>> /proc/1/fd/2" > /etc/crontabs/root

echo "[backfill-cron] Installed schedule: ${CRON_SCHEDULE} (UTC) -> node pipeline.js"

exec crond -f -l 2
