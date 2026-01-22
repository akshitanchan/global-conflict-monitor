#!/usr/bin/env bash
set -euo pipefail

# starts the streaming INSERT jobs (keeps running)
# run in its own terminal and stop with ctrl+c
# if you need a clean reset, run scripts/reset-cdc.sh and restart containers.

FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"

echo "[run] starting streaming aggregations (this will keep running)"
docker exec -it "$FLINK_JM_CONTAINER" bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/sql/02-run-aggregations.sql"
