#!/usr/bin/env bash
set -euo pipefail

# Start the Flink SQL pipeline non-interactively.
# Using `-f` submits the streaming statement set as a job and exits cleanly.

FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"
SQL_FILE_CONTAINER="${SQL_FILE_CONTAINER:-/opt/flink/sql/flink-setup.sql}"

echo "[run] submitting flink pipeline: ${SQL_FILE_CONTAINER}"
docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
  "/opt/flink/bin/sql-client.sh -f '$SQL_FILE_CONTAINER'"

echo "[ok] flink pipeline submitted. check the flink ui for job status: http://localhost:8081"