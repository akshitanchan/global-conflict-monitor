#!/usr/bin/env bash
set -euo pipefail

# Start the Flink SQL pipeline non-interactively.
# Using `-f` submits the streaming statement set as a job and exits cleanly.

FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"

# preferred split (keeps tests + workflows sane)
DDL_SQL_CONTAINER="${DDL_SQL_CONTAINER:-/opt/flink/sql/01-ddl.sql}"
PIPELINE_SQL_CONTAINER="${PIPELINE_SQL_CONTAINER:-/opt/flink/sql/02-pipeline.sql}"

# legacy single-file mode (still works)
SQL_FILE_CONTAINER="${SQL_FILE_CONTAINER:-}"

if [[ -n "${SQL_FILE_CONTAINER}" ]]; then
  echo "[run] submitting legacy flink pipeline: ${SQL_FILE_CONTAINER}"
  docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
    "/opt/flink/bin/sql-client.sh -f '$SQL_FILE_CONTAINER'"
else
  echo "[run] applying flink ddl: ${DDL_SQL_CONTAINER}"
  docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
    "/opt/flink/bin/sql-client.sh -f '$DDL_SQL_CONTAINER' >/tmp/sql_ddl.log 2>&1 || (tail -n 200 /tmp/sql_ddl.log && exit 1)"

  echo "[run] submitting flink pipeline: ${PIPELINE_SQL_CONTAINER}"
  docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
    "/opt/flink/bin/sql-client.sh -f '$PIPELINE_SQL_CONTAINER'"
fi

echo "[ok] flink pipeline submitted. check the flink ui for job status: http://localhost:8081"