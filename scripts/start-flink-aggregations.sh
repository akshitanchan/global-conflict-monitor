#!/usr/bin/env bash
set -euo pipefail

echo "[run] starting pipeline in single sql-client session"
docker exec -it flink-jobmanager bash -lc \
  "/opt/flink/bin/sql-client.sh \
     -i /opt/flink/sql/init.sql \
     -f /opt/flink/sql/run-aggregations.sql"
