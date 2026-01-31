#!/usr/bin/env bash
set -euo pipefail

echo "[run] starting pipeline in one sql-client session (init + streaming inserts)"
docker exec -it flink-jobmanager bash -lc \
  "/opt/flink/bin/sql-client.sh \
     -i /opt/flink/sql/00-init.sql \
     -f /opt/flink/sql/02-run-aggregations.sql"

    
