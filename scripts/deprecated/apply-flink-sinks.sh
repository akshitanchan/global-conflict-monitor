#!/usr/bin/env bash
set -euo pipefail

echo "[apply] running /opt/flink/sql/init.sql (cdc source + sinks)"
docker exec -it flink-jobmanager bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/sql/init.sql"

echo "[ok] verifying Flink table definitions (same session):"
docker exec -it flink-jobmanager bash -lc "
  tmp=/tmp/gcm_init_and_show.sql
  cat /opt/flink/sql/init.sql > \"$tmp\"
  echo 'SHOW TABLES;' >> \"$tmp\"
  /opt/flink/bin/sql-client.sh -f \"$tmp\"
  rm -f \"$tmp\"
"

echo "[note] if you open a brand new sql-client session and run SHOW TABLES,"
echo "       it can show Empty set because the default catalog is session-scoped."
