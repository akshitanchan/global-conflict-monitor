#!/usr/bin/env bash
set -euo pipefail

echo "[apply] running /opt/flink/sql/00-init.sql (cdc source + sinks)"
docker exec -it flink-jobmanager bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/sql/00-init.sql"

echo "[ok] verifying tables exist in this run:"
docker exec -it flink-jobmanager bash -lc "/opt/flink/bin/sql-client.sh -f <(printf 'SHOW TABLES;\n')"
