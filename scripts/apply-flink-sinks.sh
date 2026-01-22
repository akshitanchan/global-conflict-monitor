#!/usr/bin/env bash
set -euo pipefail

# applies sink table DDLs in flink sql-client

FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"

echo "[apply] create-cdc-source.sql"
docker exec -i "$FLINK_JM_CONTAINER" bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/sql/create-cdc-source.sql >/tmp/sql_apply_create_cdc_source.log 2>&1 || (tail -n 200 /tmp/sql_apply_create_cdc_source.log && exit 1)"

echo "[apply] 01-create-sinks.sql"
docker exec -i "$FLINK_JM_CONTAINER" bash -lc "/opt/flink/bin/sql-client.sh -f /opt/flink/sql/01-create-sinks.sql >/tmp/sql_apply_01_create_sinks.log 2>&1 || (tail -n 200 /tmp/sql_apply_01_create_sinks.log && exit 1)"

echo "[ok] sinks created"
echo "next: start the streaming aggregations (this command blocks):"
echo "  ./scripts/start-flink-aggregations.sh"
