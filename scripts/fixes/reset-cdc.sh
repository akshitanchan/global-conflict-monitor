#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
POSTGRES_DB="${POSTGRES_DB:-gdelt}"
POSTGRES_USER="${POSTGRES_USER:-flink_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-flink_pass}"

PUBLICATION_NAME="${PUBLICATION_NAME:-gdelt_flink_pub}"
SLOT_NAME="${SLOT_NAME:-gdelt_flink_slot}"

# optional: stop flink first (set STOP_FLINK=1)
STOP_FLINK="${STOP_FLINK:-0}"

pg_exec() {
  docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"
}

if [[ "$STOP_FLINK" == "1" ]]; then
  echo "[stop] stopping flink services via compose"
  $COMPOSE_CMD -f "$COMPOSE_FILE" stop jobmanager taskmanager || true
fi

echo "[reset] terminating active slot pid (if any)"
active_pid="$(pg_exec "select active_pid from pg_replication_slots where slot_name='${SLOT_NAME}';" | tr -d '[:space:]')"
if [[ -n "$active_pid" && "$active_pid" != "0" ]]; then
  echo "[reset] active pid: $active_pid -> terminating"
  pg_exec "select pg_terminate_backend(${active_pid});" >/dev/null || true
  sleep 1
else
  echo "[reset] no active pid for slot"
fi

echo "[reset] dropping replication slot if exists"
pg_exec "select pg_drop_replication_slot('${SLOT_NAME}') where exists (select 1 from pg_replication_slots where slot_name='${SLOT_NAME}');" >/dev/null || true

echo "[reset] dropping publication if exists"
pg_exec "drop publication if exists ${PUBLICATION_NAME};" >/dev/null || true

echo "[ok] cdc reset complete"
echo "note: if you stopped flink, restart with:"
echo "  $COMPOSE_CMD -f \"$COMPOSE_FILE\" up -d"