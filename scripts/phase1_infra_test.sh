#!/usr/bin/env bash
set -euo pipefail

# ---------- root + compose ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

# ---------- config (override via env) ----------
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"

POSTGRES_DB="${POSTGRES_DB:-gdelt}"
POSTGRES_USER="${POSTGRES_USER:-flink_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-flink_pass}"

PUBLICATION_NAME="${PUBLICATION_NAME:-gdelt_flink_pub}"
SLOT_NAME="${SLOT_NAME:-gdelt_flink_slot}"

CDC_SQL_HOST_PATH="${CDC_SQL_HOST_PATH:-$ROOT_DIR/flink/sql/create-cdc-source.sql}"
CDC_SQL_CONTAINER_PATH="${CDC_SQL_CONTAINER_PATH:-/opt/flink/sql/create-cdc-source.sql}"
SMOKE_EVENT_ID="${SMOKE_EVENT_ID:-999999999}"

# ---------- helpers ----------
pg_exec() {
  docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"
}

wait_for_container_healthy() {
  local name="$1"
  local tries="${2:-60}"
  local i=0
  echo "[wait] waiting for $name to be healthy..."
  while true; do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || true)"
    if [[ "$status" == "healthy" ]]; then
      echo "[wait] $name is healthy"
      return 0
    fi
    i=$((i+1))
    if (( i >= tries )); then
      echo "[error] $name not healthy after $tries checks"
      docker logs "$name" --tail 200 || true
      exit 1
    fi
    sleep 2
  done
}

ensure_file_exists() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "[error] missing file: $path"
    exit 1
  fi
}

# ---------- bring up ----------
echo "[up] docker compose up -d"
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d

# ---------- checks ----------
wait_for_container_healthy "$POSTGRES_CONTAINER" 90

echo "[check] wal_level"
wal_level="$(pg_exec "show wal_level;")"
if [[ "$wal_level" != "logical" ]]; then
  echo "[error] wal_level is '$wal_level' (expected: logical)"
  exit 1
fi
echo "[ok] wal_level=logical"

echo "[check] replica identity"
replica_identity="$(pg_exec "select relreplident from pg_class where relname='gdelt_events';")"
# 'f' means FULL, 'd' means DEFAULT. we want full to reliably emit old row values on updates/deletes.
if [[ "$replica_identity" != "f" ]]; then
  echo "[fix] setting replica identity full on gdelt_events"
  pg_exec "alter table public.gdelt_events replica identity full;"
fi
echo "[ok] replica identity full"

echo "[check] create publication if missing"
pub_exists="$(pg_exec "select 1 from pg_publication where pubname='${PUBLICATION_NAME}' limit 1;")"
if [[ -z "$pub_exists" ]]; then
  pg_exec "create publication ${PUBLICATION_NAME} for table public.gdelt_events;"
  echo "[ok] publication created: $PUBLICATION_NAME"
else
  echo "[ok] publication exists: $PUBLICATION_NAME"
fi

echo "[check] flink sql cdc ddl file exists"
ensure_file_exists "$CDC_SQL_HOST_PATH"

echo "[flink] verifying sql file is mounted (container path: $CDC_SQL_CONTAINER_PATH)"
if ! docker exec -i "$FLINK_JM_CONTAINER" bash -lc "test -f '$CDC_SQL_CONTAINER_PATH'"; then
  echo "[error] flink container cannot see: $CDC_SQL_CONTAINER_PATH"
  echo "hint: ensure docker-compose mounts $ROOT_DIR/flink/sql -> /opt/flink/sql"
  exit 1
fi
echo "[ok] sql file mounted in flink container"

echo "[flink] applying cdc source ddl in flink sql-client"
docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
  "/opt/flink/bin/sql-client.sh -f '$CDC_SQL_CONTAINER_PATH' >/tmp/sql_apply.log 2>&1 || (tail -n 200 /tmp/sql_apply.log && exit 1)"

echo "[ok] flink sql ddl applied"

echo "[check] insert row into gdelt_events"
pg_exec "insert into public.gdelt_events (globaleventid,event_date,event_time,actor1_country_code,actor2_country_code,event_code,goldstein_scale,num_articles,avg_tone)
values (${SMOKE_EVENT_ID}, current_date, now(), 'USA','CHN','010', 1.0, 1, 0.5)
on conflict (globaleventid) do update set avg_tone=excluded.avg_tone, last_updated=now();"

echo "[check] update row"
pg_exec "update public.gdelt_events set avg_tone = avg_tone + 1.0, last_updated = now() where globaleventid=${SMOKE_EVENT_ID};"

echo "[check] delete row"
pg_exec "delete from public.gdelt_events where globaleventid=${SMOKE_EVENT_ID};"

echo "[check] replication slot status (created when flink connects)"
slot_row="$(pg_exec "select slot_name, active from pg_replication_slots where slot_name='${SLOT_NAME}';")"
if [[ -z "$slot_row" ]]; then
  echo "[warn] slot '${SLOT_NAME}' not visible yet."
  echo "this can happen if the cdc job has not started reading. check flink ui and logs."
else
  echo "[ok] slot: $slot_row"
fi

echo
echo "[done] infra looks good."