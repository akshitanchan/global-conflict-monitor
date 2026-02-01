#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

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

# logging
info() { echo "[info] $*"; }
ok()   { echo "[ok]   $*"; }
warn() { echo "[warn] $*" >&2; }
die()  { echo "[error] $*" >&2; exit 1; }

# sql command
pg_exec() {
  docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"
}

# healthchecks
wait_for_container_healthy() {
  local name="$1"
  local tries="${2:-60}"
  local i=0
  info "waiting for $name healthcheck..."
  while true; do
    local status
    status="$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || true)"
    if [[ "$status" == "healthy" ]]; then
      ok "$name is healthy"
      return 0
    fi
    i=$((i+1))
    if (( i >= tries )); then
      warn "$name not healthy after $tries checks"
      docker logs "$name" --tail 200 || true
      exit 1
    fi
    sleep 2
  done
}

ensure_file_exists() {
  local path="$1"
  [[ -f "$path" ]] || die "missing file: $path"
}

# bring stack up
info "bringing up services"
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d

wait_for_container_healthy "$POSTGRES_CONTAINER" 90

# postgres logical wal
info "checking wal_level"
wal_level="$(pg_exec "show wal_level;")"
[[ "$wal_level" == "logical" ]] || die "wal_level is '$wal_level' (expected: logical)"
ok "wal_level=logical"

# emit old row values
info "checking replica identity"
replica_identity="$(pg_exec "select relreplident from pg_class where relname='gdelt_events';")"
if [[ "$replica_identity" != "f" ]]; then
  info "setting replica identity full on public.gdelt_events"
  pg_exec "alter table public.gdelt_events replica identity full;"
fi
ok "replica identity full"

# publication for table
info "ensuring publication exists"
pub_exists="$(pg_exec "select 1 from pg_publication where pubname='${PUBLICATION_NAME}' limit 1;")"
if [[ -z "$pub_exists" ]]; then
  pg_exec "create publication ${PUBLICATION_NAME} for table public.gdelt_events;"
  ok "publication created: $PUBLICATION_NAME"
else
  ok "publication exists: $PUBLICATION_NAME"
fi

# ddl file on host
ensure_file_exists "$CDC_SQL_HOST_PATH"
ok "cdc ddl file present: $CDC_SQL_HOST_PATH"

# mount into flink jm
info "verifying sql file mount in flink jobmanager"
if ! docker exec -i "$FLINK_JM_CONTAINER" bash -lc "test -f '$CDC_SQL_CONTAINER_PATH'"; then
  die "flink jobmanager cannot see $CDC_SQL_CONTAINER_PATH (check compose mounts $ROOT_DIR/flink/sql -> /opt/flink/sql)"
fi
ok "sql file mounted: $CDC_SQL_CONTAINER_PATH"

# apply flink ddl
info "applying cdc ddl via flink sql-client"
docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
  "/opt/flink/bin/sql-client.sh -f '$CDC_SQL_CONTAINER_PATH' >/tmp/sql_apply.log 2>&1 || (tail -n 200 /tmp/sql_apply.log && exit 1)"
ok "flink ddl applied"

# write smoke
info "smoke write: insert/update/delete on public.gdelt_events"
pg_exec "insert into public.gdelt_events (globaleventid, event_date, source_actor, target_actor, cameo_code, num_events, num_articles, quad_class, goldstein) values (${SMOKE_EVENT_ID}, to_char(current_date, 'YYYYMMDD')::int, 'USA', 'CHN', '043', 1, 4, 1, 2.8) on conflict (globaleventid) do update set goldstein = excluded.goldstein, num_events = excluded.num_events, num_articles = excluded.num_articles;"
pg_exec "update public.gdelt_events set goldstein = coalesce(goldstein, 0) + 1.0 where globaleventid = ${SMOKE_EVENT_ID};"
pg_exec "delete from public.gdelt_events where globaleventid=${SMOKE_EVENT_ID};"
ok "smoke write ok"

# slot shows up later
info "checking replication slot visibility (created when flink connects)"
slot_row="$(pg_exec "select slot_name, active from pg_replication_slots where slot_name='${SLOT_NAME}';")"
if [[ -z "$slot_row" ]]; then
  warn "slot '${SLOT_NAME}' not visible yet (cdc job may not be reading yet)"
else
  ok "slot: $slot_row"
fi

echo
ok "infra looks good"