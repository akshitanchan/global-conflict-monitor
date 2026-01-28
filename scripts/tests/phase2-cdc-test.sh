#!/usr/bin/env bash
set -euo pipefail

# root + compose (match phase1 style)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

# config (override via env)
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"
FLINK_TM_CONTAINER="${FLINK_TM_CONTAINER:-flink-taskmanager}"

POSTGRES_DB="${POSTGRES_DB:-gdelt}"
POSTGRES_USER="${POSTGRES_USER:-flink_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-flink_pass}"

FLINK_BASE="${FLINK_BASE:-http://127.0.0.1:8081}"
SLOT_NAME="${SLOT_NAME:-gdelt_flink_slot}"

# helpers (match phase1 vibe)
ok()   { echo "[ok] $*"; }
warn() { echo "[warn] $*" >&2; }
die()  { echo "[error] $*" >&2; exit 1; }

pg_exec() {
  docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"
}

wait_for_container_healthy() {
  local name="$1"
  local tries="${2:-90}"
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

wait_for_json() {
  local url="$1" tries="${2:-40}" sleep_s="${3:-2}" body=""
  for _ in $(seq 1 "$tries"); do
    body="$(curl -4 -fsS --max-time 2 "$url" 2>/dev/null || true)"
    if [[ -n "$body" ]] && python3 -c 'import json,sys; json.loads(sys.stdin.read())' <<<"$body" >/dev/null 2>&1; then
      printf '%s' "$body"
      return 0
    fi
    sleep "$sleep_s"
  done
  return 1
}

# keep tests repeatable
INSERTED_IDS=""
cleanup() {
  if [[ -n "${INSERTED_IDS}" ]]; then
    echo "[cleanup] deleting inserted ids: ${INSERTED_IDS}"
    pg_exec "delete from public.gdelt_events where globaleventid = any(string_to_array('${INSERTED_IDS}',',')::bigint[]);" >/dev/null || true
  fi
}
trap cleanup EXIT

echo "[up] docker compose up -d"
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d

# 0) containers up + postgres healthy
docker ps --format '{{.Names}}' | grep -qx "$POSTGRES_CONTAINER" || die "$POSTGRES_CONTAINER container not running"
docker ps --format '{{.Names}}' | grep -qx "$FLINK_JM_CONTAINER" || die "$FLINK_JM_CONTAINER container not running"
docker ps --format '{{.Names}}' | grep -qx "$FLINK_TM_CONTAINER" || die "$FLINK_TM_CONTAINER container not running"
ok "containers running"

wait_for_container_healthy "$POSTGRES_CONTAINER" 90

# 1) postgres sink tables exist
echo "[check] postgres sink tables"
missing=()
for t in daily_event_volume_by_quadclass dyad_interactions top_actors daily_cameo_metrics; do
  exists="$(pg_exec "select to_regclass('public.${t}') is not null;")"
  [[ "$exists" == "t" ]] || missing+=("$t")
done
((${#missing[@]}==0)) || die "missing postgres sink tables: ${missing[*]}"
ok "postgres sink tables present"

# 2) flink rest api + job running
echo "[check] flink rest api"
_="$(wait_for_json "$FLINK_BASE/overview" 40 2)" || die "flink api not responding at $FLINK_BASE"
ok "flink api responding"

echo "[check] flink job running"
jobs_overview="$(wait_for_json "$FLINK_BASE/jobs/overview" 40 2)" || die "flink /jobs/overview not returning json"

job_state="$(python3 -c '
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
print(jobs[0].get("state","NO_STATE") if jobs else "NO_JOBS")
' <<<"$jobs_overview")"

job_jid="$(python3 -c '
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
print(jobs[0].get("jid","") if jobs else "")
' <<<"$jobs_overview")"

if [[ "$job_state" == "NO_JOBS" ]]; then
  die "no flink jobs running. start the pipeline first:
./scripts/setup/start-flink-aggregations.sh
or, manually:
docker exec -it ${FLINK_JM_CONTAINER} ./bin/sql-client.sh -f /opt/flink/sql/02-pipeline.sql"
fi
[[ "$job_state" == "RUNNING" ]] || die "flink job not RUNNING (state=$job_state)"
ok "flink job is RUNNING (jid=$job_jid)"

# 3) replication streaming
echo "[check] postgres logical replication status"
rep="$(pg_exec "select count(*) from pg_stat_replication where state='streaming';")"
[[ "$rep" != "0" ]] || die "no streaming replication sessions (pg_stat_replication)"
ok "postgres logical replication streaming"

slot_active="$(pg_exec "select active from pg_replication_slots where slot_name='${SLOT_NAME}';" || true)"
if [[ "$slot_active" != "t" ]]; then
  warn "replication slot '${SLOT_NAME}' not active yet (active=$slot_active). cdc may still be starting."
else
  ok "replication slot active (${SLOT_NAME})"
fi

lsn_before="$(pg_exec "select confirmed_flush_lsn::text from pg_replication_slots where slot_name='${SLOT_NAME}';" || true)"
[[ -n "$lsn_before" ]] && ok "confirmed_flush_lsn before: $lsn_before" || warn "could not read confirmed_flush_lsn (slot missing?)"

# 4) cdc end-to-end effect test (insert -> sink updates)
echo "[check] cdc end-to-end (insert -> sink updates)"

key="$(pg_exec "select event_date||','||quad_class from public.gdelt_events where event_date is not null and quad_class is not null limit 1;")"
[[ -n "$key" ]] || die "gdelt_events seems empty"
event_date="${key%,*}"
quad_class="${key#*,}"
ok "using key event_date=$event_date quad_class=$quad_class"

baseline="$(pg_exec "select coalesce(total_events,0)||','||coalesce(total_articles,0)
from public.daily_event_volume_by_quadclass
where event_date=${event_date} and quad_class=${quad_class};" || true)"
if [[ -z "$baseline" ]]; then
  base_events=0
  base_articles=0
else
  base_events="${baseline%,*}"
  base_articles="${baseline#*,}"
fi
ok "baseline sink totals: events=$base_events articles=$base_articles"

INSERTED_IDS="$(pg_exec "
with template as (
  select
    event_date,source_actor,target_actor,cameo_code,quad_class,goldstein,
    source_geo_type,source_geo_lat,source_geo_long,
    target_geo_type,target_geo_lat,target_geo_long,
    action_geo_type,action_geo_lat,action_geo_long
  from public.gdelt_events
  where event_date=${event_date} and quad_class=${quad_class}
  limit 1
),
base as (
  select coalesce(max(globaleventid),0) as max_id from public.gdelt_events
),
ins as (
  insert into public.gdelt_events (
    globaleventid,event_date,source_actor,target_actor,cameo_code,
    num_events,num_articles,quad_class,goldstein,
    source_geo_type,source_geo_lat,source_geo_long,
    target_geo_type,target_geo_lat,target_geo_long,
    action_geo_type,action_geo_lat,action_geo_long
  )
  select
    base.max_id + gs.g,
    template.event_date,
    template.source_actor,
    template.target_actor,
    template.cameo_code,
    1,1,
    template.quad_class,
    template.goldstein,
    template.source_geo_type,
    template.source_geo_lat,
    template.source_geo_long,
    template.target_geo_type,
    template.target_geo_lat,
    template.target_geo_long,
    template.action_geo_type,
    template.action_geo_lat,
    template.action_geo_long
  from base
  cross join template
  cross join generate_series(1,3) as gs(g)
  returning globaleventid
)
select array_to_string(array_agg(globaleventid), ',') from ins;
")"
[[ -n "$INSERTED_IDS" ]] || die "failed to insert test rows"
ok "inserted test rows ids=$INSERTED_IDS"

target_events=$((base_events + 3))
target_articles=$((base_articles + 3))

deadline=$((SECONDS + 60))
while true; do
  cur="$(pg_exec "select total_events||','||total_articles
  from public.daily_event_volume_by_quadclass
  where event_date=${event_date} and quad_class=${quad_class};" || true)"

  if [[ -n "$cur" ]]; then
    cur_events="${cur%,*}"
    cur_articles="${cur#*,}"
    if [[ "$cur_events" == "$target_events" && "$cur_articles" == "$target_articles" ]]; then
      ok "cdc verified via sink: events $base_events->$cur_events, articles $base_articles->$cur_articles"
      break
    fi
  fi

  if (( SECONDS > deadline )); then
    die "cdc verification timed out: expected events=$target_events articles=$target_articles, got '${cur:-<no row>}'"
  fi
  sleep 2
done

# optional: lsn should usually advance after inserts
lsn_after="$(pg_exec "select confirmed_flush_lsn::text from pg_replication_slots where slot_name='${SLOT_NAME}';" || true)"
if [[ -n "$lsn_before" && -n "$lsn_after" && "$lsn_after" != "$lsn_before" ]]; then
  ok "confirmed_flush_lsn advanced: $lsn_before -> $lsn_after"
else
  warn "confirmed_flush_lsn did not change (or unavailable). sink update is still a valid end-to-end cdc proof."
fi

echo
echo "[done] phase 2 looks healthy."